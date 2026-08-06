"""OneshotRunner：一次性动态任务（@task + submit_task）。"""

import asyncio
import logging
from collections.abc import Sequence

from python_stdx.scheduler.decorator import OneshotDef, TaskDef
from python_stdx.scheduler.executor import Executor
from python_stdx.scheduler.runner.base import LOCK_TTL_BUFFER, TaskRunner
from python_stdx.scheduler.schedule import Schedule
from python_stdx.scheduler.store.base import TaskStore
from python_stdx.scheduler.types import TaskRetentionPolicy

logger = logging.getLogger(__name__)


class OneshotRunner(TaskRunner):
    """一次性动态任务的驱动器。

    特点:
    - identity 是 ``task_id``(每次 submit 一行),可选 ``biz_name`` 用于提交去重。
    - status 流转:``pending`` →(acquire)→ ``running`` → ``success`` / ``failed``
      (通常即终态;``run_count == max_runs`` 后不再被 list_pending 拾起)。
    - 支持 ``@oneshot(max_concurrency=N)`` 声明 cluster 级软上限。
    - 完成后按 ``TaskRetentionPolicy`` 决定立即删除或延后由 cleanup 任务处理。

    派发模型:
    - ``tick()`` 只做"列出 pending + 逐行 acquire + create_task",不等 handler
      完成。每行 handler 在独立 ``asyncio.Task`` 里跑,完成后由 finalize 闭包
      释放锁、写终态、按 retention 删除。
    - ``max_concurrency`` cap 同时看本地 in-flight 与 cluster SQL 计数,取
      max:cluster SQL 反映跨 pod 状态但对"本 tick 刚 dispatch 还没落锁"
      不可见,本地 in-flight 补这个窗口。
    """

    def __init__(
        self,
        store: TaskStore,
        executor: Executor,
        task_defs: dict[str, TaskDef],
        oneshot_defs: Sequence[OneshotDef],
        retention_policy: TaskRetentionPolicy,
    ):
        super().__init__(store, executor, task_defs)
        # name -> max_concurrency；未在表中的 handler 视为不限并发
        self._max_concurrency = {od.task_name: od.max_concurrency for od in oneshot_defs}
        self._retention_policy = retention_policy

    async def tick(self) -> None:
        """list_pending_oneshot → max_concurrency 守卫 → acquire → create_task 派发。

        max_concurrency 是 cluster 级软上限:count 查询 + 本地 in-flight 与
        acquire 之间存在窗口,计数可能短暂超出 N 共 1~K(K=pod 数)。当前用例
        (保护下游 qps)够用;硬上限需要 sentinel 行 + SELECT FOR UPDATE,
        详见 ``@oneshot`` docstring。
        """
        pending = await self._store.list_pending_oneshot_tasks()
        if not pending:
            return
        logger.info("OneshotRunner tick: %s pending tasks", len(pending))

        for task_id, handler_name, timeout, params in pending:
            td = self._task_defs.get(handler_name)
            if td is None:
                logger.warning("Oneshot task handler %s (id=%s) is not registered, skipping", handler_name, task_id)
                continue

            # 同 tick 内已经 dispatch 过的行不再重复 acquire(锁层也能挡,
            # 本地短路省一次 DB 往返)
            if task_id in self._in_flight:
                continue

            cap = self._max_concurrency.get(handler_name)
            if cap is not None:
                # 本地 + cluster 双视角:cluster SQL 给跨 pod,本地 in-flight
                # 给"本 tick 刚 dispatch 还没真正落锁"的窗口
                local_count = self.in_flight_count_by_handler(handler_name)
                if local_count >= cap:
                    logger.debug(
                        "Oneshot task %s (id=%s) skipped: local in-flight cap reached (%s)",
                        handler_name,
                        task_id,
                        local_count,
                    )
                    continue
                running_counts = await self._store.count_running_oneshot_tasks()
                if running_counts.get(handler_name, 0) >= cap:
                    logger.debug(
                        "Oneshot task %s (id=%s) skipped: cluster cap reached",
                        handler_name,
                        task_id,
                    )
                    continue

            lock_ttl = timeout + LOCK_TTL_BUFFER
            acquired = await self._store.acquire_dynamic_task_lock(task_id, lock_ttl)
            if not acquired:
                continue

            temp_td = TaskDef(
                name=handler_name,
                timeout=timeout,
                max_runs=td.max_runs,
                backoff=td.backoff,
                func=td.func,
            )
            logger.info(
                "Oneshot task dispatched: handler=%s task_id=%s timeout=%ss",
                handler_name,
                task_id,
                timeout,
            )
            bg_task = asyncio.create_task(
                self._run_one_and_finalize(temp_td, task_id, handler_name, params),
                name=f"oneshot:{handler_name}:{task_id}",
            )
            self._in_flight[task_id] = (bg_task, handler_name)
            # 默认参数捕获 task_id,避免闭包延迟绑定到循环最后一个 id
            bg_task.add_done_callback(lambda _t, tid=task_id: self._in_flight.pop(tid, None))  # type: ignore[misc]

    async def _run_one_and_finalize(
        self,
        td: TaskDef,
        task_id: str,
        handler_name: str,
        params: dict[str, object] | None,
    ) -> None:
        """后台 task 的完整闭环:execute → 释放锁 → 按 retention 删除。

        必须吞掉所有异常,绝不重抛:create_task 的未捕获异常会触发
        asyncio 的 unhandled exception warning,而且后续 done_callback 拿
        不到准确终态。所有失败路径都走 logger.exception。
        """
        succeeded = False
        try:
            succeeded = await self._run_one(td, task_id, params)
        except Exception as e:
            logger.exception("Oneshot task handler %s (id=%s) failed: %s", handler_name, task_id, e)
        finally:
            try:
                await self._store.release_dynamic_task_lock(task_id)
            except Exception as e:
                logger.exception(
                    "Oneshot task release lock failed: handler=%s task_id=%s err=%s",
                    handler_name,
                    task_id,
                    e,
                )
            try:
                if succeeded and self._store.should_delete_successful_oneshot_task(self._retention_policy):
                    await self._store.delete_oneshot_task(task_id)
                elif not succeeded and self._store.should_delete_failed_oneshot_task(self._retention_policy):
                    await self._store.delete_oneshot_task(task_id)
            except Exception as e:
                logger.exception(
                    "Oneshot task retention cleanup failed: handler=%s task_id=%s err=%s",
                    handler_name,
                    task_id,
                    e,
                )
            logger.info(
                "Oneshot task finished: handler=%s task_id=%s success=%s",
                handler_name,
                task_id,
                succeeded,
            )

    async def _run_one(
        self,
        td: TaskDef,
        task_id: str,
        params: dict[str, object] | None,
    ) -> bool:
        result = await self._executor.execute(td, params=params)
        if result.success:
            await self._store.record_dynamic_success(
                task_id,
                result.started_at,
                result.duration_ms,
                result.attempts,
                trace_id=result.trace_id,
            )
            return True
        await self._store.record_dynamic_failure(
            task_id,
            result.error or "unknown error",
            result.attempts,
            result.started_at,
            trace_id=result.trace_id,
        )
        return False

    async def submit(
        self,
        *,
        name: str,
        td: TaskDef,
        schedule: Schedule | None = None,
        biz_name: str | None = None,
        timeout: int | None = None,
        max_runs: int | None = None,
        params: dict[str, object] | None = None,
    ) -> str:
        """提交 oneshot 任务。

        - 非空 biz_name 时先按 (task_name, biz_name) 查询，命中即返回已有 task_id（幂等）。
        - 否则走 ``store.submit_oneshot_task`` INSERT 新行。
        """
        if biz_name is not None:
            existing = await self._store.get_oneshot_task_by_biz_name(name, biz_name)
            if existing is not None:
                logger.debug(
                    "Oneshot task submission deduplicated: name=%s biz_name=%s existing_task_id=%s",
                    name,
                    biz_name,
                    existing[0],
                )
                return existing[0]

        return await self._store.submit_oneshot_task(
            name=name,
            biz_name=biz_name,
            schedule=schedule,
            timeout=timeout or td.timeout,
            max_runs=max_runs,
            params=params,
        )

    async def cleanup(self) -> int:
        """按 retention_policy 清理已完成行；由 system.task_cleanup 周期调用。"""
        return await self._store.cleanup_expired_oneshot_tasks(self._retention_policy)
