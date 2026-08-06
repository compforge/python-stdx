"""TriggeredRunner：触发驱动 / reconcile 任务（@trigger + @task）。"""

import asyncio
import logging
from collections.abc import Sequence

from python_stdx.scheduler.decorator import TaskDef, TriggerDef
from python_stdx.scheduler.executor import Executor
from python_stdx.scheduler.runner.base import LOCK_TTL_BUFFER, TaskRunner
from python_stdx.scheduler.schedule import Schedule
from python_stdx.scheduler.store.base import TaskStore

logger = logging.getLogger(__name__)


class TriggeredRunner(TaskRunner):
    """触发驱动任务的驱动器（reconcile 模式）。

    特点:
    - identity 是 ``(task_name, biz_name)``,单行长生命周期。
    - submit 永远 UPSERT 同一行;多次 submit 在 cooldown 内合并为一次延后执行。
    - 跑期间又来的 submit 通过 ``request_at`` 信号合并成"完成后再跑一轮",
      实现"最后一次变动后必有一次执行"的最终一致语义。
    - status 流转:``pending`` →(acquire)→ ``running`` → ``success`` / ``failed``
      (等下次 submit)。下次 submit **不**改 ``status`` 回 pending——避免与正在
      跑的 worker 的 ``record_*`` 跨事务 race。下一轮 dispatch 由
      ``run_at < request_at`` 拾起。

    单行长生命周期,不进入终态,不走 retention 清理;行的清理由业务侧负责
    (for example, delete the row when its owning resource is removed).

    派发模型同 OneshotRunner:tick 只 dispatch,handler 在后台 task 跑;
    in-flight 用 ``task_id`` 作 key 防止单 tick 内重复派发同一行。
    """

    def __init__(
        self,
        store: TaskStore,
        executor: Executor,
        task_defs: dict[str, TaskDef],
        trigger_defs: Sequence[TriggerDef],
    ):
        super().__init__(store, executor, task_defs)
        # 用 dict 而不是 set：cooldown_seconds 等元数据要按 name 反查
        self._trigger_defs = {td.task_name: td for td in trigger_defs}

    def is_triggered(self, name: str) -> bool:
        """判断给定 handler 是否被 @trigger 标记；scheduler 派发 submit 用。"""
        return name in self._trigger_defs

    async def tick(self) -> None:
        pending = await self._store.list_pending_triggered_tasks()
        if not pending:
            return
        logger.info("TriggeredRunner tick: %s pending tasks", len(pending))

        for task_id, handler_name, timeout, params in pending:
            td = self._task_defs.get(handler_name)
            if td is None:
                logger.warning("Triggered task handler %s (id=%s) is not registered, skipping", handler_name, task_id)
                continue

            if task_id in self._in_flight:
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
                "Triggered task dispatched: handler=%s task_id=%s timeout=%ss",
                handler_name,
                task_id,
                timeout,
            )
            bg_task = asyncio.create_task(
                self._run_one_and_finalize(temp_td, task_id, handler_name, params),
                name=f"triggered:{handler_name}:{task_id}",
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
        """后台 task 的完整闭环:execute → 释放锁。

        triggered 不进入终态、不删行,所以 finalize 比 oneshot 简单。
        异常一律吞掉避免 create_task unhandled。
        """
        succeeded = False
        try:
            succeeded = await self._run_one(td, task_id, params)
        except Exception as e:
            logger.exception("Triggered task handler %s (id=%s) failed: %s", handler_name, task_id, e)
        finally:
            try:
                await self._store.release_dynamic_task_lock(task_id)
            except Exception as e:
                logger.exception(
                    "Triggered task release lock failed: handler=%s task_id=%s err=%s",
                    handler_name,
                    task_id,
                    e,
                )
            logger.info(
                "Triggered task finished: handler=%s task_id=%s success=%s",
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
        """提交 triggered 任务。

        - schedule 不接受（triggered 不是定时任务）；传入抛 ValueError。
        - biz_name 必填（identity 的一部分）。
        - 调 store.submit_triggered_task UPSERT 同一行：每次 submit 刷新 request_at，
          重置 run_count / message / error_*。**不**改 status——若行存在且当前
          status 是 success/failed，会保持原值，下一轮 dispatch 由 request_at >
          run_at 拾起后翻成 running。
        """
        if schedule is not None:
            raise ValueError(f"Task {name} is @trigger marked; schedule= is not supported")
        if biz_name is None:
            raise ValueError(f"Task {name} is @trigger marked; biz_name is required")

        trigger_def = self._trigger_defs[name]
        return await self._store.submit_triggered_task(
            name=name,
            biz_name=biz_name,
            timeout=timeout or td.timeout,
            max_runs=max_runs if max_runs is not None else td.max_runs,
            params=params,
            cooldown_seconds=trigger_def.cooldown_seconds,
        )
