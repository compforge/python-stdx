"""ScheduledRunner：常驻调度任务（@schedule + @task）。"""

import asyncio
import logging
import time
from collections.abc import Sequence

from python_stdx.scheduler.decorator import ScheduleDef, TaskDef
from python_stdx.scheduler.executor import Executor
from python_stdx.scheduler.runner.base import LOCK_TTL_BUFFER, TaskRunner
from python_stdx.scheduler.store.base import TaskStore

logger = logging.getLogger(__name__)


class ScheduledRunner(TaskRunner):
    """常驻调度任务的驱动器。

    特点:
    - identity 是 ``task_name``,全局每个 handler 一行长生命周期。
    - 不支持 submit;行只通过首次 ``acquire_lock`` 隐式 INSERT。
    - status 流转:行不存在 → ``running``(首次 acquire 同事务 INSERT)→
      ``success`` / ``failed`` →(下个周期到期)→ ``running`` → ……
      没有 ``pending`` 阶段。
    - dispatch:Python 端按 ``schedule.next_run(last_run)`` 算到期时间,
      不读 status 也不读 SQL 谓词。

    派发模型:tick 只 dispatch,handler 在后台 task 跑。in-flight key 用
    ``task_name``——一个 handler 同时只允许一个 in-flight,下一轮 tick
    若该 name 仍在 in-flight 就跳过(锁层也能挡,本地短路省一次 DB)。
    """

    def __init__(
        self,
        store: TaskStore,
        executor: Executor,
        task_defs: dict[str, TaskDef],
        schedule_defs: Sequence[ScheduleDef],
    ):
        super().__init__(store, executor, task_defs)
        self._schedule_defs = list(schedule_defs)

    @property
    def schedule_defs(self) -> list[ScheduleDef]:
        return list(self._schedule_defs)

    async def tick(self) -> None:
        now = time.time()
        for sd in self._schedule_defs:
            td = self._task_defs[sd.task_name]

            # 该 handler 上一轮派发还没收尾,跳过(锁也能挡,这里本地短路)
            if sd.task_name in self._in_flight:
                continue

            last_run = await self._store.get_last_run(sd.task_name)
            next_run = await sd.schedule.next_run(last_run)
            if now < next_run:
                continue

            lock_ttl = td.timeout + LOCK_TTL_BUFFER
            acquired = await self._store.acquire_lock(sd.task_name, lock_ttl)
            if not acquired:
                continue

            logger.debug(
                "Scheduled task dispatched: handler=%s timeout=%ss",
                sd.task_name,
                td.timeout,
            )
            bg_task = asyncio.create_task(
                self._run_one_and_finalize(td),
                name=f"scheduled:{sd.task_name}",
            )
            self._in_flight[sd.task_name] = (bg_task, sd.task_name)
            # 默认参数捕获 task_name,避免闭包延迟绑定到循环最后一个 name
            bg_task.add_done_callback(lambda _t, name=sd.task_name: self._in_flight.pop(name, None))  # type: ignore[misc]

    async def _run_one_and_finalize(self, td: TaskDef) -> None:
        """后台 task 的完整闭环:execute(内部已写 record_*) → 释放锁。

        异常一律吞掉避免 create_task unhandled。
        """
        try:
            await self._run_one(td)
        except Exception as e:
            logger.exception("Scheduled task %s failed: %s", td.name, e)
        finally:
            try:
                await self._store.release_lock(td.name)
            except Exception as e:
                logger.exception("Scheduled task release lock failed: name=%s err=%s", td.name, e)
            logger.debug("Scheduled task finished: handler=%s", td.name)

    async def _run_one(self, td: TaskDef) -> None:
        result = await self._executor.execute(td)
        if result.success:
            await self._store.record_success(
                td.name,
                result.started_at,
                result.duration_ms,
                result.attempts,
                trace_id=result.trace_id,
            )
        else:
            await self._store.record_failure(
                td.name,
                result.error or "unknown error",
                result.attempts,
                result.started_at,
                trace_id=result.trace_id,
            )
            logger.warning(f"Scheduled task {td.name} failed after {result.attempts} attempts: {result.error}")
