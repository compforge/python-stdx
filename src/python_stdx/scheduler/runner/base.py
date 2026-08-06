"""TaskRunner ABC：每个 task_type 一个子类。"""

import asyncio
import logging
from abc import ABC, abstractmethod

from python_stdx.scheduler.decorator import TaskDef
from python_stdx.scheduler.executor import Executor
from python_stdx.scheduler.schedule import Schedule
from python_stdx.scheduler.store.base import TaskStore

logger = logging.getLogger(__name__)

# 锁 TTL = task.timeout + buffer；防止任务接近超时时锁提前过期。
LOCK_TTL_BUFFER = 60


class TaskRunner(ABC):
    """单个 task_type 的状态机驱动器。

    职责：dispatch（每轮 tick 决定哪些行该跑）、submit（把新行写入 store）、
    持久化结果（按 ExecutionResult 写 record_*_success / record_*_failure）、
    维护 status 字段。

    抽这层的目的是让"task 类型"这件事在代码组织上显式存在：

    - scheduler.py 不再写一长串 if-else 判类型；类型差异收口在每个子类内。
    - 跟现有 store 接口的"按类型分方法"分层对齐：scheduled 走 acquire_lock /
      record_success，oneshot/triggered 走 acquire_dynamic_task_lock /
      record_dynamic_*。
    - **可扩展性**：未来如果出现现有三类不能覆盖的新需求（比如 priority queue、
      DAG 调度、跨集群扇出等），可以再加一个 task_type 和对应 Runner 子类，
      不用动现有类型的代码。

    共享的执行机制（参数注入、retry、timeout、tracing）走 ``Executor``；
    本基类不实现 tick / submit，子类按各自类型语义重写。

    并发派发与 in-flight 追踪
    ---------------------------
    ``tick()`` 不再 ``await`` handler 完成,而是 ``asyncio.create_task`` 派发
    到后台。基类持有 ``_in_flight`` 记录本 pod 上正在跑的行,用于:

    - 同一 pod 同一 tick 内防止重复 dispatch 同一行(锁层面 DB 也能挡住,
      本地短路可以省一次 DB 往返)
    - ``OneshotRunner`` 的 ``max_concurrency`` cap 计算把本地 in-flight
      数量纳入考量,避免单 tick 多次派发未真正落锁前 cluster 计数还来不及
      上升的窗口
    - ``shutdown()`` 时按 grace 等 in-flight 收尾,再 cancel 兜底
    - 测试通过 ``drain()`` 等 in-flight 完成,使断言能稳定看到副作用

    in-flight key 选取:scheduled 用 ``task_name`` (一个 handler 单行),
    oneshot/triggered 用 ``task_id`` (每行独立)。
    """

    def __init__(
        self,
        store: TaskStore,
        executor: Executor,
        task_defs: dict[str, TaskDef],
    ):
        self._store = store
        self._executor = executor
        self._task_defs = task_defs
        # key -> (asyncio.Task, handler_name)
        # scheduled: key = handler_name(= task_name); oneshot/triggered: key = task_id
        self._in_flight: dict[str, tuple[asyncio.Task[None], str]] = {}

    @abstractmethod
    async def tick(self) -> None:
        """单轮调度：决定哪些行该跑、acquire 锁、调 executor、回写状态。

        实现需保证 dispatch 不阻塞:命中需要执行的行后用 ``asyncio.create_task``
        派发,登记到 ``_in_flight``,本方法立即返回。
        """
        ...

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
        """部分类型不支持 submit（如 scheduled）。默认抛错；子类按需重写。"""
        raise NotImplementedError(f"{type(self).__name__} doesn't support submit")

    async def cleanup(self) -> int:
        """周期性清理本类型已完成 / 过期的行，返回删除数。

        基类默认 no-op：scheduled 行长期保留、triggered 行由业务侧联动清理，
        都不走 retention。OneshotRunner 重写本方法走 ``TaskRetentionPolicy``。

        由 ``TaskScheduler.cleanup_expired_tasks`` 周期性调用——后续若加新
        task_type 的 Runner，自带 cleanup 即可参与，不需要改 scheduler。
        """
        return 0

    # ── in-flight 管理 ──────────────────────────────────────────────────────

    def in_flight_count(self) -> int:
        """当前 pod 上本 runner 的 in-flight 行数。供调度器统计/排障。"""
        return len(self._in_flight)

    def in_flight_count_by_handler(self, handler_name: str) -> int:
        """指定 handler 在本 pod 上的 in-flight 行数(supports oneshot cap)。"""
        return sum(1 for _, name in self._in_flight.values() if name == handler_name)

    async def drain(self) -> None:
        """等所有 in-flight 后台 task 跑完。

        测试与 shutdown 共用入口。空 in-flight 即时返回。
        ``asyncio.gather(..., return_exceptions=True)`` 兜底:in-flight task
        的 finalize 已经把异常吞掉并 logger.exception,这里再加一层避免
        gather 把残余 cancel 异常抛回调用方。
        """
        if not self._in_flight:
            return
        tasks = [t for t, _ in self._in_flight.values()]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def shutdown(self, grace_seconds: float = 5.0) -> None:
        """优雅停止:先等 in-flight 在 grace 内完成,超时则 cancel。

        调用方一般是 ``TaskScheduler.stop()``。
        """
        if not self._in_flight:
            return
        tasks = [t for t, _ in self._in_flight.values()]
        try:
            await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True),
                timeout=grace_seconds,
            )
        except asyncio.TimeoutError:
            logger.warning(
                "%s shutdown grace (%ss) exceeded, cancelling %s in-flight task(s)",
                type(self).__name__,
                grace_seconds,
                len(tasks),
            )
            for t in tasks:
                if not t.done():
                    t.cancel()
            # 再等一轮让 cancel 生效,但不再卡时间
            await asyncio.gather(*tasks, return_exceptions=True)
