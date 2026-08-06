"""Task 调度器：高层入口 + tick loop + per-type Runner 派发。

设计文档：docs/scheduler.md

分层：
- ``TaskScheduler``：本文件——start/stop、tick loop、submit_task 入口派发。
- ``TaskRunner`` 三个子类（``scheduler/runner/``）：每种 task_type 自己的 dispatch /
  submit / 状态写入。
- ``Executor``（``scheduler/executor.py``）：纯执行机制——参数注入 / retry /
  timeout / tracing；不持有 store。
- ``TaskStore``（``scheduler/store/``）：持久化接口；按方法名分 scheduled vs
  动态任务，平铺一组方法。
"""

import asyncio
import logging
import time
from collections.abc import Sequence
from types import TracebackType

from python_stdx.scheduler.decorator import OneshotDef, ScheduleDef, TaskDef, TriggerDef
from python_stdx.scheduler.executor import Executor
from python_stdx.scheduler.runner import OneshotRunner, ScheduledRunner, TriggeredRunner
from python_stdx.scheduler.schedule import Schedule
from python_stdx.scheduler.store.base import TaskStore
from python_stdx.scheduler.store.namespaced import NamespacedTaskStore
from python_stdx.scheduler.types import TaskRetentionPolicy

logger = logging.getLogger(__name__)

# tick 总耗时超过 tick_interval 这个倍数则记 WARN——典型场景:某个 runner 的
# acquire/list_pending SQL 慢,或后台 task 派发阻塞了事件循环。日志直接打出
# 各 runner 耗时拆分,定位面立刻收敛到具体 runner。
TICK_SLOW_WARN_RATIO = 2.0

# scheduler.stop 时等 in-flight 后台 task 收尾的 grace,默认 5s。超时则 cancel
# 兜底,不卡进程退出。
SCHEDULER_SHUTDOWN_GRACE_SECONDS = 5.0


class TaskScheduler:
    """分布式安全的 Task 调度器。

    多 pod 场景下每个 pod 跑同一个 scheduler 实例：
    - 锁保证同一时刻只有一个 pod 执行同一行；
    - pod 崩溃时锁 TTL 自然到期，其他 pod 下一轮接手；
    - 重试 / 超时 / backoff 由 Executor 兜底。

    每 ``tick_interval`` 秒触发一次 ``_tick``，依次让三个 Runner 各跑一轮。
    Runner 的 tick 只做"派发",handler 在独立后台 task 跑;tick loop 因此
    不会被慢 handler 阻塞,只可能被 SQL 慢查询拖慢——后者会被 _tick 的耗时
    WARN 直接打出来。
    """

    def __init__(
        self,
        store: TaskStore,
        task_defs: Sequence[TaskDef],
        schedule_defs: Sequence[ScheduleDef],
        trigger_defs: Sequence[TriggerDef] = (),
        oneshot_defs: Sequence[OneshotDef] = (),
        *,
        tick_interval: float = 5.0,
        retention_policy: TaskRetentionPolicy | None = None,
        task_name_prefix: str = "",
    ):
        """
        Args:
            store: TaskStore 实例（SQLTaskStore 或 RedisTaskStore）。
            task_defs: 所有 @task 装饰的函数。
            schedule_defs: 所有 @schedule 定义的调度规则。
            trigger_defs: 所有 @trigger 标记的 task；含 cooldown_seconds 元数据。
            oneshot_defs: 所有 @oneshot 标记的 task；仅含显式声明 max_concurrency 的，
                未声明的 oneshot task 走默认不限并发路径。
            tick_interval: 轮询间隔秒数。
            retention_policy: oneshot 任务执行完成后的记录保留策略。
            task_name_prefix: 可选的 task_name 存储前缀，用于隔离共享 task 表上的
                本地/环境专属队列；handler 仍使用未加前缀的注册名执行。
        """
        normalized_prefix = task_name_prefix.strip()
        self._store = NamespacedTaskStore(store, normalized_prefix) if normalized_prefix else store
        self._task_name_prefix = normalized_prefix
        self._task_defs = {td.name: td for td in task_defs}
        self._schedule_defs = list(schedule_defs)
        self._tick_interval = tick_interval
        self._retention_policy = retention_policy or TaskRetentionPolicy()

        self._executor = Executor()
        self._scheduled = ScheduledRunner(
            store=self._store,
            executor=self._executor,
            task_defs=self._task_defs,
            schedule_defs=schedule_defs,
        )
        self._oneshot = OneshotRunner(
            store=self._store,
            executor=self._executor,
            task_defs=self._task_defs,
            oneshot_defs=oneshot_defs,
            retention_policy=self._retention_policy,
        )
        self._triggered = TriggeredRunner(
            store=self._store,
            executor=self._executor,
            task_defs=self._task_defs,
            trigger_defs=trigger_defs,
        )

        self._running_task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()

    async def __aenter__(self) -> "TaskScheduler":
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.stop()

    async def start(self) -> None:
        """启动调度器。"""
        if self._running_task is not None:
            logger.warning("TaskScheduler already started")
            return

        logger.info(
            f"Starting task scheduler with {len(self._task_defs)} tasks, "
            f"{len(self._schedule_defs)} schedules, tick_interval={self._tick_interval}s, "
            f"task_name_prefix={self._task_name_prefix or '<none>'}"
        )
        self._stop_event.clear()

        self._running_task = asyncio.create_task(self._run_loop())
        for sd in self._schedule_defs:
            td = self._task_defs[sd.task_name]
            logger.info(
                "Registered schedule: %s (schedule=%s, timeout=%ss, max_runs=%s)",
                sd.task_name,
                sd.schedule,
                td.timeout,
                td.max_runs,
            )

    async def stop(self) -> None:
        """停止调度器并优雅排空 in-flight 后台 task。"""
        if self._running_task is None:
            return

        logger.info("Stopping task scheduler")
        self._stop_event.set()
        self._running_task.cancel()
        try:
            await self._running_task
        except asyncio.CancelledError:
            pass
        self._running_task = None

        # 优雅等三个 runner 把 in-flight 后台 task 跑完(超时则 cancel)。
        # 顺序无关紧要,串行 await 避免日志交叉混乱。
        scheduled_n = self._scheduled.in_flight_count()
        oneshot_n = self._oneshot.in_flight_count()
        triggered_n = self._triggered.in_flight_count()
        for runner in (self._scheduled, self._oneshot, self._triggered):
            await runner.shutdown(SCHEDULER_SHUTDOWN_GRACE_SECONDS)
        logger.info(
            "Task scheduler stopped (in_flight drained: scheduled=%s oneshot=%s triggered=%s)",
            scheduled_n,
            oneshot_n,
            triggered_n,
        )

    async def _run_loop(self) -> None:
        """主循环：每 tick_interval 秒触发一次 _tick。"""
        while not self._stop_event.is_set():
            try:
                await self._tick()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"Tick error: {e}")

            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self._tick_interval)
            except asyncio.TimeoutError:
                pass

    async def _tick(self) -> None:
        """单次轮询：让三个 Runner 各跑一轮,带耗时拆分。

        现在 Runner.tick 只 dispatch 不等 handler,正常情况下应该亚秒级返回;
        总耗时超过 ``tick_interval * TICK_SLOW_WARN_RATIO`` 就 WARN,基本可以
        断定是 store SQL 慢或事件循环被阻塞。
        """
        total_start = time.monotonic()

        s_start = time.monotonic()
        await self._scheduled.tick()
        s_elapsed = time.monotonic() - s_start

        o_start = time.monotonic()
        await self._oneshot.tick()
        o_elapsed = time.monotonic() - o_start

        t_start = time.monotonic()
        await self._triggered.tick()
        t_elapsed = time.monotonic() - t_start

        total = time.monotonic() - total_start
        if total >= self._tick_interval * TICK_SLOW_WARN_RATIO:
            logger.warning(
                "TaskScheduler tick slow: total=%.2fs scheduled=%.2fs oneshot=%.2fs "
                "triggered=%.2fs (tick_interval=%ss) — store operation is slow or the event loop is blocked",
                total,
                s_elapsed,
                o_elapsed,
                t_elapsed,
                self._tick_interval,
            )
        else:
            logger.debug(
                "TaskScheduler tick: total=%.3fs scheduled=%.3fs oneshot=%.3fs triggered=%.3fs "
                "in_flight={scheduled=%s, oneshot=%s, triggered=%s}",
                total,
                s_elapsed,
                o_elapsed,
                t_elapsed,
                self._scheduled.in_flight_count(),
                self._oneshot.in_flight_count(),
                self._triggered.in_flight_count(),
            )

    async def submit_task(
        self,
        name: str,
        schedule: Schedule | None = None,
        *,
        biz_name: str | None = None,
        timeout: int | None = None,
        max_runs: int | None = None,
        params: dict[str, object] | None = None,
    ) -> str:
        """统一动态提交入口。

        根据 ``name`` 是否被 ``@trigger`` 标记派发到 ``TriggeredRunner.submit``
        或 ``OneshotRunner.submit``；类型特异校验（schedule 是否合法、biz_name
        是否必填等）由对应 Runner.submit 自己负责。

        Args:
            name: 已通过 @task 注册的 handler 名称。
            schedule: 仅 oneshot 接受；triggered 传入会抛 ValueError。
            biz_name: oneshot 用于去重；triggered 必填作为 identity。
            timeout: 超时秒数，None 用 @task 默认值。
            max_runs: 最大执行次数（triggered 含义为单轮 cycle 内重试次数），
                None：oneshot 默认 1，triggered 用 @task 默认值。
            params: 任务参数，按名称注入到 task 函数。

        Returns:
            任务行 id。

        Raises:
            ValueError: name 未注册，或 Runner.submit 自己抛的参数校验错误。
        """
        td = self._task_defs.get(name)
        if td is None:
            raise ValueError(f"Task {name} not registered, use @task decorator first")

        if self._triggered.is_triggered(name):
            task_id = await self._triggered.submit(
                name=name,
                td=td,
                schedule=schedule,
                biz_name=biz_name,
                timeout=timeout,
                max_runs=max_runs,
                params=params,
            )
            logger.info(
                "Submitted triggered task: name=%s biz_name=%s task_id=%s",
                name,
                biz_name,
                task_id,
            )
            return task_id

        task_id = await self._oneshot.submit(
            name=name,
            td=td,
            schedule=schedule,
            biz_name=biz_name,
            timeout=timeout,
            max_runs=max_runs if max_runs is not None else 1,
            params=params,
        )
        logger.debug(
            "Submitted oneshot task: name=%s biz_name=%s task_id=%s",
            name,
            biz_name,
            task_id,
        )
        return task_id

    async def cleanup_expired_tasks(self) -> int:
        """周期清理：让每个 Runner 自己处理本类型的过期行，返回总删除数。

        当前只有 OneshotRunner 实际删除（按 ``TaskRetentionPolicy``）；scheduled /
        triggered 默认 no-op，分别由"行长期存在"和"业务侧联动清理"承接。
        """
        deleted_total = 0
        for runner in (self._scheduled, self._oneshot, self._triggered):
            deleted_total += await runner.cleanup()
        return deleted_total

    async def list_long_pending_tasks(self, min_age_seconds: int) -> list[tuple[str, str, str, int]]:
        """委托到 store:列出卡 pending 超过 min_age_seconds 的行。

        给 ``system.task_scheduler.pending_age_check`` 用——避免业务层为了一条
        排障查询去拿 store 内部引用。基类 store 返回空 list(Redis store 不实现)。
        """
        return await self._store.list_long_pending_tasks(min_age_seconds)

    async def _drain_in_flight(self) -> None:
        """等三个 Runner 的所有 in-flight 后台 task 完成。

        给单测用:tick() 现在 fire-and-forget,如果不 drain,断言会在副作用落地
        之前先跑。生产代码不会调,正常路径走 stop()/runner.shutdown()。
        """
        for runner in (self._scheduled, self._oneshot, self._triggered):
            await runner.drain()
