"""Task 定义与注册装饰器。

设计文档：docs/scheduler.md
"""

from dataclasses import dataclass
from typing import Awaitable, Callable, TypeVar

from python_stdx.scheduler.schedule import Schedule
from python_stdx.scheduler.types import BackoffFunc, constant_backoff

F = TypeVar("F", bound=Callable[..., Awaitable[object]])


@dataclass
class TaskDef:
    """Task 定义：只包含函数本体和默认参数。"""

    name: str
    timeout: int
    max_runs: int
    backoff: BackoffFunc
    func: Callable[..., Awaitable[object]]


@dataclass
class ScheduleDef:
    """Schedule 定义：调度规则。"""

    task_name: str
    schedule: Schedule


@dataclass
class OneshotDef:
    """Oneshot 装饰器定义：仅承载 oneshot 子流程的额外配置。

    `@task` 不带 `@oneshot` / `@trigger` / `@schedule` 时仍按 oneshot 处理（不限并发），
    与历史行为保持一致；只有需要 cluster 级并发上限时才显式加 `@oneshot(max_concurrency=N)`。
    """

    task_name: str
    max_concurrency: int


@dataclass
class TriggerDef:
    """Trigger 定义：标记 task 为触发驱动（reconcile）类型。

    triggered task 的 identity 是 (task_name, biz_name)，单行长生命周期；
    submit 永远 UPSERT 不撞唯一键，跑期间再来的 submit 通过 request_at 信号
    合并成"完成后再跑一轮"。

    cooldown_seconds 表达"两次成功执行 start 时间之间的最小间隔"。submit 时
    若距上次 run_at 不到 cooldown，request_at 被推到 run_at + cooldown 再 fire；
    跑期间多次 submit 的 request_at 计算是幂等的（都基于同一个 run_at），
    最终自然合并成一次推迟的执行。0 表示无冷却，行为同未声明。
    """

    task_name: str
    cooldown_seconds: int = 0


# module-level registry
_TASK_DEFS: dict[str, TaskDef] = {}
_SCHEDULE_DEFS: list[ScheduleDef] = []
_TRIGGER_DEFS: list[TriggerDef] = []
_ONESHOT_DEFS: list[OneshotDef] = []


def task(
    name: str,
    *,
    timeout: int = 30,
    max_runs: int = 1,
    backoff: BackoffFunc = constant_backoff,
) -> Callable[[F], F]:
    """Task 函数装饰器：只负责注册函数。

    Args:
        name: 任务唯一名称。
        timeout: 单次执行超时秒数。
        max_runs: 最大执行次数（包括重试），默认 1 次。
        backoff: 退避策略函数。

    Example:
        @task(name="cleanup", timeout=60, max_runs=3)
        @inject
        async def cleanup_task(db=Provide[Container.db]):
            ...
    """

    def decorator(func: F) -> F:
        if name in _TASK_DEFS:
            raise ValueError(f"Task {name} already registered")

        _TASK_DEFS[name] = TaskDef(
            name=name,
            timeout=timeout,
            max_runs=max_runs,
            backoff=backoff,
            func=func,
        )
        return func

    return decorator


def schedule(
    schedule: Schedule,
) -> Callable[[F], F]:
    """Schedule 装饰器：定义自动调度规则。

    必须链式装饰在 @task 之上。

    Args:
        schedule: 调度策略（IntervalSchedule / CronSchedule）。

    Example:
        @schedule(IntervalSchedule(3600))
        @task(name="report", timeout=120)
        async def report_task():
            ...
    """

    def decorator(func: F) -> F:
        # 从已注册的 task 中查找对应的 TaskDef
        task_def = None
        for td in _TASK_DEFS.values():
            if td.func is func:
                task_def = td
                break

        if task_def is None:
            raise ValueError("@schedule must be used with @task decorator. Make sure @schedule is placed above @task.")

        _SCHEDULE_DEFS.append(
            ScheduleDef(
                task_name=task_def.name,
                schedule=schedule,
            )
        )
        return func

    return decorator


def trigger(*, cooldown_seconds: int = 0) -> Callable[[F], F]:
    """Trigger 装饰器：标记 task 为触发驱动（reconcile）类型。

    必须链式装饰在 @task 之上。被标记的 task 由业务方通过
    `submit_triggered_task(name, biz_name=...)` 提交；scheduler 在
    `_tick_triggered_tasks` 中按 `run_at < request_at` 拉起。

    Args:
        cooldown_seconds: 两次执行 start 之间的最小间隔（秒）。submit 时若
            距上次 run_at 不到该值，request_at 自动推到 run_at + cooldown
            再 fire；跑期间多次 submit 的 request_at 计算幂等，最终合并为
            一次推迟的执行。0（默认）表示无冷却。

    Example:
        @trigger(cooldown_seconds=600)
        @task(name="resource.refresh.executor", timeout=180, max_runs=3)
        async def refresh_resource(resource_id: str):
            ...
    """
    if cooldown_seconds < 0:
        raise ValueError(f"cooldown_seconds must be >= 0, got {cooldown_seconds}")

    def decorator(func: F) -> F:
        task_def = None
        for td in _TASK_DEFS.values():
            if td.func is func:
                task_def = td
                break

        if task_def is None:
            raise ValueError("@trigger must be used with @task decorator. Make sure @trigger is placed above @task.")

        # 一个 task 不应同时是 scheduled / triggered / oneshot
        if any(sd.task_name == task_def.name for sd in _SCHEDULE_DEFS):
            raise ValueError(f"Task {task_def.name} cannot be both @schedule and @trigger")
        if any(td.task_name == task_def.name for td in _TRIGGER_DEFS):
            raise ValueError(f"Task {task_def.name} already marked as @trigger")
        if any(od.task_name == task_def.name for od in _ONESHOT_DEFS):
            raise ValueError(f"Task {task_def.name} cannot be both @oneshot and @trigger")

        _TRIGGER_DEFS.append(TriggerDef(task_name=task_def.name, cooldown_seconds=cooldown_seconds))
        return func

    return decorator


def oneshot(*, max_concurrency: int) -> Callable[[F], F]:
    """Oneshot 装饰器：声明 oneshot task 的 cluster 级最大并发数（软上限）。

    必须链式装饰在 @task 之上。"running" 的判定见 store 层
    `count_running_oneshot_tasks`：以"持有未过期锁的行"为准，
    跑完 / 失败终态 / 重试间隔 / 崩溃残留过期锁均不计入。

    软上限说明
    ---------
    多 pod 之间是软上限：每个 pod 在自己的 tick 起点拍一次 cluster 级 snapshot，
    凭这个 snapshot 决定本轮还能 dispatch 多少个；两个 pod 同 tick 同时拿到
    相同 snapshot 时各自都可能再 dispatch 一个，理论上 cluster 计数可能短暂
    超出 max_concurrency 1~K（K=pod 数）。

    为什么不直接做硬上限：
    1. 直觉做法 `SELECT COUNT(*) … FOR UPDATE` 在没有 running 行时匹配 0 行，
       FOR UPDATE 没有可锁对象，并发 acquire 之间不会被串行化（MySQL gap lock
       行为依赖隔离级别和索引覆盖，不是稳定可依赖的跨 DB 语义）。
    2. 真正的硬上限需要"per-handler sentinel 行 + SELECT FOR UPDATE"模式，
       引入额外表 + 每次 acquire 多一次锁竞争 + 装饰器注册期 init hook，
       代价不算零。
    3. 当前需求来源是保护下游 API qps，对短暂越界 K-1 个不敏感；真严苛
       的场景再走 sentinel 模式 opt-in（暂未实现）。

    Args:
        max_concurrency: cluster 级同时运行该 handler 的最大行数，必须 >= 1。

    Example:
        @oneshot(max_concurrency=2)
        @task(name="report.export.executor", timeout=300, max_runs=3)
        async def export_executor(resource_id: str):
            ...
    """
    if max_concurrency < 1:
        raise ValueError(f"max_concurrency must be >= 1, got {max_concurrency}")

    def decorator(func: F) -> F:
        task_def = None
        for td in _TASK_DEFS.values():
            if td.func is func:
                task_def = td
                break

        if task_def is None:
            raise ValueError("@oneshot must be used with @task decorator. Make sure @oneshot is placed above @task.")

        # 一个 task 不应同时是 oneshot/triggered/scheduled
        if any(sd.task_name == task_def.name for sd in _SCHEDULE_DEFS):
            raise ValueError(f"Task {task_def.name} cannot be both @schedule and @oneshot")
        if any(td.task_name == task_def.name for td in _TRIGGER_DEFS):
            raise ValueError(f"Task {task_def.name} cannot be both @trigger and @oneshot")
        if any(od.task_name == task_def.name for od in _ONESHOT_DEFS):
            raise ValueError(f"Task {task_def.name} already marked as @oneshot")

        _ONESHOT_DEFS.append(OneshotDef(task_name=task_def.name, max_concurrency=max_concurrency))
        return func

    return decorator


def get_task_defs() -> list[TaskDef]:
    """获取所有已注册的 task 定义。"""
    return list(_TASK_DEFS.values())


def get_schedule_defs() -> list[ScheduleDef]:
    """获取所有已注册的 schedule 定义。"""
    return list(_SCHEDULE_DEFS)


def get_trigger_defs() -> list[TriggerDef]:
    """获取所有已注册的 trigger 定义。"""
    return list(_TRIGGER_DEFS)


def get_oneshot_defs() -> list[OneshotDef]:
    """获取所有已注册的 oneshot 定义（只包含显式声明 max_concurrency 的）。"""
    return list(_ONESHOT_DEFS)


def get_task_def(name: str) -> TaskDef | None:
    """根据名称获取 task 定义。"""
    return _TASK_DEFS.get(name)


def is_triggered_task(name: str) -> bool:
    """判断已注册的 task 是否被标记为 triggered。"""
    return any(td.task_name == name for td in _TRIGGER_DEFS)
