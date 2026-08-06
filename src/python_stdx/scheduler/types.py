"""Task 框架核心类型。"""

from dataclasses import dataclass
from enum import Enum
from typing import Callable

# 退避策略函数：输入尝试次数（从 1 开始），返回等待秒数
BackoffFunc = Callable[[int], float]

SECONDS_PER_DAY = 24 * 60 * 60
DEFAULT_SUCCESS_RETENTION_SECONDS = 3 * SECONDS_PER_DAY
DEFAULT_FAILURE_RETENTION_SECONDS = 10 * SECONDS_PER_DAY


class TaskType(str, Enum):
    """task.task_type 的可选值。"""

    # 常驻任务：由 @schedule 注册，按固定调度规则重复触发。
    SCHEDULED = "scheduled"
    # 一次性任务：由 submit_task() 动态提交，一行代表一次待执行记录。
    ONESHOT = "oneshot"
    # 触发驱动任务（reconcile 模式）：identity 由 (task_name, biz_name) 决定，
    # 单行长生命周期；submit 永远 UPSERT 不撞唯一键，跑期间再来的 submit 通过
    # request_at 信号合并成"完成后再跑一轮"，保证最终一致。
    TRIGGERED = "triggered"


class TaskStatus(str, Enum):
    """task.status 的可选值。

    四个值跨类型共用语义，但每类用到的子集不一样：

    - scheduled：``running`` / ``success`` / ``failed``。没有 pending——
      首次 acquire_lock 在同一事务里 INSERT 行 + 持锁，直接进 running。
    - oneshot：四个全用。
    - triggered：四个全用；resubmit 不会把 status 改回 pending（避开跨事务
      race），重新 dispatch 由 ``request_at > run_at`` 触发。

    status 不是绝对真相：dispatch / cleanup SQL 会读它简化谓词，但 stale-
    running（pod 崩残留）仍要靠 ``expire_at <= now`` 判断、triggered 是否
    需要再跑要靠 ``request_at`` 比较——单看 status 不够。

    由 store 层每个 INSERT/UPDATE 显式维护，对应写入路径见
    docs/scheduler.md 的状态机说明。
    """

    # 行已创建但还没真正跑过；只在 oneshot / triggered 首次 INSERT 时出现。
    PENDING = "pending"
    # 当前持锁执行中。pod 崩溃时会有 status=running 且 expire_at <= now 的
    # 残留行，由下一轮 dispatch 接手并重新置位。
    RUNNING = "running"
    # 最近一次执行成功；scheduled / triggered 等下次触发，oneshot 通常即终态。
    SUCCESS = "success"
    # 最近一次执行失败；scheduled 等下个周期，triggered 等下次 submit，
    # oneshot 在 run_count == max_runs 时即终态。
    FAILED = "failed"


@dataclass(frozen=True)
class TaskRetentionPolicy:
    """动态任务完成后的记录保留策略。

    success_retention_seconds=0 表示成功后立即删除；failure_retention_seconds=0
    表示最终失败后立即删除。非 0 时由 task_cleanup 按 run_at 清理。
    """

    success_retention_seconds: int = DEFAULT_SUCCESS_RETENTION_SECONDS
    failure_retention_seconds: int = DEFAULT_FAILURE_RETENTION_SECONDS

    def __post_init__(self) -> None:
        if self.success_retention_seconds < 0:
            raise ValueError("success_retention_seconds must be >= 0")
        if self.failure_retention_seconds < 0:
            raise ValueError("failure_retention_seconds must be >= 0")


# 预置退避策略
def constant_backoff(attempt: int) -> float:
    """常量退避：每次等待相同时间。"""
    return 5.0


def linear_backoff(attempt: int) -> float:
    """线性退避：等待时间 = attempt * 1.0 秒。"""
    return attempt * 1.0


def exponential_backoff(attempt: int) -> float:
    """指数退避：等待时间 = 2 ** attempt 秒。"""
    return float(2**attempt)


@dataclass
class FailureRecord:
    """最近一次失败记录。"""

    task_name: str
    error: str
    attempt: int
    failed_at: float  # timestamp


@dataclass
class RunRecord:
    """单次执行记录。"""

    id: int
    name: str
    status: str  # success / failed
    started_at: float  # timestamp
    duration_ms: int
    error: str | None
    attempt: int
