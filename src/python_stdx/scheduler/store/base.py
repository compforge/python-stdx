"""Task 存储抽象层。

设计文档：docs/scheduler.md
"""

from abc import ABC, abstractmethod

from python_stdx.scheduler.schedule import Schedule
from python_stdx.scheduler.types import FailureRecord, RunRecord, TaskRetentionPolicy


class TaskStore(ABC):
    """Task 存储抽象，支持 Redis 或 MySQL 实现。

    提供分布式锁（防多 pod 并发执行同一 task）和执行历史记录。
    """

    # ── 分布式锁 ─────────────────────────────────────────────────────────────

    @abstractmethod
    async def acquire_lock(self, name: str, ttl: int) -> bool:
        """尝试获取分布式锁。

        Args:
            name: 常驻 task 名称。
            ttl: 锁超时秒数，超时后自动释放。

        Returns:
            True 表示获取成功（当前 pod 可以执行）。
            False 表示锁已被其他 pod 持有。
        """
        ...

    @abstractmethod
    async def release_lock(self, name: str) -> None:
        """释放分布式锁。"""
        ...

    async def acquire_dynamic_task_lock(self, task_id: str, ttl: int) -> bool:
        """按动态任务执行 ID 获取锁。

        默认实现用于 Redis 这类独立锁存储；MySQL store 会覆盖为行级锁，
        避免把 task_id 当作 task_name 插入额外记录。
        """
        return await self.acquire_lock(task_id, ttl)

    async def release_dynamic_task_lock(self, task_id: str) -> None:
        """释放动态任务执行 ID 对应的锁。"""
        await self.release_lock(task_id)

    # ── 状态查询 ─────────────────────────────────────────────────────────────

    async def get_lock_owner(self, name: str) -> str | None:
        """获取当前持有锁的 pod id（用于调试）。"""
        raise NotImplementedError

    @abstractmethod
    async def get_last_run(self, name: str) -> float | None:
        """获取上次成功执行时间戳。"""
        ...

    @abstractmethod
    async def get_run_count(self, name: str) -> int:
        """获取已执行次数。"""
        ...

    # ── 记录 ─────────────────────────────────────────────────────────────────

    @abstractmethod
    async def record_success(
        self,
        name: str,
        ts: float,
        duration_ms: int,
        attempt: int = 1,
        trace_id: str | None = None,
    ) -> None:
        """记录一次成功执行。"""
        ...

    @abstractmethod
    async def record_failure(
        self,
        name: str,
        error: str,
        attempt: int,
        ts: float,
        trace_id: str | None = None,
    ) -> None:
        """记录一次失败（到达最大重试次数）。"""
        ...

    async def record_dynamic_success(
        self,
        task_id: str,
        ts: float,
        duration_ms: int,
        attempt: int = 1,
        trace_id: str | None = None,
    ) -> None:
        """记录动态任务成功。"""
        await self.record_success(task_id, ts, duration_ms, attempt, trace_id=trace_id)

    async def record_dynamic_failure(
        self,
        task_id: str,
        error: str,
        attempt: int,
        ts: float,
        trace_id: str | None = None,
    ) -> None:
        """记录动态任务失败。"""
        await self.record_failure(task_id, error, attempt, ts, trace_id=trace_id)

    # ── 历史查询 ─────────────────────────────────────────────────────────────

    async def get_last_failure(self, name: str) -> FailureRecord | None:
        """获取最近一次失败记录。"""
        raise NotImplementedError

    async def list_history(self, name: str, limit: int = 100) -> list[RunRecord]:
        """查询执行历史。"""
        raise NotImplementedError

    # ── 动态任务 ─────────────────────────────────────────────────────────────

    @abstractmethod
    async def submit_oneshot_task(
        self,
        name: str,
        biz_name: str | None,
        schedule: Schedule | None,
        timeout: int,
        max_runs: int | None,
        params: dict[str, object] | None = None,
    ) -> str:
        """提交 oneshot 任务。

        语义：每次提交对应一行记录，达到 max_runs 后进入终态。biz_name
        非空时由 (task_name, biz_name) 唯一索引兜底；同 biz_name 二次
        提交是幂等的，无论已存在的那一行处于活跃还是终态都返回同一个
        task_id，不再 INSERT 也不抛异常。终态行的回收交给
        TaskRetentionPolicy。

        Args:
            name: 已注册的 task handler 名称。
            biz_name: 业务实例名，oneshot 仅用于提交去重；为空时不去重。
            schedule: 调度策略，None 表示立即执行。
            timeout: 超时秒数。
            max_runs: 最大执行次数，None 表示无限次。
            params: 任务参数，将以 JSON 格式存储并在执行时注入。

        Returns:
            不透明的任务执行 ID。
        """
        ...

    @abstractmethod
    async def get_oneshot_task_by_biz_name(
        self,
        name: str,
        biz_name: str,
    ) -> tuple[str, str, int, dict[str, object] | None] | None:
        """按 (task_name, biz_name) 查询已存在的 oneshot 任务行。

        语义跟 DB 上的 ``uk_task_name_biz_name`` 严格对齐：只要存在该
        identity 的行（无论活跃或终态）都返回，作为 oneshot submit 撞唯一
        索引时的 dedup tie-breaker；不要在这里加 ``run_count < max_runs``
        之类的状态过滤，否则会和 DB 唯一索引谓词不一致，让 INSERT 撞 uk
        后兜底找不到行而误抛 IntegrityError。

        triggered 任务的 dedup 走 UPSERT，不经过本方法。

        Returns:
            (task_id, handler_name, timeout, params) or None
        """
        ...

    @abstractmethod
    async def list_pending_oneshot_tasks(self) -> list[tuple[str, str, int, dict[str, object] | None]]:
        """列出所有待执行的 oneshot 任务（triggered 走 list_pending_triggered_tasks）。

        Returns:
            [(task_id, handler_name, timeout, params), ...]
        """
        ...

    async def count_running_oneshot_tasks(self) -> dict[str, int]:
        """统计每个 handler 当前正在运行的 oneshot 行数。

        "running" 的判定：持有未过期锁的行（`expire_at IS NOT NULL AND expire_at > NOW`），
        与 `list_pending_oneshot_tasks` 的 pending 条件严格互补。崩溃 pod 残留的过期锁、
        已结束（成功 / 失败终态）的行均不计入。

        默认返回空字典（视作不限并发）；具体 store 应覆盖此方法以提供 cluster 级计数。

        Returns:
            {handler_name: running_count}，未出现的 handler 视为 0。
        """
        return {}

    @abstractmethod
    async def delete_oneshot_task(self, task_id: str) -> None:
        """删除 oneshot 任务（retention 清理或立即移除）。

        triggered 不走该路径——design 上 triggered 行长生命周期，由业务侧
        在资源销毁时联动清理。
        """
        ...

    def should_delete_successful_oneshot_task(self, retention_policy: TaskRetentionPolicy) -> bool:
        """成功后是否立即移除 oneshot 任务队列记录。

        MySQL 的 task 行兼做历史记录，默认只在成功保留时间为 0 时删除；
        Redis pending set 只表示待执行队列，会覆盖为失败后总是移除。
        """
        return retention_policy.success_retention_seconds == 0

    def should_delete_failed_oneshot_task(self, retention_policy: TaskRetentionPolicy) -> bool:
        """失败后是否立即移除 oneshot 任务队列记录。

        MySQL 的 task 行兼做历史记录，默认只在失败保留时间为 0 时删除；
        Redis pending set 只表示待执行队列，会覆盖为成功后总是移除。
        """
        return retention_policy.failure_retention_seconds == 0

    # ── 触发驱动任务（triggered） ────────────────────────────────────────────

    async def submit_triggered_task(
        self,
        name: str,
        biz_name: str,
        timeout: int,
        max_runs: int | None,
        params: dict[str, object] | None = None,
        *,
        cooldown_seconds: int = 0,
    ) -> str:
        """提交触发驱动任务（UPSERT 语义）。

        identity 是 (task_name, biz_name)，存在则只刷新 request_at 和重置
        失败痕迹；不存在则 INSERT。无论上次状态是 completed / failed 都能
        被同一行重新激活，避免撞 uk_task_name_biz_name。

        Args:
            cooldown_seconds: 由 @trigger 声明的冷却时间。store 在同一事务
                里读取现有 run_at，将 request_at 钳到 ``max(now, run_at + cooldown)``。
                0 表示无冷却，request_at 直接写 now。

        Returns:
            该触发驱动任务的行 id。
        """
        raise NotImplementedError

    async def list_pending_triggered_tasks(self) -> list[tuple[str, str, int, dict[str, object] | None]]:
        """列出待执行的触发驱动任务（run_at < request_at 或 run_at IS NULL）。

        Returns:
            [(task_id, handler_name, timeout, params), ...]
        """
        raise NotImplementedError

    @abstractmethod
    async def cleanup_old_successful_tasks(self, days: int = 10) -> int:
        """清理指定天数前成功执行的动态任务。

        Args:
            days: 保留最近多少天的记录，默认 10 天

        Returns:
            删除的记录数
        """
        ...

    async def cleanup_expired_oneshot_tasks(self, retention_policy: TaskRetentionPolicy) -> int:
        """基类实现不支持基于时间的历史清理，始终返回 0。

        子类（如 SQLTaskStore）应覆盖此方法，实现基于 retention_policy 的清理逻辑。
        RedisTaskStore 不需要此方法，因为动态任务执行完成后会立即从 pending set 中删除。
        """
        return 0

    async def cleanup_expired_oneshot_tasks_by_name_prefix(
        self,
        retention_policy: TaskRetentionPolicy,
        task_name_prefix: str,
    ) -> int:
        """按 task_name 前缀清理过期 oneshot 行。

        NamespacedTaskStore 用它避免本地/环境专属 scheduler 清理共享 task 表上
        其它 namespace 的历史记录。底层 store 不支持时保守返回 0。
        """
        if not task_name_prefix:
            return await self.cleanup_expired_oneshot_tasks(retention_policy)
        return 0

    # ── 排障 / 健康检查 ──────────────────────────────────────────────────────

    async def list_long_pending_tasks(
        self,
        min_age_seconds: int,
    ) -> list[tuple[str, str, str, int]]:
        """列出 ``status=pending`` 且 ``created_at`` 早于 now - min_age_seconds 的
        oneshot / triggered 任务行。

        给 ``system.task_scheduler.pending_age_check`` 用,当 tick loop 因任何原因
        长时间不调度时,这条查询能让日志主动复述"卡了多久 / 涉及哪些行"。

        基类默认返回空 list,Redis store 暂未实现(triggered 不支持,oneshot 在
        Redis 上靠 pending set 不需要 age 概念)。

        Returns:
            ``[(task_id, handler_name, task_type, age_seconds), ...]``,按 age 降序
        """
        return []
