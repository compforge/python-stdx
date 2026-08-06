"""SQLAlchemy-backed task persistence using a portable relational schema.

The adapter keeps task-type state, expiring locks, execution outcomes, and
retention data in one ``task`` table. Runtime statements avoid dialect-specific
date functions and upsert syntax. Applications should own production schema
migrations; ``auto_migrate=True`` exists for local development and tests.
"""

import asyncio
import logging
import uuid
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, Index, Integer, String, Text, UniqueConstraint, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from python_stdx.database import Database
from python_stdx.scheduler.store.base import TaskStore
from python_stdx.scheduler.types import FailureRecord, RunRecord, TaskRetentionPolicy, TaskStatus, TaskType

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from python_stdx.scheduler.schedule import Schedule


# The sentinel makes the scheduled-task identity non-null on databases whose
# unique indexes allow multiple NULL values.
SCHEDULED_BIZ_NAME = "__scheduled__"
SQL_LIKE_ESCAPE = "!"


class Base(DeclarativeBase):
    pass


def _escape_like_pattern(value: str) -> str:
    return (
        value.replace(SQL_LIKE_ESCAPE, SQL_LIKE_ESCAPE * 2)
        .replace("%", f"{SQL_LIKE_ESCAPE}%")
        .replace("_", f"{SQL_LIKE_ESCAPE}_")
    )


def _result_rowcount(result: object) -> int:
    rowcount = getattr(result, "rowcount", 0)
    return rowcount if isinstance(rowcount, int) else 0


class TaskRow(Base):
    """一次执行对应一行。

    时间列读写约定（重要,踩过坑）：
    所有时间列(``created_at`` / ``updated_at`` / ``locked_at`` / ``expire_at`` /
    ``run_at`` / ``request_at`` / ``error_at``)都由 Python 端 ``datetime.now()``
    落库,跟随 pod 容器本地 TZ;表 schema 不挂 ``server_default=CURRENT_TIMESTAMP``、
    也不靠 DB ``NOW()`` 写值。原因:pod 容器 TZ 与 DB session TZ 不一致时,
    DB 默认值会按 DB session 时区落库,而代码后续写入 / 读取又是 pod TZ,
    谓词比较会整体偏移一个 TZ offset(典型表现:pod=Asia/Shanghai + DB=UTC
    时,stale-running 行要 8 小时后才被 ``list_pending_oneshot_tasks`` 回收)。
    任何对时间列的 SQL 比较也必须用 Python ``now`` / ``cutoff`` 透传,
    不要写 ``NOW(3)`` / ``UTC_TIMESTAMP(3)`` / ``DATE_SUB(...)`` 等方言函数。

    ``auto_migrate=True`` creates the same indexes declared by the model so a
    development database retains the uniqueness and dispatch invariants.
    """

    __tablename__ = "task"
    __table_args__ = (
        UniqueConstraint("task_name", "biz_name", name="uk_task_name_biz_name"),
        Index("idx_task_name_id", "task_name", "id"),
        Index("idx_task_name_owner", "task_name", "owner"),
        Index("idx_task_trace_id", "trace_id"),
        # triggered tick: WHERE task_type='triggered' AND request_at <= :now
        Index("idx_task_type_request_at", "task_type", "request_at"),
        # oneshot tick + count_running: WHERE task_type='oneshot' AND expire_at > :now
        Index("idx_task_type_expire_at", "task_type", "expire_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    task_name: Mapped[str] = mapped_column(String(128), nullable=False)
    biz_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    task_type: Mapped[str] = mapped_column(String(16), default=TaskType.SCHEDULED.value, nullable=False)
    # status 是"行的当前状态"显式表达。dispatch / cleanup SQL 会读它简化谓词；
    # stale-running、triggered resubmit 这些边角仍要靠 expire_at / request_at 配合判断。
    # 由 store 层每个 INSERT/UPDATE 显式维护，对应写入路径见各 record_*/submit_*/acquire_* 实现。
    status: Mapped[str] = mapped_column(
        String(16),
        default=TaskStatus.PENDING.value,
        server_default=text("'pending'"),
        nullable=False,
    )
    # 由 store 层每个 INSERT 显式 set created_at=:now;不挂 server_default
    # CURRENT_TIMESTAMP,统一用 Python `datetime.now()`,见类顶部"时间列读写约定"。
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    # 行最近一次写入时间；仅供排查"这行最近动过没"，业务逻辑不依赖。
    # store 层每个 INSERT/UPDATE 自己显式 set updated_at=:now;不挂 server_default
    # 也不挂 ON UPDATE,与 created_at 同一约定。
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    # 锁
    owner: Mapped[str | None] = mapped_column(String(128), nullable=True)
    locked_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    expire_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    # 运行状态
    run_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    # triggered 类型：最近一次 submit 时刻；run_at < request_at 表示还有未覆盖的请求需要再跑一轮
    request_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    run_count: Mapped[int] = mapped_column(Integer, default=0, server_default=text("0"), nullable=False)
    max_runs: Mapped[int | None] = mapped_column(Integer, nullable=True)  # NULL = 无限次
    trace_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    # 执行控制
    timeout: Mapped[int | None] = mapped_column(Integer, nullable=True)  # 超时秒数
    # 失败记录
    message: Mapped[str | None] = mapped_column(Text, nullable=True)  # 最近一次失败的错误信息
    error_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)  # 最近一次失败时间
    error_attempt: Mapped[int | None] = mapped_column(Integer, nullable=True)  # 最近一次失败对应的尝试次数
    # 动态任务参数
    params: Mapped[str | None] = mapped_column(Text, nullable=True)  # JSON-encoded task parameters


class SQLTaskStore(TaskStore):
    """Persist scheduler state through the shared synchronous Database wrapper."""

    def __init__(
        self,
        db: Database,
        pod_id: str | None = None,
        auto_migrate: bool = False,
    ):
        self._db = db
        self._pod_id = pod_id or uuid.uuid4().hex[:8]
        self._auto_migrate = auto_migrate

    async def init(self) -> None:
        if not self._auto_migrate:
            return

        def _sync() -> None:
            Base.metadata.create_all(self._db.engine)

        await asyncio.to_thread(_sync)

    # ── 分布式锁 ─────────────────────────────────────────────────────────────

    async def acquire_lock(self, name: str, ttl: int) -> bool:
        """原子获取 scheduled 锁：SELECT FOR UPDATE + 条件更新。

        scheduled 行的 identity 是 (task_name, biz_name=SCHEDULED_BIZ_NAME)；
        uk_task_name_biz_name 在该 sentinel 上是真正唯一的，能阻止双 pod 并发首次
        INSERT。SELECT 也按 sentinel 过滤，避免误命中同 task_name 下 oneshot
        / triggered 的行（它们的锁路径是 acquire_dynamic_task_lock）。

        首次执行时 INSERT 新行；后续执行时检查锁状态并尝试获取。
        只有当 owner IS NULL 或锁已过期时才能获取成功。
        """

        def _sync() -> bool:
            with self._db.transaction() as s:
                # 1. 尝试 SELECT FOR UPDATE（如果行存在）
                row = s.execute(
                    select(TaskRow.id, TaskRow.owner, TaskRow.expire_at)
                    .where(TaskRow.task_name == name, TaskRow.biz_name == SCHEDULED_BIZ_NAME)
                    .limit(1)
                    .with_for_update()
                ).one_or_none()

                now = datetime.now()

                if row is None:
                    # 首次执行：INSERT 新行并持有锁
                    # uk_task_name_biz_name 在 sentinel 上去重；并发首次 INSERT 只会有
                    # 一个赢，其它 pod 命中 IntegrityError 后视为没拿到锁——下一轮 tick
                    # 再走 UPDATE 路径。IntegrityError 必须离开 transaction scope
                    # 后再转换成 False，确保 context manager 先完成 rollback。
                    exec_id = uuid.uuid4().hex
                    s.execute(
                        text(
                            """
                            INSERT INTO task (id, task_name, biz_name, task_type, status,
                                               created_at, updated_at,
                                               owner, locked_at, expire_at)
                            VALUES (:id, :name, :biz_name, :task_type, :status,
                                    :now, :now,
                                    :owner, :now, :expires)
                            """
                        ),
                        {
                            "id": exec_id,
                            "name": name,
                            "biz_name": SCHEDULED_BIZ_NAME,
                            "task_type": TaskType.SCHEDULED.value,
                            "status": TaskStatus.RUNNING.value,
                            "owner": self._pod_id,
                            "now": now,
                            "expires": now + timedelta(seconds=ttl),
                        },
                    )
                    return True

                # 2. 检查锁状态
                row_id, owner, expire_at = row[0], row[1], row[2]
                is_locked = owner is not None
                is_expired = expire_at is None or expire_at <= now

                if is_locked and not is_expired:
                    # 锁被其他 pod 持有且未过期
                    return False

                # 3. 锁空闲或已过期，尝试获取
                s.execute(
                    text(
                        """
                        UPDATE task
                        SET owner = :owner, locked_at = :now, expire_at = :expire_at,
                            status = :status, updated_at = :now
                        WHERE id = :id
                        """
                    ),
                    {
                        "owner": self._pod_id,
                        "now": now,
                        "expire_at": now + timedelta(seconds=ttl),
                        "status": TaskStatus.RUNNING.value,
                        "id": row_id,
                    },
                )
                return True

        try:
            return await asyncio.to_thread(_sync)
        except IntegrityError:
            return False

    async def release_lock(self, name: str) -> None:
        """释放锁：清空 owner 和过期时间。"""

        def _sync() -> None:
            with self._db.transaction() as s:
                s.execute(
                    text(
                        """
                        UPDATE task
                        SET owner = NULL, locked_at = NULL, expire_at = NULL,
                            updated_at = :now
                        WHERE task_name = :name AND owner = :owner
                        """
                    ),
                    {"name": name, "owner": self._pod_id, "now": datetime.now()},
                )

        await asyncio.to_thread(_sync)

    async def acquire_dynamic_task_lock(self, task_id: str, ttl: int) -> bool:
        """按动态任务行 id 获取锁。

        动态任务的 task_name 必须保留为已注册 handler 名；这里不能复用
        acquire_lock(task_id)，否则 task_id 会被误当成 task_name 插入新行。
        """

        def _sync() -> bool:
            with self._db.transaction() as s:
                row = s.execute(
                    select(TaskRow.owner, TaskRow.expire_at).where(TaskRow.id == task_id).with_for_update()
                ).one_or_none()

                if row is None:
                    return False

                now = datetime.now()
                owner, expire_at = row[0], row[1]
                is_locked = owner is not None
                is_expired = expire_at is None or expire_at <= now

                if is_locked and not is_expired:
                    return False

                s.execute(
                    text(
                        """
                        UPDATE task
                        SET owner = :owner, locked_at = :now, expire_at = :expire_at,
                            status = :status, updated_at = :now
                        WHERE id = :task_id
                        """
                    ),
                    {
                        "owner": self._pod_id,
                        "now": now,
                        "expire_at": now + timedelta(seconds=ttl),
                        "status": TaskStatus.RUNNING.value,
                        "task_id": task_id,
                    },
                )
                return True

        return await asyncio.to_thread(_sync)

    async def release_dynamic_task_lock(self, task_id: str) -> None:
        """释放动态任务行锁。"""

        def _sync() -> None:
            with self._db.transaction() as s:
                s.execute(
                    text(
                        """
                        UPDATE task
                        SET owner = NULL, locked_at = NULL, expire_at = NULL,
                            updated_at = :now
                        WHERE id = :task_id AND owner = :owner
                        """
                    ),
                    {"task_id": task_id, "owner": self._pod_id, "now": datetime.now()},
                )

        await asyncio.to_thread(_sync)

    async def get_lock_owner(self, name: str) -> str | None:
        def _sync() -> str | None:
            with self._db.session() as s:
                row = s.execute(
                    text("SELECT owner FROM task WHERE task_name = :name ORDER BY created_at DESC, id DESC LIMIT 1"),
                    {"name": name},
                ).fetchone()
                return row[0] if row else None

        return await asyncio.to_thread(_sync)

    # ── 状态查询 ─────────────────────────────────────────────────────────────

    async def get_last_run(self, name: str) -> float | None:
        def _sync() -> float | None:
            with self._db.session() as s:
                run_at = s.execute(
                    select(TaskRow.run_at)
                    .where(TaskRow.task_name == name)
                    .order_by(TaskRow.created_at.desc(), TaskRow.id.desc())
                    .limit(1)
                ).scalar_one_or_none()
                return run_at.timestamp() if run_at is not None else None

        return await asyncio.to_thread(_sync)

    async def get_run_count(self, name: str) -> int:
        def _sync() -> int:
            with self._db.session() as s:
                row = s.execute(
                    text(
                        "SELECT run_count FROM task WHERE task_name = :name ORDER BY created_at DESC, id DESC LIMIT 1"
                    ),
                    {"name": name},
                ).fetchone()
                return row[0] if row else 0

        return await asyncio.to_thread(_sync)

    # ── 记录 ─────────────────────────────────────────────────────────────────

    async def record_success(
        self,
        name: str,
        ts: float,
        duration_ms: int,
        attempt: int = 1,
        trace_id: str | None = None,
    ) -> None:
        started_at = datetime.fromtimestamp(ts)

        def _sync() -> None:
            with self._db.transaction() as s:
                s.execute(
                    text(
                        """
                        UPDATE task
                        SET run_at = :ts, run_count = run_count + 1,
                            trace_id = :trace_id,
                            message = NULL, error_at = NULL, error_attempt = NULL,
                            status = :status, updated_at = :now
                        WHERE task_name = :name AND owner = :owner
                        """
                    ),
                    {
                        "name": name,
                        "ts": started_at,
                        "trace_id": trace_id,
                        "owner": self._pod_id,
                        "status": TaskStatus.SUCCESS.value,
                        "now": datetime.now(),
                    },
                )

        await asyncio.to_thread(_sync)

    async def record_failure(
        self,
        name: str,
        error: str,
        attempt: int,
        ts: float,
        trace_id: str | None = None,
    ) -> None:
        started_at = datetime.fromtimestamp(ts)

        def _sync() -> None:
            with self._db.transaction() as s:
                s.execute(
                    text(
                        """
                        UPDATE task
                        SET run_at = :ts, run_count = run_count + 1,
                            trace_id = :trace_id,
                            message = :error, error_at = :ts_dt, error_attempt = :attempt,
                            status = :status, updated_at = :now
                        WHERE task_name = :name AND owner = :owner
                        """
                    ),
                    {
                        "name": name,
                        "ts": started_at,
                        "ts_dt": started_at,
                        "trace_id": trace_id,
                        "error": error,
                        "attempt": attempt,
                        "owner": self._pod_id,
                        "status": TaskStatus.FAILED.value,
                        "now": datetime.now(),
                    },
                )

        await asyncio.to_thread(_sync)

    async def record_dynamic_success(
        self,
        task_id: str,
        ts: float,
        duration_ms: int,
        attempt: int = 1,
        trace_id: str | None = None,
    ) -> None:
        started_at = datetime.fromtimestamp(ts)

        def _sync() -> None:
            with self._db.transaction() as s:
                s.execute(
                    text(
                        """
                        UPDATE task
                        SET run_at = :ts, run_count = run_count + 1,
                            trace_id = :trace_id,
                            message = NULL, error_at = NULL, error_attempt = NULL,
                            status = :status, updated_at = :now
                        WHERE id = :task_id AND owner = :owner
                        """
                    ),
                    {
                        "task_id": task_id,
                        "ts": started_at,
                        "trace_id": trace_id,
                        "owner": self._pod_id,
                        "status": TaskStatus.SUCCESS.value,
                        "now": datetime.now(),
                    },
                )

        await asyncio.to_thread(_sync)

    async def record_dynamic_failure(
        self,
        task_id: str,
        error: str,
        attempt: int,
        ts: float,
        trace_id: str | None = None,
    ) -> None:
        started_at = datetime.fromtimestamp(ts)

        def _sync() -> None:
            with self._db.transaction() as s:
                s.execute(
                    text(
                        """
                        UPDATE task
                        SET run_at = :ts, run_count = run_count + 1,
                            trace_id = :trace_id,
                            message = :error, error_at = :error_at, error_attempt = :attempt,
                            status = :status, updated_at = :now
                        WHERE id = :task_id AND owner = :owner
                        """
                    ),
                    {
                        "task_id": task_id,
                        "ts": started_at,
                        "trace_id": trace_id,
                        "error": error,
                        "error_at": started_at,
                        "attempt": attempt,
                        "owner": self._pod_id,
                        "status": TaskStatus.FAILED.value,
                        "now": datetime.now(),
                    },
                )

        await asyncio.to_thread(_sync)

    # ── 历史查询 ─────────────────────────────────────────────────────────────

    async def get_last_failure(self, name: str) -> FailureRecord | None:
        def _sync() -> FailureRecord | None:
            with self._db.session() as s:
                row = s.execute(
                    select(TaskRow.message, TaskRow.error_at, TaskRow.error_attempt)
                    .where(TaskRow.task_name == name)
                    .order_by(TaskRow.created_at.desc(), TaskRow.id.desc())
                    .limit(1)
                ).one_or_none()
                if row and row[0]:
                    return FailureRecord(
                        task_name=name,
                        error=row[0],
                        failed_at=row[1].timestamp() if row[1] else 0.0,
                        attempt=row[2] or 1,
                    )
                return None

        return await asyncio.to_thread(_sync)

    async def list_history(self, name: str, limit: int = 100) -> list[RunRecord]:
        def _sync() -> list[RunRecord]:
            with self._db.session() as s:
                rows = s.execute(
                    select(
                        TaskRow.id,
                        TaskRow.run_at,
                        TaskRow.run_count,
                        TaskRow.message,
                        TaskRow.error_at,
                        TaskRow.error_attempt,
                    )
                    .where(TaskRow.task_name == name)
                    .order_by(TaskRow.created_at.desc(), TaskRow.id.desc())
                    .limit(limit)
                ).all()
                return [
                    RunRecord(
                        id=int(row[2]) * 1000 + (int(row[0][-4:], 16) if row[0] else 0),
                        name=name,
                        status="failed" if row[3] else "success",
                        started_at=row[1].timestamp() if row[1] else 0.0,
                        duration_ms=0,
                        error=row[3],
                        attempt=row[5] or 1,
                    )
                    for row in rows
                ]

        return await asyncio.to_thread(_sync)

    # ── 动态任务 ─────────────────────────────────────────────────────────────

    async def submit_oneshot_task(
        self,
        name: str,
        biz_name: str | None,
        schedule: "Schedule | None",
        timeout: int,
        max_runs: int | None,
        params: dict[str, object] | None = None,
    ) -> str:
        """提交 oneshot 任务：INSERT 新行，task_type=TaskType.ONESHOT。

        非空 biz_name 时是幂等的：DB 上 (task_name, biz_name) 唯一索引
        兜底，已存在的 identity 直接 return 已有 task_id（无论那一行是
        活跃还是终态），不再 INSERT。终态行的回收交给
        TaskRetentionPolicy。
        """
        import json

        if biz_name is not None:
            existing = await self.get_oneshot_task_by_biz_name(name, biz_name)
            if existing is not None:
                return existing[0]

        task_id = uuid.uuid4().hex
        params_json = json.dumps(params, ensure_ascii=False) if params else None

        def _sync() -> None:
            now = datetime.now()
            with self._db.transaction() as s:
                s.execute(
                    text(
                        """
                        INSERT INTO task (id, task_name, biz_name, task_type, status,
                                          created_at, updated_at,
                                          max_runs, timeout, params)
                        VALUES (:id, :name, :biz_name, :task_type, :status,
                                :now, :now,
                                :max_runs, :timeout, :params)
                        """
                    ),
                    {
                        "id": task_id,
                        "name": name,
                        "biz_name": biz_name,
                        "task_type": TaskType.ONESHOT.value,
                        "status": TaskStatus.PENDING.value,
                        "now": now,
                        "max_runs": max_runs,
                        "timeout": timeout,
                        "params": params_json,
                    },
                )

        try:
            await asyncio.to_thread(_sync)
        except IntegrityError:
            if biz_name is not None:
                existing = await self.get_oneshot_task_by_biz_name(name, biz_name)
                if existing is not None:
                    return existing[0]
            raise
        return task_id

    async def get_oneshot_task_by_biz_name(
        self,
        name: str,
        biz_name: str,
    ) -> tuple[str, str, int, dict[str, object] | None] | None:
        """按 (task_name, biz_name) 查询已存在的 oneshot 任务行。

        谓词跟 ``uk_task_name_biz_name`` 严格对齐，不带 run_count /
        run_at 等状态过滤；oneshot submit 撞唯一索引时靠这条查询拿到已
        有 task_id 实现幂等返回，过滤掉终态行会让 INSERT 撞 uk 后兜底
        找不到行而误抛 IntegrityError。
        """
        import json

        def _sync() -> tuple[str, str, int, dict[str, object] | None] | None:
            with self._db.session() as s:
                row = s.execute(
                    text(
                        """
                        SELECT id, task_name, timeout, params
                        FROM task
                        WHERE task_type = :task_type
                          AND task_name = :name
                          AND biz_name = :biz_name
                        LIMIT 1
                        """
                    ),
                    {"task_type": TaskType.ONESHOT.value, "name": name, "biz_name": biz_name},
                ).fetchone()
                if row is None:
                    return None
                return (row[0], row[1], row[2] or 30, json.loads(row[3]) if row[3] else None)

        return await asyncio.to_thread(_sync)

    async def list_pending_oneshot_tasks(self) -> list[tuple[str, str, int, dict[str, object] | None]]:
        """列出所有待执行的 oneshot 任务。

        谓词解释：
        - status='pending'：刚 submit、未被任何 pod acquire 过的行
        - status='running' AND expire_at <= :now：pod 崩残留行，由本轮接手
        - run_count < max_runs：兜底防止"已经跑完但 status 没 final"的行被
          重复拾起。在当前 _execute retry 语义下（独立 bug，参见 design 文档），
          oneshot 跑失败后 status='failed' 但 run_count 可能 < max_runs；status
          已经把它从 list_pending 排除，但保留该谓词以防未来 retry 语义改动。

        与 triggered 路径一致,这里也由 Python 端传入 ``now``——与
        ``acquire_dynamic_task_lock`` / ``record_*`` 写入 ``expire_at`` /
        ``locked_at`` 用的 ``datetime.now()`` 共享同一时间基准;即便 DB session
        TZ 没对齐(历史连接、未来跨库迁移),stale-running 也能按预期被回收。

        Returns:
            [(task_id, handler_name, timeout, params), ...]
        """
        import json

        def _sync() -> list[tuple[str, str, int, dict[str, object] | None]]:
            now = datetime.now()
            with self._db.session() as s:
                rows = s.execute(
                    text(
                        """
                        SELECT id, task_name, timeout, params
                        FROM task
                        WHERE task_type = :task_type
                          AND ((status = :pending)
                               OR (status = :running AND expire_at <= :now))
                          AND (max_runs IS NULL OR run_count < max_runs)
                        ORDER BY created_at ASC, id ASC
                        """
                    ),
                    {
                        "task_type": TaskType.ONESHOT.value,
                        "pending": TaskStatus.PENDING.value,
                        "running": TaskStatus.RUNNING.value,
                        "now": now,
                    },
                ).fetchall()
                return [(row[0], row[1], row[2] or 30, json.loads(row[3]) if row[3] else None) for row in rows]

        return await asyncio.to_thread(_sync)

    async def delete_oneshot_task(self, task_id: str) -> None:
        """删除 oneshot 任务行。"""

        def _sync() -> None:
            with self._db.transaction() as s:
                s.execute(
                    text("DELETE FROM task WHERE id = :id"),
                    {"id": task_id},
                )

        await asyncio.to_thread(_sync)

    async def count_running_oneshot_tasks(self) -> dict[str, int]:
        """聚合 cluster 级 oneshot running 行数。

        判定与 `list_pending_oneshot_tasks` 严格互补：running 是 status='running'
        且 expire_at 还未过期；崩溃 pod 残留行（status='running' AND expire_at<=:now）
        归 pending 一侧，不计入 running。

        与 list_pending 同样由 Python 端传 ``now``,避免 pod TZ 与 DB session TZ
        不一致时 stale-running 行被同时算进 running 计数 + 排除在 pending 之外,
        把 ``max_concurrency`` cap 也一起卡死。
        """

        def _sync() -> dict[str, int]:
            now = datetime.now()
            with self._db.session() as s:
                rows = s.execute(
                    text(
                        """
                        SELECT task_name, COUNT(*)
                        FROM task
                        WHERE task_type = :task_type
                          AND status = :running
                          AND expire_at IS NOT NULL
                          AND expire_at > :now
                        GROUP BY task_name
                        """
                    ),
                    {
                        "task_type": TaskType.ONESHOT.value,
                        "running": TaskStatus.RUNNING.value,
                        "now": now,
                    },
                ).fetchall()
                return {row[0]: int(row[1]) for row in rows}

        return await asyncio.to_thread(_sync)

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
        """提交触发驱动任务：SELECT FOR UPDATE → UPDATE/INSERT。

        UPSERT 走 ORM 通用语法（select.with_for_update + 显式 update / add），
        而不是 ``INSERT ... ON DUPLICATE KEY UPDATE``——同一段代码可以直接
        复用到 PostgreSQL/SQLite 等 store。

        cooldown_seconds 在同一事务里基于现有 run_at 计算：
        - 行不存在或没有 run_at（首次提交 / 还没跑过）→ request_at = now
        - 行存在且已跑过 → request_at = max(now, run_at + cooldown)

        多个 submit 并发命中同一行时，``SELECT FOR UPDATE`` 行锁串行化它们；
        同窗口内 run_at 不变，所以多次计算结果幂等。

        - 不存在 → INSERT 新行，task_type=triggered
        - 已存在 → 刷新 request_at 并清掉上一轮失败痕迹（重置 run_count 等
          是为了让本轮重新按 max_runs 计数）；不动 owner/locked_at/expire_at/
          run_at，避免破坏正在跑的 worker。

        Returns:
            该 triggered 任务的行 id（新建或既有）。
        """
        import json

        params_json = json.dumps(params, ensure_ascii=False) if params else None

        def _sync() -> str:
            with self._db.transaction() as s:
                # 行级锁：并发 submit 同一 (task_name, biz_name) 串行化，避免双 INSERT
                existing = s.execute(
                    select(TaskRow).where(TaskRow.task_name == name, TaskRow.biz_name == biz_name).with_for_update()
                ).scalar_one_or_none()

                # 必须用本地 now()：record_*/server_default/SQL NOW(3) 全是 MySQL 会话本地时间，
                # 这里若混 utcnow() 会让 request_at 落在 8h 后，被 list_pending_triggered_tasks
                # 的 `request_at <= now` 判负，executor 永远不重新拾起。
                now = datetime.now()
                if existing is None:
                    request_at = now
                else:
                    # cooldown=0 等价于"没有冷却"，与未声明语义一致
                    if cooldown_seconds and existing.run_at is not None:
                        not_before = existing.run_at + timedelta(seconds=cooldown_seconds)
                        request_at = max(now, not_before)
                    else:
                        request_at = now

                if existing is None:
                    new_task = TaskRow(
                        id=uuid.uuid4().hex,
                        task_name=name,
                        biz_name=biz_name,
                        task_type=TaskType.TRIGGERED.value,
                        status=TaskStatus.PENDING.value,
                        created_at=now,
                        updated_at=now,
                        max_runs=max_runs,
                        timeout=timeout,
                        params=params_json,
                        request_at=request_at,
                    )
                    s.add(new_task)
                    s.flush()
                    return new_task.id

                # UPDATE existing：保留上一轮 status（success / failed / running）
                # 不改回 pending——避免与正在跑的 worker 的 record_*_success/failure
                # 形成跨事务 race。resubmit 信号由 request_at 表达，dispatch 看
                # `run_at < request_at` 即可拾起。
                existing.request_at = request_at
                existing.run_count = 0
                existing.message = None
                existing.error_at = None
                existing.error_attempt = None
                existing.timeout = timeout
                existing.max_runs = max_runs
                existing.params = params_json
                existing.updated_at = now
                return existing.id

        try:
            return await asyncio.to_thread(_sync)
        except IntegrityError:
            # Two first submissions can both observe a missing row. The unique
            # constraint selects a winner; a single retry then follows the
            # existing-row path under its row lock.
            return await asyncio.to_thread(_sync)

    async def list_pending_triggered_tasks(self) -> list[tuple[str, str, int, dict[str, object] | None]]:
        """列出待执行的 triggered 任务。

        条件：
        - task_type = triggered
        - 锁空闲：status != 'running'，或 status='running' 但 expire_at 已过（pod 崩残留）
        - request_at 不为 NULL（曾被 submit 过）
        - request_at <= now（cooldown 决定的"最早可执行时间"已到达）
        - run_at IS NULL（从未跑过）或 run_at < request_at（上次跑完后又来过新请求）

        说明：
        - 严格 `<` 比较；ms 精度 + frequently triggered jobs 触发频率下，同毫秒
          撞值实际不会发生。`<=` 会让"干净跑完"陷入死循环，不可用。
        - now 由 Python 传入（不用 ``NOW(3)``），便于 store 实现迁到非 MySQL DB
          时复用同一句 SQL；和 expire_at 检查共享同一时间基准也更直观。
        - 锁判断是 ``status != 'running' OR expire_at <= :now``——比旧的 ``owner IS NULL
          OR expire_at IS NULL OR expire_at <= :now`` 语义更直观，覆盖：success/failed
          两种 idle 状态、pending（首次未跑）、stale-running。
        """
        import json

        def _sync() -> list[tuple[str, str, int, dict[str, object] | None]]:
            # 用本地 now() 与 request_at / expire_at（由 record_*/submit_* 以本地时间写入）对齐；
            # 与同模块其它 SQL 谓词里的 :now 也保持同一基准。
            now = datetime.now()
            with self._db.session() as s:
                rows = s.execute(
                    text(
                        """
                        SELECT id, task_name, timeout, params
                        FROM task
                        WHERE task_type = :task_type
                          AND (status != :running OR expire_at <= :now)
                          AND request_at IS NOT NULL
                          AND request_at <= :now
                          AND (run_at IS NULL OR run_at < request_at)
                        ORDER BY created_at ASC, id ASC
                        """
                    ),
                    {
                        "task_type": TaskType.TRIGGERED.value,
                        "running": TaskStatus.RUNNING.value,
                        "now": now,
                    },
                ).fetchall()
                return [(row[0], row[1], row[2] or 30, json.loads(row[3]) if row[3] else None) for row in rows]

        return await asyncio.to_thread(_sync)

    async def cleanup_old_successful_tasks(self, days: int = 10) -> int:
        """清理 N 天前成功执行的 oneshot 任务。

        条件：task_type='oneshot'、status='success'、owner IS NULL（未被锁定）、
        run_at < cutoff（Python 端按 :days 算出的截止时间）。

        cutoff 由 Python 端计算后透传 :cutoff，不在 SQL 里用 DATE_SUB / INTERVAL /
        NOW(3) 等方言函数——既保持跨 DB 通用，又与文件顶部"必须 Python now 透传"
        的时间列读写约定对齐。

        Returns:
            删除的记录数
        """

        def _sync() -> int:
            cutoff = datetime.now() - timedelta(days=days)
            with self._db.transaction() as s:
                result = s.execute(
                    text(
                        """
                        DELETE FROM task
                        WHERE task_type = :task_type
                          AND status = :status
                          AND owner IS NULL
                          AND run_at IS NOT NULL
                          AND run_at < :cutoff
                        """
                    ),
                    {
                        "task_type": TaskType.ONESHOT.value,
                        "status": TaskStatus.SUCCESS.value,
                        "cutoff": cutoff,
                    },
                )
                deleted_count = _result_rowcount(result)
                if deleted_count > 0:
                    logger.info(f"Cleaned up {deleted_count} old successful tasks (older than {days} days)")
                return deleted_count

        return await asyncio.to_thread(_sync)

    async def list_long_pending_tasks(
        self,
        min_age_seconds: int,
    ) -> list[tuple[str, str, str, int]]:
        """SQL 实现:status=pending 且 created_at < now - min_age_seconds 的 oneshot
        / triggered 行,按 age 降序。

        cutoff 由 Python 端算后透传,不写 NOW(3)/DATE_SUB 等方言函数;与表内其他
        SQL 谓词的时间基准一致。
        """

        def _sync() -> list[tuple[str, str, str, int]]:
            now = datetime.now()
            cutoff = now - timedelta(seconds=min_age_seconds)
            with self._db.session() as s:
                rows = s.execute(
                    select(TaskRow.id, TaskRow.task_name, TaskRow.task_type, TaskRow.created_at)
                    .where(
                        TaskRow.status == TaskStatus.PENDING.value,
                        TaskRow.task_type.in_((TaskType.ONESHOT.value, TaskType.TRIGGERED.value)),
                        TaskRow.created_at < cutoff,
                    )
                    .order_by(TaskRow.created_at.asc())
                ).all()
                return [(row[0], row[1], row[2], int((now - row[3]).total_seconds()) if row[3] else 0) for row in rows]

        return await asyncio.to_thread(_sync)

    async def cleanup_expired_oneshot_tasks(self, retention_policy: TaskRetentionPolicy) -> int:
        """按全局成功/失败保留时间清理已完成 oneshot 任务。

        用 run_at 判断保留期：一般任务执行时间不长，用 run_at 与 error_at 的差异可忽略不计，
        这样判断逻辑更简单（且 status='success' 时 error_at 是 NULL，无法用作时间基准）。

        cutoff 由 Python 端计算后透传 :cutoff，不在 SQL 里用 DATE_SUB / INTERVAL /
        NOW(3) 等方言函数，跨 DB 通用。
        """

        def _sync() -> int:
            deleted_count = 0
            now = datetime.now()
            with self._db.transaction() as s:
                if retention_policy.success_retention_seconds > 0:
                    cutoff = now - timedelta(seconds=retention_policy.success_retention_seconds)
                    result = s.execute(
                        text(
                            """
                            DELETE FROM task
                            WHERE task_type = :task_type
                              AND status = :status
                              AND owner IS NULL
                              AND run_at IS NOT NULL
                              AND run_at <= :cutoff
                            """
                        ),
                        {
                            "task_type": TaskType.ONESHOT.value,
                            "status": TaskStatus.SUCCESS.value,
                            "cutoff": cutoff,
                        },
                    )
                    deleted_count += _result_rowcount(result)

                if retention_policy.failure_retention_seconds > 0:
                    cutoff = now - timedelta(seconds=retention_policy.failure_retention_seconds)
                    result = s.execute(
                        text(
                            """
                            DELETE FROM task
                            WHERE task_type = :task_type
                              AND status = :status
                              AND owner IS NULL
                              AND run_at IS NOT NULL
                              AND run_at <= :cutoff
                            """
                        ),
                        {
                            "task_type": TaskType.ONESHOT.value,
                            "status": TaskStatus.FAILED.value,
                            "cutoff": cutoff,
                        },
                    )
                    deleted_count += _result_rowcount(result)
            if deleted_count > 0:
                logger.info(f"Cleaned up {deleted_count} expired dynamic tasks")
            return deleted_count

        return await asyncio.to_thread(_sync)

    async def cleanup_expired_oneshot_tasks_by_name_prefix(
        self,
        retention_policy: TaskRetentionPolicy,
        task_name_prefix: str,
    ) -> int:
        """按 task_name 前缀清理当前 namespace 的已完成 oneshot 任务。"""
        if not task_name_prefix:
            return await self.cleanup_expired_oneshot_tasks(retention_policy)

        def _sync() -> int:
            deleted_count = 0
            now = datetime.now()
            name_like = f"{_escape_like_pattern(task_name_prefix)}%"
            with self._db.transaction() as s:
                if retention_policy.success_retention_seconds > 0:
                    cutoff = now - timedelta(seconds=retention_policy.success_retention_seconds)
                    result = s.execute(
                        text(
                            """
                            DELETE FROM task
                            WHERE task_type = :task_type
                              AND task_name LIKE :name_like ESCAPE '!'
                              AND status = :status
                              AND owner IS NULL
                              AND run_at IS NOT NULL
                              AND run_at <= :cutoff
                            """
                        ),
                        {
                            "task_type": TaskType.ONESHOT.value,
                            "name_like": name_like,
                            "status": TaskStatus.SUCCESS.value,
                            "cutoff": cutoff,
                        },
                    )
                    deleted_count += _result_rowcount(result)

                if retention_policy.failure_retention_seconds > 0:
                    cutoff = now - timedelta(seconds=retention_policy.failure_retention_seconds)
                    result = s.execute(
                        text(
                            """
                            DELETE FROM task
                            WHERE task_type = :task_type
                              AND task_name LIKE :name_like ESCAPE '!'
                              AND status = :status
                              AND owner IS NULL
                              AND run_at IS NOT NULL
                              AND run_at <= :cutoff
                            """
                        ),
                        {
                            "task_type": TaskType.ONESHOT.value,
                            "name_like": name_like,
                            "status": TaskStatus.FAILED.value,
                            "cutoff": cutoff,
                        },
                    )
                    deleted_count += _result_rowcount(result)
            if deleted_count > 0:
                logger.info(
                    "Cleaned up %s expired dynamic tasks for task_name_prefix=%s",
                    deleted_count,
                    task_name_prefix,
                )
            return deleted_count

        return await asyncio.to_thread(_sync)
