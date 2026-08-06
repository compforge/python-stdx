"""Redis TaskStore 实现（可选，用于锁的高性能场景）。"""

import json
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from python_stdx.redis import RedisClient
from python_stdx.scheduler.store.base import TaskStore
from python_stdx.scheduler.types import FailureRecord, RunRecord, TaskRetentionPolicy

if TYPE_CHECKING:
    from python_stdx.scheduler.schedule import Schedule


class RedisTaskStore(TaskStore):
    """Redis 实现的 TaskStore。

    用于分布式锁和轻量记录（不保证查询友好性）。
    如需可观测的历史记录，建议使用 SQLTaskStore。
    """

    def __init__(
        self,
        client: RedisClient,
        pod_id: str | None = None,
        lock_prefix: str = "task:lock:",
        history_prefix: str = "task:history:",
        failure_prefix: str = "task:failure:",
        last_run_prefix: str = "task:last_run:",
        history_max: int = 1000,
    ):
        import uuid

        self._redis = client
        self._pod_id = pod_id or uuid.uuid4().hex[:8]
        self._lock_prefix = lock_prefix
        self._history_prefix = history_prefix
        self._failure_prefix = failure_prefix
        self._last_run_prefix = last_run_prefix
        self._history_max = history_max

    @asynccontextmanager
    async def _client(self) -> AsyncIterator[RedisClient]:
        yield self._redis

    # ── 分布式锁 ─────────────────────────────────────────────────────────────

    async def acquire_lock(self, name: str, ttl: int) -> bool:
        key = f"{self._lock_prefix}{name}"
        async with self._client() as redis:
            result = await redis.set(key, self._pod_id, nx=True, ex=ttl)
            return result is not None

    async def release_lock(self, name: str) -> None:
        key = f"{self._lock_prefix}{name}"

        # Lua 脚本：只有自己持有的锁才能释放
        lua = """
        if redis.call('GET', KEYS[1]) == ARGV[1] then
            return redis.call('DEL', KEYS[1])
        else
            return 0
        end
        """
        async with self._client() as redis:
            await redis.eval(lua, 1, key, self._pod_id)

    async def get_lock_owner(self, name: str) -> str | None:
        key = f"{self._lock_prefix}{name}"
        async with self._client() as redis:
            owner = await redis.get(key)
            return owner.decode() if isinstance(owner, bytes) else owner

    # ── 状态查询 ─────────────────────────────────────────────────────────────

    async def get_last_run(self, name: str) -> float | None:
        key = f"{self._last_run_prefix}{name}"
        async with self._client() as redis:
            val = await redis.get(key)
            return float(val) if val else None

    async def get_run_count(self, name: str) -> int:
        key = f"task:run_count:{name}"
        async with self._client() as redis:
            val = await redis.get(key)
            return int(val) if val else 0

    # ── 记录 ─────────────────────────────────────────────────────────────────

    async def record_success(
        self,
        name: str,
        ts: float,
        duration_ms: int,
        attempt: int = 1,
        trace_id: str | None = None,
    ) -> None:
        key = f"{self._last_run_prefix}{name}"
        record = json.dumps(
            {"status": "success", "ts": ts, "duration_ms": duration_ms, "attempt": attempt, "trace_id": trace_id},
            ensure_ascii=False,
        )
        history_key = f"{self._history_prefix}{name}"
        count_key = f"task:run_count:{name}"

        async with self._client() as redis:
            pipe = redis.pipeline()
            pipe.set(key, str(ts))
            pipe.incr(count_key)
            pipe.lpush(history_key, record)
            pipe.ltrim(history_key, 0, self._history_max - 1)
            await pipe.execute()

    async def record_failure(
        self,
        name: str,
        error: str,
        attempt: int,
        ts: float,
        trace_id: str | None = None,
    ) -> None:
        key = f"{self._failure_prefix}{name}"
        failure = json.dumps(
            {"error": error, "attempt": attempt, "failed_at": ts, "trace_id": trace_id},
            ensure_ascii=False,
        )
        history_key = f"{self._history_prefix}{name}"

        async with self._client() as redis:
            pipe = redis.pipeline()
            pipe.set(key, failure)
            record = json.dumps(
                {"status": "failed", "ts": ts, "error": error, "attempt": attempt, "trace_id": trace_id},
                ensure_ascii=False,
            )
            pipe.lpush(history_key, record)
            pipe.ltrim(history_key, 0, self._history_max - 1)
            await pipe.execute()

    # ── 历史查询 ─────────────────────────────────────────────────────────────

    async def get_last_failure(self, name: str) -> FailureRecord | None:
        key = f"{self._failure_prefix}{name}"
        async with self._client() as redis:
            val = await redis.get(key)
            if not val:
                return None
            data = json.loads(val)
            return FailureRecord(
                task_name=name,
                error=data["error"],
                attempt=data["attempt"],
                failed_at=data["failed_at"],
            )

    async def list_history(self, name: str, limit: int = 100) -> list[RunRecord]:
        key = f"{self._history_prefix}{name}"
        async with self._client() as redis:
            items = await redis.lrange(key, 0, limit - 1)
            records = []
            for i, item in enumerate(items):
                data = json.loads(item)
                records.append(
                    RunRecord(
                        id=i,
                        name=name,
                        status=data["status"],
                        started_at=data["ts"],
                        duration_ms=data.get("duration_ms") or 0,
                        error=data.get("error"),
                        attempt=data.get("attempt", 1),
                    )
                )
            return records

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
        """提交 oneshot 任务：添加到 Redis Set。"""
        task_id = uuid.uuid4().hex
        key = "task:dynamic:pending"
        if biz_name is not None:
            existing = await self.get_oneshot_task_by_biz_name(name, biz_name)
            if existing is not None:
                return existing[0]

        value = json.dumps(
            {
                "id": task_id,
                "name": name,
                "biz_name": biz_name,
                "timeout": timeout,
                "max_runs": max_runs,
                "params": params,
            },
            ensure_ascii=False,
        )

        async with self._client() as redis:
            await redis.sadd(key, value)

        return task_id

    async def get_oneshot_task_by_biz_name(
        self,
        name: str,
        biz_name: str,
    ) -> tuple[str, str, int, dict[str, object] | None] | None:
        """按 (task_name, biz_name) 查询 pending 队列里的 oneshot 任务。

        Redis pending set 只装未完成的 oneshot 任务，本身就没有"终态行
        卡住唯一索引"的问题；MySQL store 的同名方法说明里强调的状态过滤
        约束在 Redis 这边天然成立。
        """
        key = "task:dynamic:pending"

        async with self._client() as redis:
            items = await redis.smembers(key)
            for item in items:
                data = json.loads(item)
                if data["name"] == name and data.get("biz_name") == biz_name:
                    return (data["id"], data["name"], data["timeout"], data.get("params"))
            return None

    async def list_pending_oneshot_tasks(self) -> list[tuple[str, str, int, dict[str, object] | None]]:
        """列出所有待执行的 oneshot 任务。"""
        key = "task:dynamic:pending"

        async with self._client() as redis:
            items = await redis.smembers(key)
            result = []
            for item in items:
                data = json.loads(item)
                result.append((data["id"], data["name"], data["timeout"], data.get("params")))
            return result

    async def count_running_oneshot_tasks(self) -> dict[str, int]:
        """统计 oneshot 行 running 数：扫 pending set，对每个 task_id 探锁存在性。

        Redis 没有"行"和"锁"在同表的 join 能力；这里 O(N) 扫 pending set 拉起 task_id，
        再用一次 pipeline `EXISTS` 批量探 lock key。N 通常远小于 cluster 级活跃 oneshot
        总数，且仅在 tick 起点调一次，开销可控。
        """
        pending_key = "task:dynamic:pending"

        async with self._client() as redis:
            items = await redis.smembers(pending_key)
            if not items:
                return {}

            entries = [json.loads(item) for item in items]
            pipe = redis.pipeline()
            for entry in entries:
                pipe.exists(f"{self._lock_prefix}{entry['id']}")
            results = await pipe.execute()

        counts: dict[str, int] = {}
        for entry, exists in zip(entries, results, strict=True):
            if not exists:
                continue
            counts[entry["name"]] = counts.get(entry["name"], 0) + 1
        return counts

    async def delete_oneshot_task(self, task_id: str) -> None:
        """删除 oneshot 任务。"""
        key = "task:dynamic:pending"

        async with self._client() as redis:
            items = await redis.smembers(key)
            for item in items:
                data = json.loads(item)
                if data["id"] == task_id:
                    await redis.srem(key, item)
                    break

    async def cleanup_old_successful_tasks(self, days: int = 10) -> int:
        """Redis 实现不支持基于时间的历史清理。

        Redis store 设计为轻量记录，不保留长期历史。
        动态任务执行完成后会立即从 pending set 中删除。

        Args:
            days: 保留天数（此参数在 Redis 实现中无效）

        Returns:
            始终返回 0
        """
        return 0

    def should_delete_successful_oneshot_task(self, retention_policy: TaskRetentionPolicy) -> bool:
        """Redis 的 task 行只表示待执行队列，成功后总是移除。"""
        del retention_policy
        return True

    def should_delete_failed_oneshot_task(self, retention_policy: TaskRetentionPolicy) -> bool:
        """Redis 的 task 行只表示待执行队列，失败后总是移除。"""
        del retention_policy
        return True
