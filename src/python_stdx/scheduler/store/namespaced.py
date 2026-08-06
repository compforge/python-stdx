"""TaskStore wrapper that isolates task rows by task-name prefix."""

from collections.abc import Iterable

from python_stdx.scheduler.schedule import Schedule
from python_stdx.scheduler.store.base import TaskStore
from python_stdx.scheduler.types import FailureRecord, RunRecord, TaskRetentionPolicy


class NamespacedTaskStore(TaskStore):
    """Prefix task handler names at the storage boundary.

    The scheduler and task handlers keep using canonical registered names such as
    ``mindmap.generate.v2``. The shared store sees a scoped name such as
    ``local.mindmap.generate.v2``, so a local worker cannot consume or update
    another environment's task rows.
    """

    def __init__(self, store: TaskStore, prefix: str):
        self._store = store
        self._prefix = prefix.strip()

    @property
    def prefix(self) -> str:
        return self._prefix

    def _to_store_name(self, name: str) -> str:
        if not self._prefix:
            return name
        return f"{self._prefix}{name}"

    def _from_store_name(self, name: str) -> str | None:
        if not self._prefix:
            return name
        if not name.startswith(self._prefix):
            return None
        return name[len(self._prefix) :]

    def _filter_pending_rows(
        self,
        rows: Iterable[tuple[str, str, int, dict[str, object] | None]],
    ) -> list[tuple[str, str, int, dict[str, object] | None]]:
        scoped_rows: list[tuple[str, str, int, dict[str, object] | None]] = []
        for task_id, store_name, timeout, params in rows:
            name = self._from_store_name(store_name)
            if name is None:
                continue
            scoped_rows.append((task_id, name, timeout, params))
        return scoped_rows

    async def acquire_lock(self, name: str, ttl: int) -> bool:
        return await self._store.acquire_lock(self._to_store_name(name), ttl)

    async def release_lock(self, name: str) -> None:
        await self._store.release_lock(self._to_store_name(name))

    async def acquire_dynamic_task_lock(self, task_id: str, ttl: int) -> bool:
        return await self._store.acquire_dynamic_task_lock(task_id, ttl)

    async def release_dynamic_task_lock(self, task_id: str) -> None:
        await self._store.release_dynamic_task_lock(task_id)

    async def get_lock_owner(self, name: str) -> str | None:
        return await self._store.get_lock_owner(self._to_store_name(name))

    async def get_last_run(self, name: str) -> float | None:
        return await self._store.get_last_run(self._to_store_name(name))

    async def get_run_count(self, name: str) -> int:
        return await self._store.get_run_count(self._to_store_name(name))

    async def record_success(
        self,
        name: str,
        ts: float,
        duration_ms: int,
        attempt: int = 1,
        trace_id: str | None = None,
    ) -> None:
        await self._store.record_success(
            self._to_store_name(name),
            ts,
            duration_ms,
            attempt,
            trace_id=trace_id,
        )

    async def record_failure(
        self,
        name: str,
        error: str,
        attempt: int,
        ts: float,
        trace_id: str | None = None,
    ) -> None:
        await self._store.record_failure(
            self._to_store_name(name),
            error,
            attempt,
            ts,
            trace_id=trace_id,
        )

    async def record_dynamic_success(
        self,
        task_id: str,
        ts: float,
        duration_ms: int,
        attempt: int = 1,
        trace_id: str | None = None,
    ) -> None:
        await self._store.record_dynamic_success(
            task_id,
            ts,
            duration_ms,
            attempt,
            trace_id=trace_id,
        )

    async def record_dynamic_failure(
        self,
        task_id: str,
        error: str,
        attempt: int,
        ts: float,
        trace_id: str | None = None,
    ) -> None:
        await self._store.record_dynamic_failure(
            task_id,
            error,
            attempt,
            ts,
            trace_id=trace_id,
        )

    async def get_last_failure(self, name: str) -> FailureRecord | None:
        return await self._store.get_last_failure(self._to_store_name(name))

    async def list_history(self, name: str, limit: int = 100) -> list[RunRecord]:
        rows = await self._store.list_history(self._to_store_name(name), limit=limit)
        return [
            RunRecord(
                id=row.id,
                name=name,
                status=row.status,
                started_at=row.started_at,
                duration_ms=row.duration_ms,
                error=row.error,
                attempt=row.attempt,
            )
            for row in rows
        ]

    async def submit_oneshot_task(
        self,
        name: str,
        biz_name: str | None,
        schedule: Schedule | None,
        timeout: int,
        max_runs: int | None,
        params: dict[str, object] | None = None,
    ) -> str:
        return await self._store.submit_oneshot_task(
            self._to_store_name(name),
            biz_name,
            schedule,
            timeout,
            max_runs,
            params=params,
        )

    async def get_oneshot_task_by_biz_name(
        self,
        name: str,
        biz_name: str,
    ) -> tuple[str, str, int, dict[str, object] | None] | None:
        row = await self._store.get_oneshot_task_by_biz_name(self._to_store_name(name), biz_name)
        if row is None:
            return None
        task_id, store_name, timeout, params = row
        return (task_id, self._from_store_name(store_name) or store_name, timeout, params)

    async def list_pending_oneshot_tasks(self) -> list[tuple[str, str, int, dict[str, object] | None]]:
        return self._filter_pending_rows(await self._store.list_pending_oneshot_tasks())

    async def count_running_oneshot_tasks(self) -> dict[str, int]:
        counts = await self._store.count_running_oneshot_tasks()
        scoped_counts: dict[str, int] = {}
        for store_name, count in counts.items():
            name = self._from_store_name(store_name)
            if name is None:
                continue
            scoped_counts[name] = scoped_counts.get(name, 0) + count
        return scoped_counts

    async def delete_oneshot_task(self, task_id: str) -> None:
        await self._store.delete_oneshot_task(task_id)

    def should_delete_successful_oneshot_task(self, retention_policy: TaskRetentionPolicy) -> bool:
        return self._store.should_delete_successful_oneshot_task(retention_policy)

    def should_delete_failed_oneshot_task(self, retention_policy: TaskRetentionPolicy) -> bool:
        return self._store.should_delete_failed_oneshot_task(retention_policy)

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
        return await self._store.submit_triggered_task(
            self._to_store_name(name),
            biz_name,
            timeout,
            max_runs,
            params=params,
            cooldown_seconds=cooldown_seconds,
        )

    async def list_pending_triggered_tasks(self) -> list[tuple[str, str, int, dict[str, object] | None]]:
        return self._filter_pending_rows(await self._store.list_pending_triggered_tasks())

    async def cleanup_old_successful_tasks(self, days: int = 10) -> int:
        if self._prefix:
            return 0
        return await self._store.cleanup_old_successful_tasks(days)

    async def cleanup_expired_oneshot_tasks(self, retention_policy: TaskRetentionPolicy) -> int:
        return await self._store.cleanup_expired_oneshot_tasks_by_name_prefix(retention_policy, self._prefix)

    async def list_long_pending_tasks(
        self,
        min_age_seconds: int,
    ) -> list[tuple[str, str, str, int]]:
        rows = await self._store.list_long_pending_tasks(min_age_seconds)
        scoped_rows: list[tuple[str, str, str, int]] = []
        for task_id, store_name, task_type, age_seconds in rows:
            name = self._from_store_name(store_name)
            if name is None:
                continue
            scoped_rows.append((task_id, name, task_type, age_seconds))
        return scoped_rows
