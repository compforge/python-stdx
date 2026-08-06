from pathlib import Path

import pytest

from python_stdx.database import Database
from python_stdx.scheduler.store.sqlalchemy import SQLTaskStore


def _database(tmp_path: Path) -> Database:
    return Database(
        f"sqlite:///{tmp_path / 'tasks.db'}",
        pool_size=2,
        max_overflow=1,
        pool_timeout=1,
        connect_args={"check_same_thread": False},
    )


@pytest.mark.asyncio
async def test_sql_store_runs_scheduled_lifecycle(tmp_path: Path) -> None:
    database = _database(tmp_path)
    store = SQLTaskStore(database, pod_id="worker-a", auto_migrate=True)
    try:
        await store.init()
        assert await store.acquire_lock("cleanup", ttl=60) is True
        assert await store.acquire_lock("cleanup", ttl=60) is False
        assert await store.get_lock_owner("cleanup") == "worker-a"

        await store.record_success("cleanup", ts=1_700_000_000.0, duration_ms=25)
        await store.release_lock("cleanup")

        assert await store.get_last_run("cleanup") == 1_700_000_000.0
        assert await store.get_run_count("cleanup") == 1
    finally:
        database.close()


@pytest.mark.asyncio
async def test_sql_store_runs_oneshot_lifecycle(tmp_path: Path) -> None:
    database = _database(tmp_path)
    store = SQLTaskStore(database, pod_id="worker-a", auto_migrate=True)
    try:
        await store.init()
        task_id = await store.submit_oneshot_task(
            "report.generate",
            "report:1",
            schedule=None,
            timeout=30,
            max_runs=1,
            params={"report_id": "1"},
        )

        assert await store.list_pending_oneshot_tasks() == [(task_id, "report.generate", 30, {"report_id": "1"})]
        assert await store.acquire_dynamic_task_lock(task_id, ttl=60) is True
        assert await store.count_running_oneshot_tasks() == {"report.generate": 1}

        await store.record_dynamic_success(task_id, ts=1_700_000_000.0, duration_ms=25)
        await store.release_dynamic_task_lock(task_id)

        assert await store.list_pending_oneshot_tasks() == []
        assert (await store.get_oneshot_task_by_biz_name("report.generate", "report:1"))[0] == task_id
    finally:
        database.close()


@pytest.mark.asyncio
async def test_sql_store_coalesces_triggered_submissions(tmp_path: Path) -> None:
    database = _database(tmp_path)
    store = SQLTaskStore(database, pod_id="worker-a", auto_migrate=True)
    try:
        await store.init()
        first_id = await store.submit_triggered_task(
            "resource.refresh",
            "resource:1",
            timeout=30,
            max_runs=3,
            params={"version": 1},
        )
        second_id = await store.submit_triggered_task(
            "resource.refresh",
            "resource:1",
            timeout=30,
            max_runs=3,
            params={"version": 2},
        )

        assert second_id == first_id
        assert await store.list_pending_triggered_tasks() == [(first_id, "resource.refresh", 30, {"version": 2})]
    finally:
        database.close()
