from typing import Any

import pytest

from python_stdx.scheduler.store.redis import RedisTaskStore


class FakeRedisClient:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    async def set(self, key: str, value: str, *, nx: bool = False, ex: int | None = None) -> bool | None:
        del ex
        if nx and key in self.values:
            return None
        self.values[key] = value
        return True

    async def get(self, key: str) -> str | None:
        return self.values.get(key)

    async def eval(self, script: str, number_of_keys: int, key: str, owner: str) -> int:
        del script, number_of_keys
        if self.values.get(key) != owner:
            return 0
        del self.values[key]
        return 1

    def __getattr__(self, name: str) -> Any:
        raise AssertionError(f"unexpected Redis operation: {name}")


@pytest.mark.asyncio
async def test_redis_store_accepts_native_client_without_connection_wrapper() -> None:
    client = FakeRedisClient()
    store = RedisTaskStore(client, pod_id="worker-a")  # type: ignore[arg-type]

    assert await store.acquire_lock("cleanup", ttl=30) is True
    assert await store.acquire_lock("cleanup", ttl=30) is False
    assert await store.get_lock_owner("cleanup") == "worker-a"

    await store.release_lock("cleanup")
    assert await store.get_lock_owner("cleanup") is None
