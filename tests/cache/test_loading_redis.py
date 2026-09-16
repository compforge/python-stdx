import asyncio
from typing import cast

import pytest
from fakeredis.aioredis import FakeRedis

from python_stdx.cache.loading.redis import RedisLoadingCache
from python_stdx.redis import RedisClient


def make_cache(redis: FakeRedis) -> RedisLoadingCache[str, str]:
    return RedisLoadingCache(
        cast(RedisClient, redis),
        namespace="test",
        ttl=7,
        load_timeout=1.0,
        lease_ttl=2.0,
        wait_timeout=2.0,
        poll_interval=0.001,
        value_dumps=lambda value: value,
        value_loads=lambda value: value,
    )


async def test_redis_loading_cache_coalesces_same_key_loads():
    redis = FakeRedis()
    cache = make_cache(redis)
    calls = 0

    async def loader(key: str) -> str:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.02)
        return f"value:{key}"

    results = await asyncio.gather(*(cache.get_or_load("same", loader) for _ in range(5)))

    assert results == ["value:same"] * 5
    assert calls == 1


async def test_redis_loading_cache_releases_lease_after_loader_failure():
    redis = FakeRedis()
    cache = make_cache(redis)

    async def failing_loader(key: str) -> str:
        raise RuntimeError(key)

    with pytest.raises(RuntimeError, match="key"):
        await cache.get_or_load("key", failing_loader)

    assert await cache.get_or_load("key", lambda key: asyncio.sleep(0, result=f"value:{key}")) == "value:key"


async def test_redis_loading_cache_does_not_cache_none():
    redis = FakeRedis()
    cache = make_cache(redis)

    async def loader(key: str) -> None:
        return None

    assert await cache.get_or_load("key", loader) is None
    assert await cache.get("key") is None


def test_redis_loading_cache_requires_lease_longer_than_load_timeout():
    with pytest.raises(ValueError, match="lease_ttl"):
        RedisLoadingCache(cast(RedisClient, FakeRedis()), namespace="test", load_timeout=2, lease_ttl=2)
