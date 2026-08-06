from typing import cast

from python_stdx.cache.tagged.redis import RedisTaggedCache
from python_stdx.redis import RedisClient

from .fake_redis import FakeRedis


async def test_redis_tagged_cache_invalidates_by_generation():
    redis = FakeRedis()
    cache = RedisTaggedCache[str](cast(RedisClient, redis), namespace="test", ttl=7)
    await cache.set("one", "value-one", tags=["shared"])
    await cache.set("two", "value-two", tags=["other"])

    await cache.invalidate_tag("shared")

    assert await cache.get("one") is None
    assert await cache.get("two") == "value-two"
    assert {expiry.get("ex") for expiry in redis.expirations.values()} == {7}


async def test_redis_tagged_cache_clear_changes_namespace_generation():
    redis = FakeRedis()
    cache = RedisTaggedCache[dict[str, int]](cast(RedisClient, redis), namespace="test")
    await cache.set("key", {"value": 1})

    await cache.clear()

    assert await cache.get("key") is None


async def test_redis_tagged_cache_supports_explicit_codec():
    redis = FakeRedis()
    cache = RedisTaggedCache[str](
        cast(RedisClient, redis),
        namespace="test",
        value_dumps=lambda value: value,
        value_loads=lambda value: value,
    )

    await cache.set("key", "plain-text")

    assert await cache.get("key") == "plain-text"
