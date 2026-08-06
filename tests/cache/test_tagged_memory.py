import asyncio

from python_stdx.cache.tagged.memory import MemoryTaggedCache


async def test_memory_tagged_cache_invalidates_union_of_tags():
    cache = MemoryTaggedCache[str]()
    await cache.set("one", "value-one", tags=["shared", "first"])
    await cache.set("two", "value-two", tags=["shared"])
    await cache.set("three", "value-three", tags=["other"])

    await cache.invalidate_tag("shared")

    assert await cache.get("one") is None
    assert await cache.get("two") is None
    assert await cache.get("three") == "value-three"


async def test_memory_tagged_cache_replaces_tag_bindings():
    cache = MemoryTaggedCache[str]()
    await cache.set("key", "old", tags=["old-tag"])
    await cache.set("key", "new", tags=["new-tag"])

    await cache.invalidate_tag("old-tag")

    assert await cache.get("key") == "new"


async def test_memory_tagged_cache_expires_values():
    cache = MemoryTaggedCache[str](ttl=0.01)
    await cache.set("key", "value", tags=["tag"])

    await asyncio.sleep(0.02)

    assert await cache.get("key") is None


async def test_memory_tagged_cache_batch_defaults_are_untagged():
    cache = MemoryTaggedCache[str]()
    await cache.set_many({"one": "1", "two": "2"})

    assert await cache.get_many(["one", "missing", "two"]) == {"one": "1", "two": "2"}
