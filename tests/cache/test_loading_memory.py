import asyncio

import pytest

from python_stdx.cache.loading.memory import MemoryLoadingCache


async def test_same_key_shares_load_while_other_keys_progress():
    cache = MemoryLoadingCache[str, str]()
    started = asyncio.Event()
    finish = asyncio.Event()
    calls = []

    async def load(key):
        calls.append(key)
        if key == "first":
            started.set()
            await finish.wait()
        return key

    owner = asyncio.create_task(cache.get_or_load("first", load))
    await started.wait()
    waiter = asyncio.create_task(cache.get_or_load("first", load))
    await asyncio.sleep(0)
    assert await cache.get("first") is None
    assert await cache.get_or_load("other", load) == "other"
    finish.set()
    assert await asyncio.gather(owner, waiter) == ["first", "first"]
    assert await cache.get_or_load("first", load) == "first"
    assert calls == ["first", "other"]


@pytest.mark.parametrize("outcome", ["failure", "cancel", "miss"])
async def test_waiter_can_load_after_owner_finishes_without_a_value(outcome):
    cache = MemoryLoadingCache[str, str]()
    started = asyncio.Event()
    finish = asyncio.Event()

    async def load(key):
        started.set()
        await finish.wait()
        if outcome == "failure":
            raise ValueError("load failed")
        return None

    owner = asyncio.create_task(cache.get_or_load("key", load))
    await started.wait()
    calls = 0

    async def retry(key):
        nonlocal calls
        calls += 1
        await asyncio.sleep(0)
        return "recovered"

    waiters = [asyncio.create_task(cache.get_or_load("key", retry)) for _ in range(3)]
    await asyncio.sleep(0)
    if outcome == "cancel":
        owner.cancel()
    finish.set()
    if outcome == "cancel":
        with pytest.raises(asyncio.CancelledError):
            await owner
    elif outcome == "failure":
        with pytest.raises(ValueError, match="load failed"):
            await owner
    else:
        assert await owner is None
    assert await asyncio.gather(*waiters) == ["recovered"] * 3
    assert calls == 1
    assert await cache.get("key") == "recovered"


async def test_cancelled_waiter_and_explicit_write_during_load():
    cache = MemoryLoadingCache[str, str]()
    started = asyncio.Event()
    finish = asyncio.Event()

    async def load(key):
        started.set()
        await finish.wait()
        return "loaded"

    owner = asyncio.create_task(cache.get_or_load("key", load))
    await started.wait()
    cancelled = asyncio.create_task(cache.get_or_load("key", load))
    waiter = asyncio.create_task(cache.get_or_load("key", load))
    await asyncio.sleep(0)
    cancelled.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled
    await cache.set("key", "explicit")
    finish.set()
    assert await asyncio.gather(owner, waiter) == ["loaded", "loaded"]
    assert await cache.get("key") == "explicit"


async def test_capacity_and_falsey_value():
    cache = MemoryLoadingCache[str, int](max_size=1)
    await cache.set("a", 0)
    assert await cache.get_or_load("a", lambda _: pytest.fail("unexpected load")) == 0
    await cache.set("b", 1)
    assert await cache.get("a") is None
    with pytest.raises(ValueError, match="None"):
        await cache.set("b", None)
