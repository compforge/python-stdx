"""Concurrent loading tests execute the Redis Lua scripts, not a Python model of them."""

import asyncio
import json
from unittest.mock import patch

import pytest
from fakeredis.aioredis import FakeRedis
from redis.cluster import key_slot

from python_stdx.cache.loading.redis import RedisLoadingCache


@pytest.fixture
async def redis():
    async with FakeRedis(decode_responses=True) as client:
        yield client


def cache(client, **kwargs):
    return RedisLoadingCache(
        client,
        namespace="loading-test",
        load_timeout=1,
        lease_ttl=2,
        wait_timeout=10,
        poll_interval=0.001,
        **kwargs,
    )


async def lease_key(client):
    keys = await client.keys("loading-test:*:lease")
    assert len(keys) == 1
    return keys[0]


async def test_failure_marker_is_diagnostic_state_not_a_cached_error(redis):
    instance = cache(redis)
    error = ValueError("private request body")

    async def fail(key):
        raise error

    with pytest.raises(ValueError) as raised:
        await instance.get_or_load("key", fail)
    assert raised.value is error
    key = await lease_key(redis)
    raw = await redis.get(key)
    assert raw.startswith("__FAILED__:")
    assert json.loads(raw.removeprefix("__FAILED__:")) == {"code": "ValueError"}
    assert "private request body" not in raw
    assert 0 < await redis.pttl(key) <= 2000
    assert await instance.get("key") is None

    assert await instance.get_or_load("key", lambda _: asyncio.sleep(0, result="fixed")) == "fixed"
    assert await redis.get(key) is None
    assert await instance.get_or_load("key", fail) == "fixed"


async def test_application_can_serialize_its_public_error_details(redis):
    instance = cache(redis, error_dumps=lambda _: json.dumps({"code": "NOT_CONFIGURED", "message": "model missing"}))

    async def fail(key):
        raise ValueError("untrusted downstream details")

    with pytest.raises(ValueError):
        await instance.get_or_load("key", fail)
    raw = await redis.get(await lease_key(redis))
    assert json.loads(raw.removeprefix("__FAILED__:")) == {"code": "NOT_CONFIGURED", "message": "model missing"}


async def test_waiting_followers_atomically_elect_one_retry(redis):
    instance = cache(redis)
    started = asyncio.Event()
    release = asyncio.Event()
    retry_started = asyncio.Event()
    finish_retry = asyncio.Event()
    calls = 0

    async def loader(key):
        nonlocal calls
        calls += 1
        if calls == 1:
            started.set()
            await release.wait()
            raise ValueError("first attempt failed")
        retry_started.set()
        await finish_retry.wait()
        return "recovered"

    async with asyncio.timeout(2):
        leader = asyncio.create_task(instance.get_or_load("key", loader))
        await started.wait()
        followers = [asyncio.create_task(cache(redis).get_or_load("key", loader)) for _ in range(12)]
        await asyncio.sleep(0.02)
        assert calls == 1
        release.set()
        with pytest.raises(ValueError):
            await leader
        await retry_started.wait()
        await asyncio.sleep(0.02)
        assert calls == 2
        finish_retry.set()
        assert await asyncio.gather(*followers) == ["recovered"] * 12
    assert calls == 2


async def test_each_failing_caller_raises_its_own_error_once(redis):
    release = asyncio.Event()
    calls = 0

    async def fail(key):
        nonlocal calls
        calls += 1
        number = calls
        await release.wait()
        raise ValueError(f"attempt {number}")

    async with asyncio.timeout(2):
        requests = [asyncio.create_task(cache(redis).get_or_load("key", fail)) for _ in range(5)]
        await asyncio.sleep(0.02)
        release.set()
        results = await asyncio.gather(*requests, return_exceptions=True)
    assert all(isinstance(result, ValueError) for result in results)
    assert {str(result) for result in results} == {f"attempt {n}" for n in range(1, 6)}
    assert calls == 5
    assert await cache(redis).get_or_load("key", lambda _: asyncio.sleep(0, result="fixed")) == "fixed"


@pytest.mark.parametrize("outcome", ["failure", "success", "none", "cancel"])
@pytest.mark.parametrize("successor_done", [False, True])
async def test_expired_owner_never_changes_successor_state(redis, outcome, successor_done):
    first_started = asyncio.Event()
    first_finish = asyncio.Event()
    second_started = asyncio.Event()
    second_finish = asyncio.Event()

    async def first_loader(key):
        first_started.set()
        await first_finish.wait()
        if outcome == "failure":
            raise ValueError("old leader")
        return "old" if outcome == "success" else None

    async def second_loader(key):
        second_started.set()
        await second_finish.wait()
        return "new"

    async with asyncio.timeout(2):
        first = asyncio.create_task(cache(redis).get_or_load("key", first_loader))
        await first_started.wait()
        key = await lease_key(redis)
        await redis.delete(key)  # Simulate lease expiry while the loader is still running.
        second = asyncio.create_task(cache(redis).get_or_load("key", second_loader))
        await second_started.wait()
        if successor_done:
            second_finish.set()
            assert await second == "new"
        expected = await redis.get(key)
        if outcome == "cancel":
            first.cancel()
            with pytest.raises(asyncio.CancelledError):
                await first
        else:
            first_finish.set()
            if outcome == "failure":
                with pytest.raises(ValueError):
                    await first
            else:
                await first
        assert await redis.get(key) == expected
        second_finish.set()
        assert await second == "new"
    assert await cache(redis).get("key") == "new"


@pytest.mark.parametrize("outcome", ["cancel", "none", "timeout"])
async def test_follower_retries_after_leader_cannot_produce_a_value(redis, outcome):
    started = asyncio.Event()
    release = asyncio.Event()
    instance = RedisLoadingCache(
        redis, namespace="loading-test", load_timeout=0.1, lease_ttl=1, wait_timeout=10, poll_interval=0.001
    )

    async def loader(key):
        started.set()
        await release.wait()
        return None

    async with asyncio.timeout(2):
        first = asyncio.create_task(instance.get_or_load("key", loader))
        await started.wait()
        follower = asyncio.create_task(cache(redis).get_or_load("key", lambda _: asyncio.sleep(0, result="retry")))
        if outcome == "cancel":
            first.cancel()
            with pytest.raises(asyncio.CancelledError):
                await first
        elif outcome == "timeout":
            with pytest.raises(TimeoutError):
                await first
        else:
            release.set()
            assert await first is None
        assert await follower == "retry"


async def test_redis_cleanup_failure_preserves_original_loader_exception(redis):
    original_eval = redis.eval
    error = ValueError("original error")

    async def fail(key):
        raise error

    async def broken_update(script, numkeys, *args):
        if args[1] == "__FAILED__:":
            return await original_eval(script, numkeys, *args)
        raise ConnectionError("Redis is unavailable")

    with patch.object(redis, "eval", side_effect=broken_update):
        with pytest.raises(ValueError) as raised:
            await cache(redis).get_or_load("key", fail)
    assert raised.value is error


async def test_error_serializer_failure_preserves_original_exception(redis):
    error = RuntimeError("loader error")

    def bad_serializer(exc):
        raise ValueError("serializer error")

    async def fail(key):
        raise error

    with pytest.raises(RuntimeError) as raised:
        await cache(redis, error_dumps=bad_serializer).get_or_load("key", fail)
    assert raised.value is error


async def test_follower_wait_timeout_never_starts_duplicate_loader(redis):
    started = asyncio.Event()
    finish = asyncio.Event()
    calls = 0

    async def loader(key):
        nonlocal calls
        calls += 1
        started.set()
        await finish.wait()
        return "value"

    first = asyncio.create_task(cache(redis).get_or_load("key", loader))
    await started.wait()
    impatient = RedisLoadingCache(redis, namespace="loading-test", wait_timeout=0.02, poll_interval=0.001)
    try:
        with pytest.raises(TimeoutError, match="waiting"):
            await impatient.get_or_load("key", loader)
        assert calls == 1
    finally:
        finish.set()
        assert await first == "value"


@pytest.mark.parametrize("decode_responses", [False, True])
async def test_payloads_are_separate_from_state_and_keys_share_cluster_slot(decode_responses):
    async with FakeRedis(decode_responses=decode_responses) as client:
        instance = cache(client, value_dumps=str, value_loads=str)
        for value in ["__FAILED__:user data", "__IN_PROGRESS__:user data", ""]:
            await instance.set("key", value)
            assert await instance.get("key") == value
        value_keys = await client.keys("loading-test:*:value")
        assert len(value_keys) == 1
        value_key = value_keys[0]
        if isinstance(value_key, str):
            value_key = value_key.encode()
        lease = value_key.removesuffix(b":value") + b":lease"
        assert key_slot(value_key) == key_slot(lease)
