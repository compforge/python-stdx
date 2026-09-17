"""The same public API must satisfy the same contract for all topologies."""

import asyncio
from contextlib import suppress
from dataclasses import replace

import pytest
import redis
from redis.exceptions import DataError, RedisError

from python_stdx.redis import RedisConnectionConfig, RedisConnector, RedisEndpoint, RedisPoolExhaustedError
from python_stdx.redis._commands import blocking_timeout


def connector(target, **updates):
    port, topology = target
    config = RedisConnectionConfig(
        topology=topology,
        endpoints=(RedisEndpoint("127.0.0.1", port),),
        max_connections=1,
        command_timeout=0.1,
    )
    return RedisConnector(replace(config, **updates))


async def wait_for(predicate):
    async with asyncio.timeout(5):
        while not predicate():
            await asyncio.sleep(0.01)


async def message(subscription):
    async with asyncio.timeout(3):
        while True:
            result = await subscription.get_message(ignore_subscribe_messages=True, timeout=0.1)
            if result is not None:
                return result


async def test_lazy_pools_and_shared_client(target):
    owner = connector(target)
    assert owner._backend._clients == {}
    async with owner as client:
        assert await owner.connect() is client
        await client.set("plain", "ok")
        assert await client.get("plain") == "ok"
        unused = client.pubsub()
        assert set(owner._backend._clients) == {False}
        assert owner._lifetime.long_in_use == 0
        await unused.aclose()
    with pytest.raises(RedisError, match="closed"):
        await owner.connect()


async def test_long_exhaustion_keeps_short_commands_available(target):
    owner = connector(target)
    async with owner as client:
        task = asyncio.create_task(client.xread({"empty": "0"}, block=0))
        try:
            await wait_for(lambda: owner._lifetime.long_in_use == 1)
            await asyncio.sleep(0.15)  # Exceed ordinary command timeout.
            assert not task.done()
            with pytest.raises(RedisPoolExhaustedError):
                await client.blpop("empty-list", timeout=0.01)
            await client.set("mixed", "value")
            assert await client.get("mixed") == "value"
            assert await client.expire("mixed", 30)
            assert await client.xread({"empty": "0"}) == []
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        assert owner._lifetime.long_in_use == 0
        assert await client.blpop("empty-list", timeout=0.01) is None


async def test_pubsub_uses_long_capacity_and_ordinary_publish(target):
    owner = connector(target)
    async with owner as client:
        async with client.pubsub() as subscription:
            await subscription.subscribe("topic")
            assert owner._lifetime.long_in_use == 1
            with pytest.raises(RedisPoolExhaustedError):
                await client.xread({"empty": "0"}, block=1)
            await client.publish("topic", "hello")
            assert (await message(subscription))["data"] == "hello"
        assert owner._lifetime.long_in_use == 0
        assert await client.blpop("empty-list", timeout=0.01) is None


async def test_subscription_cancel_releases_capacity(target):
    owner = connector(target)
    async with owner as client:
        subscription = client.pubsub()
        await subscription.subscribe("topic-cancel")
        await subscription.get_message(timeout=1)  # consume subscription ACK
        task = asyncio.create_task(subscription.get_message(timeout=None))
        await asyncio.sleep(0.15)
        assert not task.done()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert owner._lifetime.long_in_use == 0
        await subscription.subscribe("topic-cancel")
        await subscription.aclose()


async def test_transaction_stays_short_and_blocking_batch_routes_as_unit(target):
    owner = connector(target)
    async with owner as client:
        async with client.pipeline() as pipe:
            pipe.set("{batch}:value", "ok")
            pipe.blpop("{batch}:empty", timeout=0)
            pipe.get("{batch}:value")
            assert await pipe.execute() == [True, None, "ok"]
        assert True not in owner._backend._clients
        async with client.pipeline(transaction=False) as pipe:
            pipe.set("{batch}:value", "next")
            pipe.blpop("{batch}:empty", timeout=0.01)
            pipe.get("{batch}:value")
            assert await pipe.execute() == [True, None, "next"]
        assert True in owner._backend._clients
        assert owner._lifetime.long_in_use == 0


async def test_watch_transaction_retains_connection(target):
    owner = connector(target, max_connections=2)
    async with owner as client:
        await client.set("{watch}:key", "old")
        async with client.pipeline() as pipe:
            await pipe.watch("{watch}:key")
            assert await pipe.get("{watch}:key") == "old"
            with pytest.raises(DataError, match="WATCH"):
                pipe.blpop("{watch}:empty", timeout=0)
            pipe.multi()
            pipe.set("{watch}:key", "new")
            assert await pipe.execute() == [True]
        assert await client.get("{watch}:key") == "new"


async def test_watch_conflict_raises_native_watch_error(target):
    owner = connector(target, max_connections=2)
    async with owner as client:
        async with client.pipeline() as pipe:
            await pipe.watch("{watch}:conflict")
            await client.set("{watch}:conflict", "external")
            pipe.multi()
            pipe.set("{watch}:conflict", "mine")
            with pytest.raises(redis.WatchError):
                await pipe.execute()
        assert await client.get("{watch}:conflict") == "external"


async def test_close_cancels_blocking_io_and_rejects_retained_handles(target):
    owner = connector(target, max_long_connections=2)
    client = await owner.connect()
    subscription = client.pubsub()
    await subscription.subscribe("close-topic")
    pipe = client.pipeline()
    task = asyncio.create_task(client.blpop("close-empty", timeout=0))
    await wait_for(lambda: owner._lifetime.long_in_use == 2)
    await asyncio.wait_for(owner.aclose(), 3)
    with pytest.raises(asyncio.CancelledError):
        await task
    assert owner._lifetime.long_in_use == 0
    for operation in (client.ping(), subscription.subscribe("close-topic"), pipe.execute()):
        with pytest.raises(RedisError, match="closed"):
            await operation
    await owner.aclose()


async def test_monitor_is_long_and_cleans_up(target):
    owner = connector(target)
    async with owner as client:
        async with client.monitor() as monitor:
            assert owner._lifetime.long_in_use == 1
            task = asyncio.create_task(monitor.next_command())
            await client.ping()  # Cluster ping broadcasts, reaching the monitored node too.
            assert "PING" in (await asyncio.wait_for(task, 2))["command"]
        assert owner._lifetime.long_in_use == 0


@pytest.mark.parametrize(
    "args",
    [
        ("BLPOP", "bad", "oops"),
        ("BLMPOP",),
        ("XREAD", "BLOCK", "oops", "STREAMS", "bad", "0"),
        ("XREAD", "BLOCK"),
        ("BLPOP", "bad", None),
        ("BLPOP", "bad", float("nan")),
    ],
)
async def test_invalid_timeout_keeps_native_error(target, args):
    owner = connector(target)
    async with owner as client:
        native = await owner._backend.ready()
        with pytest.raises(Exception) as expected:
            await native.execute_command(*args)
        async with client.pubsub() as subscription:
            await subscription.subscribe("invalid-timeout")
            with pytest.raises(type(expected.value)) as actual:
                await client.execute_command(*args)
            assert str(actual.value) == str(expected.value)


@pytest.mark.parametrize(
    "args,expected",
    [
        (("XREAD", "STREAMS", "BLOCK", "0"), None),
        (("XREADGROUP", "GROUP", "BLOCK", "BLOCK", "STREAMS", "BLOCK", ">"), None),
        ((b"XREAD BLOCK", b"1", b"STREAMS", b"s", b"0"), 0.001),
        (("BLPOP", "key", b"2"), 2),
        (("BLPOP", "key", 10**400), None),
        (("GET", b"\xff"), None),
    ],
)
def test_timeout_parser_only_reads_options(args, expected):
    assert blocking_timeout(args) == expected


async def test_close_reclaims_idle_watch_connection(target):
    owner = connector(target)
    client = await owner.connect()
    pipe = client.pipeline()
    await pipe.watch("{watch}:closing")
    with pytest.raises(RedisPoolExhaustedError):
        await client.get("{watch}:closing")
    await owner.aclose()
    await pipe.reset()
    assert not owner._lifetime.resources


async def test_unwatch_releases_connection_before_blocking_batch(target):
    owner = connector(target)
    async with owner as client:
        async with client.pipeline(transaction=False) as pipe:
            await pipe.watch("{watch}:unwatch")
            await pipe.unwatch()
            pipe.blpop("{watch}:empty", timeout=0.01)
            assert await pipe.execute() == [None]
        assert True in owner._backend._clients


async def test_scan_iter_hides_topology(target):
    owner = connector(target)
    async with owner as client:
        for i in range(35):
            await client.set(f"scan-contract:{i}", "ok")
        async with asyncio.timeout(3):
            keys = {key async for key in client.scan_iter(match="scan-contract:*", count=1)}
        assert keys == {f"scan-contract:{i}" for i in range(35)}


async def test_registered_script_works_in_client_and_pipeline(target):
    owner = connector(target)
    async with owner as client:
        script = client.register_script("return ARGV[1]")
        assert await script(keys=["{script}:key"], args=["ok"]) == "ok"
        async with client.pipeline() as pipe:
            await script(keys=["{script}:key"], args=["batched"], client=pipe)
            assert await pipe.execute() == ["batched"]


async def test_close_cleanup_survives_cancelled_waiter(target, monkeypatch):
    owner = connector(target)
    await owner.connect()
    started, proceed = asyncio.Event(), asyncio.Event()
    original = owner._backend.aclose

    async def slow_close():
        started.set()
        await proceed.wait()
        await original()

    monkeypatch.setattr(owner._backend, "aclose", slow_close)
    closing = asyncio.create_task(owner.aclose())
    await started.wait()
    closing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await closing
    proceed.set()
    await owner.aclose()
    assert owner._lifetime.closed


async def test_cancelled_response_is_not_reused_as_next_command_response(target):
    owner = connector(target)
    async with owner as client:
        await client.delete("cancel-reply")
        blocked = asyncio.create_task(client.blpop("cancel-reply", timeout=0))
        await wait_for(lambda: owner._lifetime.long_in_use == 1)
        await asyncio.sleep(0.1)
        blocked.cancel()
        with pytest.raises(asyncio.CancelledError):
            await blocked
        await client.rpush("cancel-reply", "fresh")
        assert await client.blpop("cancel-reply", timeout=1) == ("cancel-reply", "fresh")


async def test_lost_response_never_replays_destructive_command(target, monkeypatch):
    from python_stdx.redis._transport import ManagedConnection

    owner = connector(target)
    original = ManagedConnection.read_response

    async def lose_reply(connection, *args, **kwargs):
        result = await original(connection, *args, **kwargs)
        if result == ["lost-reply", "first"]:
            await connection.disconnect()
            raise redis.ConnectionError("response lost after consuming first item")
        return result

    async with owner as client:
        await client.delete("lost-reply")
        await client.rpush("lost-reply", "first", "second")
        monkeypatch.setattr(ManagedConnection, "read_response", lose_reply)
        with pytest.raises(redis.ConnectionError, match="response lost"):
            await client.blpop("lost-reply", timeout=1)
        assert await client.lrange("lost-reply", 0, -1) == ["second"]
        assert owner._lifetime.long_in_use == 0
