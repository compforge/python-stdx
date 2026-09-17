import asyncio

import pytest
import redis
from test_pools import connector

from python_stdx.redis import RedisTopology


async def test_block_zero_keeps_handshake_bounded_and_cancellation_closes_socket():
    writers = set()

    async def blackhole(reader, writer):
        writers.add(writer)
        try:
            await reader.read()
        finally:
            writer.close()
            await writer.wait_closed()
            writers.discard(writer)

    server = await asyncio.start_server(blackhole, "127.0.0.1", 0)
    owner = connector(
        (server.sockets[0].getsockname()[1], RedisTopology.STANDALONE), command_timeout=0.05, connect_timeout=0.05
    )
    try:
        # Test a newly opened long connection directly: the public connect()
        # probe is deliberately skipped because this peer never replies.
        client = owner._client
        with pytest.raises(redis.TimeoutError):
            await asyncio.wait_for(client.blpop("empty", timeout=0), 1)
        blocked = asyncio.create_task(client.blpop("empty", timeout=0))
        await asyncio.sleep(0.01)
        blocked.cancel()
        with pytest.raises(asyncio.CancelledError):
            await blocked
        pool = owner._backend.client(True).connection_pool
        assert owner._lifetime.long_in_use == 0
        assert not pool._in_use_connections
        assert all(not connection.is_connected for connection in pool._available_connections)
    finally:
        await owner.aclose()
        server.close()
        await server.wait_closed()
        for writer in list(writers):
            writer.close()


async def test_sentinel_long_pool_recovers_after_master_switch(redis_processes):
    master = redis_processes()
    replica = redis_processes(replica_of=master)
    sentinel = redis_processes(sentinel_master=master)
    pool = connector((sentinel, RedisTopology.SENTINEL))
    admin = redis.asyncio.Redis(host="127.0.0.1", port=sentinel, decode_responses=True)
    try:
        client = await pool.connect()
        assert await client.blpop("failover-empty", timeout=0.01) is None

        async def ready_and_failover():
            while not await admin.execute_command("SENTINEL", "REPLICAS", "mymaster"):
                await asyncio.sleep(0.1)
            while True:
                try:
                    await admin.execute_command("SENTINEL", "FAILOVER", "mymaster")
                    break
                except redis.ResponseError as exc:
                    if "NOGOODSLAVE" not in str(exc):
                        raise
                    await asyncio.sleep(0.1)
            while (await admin.execute_command("SENTINEL", "GET-MASTER-ADDR-BY-NAME", "mymaster"))[1] != str(replica):
                await asyncio.sleep(0.1)

        await asyncio.wait_for(ready_and_failover(), 20)
        old_master = redis.asyncio.Redis(host="127.0.0.1", port=master, decode_responses=True)
        try:

            async def wait_demoted():
                while (await old_master.role())[0] == "master":
                    await asyncio.sleep(0.1)

            await asyncio.wait_for(wait_demoted(), 20)
        finally:
            await old_master.aclose()
        # An already-connected old master can reject the first write-like command.
        # Native Sentinel invalidates that socket; the following call rediscovers.
        for _ in range(30):
            try:
                assert await client.blpop("failover-empty", timeout=0.01) is None
                if pool._backend.client(True).connection_pool.master_address[1] == replica:
                    break
            except redis.ConnectionError:
                pass
            await asyncio.sleep(0.1)
        assert pool._backend.client(True).connection_pool.master_address[1] == replica
        assert pool._lifetime.long_in_use == 0
    finally:
        await admin.aclose()
        await pool.aclose()
