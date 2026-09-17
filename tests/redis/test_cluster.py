import asyncio

import redis
from test_pools import connector


async def test_cluster_reuses_connections_and_follows_moved(redis_cluster):
    pool = connector(redis_cluster)
    admins = []
    slot = original_id = None
    try:
        client = await pool.connect()
        key = "backend-moved-empty"
        assert await client.xread({key: "0"}, block=1) == []
        long_client = pool._backend.client(True)
        source = long_client.get_node_from_key(key)
        observer = redis.asyncio.Redis(host=source.host, port=source.port, decode_responses=True)
        admins.append(observer)
        before = (await observer.info("stats"))["total_connections_received"]
        for _ in range(3):
            assert await client.xread({key: "0"}, block=1) == []
        assert (await observer.info("stats"))["total_connections_received"] == before
        slot = long_client.keyslot(key)
        original_id = await observer.execute_command("CLUSTER", "MYID")
        destination = next(node for node in long_client.get_primaries() if node.name != source.name)
        target = redis.asyncio.Redis(host=destination.host, port=destination.port, decode_responses=True)
        admins.append(target)
        new_id = await target.execute_command("CLUSTER", "MYID")
        for node in long_client.get_primaries():
            admin = redis.asyncio.Redis(host=node.host, port=node.port)
            admins.append(admin)
            await admin.execute_command("CLUSTER", "SETSLOT", slot, "NODE", new_id)
        assert await client.xread({key: "0"}, block=1) == []
        assert long_client.get_node_from_key(key).name == destination.name
        assert await client.xread({key: "0"}, block=1) == []
    finally:
        if slot is not None and original_id is not None:
            for admin in admins:
                await admin.execute_command("CLUSTER", "SETSLOT", slot, "NODE", original_id)
        await asyncio.gather(*(admin.aclose() for admin in admins))
        await pool.aclose()


async def test_cluster_ask_redirect_keeps_asking_and_command_on_same_socket(redis_cluster):
    pool = connector(redis_cluster, max_long_connections=2)
    admins = []
    slot = None
    try:
        client = await pool.connect()
        key = "backend-ask-empty"
        await client.xread({key: "0"}, block=1)
        long_client = pool._backend.client(True)
        source = long_client.get_node_from_key(key)
        destination = next(node for node in long_client.get_primaries() if node.name != source.name)
        # Leave two idle connections at the destination: ASKING must not be
        # returned to a FIFO pool before the redirected command borrows again.
        other = next(
            f"ask-warm-{i}"
            for i in range(100)
            if long_client.get_node_from_key(f"ask-warm-{i}").name == destination.name
        )
        await asyncio.gather(*(client.xread({other: "0"}, block=100) for _ in range(2)))
        old = redis.asyncio.Redis(host=source.host, port=source.port, decode_responses=True)
        new = redis.asyncio.Redis(host=destination.host, port=destination.port, decode_responses=True)
        admins.extend((old, new))
        old_id = await old.execute_command("CLUSTER", "MYID")
        new_id = await new.execute_command("CLUSTER", "MYID")
        slot = long_client.keyslot(key)
        await new.execute_command("CLUSTER", "SETSLOT", slot, "IMPORTING", old_id)
        await old.execute_command("CLUSTER", "SETSLOT", slot, "MIGRATING", new_id)
        assert await client.xread({key: "0"}, block=1) == []
        assert pool._lifetime.long_in_use == 0
    finally:
        if slot is not None:
            for admin in admins:
                await admin.execute_command("CLUSTER", "SETSLOT", slot, "STABLE")
        await asyncio.gather(*(admin.aclose() for admin in admins))
        await pool.aclose()
