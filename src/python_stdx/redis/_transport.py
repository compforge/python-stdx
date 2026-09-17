"""redis-py adapters: response deadlines and connection-affine ASK redirects.

Topology discovery, connection pooling, RESP, and reconnects remain redis-py's.
Keep the few native extension points here so dependency upgrades are testable.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from math import inf
from typing import Any, cast

from redis.asyncio import RedisCluster
from redis.asyncio.cluster import ClusterNode
from redis.asyncio.connection import Connection, SSLConnection
from redis.asyncio.sentinel import SentinelManagedConnection, SentinelManagedSSLConnection
from redis.exceptions import ConnectionError as RedisConnectionError

from python_stdx.redis._lifetime import Lifetime

_redirecting: ContextVar[bool] = ContextVar("stdx_redis_redirecting", default=False)

_response_timeout: ContextVar[float | None] = ContextVar("stdx_redis_response_timeout", default=None)


@contextmanager
def response_timeout(seconds: float | None) -> Iterator[None]:
    token = _response_timeout.set(seconds)
    try:
        yield
    finally:
        _response_timeout.reset(token)


class ManagedConnection(Connection):
    def __init__(self, *, lifetime: Lifetime, **kwargs: Any) -> None:
        self.lifetime = lifetime
        super().__init__(**kwargs)

    async def connect(self) -> None:
        self.lifetime.check_open()
        # why: AUTH/HELLO and discovery must stay bounded even for BLOCK 0.
        with response_timeout(None):
            try:
                await cast(Callable[[], Awaitable[None]], super().connect)()
            except BaseException:
                await self.disconnect()
                raise

    async def _send_ping(self) -> None:
        with response_timeout(None):
            await cast(Callable[[], Awaitable[None]], super()._send_ping)()

    async def send_packed_command(self, *args: Any, **kwargs: Any) -> None:
        if self.lifetime.closed:
            # Native pipeline reset catches ConnectionError and relinquishes
            # WATCH without trying to open a new socket during shutdown.
            await self.disconnect()
            raise RedisConnectionError("Redis client is closed")
        await super().send_packed_command(*args, **kwargs)

    async def read_response(self, *args: Any, **kwargs: Any) -> Any:
        original = self.socket_timeout
        timeout = _response_timeout.get()
        # The socket has one borrower. Changing a shared client setting would
        # race other commands, whereas this state lasts for exactly one read.
        if timeout is not None:
            self.socket_timeout = None if timeout == inf else timeout
        try:
            return await super().read_response(*args, **kwargs)
        finally:
            self.socket_timeout = original


class ManagedSSLConnection(ManagedConnection, SSLConnection):
    pass


class ManagedSentinelConnection(ManagedConnection, SentinelManagedConnection):
    pass


class ManagedSentinelSSLConnection(ManagedConnection, SentinelManagedSSLConnection):
    pass


class _AskingNode:
    def __init__(self, node: ClusterNode) -> None:
        self.node = node
        self.asking = False

    def __getattr__(self, attribute: str) -> Any:
        # A native node proxy at the dependency boundary, including metric fields.
        return getattr(self.node, attribute)

    async def execute_command(self, *args: Any, **kwargs: Any) -> Any:
        if args == ("ASKING",):
            self.asking = True
            return None
        if not self.asking:
            return await self.node.execute_command(*args, **kwargs)
        self.asking = False
        connection = self.node.acquire_connection()
        try:
            # why: ASKING and the redirected command must use one lease (rueidis
            # sends them as a batch). Native redis-py otherwise borrows twice.
            await connection.send_command("ASKING")
            with response_timeout(None):
                await self.node.parse_response(connection, "ASKING")
            await connection.send_command(*args)
            return await self.node.parse_response(connection, args[0], **kwargs)
        except BaseException:
            await connection.disconnect()
            raise
        finally:
            self.node.release(connection)


class ManagedCluster(RedisCluster):
    def __init__(self, *, lifetime: Lifetime, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        connection_class = ManagedSSLConnection if kwargs.get("ssl") else ManagedConnection
        self.connection_kwargs.update(connection_class=connection_class, lifetime=lifetime)
        for node in self.nodes_manager.startup_nodes.values():
            node.connection_class = connection_class
            node.connection_kwargs["lifetime"] = lifetime

    async def _execute_command(self, target_node: ClusterNode, *args: Any, **kwargs: Any) -> Any:
        token = _redirecting.set(True)
        try:
            return await super()._execute_command(target_node, *args, **kwargs)
        finally:
            _redirecting.reset(token)

    def get_node(
        self, host: str | None = None, port: int | None = None, node_name: str | None = None
    ) -> ClusterNode | None:
        node = super().get_node(host=host, port=port, node_name=node_name)
        return (
            cast(ClusterNode, _AskingNode(node))
            if _redirecting.get() and node_name is not None and node is not None
            else node
        )
