"""Topology owns native resources; command policy does not branch on topology."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any, cast

from redis.asyncio import ConnectionPool, Redis, RedisCluster
from redis.asyncio.cluster import ClusterNode
from redis.asyncio.retry import Retry
from redis.asyncio.sentinel import Sentinel, SentinelConnectionPool
from redis.backoff import NoBackoff

from python_stdx.redis._config import RedisConnectionConfig, RedisTopology
from python_stdx.redis._lifetime import Lifetime
from python_stdx.redis._transport import (
    ManagedCluster,
    ManagedConnection,
    ManagedSentinelConnection,
    ManagedSentinelSSLConnection,
    ManagedSSLConnection,
    response_timeout,
)

NativeClient = Redis | RedisCluster


class Backend:
    def __init__(self, config: RedisConnectionConfig, lifetime: Lifetime) -> None:
        self.config = config
        self.lifetime = lifetime
        self._clients: dict[bool, NativeClient] = {}

    def options(self, long: bool) -> dict[str, Any]:
        c = self.config
        return dict(
            username=c.username,
            password=c.password,
            socket_connect_timeout=c.connect_timeout,
            socket_timeout=c.command_timeout,
            socket_keepalive=True,
            health_check_interval=c.health_check_interval,
            max_connections=c.max_long_connections if long else c.max_connections,
            decode_responses=c.decode_responses,
            encoding="utf-8",
            protocol=c.protocol,
            client_name=c.client_name,
            # Never replay potentially consumed writes/BLPOP after a lost response.
            retry=Retry(NoBackoff(), 0),
        )

    def client(self, long: bool = False) -> NativeClient:
        self.lifetime.check_open()
        if long not in self._clients:
            self._clients[long] = self.build(long)
        return self._clients[long]

    def build(self, long: bool) -> NativeClient:
        raise NotImplementedError

    async def ready(self, long: bool = False) -> NativeClient:
        client = self.client(long)
        # Cluster discovery is bounded independently of a pending BLOCK 0.
        with response_timeout(None):
            await client.initialize()
        return client

    async def protocol_pool(self, channel: str | bytes | None = None) -> ConnectionPool:
        raise NotImplementedError

    async def aclose(self) -> None:
        results = await asyncio.gather(*(client.aclose() for client in self._clients.values()), return_exceptions=True)
        for result in results:
            if isinstance(result, BaseException):
                raise result


class StandaloneBackend(Backend):
    def pool(self, long: bool) -> ConnectionPool:
        endpoint = self.config.endpoints[0]
        return ConnectionPool(
            host=endpoint.host,
            port=endpoint.port,
            db=self.config.database,
            connection_class=ManagedSSLConnection if self.config.tls else ManagedConnection,
            lifetime=self.lifetime,
            **self.options(long),
        )

    def build(self, long: bool) -> Redis:
        return Redis.from_pool(self.pool(long))

    async def protocol_pool(self, channel: str | bytes | None = None) -> ConnectionPool:
        return cast(Redis, self.client(True)).connection_pool


class SentinelBackend(StandaloneBackend):
    def __init__(self, config: RedisConnectionConfig, lifetime: Lifetime) -> None:
        super().__init__(config, lifetime)
        self.sentinel = Sentinel(  # type: ignore[no-untyped-call]
            [(endpoint.host, endpoint.port) for endpoint in config.endpoints],
            sentinel_kwargs=dict(
                username=config.sentinel_username if config.sentinel_username is not None else config.username,
                password=config.sentinel_password if config.sentinel_password is not None else config.password,
                socket_connect_timeout=config.connect_timeout,
                socket_timeout=config.command_timeout,
                max_connections=config.max_connections,
                ssl=config.tls,
            ),
        )

    def pool(self, long: bool) -> ConnectionPool:
        return cast(Callable[..., SentinelConnectionPool], SentinelConnectionPool)(
            self.config.sentinel_service,
            self.sentinel,
            db=self.config.database,
            connection_class=ManagedSentinelSSLConnection if self.config.tls else ManagedSentinelConnection,
            lifetime=self.lifetime,
            **self.options(long),
        )

    async def aclose(self) -> None:
        try:
            await super().aclose()
        finally:
            await asyncio.gather(*(client.aclose() for client in self.sentinel.sentinels))


class ClusterBackend(Backend):
    def __init__(self, config: RedisConnectionConfig, lifetime: Lifetime) -> None:
        super().__init__(config, lifetime)
        self._protocol_pools: dict[str, ConnectionPool] = {}

    def build(self, long: bool) -> RedisCluster:
        return ManagedCluster(
            startup_nodes=[ClusterNode(endpoint.host, endpoint.port) for endpoint in self.config.endpoints],
            ssl=self.config.tls,
            lifetime=self.lifetime,
            **self.options(long),
        )

    async def protocol_pool(self, channel: str | bytes | None = None) -> ConnectionPool:
        client = cast(RedisCluster, await self.ready())
        node = (
            client.nodes_manager.get_node_from_slot(client.keyslot(channel))
            if channel is not None
            else client.get_default_node()
        )
        assert node is not None
        self.lifetime.check_open()
        if node.name not in self._protocol_pools:
            self._protocol_pools[node.name] = ConnectionPool(
                host=node.host,
                port=node.port,
                connection_class=ManagedSSLConnection if self.config.tls else ManagedConnection,
                lifetime=self.lifetime,
                **self.options(True),
            )
        return self._protocol_pools[node.name]

    async def aclose(self) -> None:
        try:
            await super().aclose()
        finally:
            await asyncio.gather(*(pool.aclose() for pool in self._protocol_pools.values()))


def create_backend(config: RedisConnectionConfig, lifetime: Lifetime) -> Backend:
    return {
        RedisTopology.STANDALONE: StandaloneBackend,
        RedisTopology.SENTINEL: SentinelBackend,
        RedisTopology.CLUSTER: ClusterBackend,
    }[config.topology](config, lifetime)
