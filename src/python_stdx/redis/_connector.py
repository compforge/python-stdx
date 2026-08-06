"""Build and own a native async redis-py client for any supported topology."""

from __future__ import annotations

import asyncio
from typing import TypeAlias, cast

from redis.asyncio import Redis
from redis.asyncio.cluster import ClusterNode, RedisCluster
from redis.asyncio.sentinel import Sentinel

from python_stdx.redis._config import RedisConnectionConfig, RedisTopology

RedisClient: TypeAlias = Redis | RedisCluster


class RedisConnector:
    """Create one shared Redis client while hiding deployment-specific setup.

    ``connect()`` is idempotent and validates connectivity. The returned object
    is the native redis-py client, so callers retain the complete command API.
    The connector owns clients it creates and closes them with ``aclose()``.
    """

    def __init__(self, config: RedisConnectionConfig) -> None:
        self._config = config
        self._lock = asyncio.Lock()
        self._client: RedisClient | None = None
        self._sentinel: Sentinel | None = None

    async def connect(self) -> RedisClient:
        """Return the process-shared client, creating and validating it once."""
        async with self._lock:
            if self._client is not None:
                return self._client

            client = self._build_client()
            try:
                if self._config.topology is RedisTopology.CLUSTER:
                    await client.initialize()
                await client.ping()
            except BaseException:
                await client.aclose()
                await self._close_sentinel_clients()
                raise
            self._client = client
            return client

    async def aclose(self) -> None:
        """Close the owned data client and Sentinel discovery clients."""
        async with self._lock:
            client, self._client = self._client, None
            if client is not None:
                await client.aclose()
            await self._close_sentinel_clients()

    def _build_client(self) -> RedisClient:
        config = self._config
        if config.topology is RedisTopology.CLUSTER:
            nodes = [ClusterNode(endpoint.host, endpoint.port) for endpoint in config.endpoints]
            return RedisCluster(
                startup_nodes=nodes,
                username=config.username,
                password=config.password,
                ssl=config.tls,
                socket_connect_timeout=config.connect_timeout,
                socket_timeout=config.command_timeout,
                socket_keepalive=True,
                health_check_interval=config.health_check_interval,
                max_connections=config.max_connections,
                decode_responses=config.decode_responses,
                encoding="utf-8",
                protocol=config.protocol,
                client_name=config.client_name,
            )

        if config.topology is RedisTopology.SENTINEL:
            sentinel_credentials = {
                "username": config.sentinel_username if config.sentinel_username is not None else config.username,
                "password": config.sentinel_password if config.sentinel_password is not None else config.password,
                "socket_connect_timeout": config.connect_timeout,
                "socket_timeout": config.command_timeout,
                "socket_keepalive": True,
                "ssl": config.tls,
            }
            self._sentinel = Sentinel(  # type: ignore[no-untyped-call]
                [(endpoint.host, endpoint.port) for endpoint in config.endpoints],
                sentinel_kwargs=sentinel_credentials,
            )
            return cast(
                Redis,
                self._sentinel.master_for(
                    config.sentinel_service,
                    db=config.database,
                    ssl=config.tls,
                    username=config.username,
                    password=config.password,
                    socket_connect_timeout=config.connect_timeout,
                    socket_timeout=config.command_timeout,
                    socket_keepalive=True,
                    health_check_interval=config.health_check_interval,
                    max_connections=config.max_connections,
                    decode_responses=config.decode_responses,
                    encoding="utf-8",
                    protocol=config.protocol,
                    client_name=config.client_name,
                ),
            )

        endpoint = config.endpoints[0]
        return Redis(
            host=endpoint.host,
            port=endpoint.port,
            db=config.database,
            username=config.username,
            password=config.password,
            ssl=config.tls,
            socket_connect_timeout=config.connect_timeout,
            socket_timeout=config.command_timeout,
            socket_keepalive=True,
            health_check_interval=config.health_check_interval,
            max_connections=config.max_connections,
            decode_responses=config.decode_responses,
            encoding="utf-8",
            protocol=config.protocol,
            client_name=config.client_name,
        )

    async def _close_sentinel_clients(self) -> None:
        sentinel, self._sentinel = self._sentinel, None
        if sentinel is None:
            return
        await asyncio.gather(*(client.aclose() for client in sentinel.sentinels), return_exceptions=True)
