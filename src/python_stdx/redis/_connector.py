"""Build and own one topology-neutral async Redis client."""

from __future__ import annotations

import asyncio
from typing import Any

from python_stdx.redis._backend import create_backend
from python_stdx.redis._client import RedisClient
from python_stdx.redis._config import RedisConnectionConfig
from python_stdx.redis._lifetime import Lifetime


class RedisConnector:
    """Own a shared Redis client and both of its automatically selected pools.

    ``connect()`` validates connectivity once. Construction opens no sockets;
    long-pool resources are created only by blocking calls or subscriptions.
    ``aclose()`` is idempotent and terminal, including for retained client handles.
    """

    def __init__(self, config: RedisConnectionConfig) -> None:
        self._lock = asyncio.Lock()
        assert config.max_long_connections is not None
        self._lifetime = Lifetime(config.max_long_connections)
        self._backend = create_backend(config, self._lifetime)
        self._client = RedisClient(self._backend, self._lifetime)
        self._connected = False

    async def connect(self) -> RedisClient:
        with self._lifetime.operation():
            async with self._lock:
                if not self._connected:
                    try:
                        await self._client.ping()
                    except BaseException:
                        # Keep a failed first attempt retryable without leaking discovery sockets.
                        await self._backend.aclose()
                        raise
                    self._connected = True
                return self._client

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> RedisClient:
        return await self.connect()

    async def __aexit__(self, *args: Any) -> None:
        await self.aclose()
