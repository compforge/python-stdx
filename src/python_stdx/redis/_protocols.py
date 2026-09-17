"""Lazy connection-bound protocols with the same lifecycle on every topology."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from math import inf
from typing import Any, cast

from redis.asyncio.client import Monitor, PubSub
from redis.exceptions import MaxConnectionsError

from python_stdx.redis._backend import Backend
from python_stdx.redis._lifetime import Lifetime, RedisPoolExhaustedError
from python_stdx.redis._transport import response_timeout


class RedisPubSub:
    """Native Pub/Sub semantics; close the subscription or use ``async with``.

    One long permit is held from the first subscription until ``aclose()``.
    Unsubscribing does not relinquish the reusable connection. The object can
    be reused after closing while its owning client remains open.
    """

    def __init__(self, backend: Backend, lifetime: Lifetime, *, ignore_subscribe_messages: bool = False) -> None:
        self._backend = backend
        self._lifetime = lifetime
        self._ignore_subscribe_messages = ignore_subscribe_messages
        self._native: PubSub | None = None
        self._lock = asyncio.Lock()

    async def _get(self, channel: str | bytes | None = None) -> PubSub:
        async with self._lock:
            self._lifetime.check_open()
            if self._native is None:
                self._lifetime.acquire()
                try:
                    pool = await self._backend.protocol_pool(channel)
                    self._native = PubSub(pool, ignore_subscribe_messages=self._ignore_subscribe_messages)
                    self._lifetime.resources.add(self)
                except BaseException:
                    self._lifetime.release()
                    raise
            return self._native

    async def __aenter__(self) -> RedisPubSub:
        self._lifetime.check_open()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.aclose()

    async def _command(self, command: str, channels: tuple[str | bytes, ...], handlers: dict[str, Any]) -> None:
        with self._lifetime.operation():
            try:
                channel = channels[0] if channels else next(iter(handlers), None)
                native = await self._get(channel)
                # Explicit dispatch keeps the wire protocol and handler semantics native.
                if command == "SUBSCRIBE":
                    await native.subscribe(*channels, **handlers)
                elif command == "PSUBSCRIBE":
                    await native.psubscribe(*channels, **handlers)
                elif command == "UNSUBSCRIBE":
                    await native.unsubscribe(*channels)
                else:
                    await native.punsubscribe(*channels)
            except BaseException as exc:
                await self.aclose()
                if isinstance(exc, MaxConnectionsError):
                    raise RedisPoolExhaustedError("Redis long pool exhausted") from exc
                raise

    async def subscribe(self, *channels: str | bytes, **handlers: Any) -> None:
        await self._command("SUBSCRIBE", channels, handlers)

    async def psubscribe(self, *patterns: str | bytes, **handlers: Any) -> None:
        await self._command("PSUBSCRIBE", patterns, handlers)

    async def unsubscribe(self, *channels: str | bytes) -> None:
        await self._command("UNSUBSCRIBE", channels, {})

    async def punsubscribe(self, *patterns: str | bytes) -> None:
        await self._command("PUNSUBSCRIBE", patterns, {})

    async def get_message(self, ignore_subscribe_messages: bool = False, timeout: float | None = 0.0) -> Any:
        with self._lifetime.operation():
            native = await self._get()
            try:
                return await native.get_message(ignore_subscribe_messages=ignore_subscribe_messages, timeout=timeout)
            except BaseException:
                await self.aclose()
                raise

    async def listen(self) -> AsyncIterator[Any]:
        while self._native is not None and self._native.subscribed:
            message = await self.get_message(timeout=None)
            if message is not None:
                yield message

    async def ping(self, message: str | bytes | None = None) -> None:
        with self._lifetime.operation():
            native = await self._get()
            await native.ping(message)

    async def aclose(self) -> None:
        async with self._lock:
            native, self._native = self._native, None
            try:
                if native is not None:
                    await cast(Callable[[], Awaitable[None]], native.aclose)()
            finally:
                if native is not None:
                    self._lifetime.release()
                self._lifetime.resources.discard(self)


class RedisMonitor:
    """Monitor one server selected by the backend, owning one long permit."""

    def __init__(self, backend: Backend, lifetime: Lifetime) -> None:
        self._backend = backend
        self._lifetime = lifetime
        self._native: Monitor | None = None

    async def __aenter__(self) -> RedisMonitor:
        with self._lifetime.operation():
            if self._native is not None:
                return self
            self._lifetime.acquire()
            try:
                pool = await self._backend.protocol_pool()
                self._native = Monitor(pool)
                self._lifetime.resources.add(self)
                await cast(Callable[[], Awaitable[Monitor]], self._native.__aenter__)()
            except BaseException:
                if self._native is not None:
                    await self.aclose()
                else:
                    self._lifetime.release()
                raise
            return self

    async def __aexit__(self, *args: Any) -> None:
        await self.aclose()

    async def next_command(self) -> Any:
        with self._lifetime.operation(), response_timeout(inf):
            if self._native is None:
                raise RuntimeError("Enter monitor with async with before reading")
            return await self._native.next_command()

    async def listen(self) -> AsyncIterator[Any]:
        while True:
            yield await self.next_command()

    async def aclose(self) -> None:
        native, self._native = self._native, None
        try:
            if native is not None and native.connection is not None:
                try:
                    await native.connection.disconnect()
                finally:
                    await native.connection_pool.release(native.connection)
        finally:
            if native is not None:
                self._lifetime.release()
            self._lifetime.resources.discard(self)
