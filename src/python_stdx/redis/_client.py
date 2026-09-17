"""One command API; connection roles and topology remain implementation details."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Iterator, Sequence
from contextlib import contextmanager
from math import inf
from typing import Any, Literal, cast

from redis.asyncio import Redis
from redis.asyncio.client import Pipeline
from redis.asyncio.cluster import ClusterPipeline
from redis.commands.core import AsyncCoreCommands, AsyncScript
from redis.commands.redismodules import AsyncRedisModuleCommands
from redis.event import EventDispatcher
from redis.exceptions import DataError, MaxConnectionsError
from redis.typing import EncodableT, KeyT, PatternT, ScriptTextT

from python_stdx.redis._backend import Backend
from python_stdx.redis._commands import blocking_timeout, name, tokens
from python_stdx.redis._lifetime import Lifetime, RedisPoolExhaustedError
from python_stdx.redis._protocols import RedisMonitor, RedisPubSub
from python_stdx.redis._transport import response_timeout


class Commands(AsyncCoreCommands, AsyncRedisModuleCommands):
    _is_async_client: Literal[True] = True
    _event_dispatcher = EventDispatcher()

    def command_info(self, **kwargs: Any) -> Any:
        # redis-py's async mixin and module mixin declare conflicting signatures.
        # Resolve their shared wire operation here without duplicating commands.
        return self.execute_command("COMMAND INFO", **kwargs)


class RedisClient(Commands):
    """Shared async Redis command API with automatic capacity isolation.

    Use the same commands for standalone, Sentinel, and Cluster. Blocking calls
    and subscriptions borrow the long pool automatically. Multi-key operations
    still obey Redis Cluster's same-slot requirements. Close through ``aclose``
    or the owning connector; closing either is terminal.
    """

    def __init__(self, backend: Backend, lifetime: Lifetime) -> None:
        self._backend = backend
        self._lifetime = lifetime

    @contextmanager
    def _execution(self, timeouts: list[float]) -> Iterator[None]:
        with self._lifetime.operation():
            if timeouts:
                self._lifetime.acquire()
            # Each finite wait gets transport slack; zero blocks until cancelled.
            timeout = inf if 0 in timeouts else max(sum(timeouts) + 1, self._backend.config.command_timeout)
            try:
                with response_timeout(timeout if timeouts else None):
                    yield
            except MaxConnectionsError as exc:
                role = "long" if timeouts else "short"
                raise RedisPoolExhaustedError(f"Redis {role} pool exhausted") from exc
            finally:
                if timeouts:
                    self._lifetime.release()

    async def execute_command(self, *args: Any, **kwargs: Any) -> Any:
        parts = tokens(args)
        command = name(parts[0]) if parts else ""
        if command in {"SUBSCRIBE", "PSUBSCRIBE", "SSUBSCRIBE", "MONITOR"}:
            raise DataError("Use pubsub() or monitor() for connection-bound protocols")
        if command in {"WAIT", "WAITAOF"}:
            raise DataError(
                "Replication acknowledgements need an explicitly dedicated write connection; "
                "not supported by this shared client"
            )
        timeout = blocking_timeout(args)
        with self._execution([] if timeout is None else [timeout]):
            native = await self._backend.ready(timeout is not None)
            return await native.execute_command(*args, **kwargs)

    def scan_iter(
        self, match: PatternT | None = None, count: int | None = None, _type: str | None = None, **kwargs: Any
    ) -> Any:
        return self._scan_iter(match, count, _type, **kwargs)

    async def _scan_iter(
        self, match: PatternT | None = None, count: int | None = None, _type: str | None = None, **kwargs: Any
    ) -> AsyncIterator[Any]:
        with self._execution([]):
            native = await self._backend.ready()
            iterator = native.scan_iter(match=match, count=count, _type=_type, **kwargs)
        while True:
            with self._execution([]):
                try:
                    item = await anext(iterator)
                except StopAsyncIteration:
                    return
            yield item

    def register_script(self, script: ScriptTextT) -> Any:
        self._lifetime.check_open()
        return RedisScript(self, script)

    def pipeline(self, transaction: bool = True) -> RedisPipeline:
        """Batch commands; transactions and WATCH retain native connection affinity.

        A nontransactional batch containing a blocking command uses the long
        pool as a unit. Cluster transactions require all keys in one slot.
        """
        self._lifetime.check_open()
        return RedisPipeline(self, transaction)

    def pubsub(self, *, ignore_subscribe_messages: bool = False) -> RedisPubSub:
        self._lifetime.check_open()
        return RedisPubSub(self._backend, self._lifetime, ignore_subscribe_messages=ignore_subscribe_messages)

    def monitor(self) -> RedisMonitor:
        self._lifetime.check_open()
        return RedisMonitor(self._backend, self._lifetime)

    async def aclose(self) -> None:
        await self._lifetime.close(self._backend.aclose)


class RedisPipeline(Commands):
    """Reusable command batch with automatic role selection at execution time."""

    def __init__(self, client: RedisClient, transaction: bool) -> None:
        self._client = client
        self._transaction = transaction
        self._commands: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
        self._native: Pipeline | ClusterPipeline | None = None
        self._watching = False
        self._explicit_transaction = False

    async def __aenter__(self) -> RedisPipeline:
        self._client._lifetime.check_open()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.reset()

    def execute_command(self, *args: Any, **kwargs: Any) -> Any:
        self._client._lifetime.check_open()
        parts = tokens(args)
        command = name(parts[0]) if parts else ""
        if command in {"SUBSCRIBE", "PSUBSCRIBE", "SSUBSCRIBE", "MONITOR"}:
            raise DataError("Use pubsub() or monitor() for connection-bound protocols")
        if command in {"WAIT", "WAITAOF"}:
            raise DataError("Replication acknowledgements need an explicitly dedicated write connection")
        if self._watching and not self._explicit_transaction:
            if blocking_timeout(args) is not None:
                raise DataError("Blocking commands cannot run between WATCH and MULTI")
            return self._immediate(*args, **kwargs)
        self._commands.append((args, kwargs))
        return self

    async def _immediate(self, *args: Any, **kwargs: Any) -> Any:
        assert self._native is not None
        with self._client._execution([]):
            return await self._native.execute_command(*args, **kwargs)

    def watch(self, *names: str | bytes | memoryview) -> Any:
        return self._watch(*names)

    async def _watch(self, *names: str | bytes | memoryview) -> None:
        with self._client._execution([]):
            if self._commands or self._explicit_transaction:
                raise DataError("WATCH must precede queued commands and MULTI")
            if self._native is None:
                native = await self._client._backend.ready()
                self._native = native.pipeline(transaction=True)
                self._client._lifetime.resources.add(self)
            try:
                await self._native.watch(*names)
                self._watching = True
            except BaseException:
                await self.reset()
                raise

    def multi(self) -> None:
        self._client._lifetime.check_open()
        if self._explicit_transaction or self._commands:
            raise DataError("MULTI must precede queued commands")
        if self._native is not None:
            cast(Callable[[], None], self._native.multi)()
        self._explicit_transaction = True

    def unwatch(self) -> Any:
        return self._unwatch()

    async def _unwatch(self) -> None:
        if self._native is not None:
            with self._client._execution([]):
                await cast(Callable[[], Awaitable[None]], self._native.unwatch)()
        await self.reset()

    async def execute(self, raise_on_error: bool = True) -> list[Any]:
        timeouts = []
        if not (self._transaction or self._explicit_transaction or self._watching):
            timeouts = [t for args, _ in self._commands if (t := blocking_timeout(args)) is not None]
        try:
            with self._client._execution(timeouts):
                if self._native is None:
                    native = await self._client._backend.ready(bool(timeouts))
                    self._native = native.pipeline(transaction=self._transaction or self._explicit_transaction)
                for args, kwargs in self._commands:
                    self._native.execute_command(*args, **kwargs)
                return await self._native.execute(raise_on_error=raise_on_error)
        finally:
            await self.reset()

    def reset(self) -> Any:
        return self._reset()

    async def _reset(self) -> None:
        native, self._native = self._native, None
        self._commands.clear()
        self._watching = self._explicit_transaction = False
        try:
            if native is not None:
                await cast(Callable[[], Awaitable[None]], native.reset)()
        finally:
            self._client._lifetime.resources.discard(self)

    async def aclose(self) -> None:
        await self.reset()


class RedisScript(AsyncScript):
    """Native script caching, with batch execution independent of node caches."""

    def __init__(self, client: RedisClient, script: ScriptTextT) -> None:
        # Native initialization only needs an encoder and computes the SHA.
        super().__init__(client._backend.client(), script)
        self._client = client

    async def __call__(
        self,
        keys: Sequence[KeyT] | None = None,
        args: Iterable[EncodableT] | None = None,
        client: Any = None,
    ) -> Any:
        target = self._client if client is None else client
        if isinstance(target, RedisPipeline):
            # A batch cannot catch NOSCRIPT and replay safely. EVAL also works
            # when a Cluster redirect chooses a node without the cached script.
            return target.execute_command("EVAL", self.script, len(keys or ()), *(keys or ()), *(args or ()))
        return await super().__call__(keys=keys, args=args, client=cast(Redis, target))
