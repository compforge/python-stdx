"""One shutdown barrier and long-operation budget for every topology."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine, Iterator
from contextlib import contextmanager
from typing import Any, Protocol

from redis.exceptions import RedisError


class RedisPoolExhaustedError(RedisError):
    """Capacity is exhausted; retrying a transport cannot create capacity."""


class Closable(Protocol):
    async def aclose(self) -> None: ...


class Lifetime:
    def __init__(self, maximum: int) -> None:
        self.maximum = maximum
        self.long_in_use = 0
        self.closed = False
        self.resources: set[Closable] = set()
        self.tasks: dict[asyncio.Task[Any], int] = {}
        self._close_task: asyncio.Task[None] | None = None

    def check_open(self) -> None:
        if self.closed:
            raise RedisError("Redis client is closed")

    @contextmanager
    def operation(self) -> Iterator[None]:
        self.check_open()
        task = asyncio.current_task()
        assert task is not None
        self.tasks[task] = self.tasks.get(task, 0) + 1
        try:
            yield
        finally:
            self.tasks[task] -= 1
            if not self.tasks[task]:
                del self.tasks[task]

    def acquire(self) -> None:
        self.check_open()
        # Event-loop-local: no await between testing and reserving capacity.
        if self.long_in_use >= self.maximum:
            raise RedisPoolExhaustedError(f"Redis long pool exhausted (limit={self.maximum})")
        self.long_in_use += 1

    def release(self) -> None:
        self.long_in_use -= 1

    async def close(self, close_backend: Callable[[], Coroutine[Any, Any, None]]) -> None:
        if self._close_task is None:
            # spec: Closing is terminal, including for retained pipelines/subscriptions.
            self.closed = True
            self._close_task = asyncio.create_task(self._close(close_backend, asyncio.current_task()))
        await asyncio.shield(self._close_task)

    async def _close(
        self, close_backend: Callable[[], Coroutine[Any, Any, None]], initiator: asyncio.Task[Any] | None
    ) -> None:
        tasks = [task for task in self.tasks if task is not initiator]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        try:
            results = await asyncio.gather(
                *(resource.aclose() for resource in list(self.resources)), return_exceptions=True
            )
        finally:
            await close_backend()
        for result in results:
            if isinstance(result, BaseException):
                raise result
