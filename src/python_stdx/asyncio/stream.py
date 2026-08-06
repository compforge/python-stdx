"""Deterministic lifecycle management for asynchronous streams."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterable, AsyncIterator
from contextlib import asynccontextmanager
from enum import Enum
from typing import Any, Generic, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


class StreamExitPolicy(str, Enum):
    """Control how a stream is finalized after leaving its scope normally."""

    CLOSE = "close"
    DRAIN = "drain"


@asynccontextmanager
async def scoped_stream(
    stream: AsyncIterable[T],
    *,
    exit_policy: StreamExitPolicy = StreamExitPolicy.CLOSE,
) -> AsyncIterator[AsyncIterator[T]]:
    """Keep an async stream's exit behavior explicit and deterministic.

    ``DRAIN`` consumes remaining items only after a normal exit, allowing the
    producer to finish naturally. Exceptions and cancellation always close the
    stream immediately. ``CLOSE`` preserves close-on-exit behavior.
    Non-cancellation errors raised while closing are logged without replacing
    the scope's result or its original exception.
    """
    iterator = _AcloseOnceAsyncIterator(stream)
    exited_normally = False
    try:
        yield iterator
        exited_normally = True
    finally:
        try:
            if exited_normally and exit_policy is StreamExitPolicy.DRAIN:
                async for _ in iterator:
                    pass
        finally:
            await iterator.aclose()


class _AcloseOnceAsyncIterator(Generic[T]):
    def __init__(self, stream: AsyncIterable[T]) -> None:
        self._stream = stream
        self._iterator = stream.__aiter__()
        self._closed = False

    def __aiter__(self) -> _AcloseOnceAsyncIterator[T]:
        return self

    async def __anext__(self) -> T:
        return await self._iterator.__anext__()

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        close = getattr(self._iterator, "aclose", None)
        if close is None and self._iterator is not self._stream:
            close = getattr(self._stream, "aclose", None)
        if close is not None:
            await _shield_close(close)


async def _shield_close(close: Any) -> None:
    close_task = asyncio.ensure_future(close())
    try:
        await asyncio.shield(close_task)
    except asyncio.CancelledError:
        try:
            await asyncio.shield(close_task)
        except Exception:
            logger.warning("Exception occurred while closing async stream", exc_info=True)
        raise
    except Exception:
        logger.warning("Exception occurred while closing async stream", exc_info=True)
