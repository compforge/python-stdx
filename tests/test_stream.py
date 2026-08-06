import asyncio
from collections.abc import AsyncIterator
from unittest.mock import patch

import pytest

from python_stdx.asyncio import StreamExitPolicy, scoped_stream
from python_stdx.asyncio import stream as stream_module


class _TrackedStream:
    def __init__(self, values: list[int], *, error_after: int | None = None) -> None:
        self._values = values
        self._error_after = error_after
        self.consumed: list[int] = []
        self.close_calls = 0

    def __aiter__(self) -> AsyncIterator[int]:
        return self

    async def __anext__(self) -> int:
        if self._error_after is not None and len(self.consumed) >= self._error_after:
            raise RuntimeError("stream failed")
        if not self._values:
            raise StopAsyncIteration
        value = self._values.pop(0)
        self.consumed.append(value)
        return value

    async def aclose(self) -> None:
        self.close_calls += 1


class _FailingCloseStream(_TrackedStream):
    async def aclose(self) -> None:
        await super().aclose()
        raise RuntimeError("close failed")


class _BlockingCloseStream(_TrackedStream):
    def __init__(self, values: list[int]) -> None:
        super().__init__(values)
        self.close_started = asyncio.Event()
        self.allow_close = asyncio.Event()
        self.close_finished = asyncio.Event()

    async def aclose(self) -> None:
        await super().aclose()
        self.close_started.set()
        await self.allow_close.wait()
        self.close_finished.set()


async def test_scoped_stream_closes_on_normal_exit_by_default() -> None:
    source = _TrackedStream([1, 2, 3])

    async with scoped_stream(source) as stream:
        assert await anext(stream) == 1

    assert source.consumed == [1]
    assert source.close_calls == 1


async def test_scoped_stream_drains_on_normal_exit_when_requested() -> None:
    source = _TrackedStream([1, 2, 3])

    async with scoped_stream(source, exit_policy=StreamExitPolicy.DRAIN) as stream:
        assert await anext(stream) == 1

    assert source.consumed == [1, 2, 3]
    assert source.close_calls == 1


async def test_scoped_stream_closes_without_draining_on_error() -> None:
    source = _TrackedStream([1, 2, 3])

    with pytest.raises(ValueError, match="consumer failed"):
        async with scoped_stream(source, exit_policy=StreamExitPolicy.DRAIN) as stream:
            assert await anext(stream) == 1
            raise ValueError("consumer failed")

    assert source.consumed == [1]
    assert source.close_calls == 1


async def test_scoped_stream_propagates_drain_error_and_closes() -> None:
    source = _TrackedStream([1, 2, 3], error_after=2)

    with pytest.raises(RuntimeError, match="stream failed"):
        async with scoped_stream(source, exit_policy=StreamExitPolicy.DRAIN) as stream:
            assert await anext(stream) == 1

    assert source.consumed == [1, 2]
    assert source.close_calls == 1


async def test_scoped_stream_logs_close_error_without_raising() -> None:
    source = _FailingCloseStream([1])

    with patch.object(stream_module, "logger") as mock_logger:
        async with scoped_stream(source) as stream:
            assert await anext(stream) == 1

    assert source.close_calls == 1
    mock_logger.warning.assert_called_once_with("Exception occurred while closing async stream", exc_info=True)


async def test_scoped_stream_finishes_close_before_propagating_cancellation() -> None:
    source = _BlockingCloseStream([1])

    async def consume() -> None:
        async with scoped_stream(source) as stream:
            assert await anext(stream) == 1

    task = asyncio.create_task(consume())
    await source.close_started.wait()

    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()

    source.allow_close.set()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert source.close_finished.is_set()
    assert source.close_calls == 1
