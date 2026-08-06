"""Executor 单测：验证 retry / timeout / ExecutionResult 字段。

不需要 store——Executor 不持有 store，给 TaskDef 还 Result，结束。
"""

import asyncio

from python_stdx.scheduler.decorator import TaskDef
from python_stdx.scheduler.executor import ExecutionResult, Executor


def _make_td(func, *, timeout=30, max_runs=1, backoff=lambda attempt: 0.01):
    """构造测试用 TaskDef；默认 backoff 极短，避免测试拖慢。"""
    return TaskDef(name="t.test", timeout=timeout, max_runs=max_runs, backoff=backoff, func=func)


def test_executor_returns_success_on_first_attempt():
    async def run():
        called = 0

        async def fn():
            nonlocal called
            called += 1

        executor = Executor()
        result = await executor.execute(_make_td(fn))

        assert result.success is True
        assert result.attempts == 1
        assert result.error is None
        assert called == 1
        assert result.duration_ms >= 0

    asyncio.run(run())


def test_executor_retries_until_success():
    async def run():
        called = 0

        async def flaky():
            nonlocal called
            called += 1
            if called < 3:
                raise RuntimeError(f"fail {called}")

        executor = Executor()
        result = await executor.execute(_make_td(flaky, max_runs=5))

        assert result.success is True
        assert result.attempts == 3
        assert called == 3
        assert result.error is None

    asyncio.run(run())


def test_executor_returns_failure_after_max_runs():
    async def run():
        called = 0

        async def always_fail():
            nonlocal called
            called += 1
            raise ValueError("nope")

        executor = Executor()
        result = await executor.execute(_make_td(always_fail, max_runs=3))

        assert result.success is False
        assert result.attempts == 3
        assert called == 3
        assert result.error is not None
        assert "ValueError" in result.error
        assert "nope" in result.error

    asyncio.run(run())


def test_executor_returns_failure_on_timeout():
    async def run():
        async def slow():
            await asyncio.sleep(10)

        executor = Executor()
        # timeout=0 让 asyncio.timeout 立刻触发；max_runs=1 不重试
        result = await executor.execute(_make_td(slow, timeout=0, max_runs=1))

        assert result.success is False
        assert result.attempts == 1
        assert result.error is not None
        assert "timed out" in result.error.lower()

    asyncio.run(run())


def test_executor_injects_params_by_signature():
    async def run():
        captured = {}

        async def with_params(a: int, b: str):
            captured["a"] = a
            captured["b"] = b

        executor = Executor()
        result = await executor.execute(
            _make_td(with_params),
            params={"a": 42, "b": "hi", "extra": "ignored"},
        )

        assert result.success is True
        assert captured == {"a": 42, "b": "hi"}

    asyncio.run(run())


def test_executor_propagates_cancelled_error():
    async def run():
        async def cancellable():
            raise asyncio.CancelledError()

        executor = Executor()
        try:
            await executor.execute(_make_td(cancellable))
        except asyncio.CancelledError:
            return  # expected: CancelledError 不应被 retry 吞掉
        assert False, "CancelledError should bubble up"

    asyncio.run(run())


def test_execution_result_dataclass_fields():
    """确保 ExecutionResult 暴露的字段是 Runner 写 store 需要的所有信息。"""
    result = ExecutionResult(
        success=True,
        attempts=1,
        started_at=123.0,
        duration_ms=10,
        error=None,
        trace_id="abc",
    )
    assert result.success is True
    assert result.attempts == 1
    assert result.started_at == 123.0
    assert result.duration_ms == 10
    assert result.error is None
    assert result.trace_id == "abc"
