"""Executor：执行一次 TaskDef，含 retry / timeout / backoff / OTel span。

无副作用：不持有 store，不写持久化。给 TaskDef 还 ExecutionResult，结束。

由 Runner 调用：Runner 拿到 ExecutionResult 后自行决定写哪个 store 方法
（record_success / record_dynamic_success / record_failure / record_dynamic_failure）。
"""

import asyncio
import inspect
import logging
import time
from dataclasses import dataclass

from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode

from python_stdx.scheduler.decorator import TaskDef

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


@dataclass
class ExecutionResult:
    """一次 Executor.execute 的结果。

    Runner 据此决定写 record_success / record_failure 等持久化方法。
    """

    success: bool
    attempts: int  # 实际尝试次数，从 1 开始
    started_at: float  # 整次执行（含全部 retry）开始时间，timestamp
    duration_ms: int  # 仅最后一次 attempt 的耗时；retry 间的 backoff sleep 不计入
    error: str | None  # 失败时填，成功时 None
    trace_id: str | None


class Executor:
    """执行一次 TaskDef。

    内部 retry 语义跟旧实现 ``TaskScheduler._execute`` 完全一致：
    - 在同一调用内最多重试 ``td.max_runs`` 次（含首次）；
    - 每次 attempt 失败后按 ``td.backoff(attempt)`` sleep；
    - 每次 attempt 受 ``td.timeout`` 控制；
    - 整次调用包在一个 ``task.execute`` span 里，attempt 之间共用同一 trace_id。

    本 PR 不修复 oneshot retry 计数 bug（``record_dynamic_failure`` 仅在最后
    一次 attempt 失败时 +1，导致 max_runs > 1 时一行可能被 list_pending 多次拾起）。
    Executor 只是把现有行为搬迁过来；该问题由后续独立 PR 处理。
    """

    async def execute(
        self,
        td: TaskDef,
        params: dict[str, object] | None = None,
    ) -> ExecutionResult:
        with tracer.start_as_current_span("task.execute", attributes={"task.name": td.name}) as span:
            span_context = span.get_span_context()
            trace_id = f"{span_context.trace_id:032x}" if span_context.is_valid else None
            started_at = time.time()
            error: str | None = None

            for attempt in range(1, td.max_runs + 1):
                try:
                    exec_start = time.time()
                    bound_args = self._bind_params(td, params, attempt)
                    async with asyncio.timeout(td.timeout):
                        await td.func(**bound_args)
                    duration_ms = int((time.time() - exec_start) * 1000)
                    return ExecutionResult(
                        success=True,
                        attempts=attempt,
                        started_at=started_at,
                        duration_ms=duration_ms,
                        error=None,
                        trace_id=trace_id,
                    )
                except asyncio.CancelledError:
                    raise
                except asyncio.TimeoutError as e:
                    error = f"Task {td.name} timed out after {td.timeout}s"
                    # 附 params 便于按业务 id（如 source_id/dataset_id）grep 定位是哪条任务超时
                    logger.warning(f"{error} (params={params})")
                    span.record_exception(e)
                except Exception as e:
                    error = f"{type(e).__name__}: {e}"
                    logger.exception(f"Task {td.name} failed: {e} (params={params})")
                    span.record_exception(e)

                if attempt < td.max_runs:
                    await self._sleep_backoff(td, attempt)

            # 所有 attempt 跑完仍失败: task func 的异常被上面各 attempt 的 except 吞掉、
            # 不会传播到 create_span, 其 set_status_on_exception 不触发。这里显式把
            # task.execute span 标 ERROR(异常已逐 attempt record_exception), 让 task
            # call stack 的根能显示 [ERROR]; 否则失败 task 的根 span 看起来是 OK。
            duration_ms = int((time.time() - started_at) * 1000)
            span.set_status(Status(StatusCode.ERROR, error or f"Task {td.name} failed"))
            return ExecutionResult(
                success=False,
                attempts=td.max_runs,
                started_at=started_at,
                duration_ms=duration_ms,
                error=error,
                trace_id=trace_id,
            )

    @staticmethod
    def _bind_params(td: TaskDef, params: dict[str, object] | None, attempt: int) -> dict[str, object]:
        sig = inspect.signature(td.func)
        bound: dict[str, object] = {}
        provided = params or {}
        for param_name, param_value in provided.items():
            if param_name in sig.parameters:
                bound[param_name] = param_value
            else:
                logger.warning(f"Task {td.name} does not accept parameter '{param_name}', ignoring")
        # 按需注入当前执行轮次（attempt，从 1 开始）：仅当 handler 显式声明 attempt 形参、且 submit
        # 未提供同名参数时注入，让下游能按重试次数自适应降级（如缩小 map-reduce 采样段数）。
        # 未声明 attempt 的 handler 完全不受影响。
        if "attempt" in sig.parameters and "attempt" not in provided:
            bound["attempt"] = attempt
        return bound

    @staticmethod
    async def _sleep_backoff(td: TaskDef, attempt: int) -> None:
        backoff_seconds = td.backoff(attempt)
        logger.info(f"Task {td.name} will retry in {backoff_seconds:.1f}s (attempt {attempt + 1})")
        await asyncio.sleep(backoff_seconds)
