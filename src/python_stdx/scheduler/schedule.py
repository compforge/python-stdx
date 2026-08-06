"""调度策略抽象。"""

import asyncio
import hashlib
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass


class Schedule(ABC):
    """调度策略抽象。"""

    @abstractmethod
    async def next_run(self, last_run: float | None) -> float:
        """计算下次执行时间戳（Unix timestamp）。"""
        pass

    async def wait_until_next(self, last_run: float | None) -> None:
        """阻塞等待直到下次执行时间。"""
        next_ts = await self.next_run(last_run)
        delay = next_ts - time.time()
        if delay > 0:
            await asyncio.sleep(delay)


@dataclass
class IntervalSchedule(Schedule):
    """轮询触发：每隔指定秒数执行一次。"""

    interval: int  # 秒
    jitter: float | None = None  # 秒；None 使用默认小扰动，0 明确关闭

    async def next_run(self, last_run: float | None) -> float:
        if last_run is None:
            # First interval run must be immediately due. TaskScheduler captures
            # `now` before asking the schedule, so returning the current clock
            # here would always be slightly in the future and skip forever.
            return 0.0
        return last_run + self.interval + self._jitter_for(last_run)

    def _jitter_for(self, last_run: float) -> float:
        jitter = self._jitter_seconds()
        if jitter <= 0:
            return 0.0

        # Keep the jitter stable for the same last_run. A fresh random value on
        # every scheduler tick can move the due time while the task is waiting.
        seed = f"{last_run:.6f}:{self.interval}:{jitter:.6f}".encode()
        value = int.from_bytes(hashlib.sha256(seed).digest()[:8], "big")
        return (value / (1 << 63) - 1.0) * jitter

    def _jitter_seconds(self) -> float:
        if self.jitter is not None:
            return max(0.0, float(self.jitter))
        return min(max(self.interval * 0.2, 1.0), 2.0)


@dataclass
class CronSchedule(Schedule):
    """cron 表达式触发。"""

    cron_expr: str

    def __post_init__(self) -> None:
        import importlib.util

        if importlib.util.find_spec("croniter") is None:
            raise ImportError("croniter is required for CronSchedule. Install it with: pip install croniter")

    async def next_run(self, last_run: float | None) -> float:
        from croniter import croniter  # type: ignore[import-untyped]

        base = datetime.fromtimestamp(last_run or time.time())
        itr = croniter(self.cron_expr, base)
        return float(itr.get_next(float))
