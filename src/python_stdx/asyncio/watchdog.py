"""Observe event-loop progress from a dedicated OS thread."""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from functools import partial

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LoopStall:
    """One transition from a responsive event loop to a stalled one."""

    stalled_for: float
    last_pulse: float
    detected_at: float


class EventLoopWatchdog:
    """Detect stale event-loop pulses from an independent OS thread.

    The observed event loop must call :meth:`pulse` periodically. The watchdog
    never refreshes a pulse itself, so a blocked loop inevitably becomes stale.
    Transition callbacks run on the watchdog thread and must be thread-safe.
    """

    def __init__(
        self,
        *,
        timeout: float,
        check_interval: float = 1.0,
        on_stall: Callable[[LoopStall], None] | None = None,
        on_recovered: Callable[[], None] | None = None,
        clock: Callable[[], float] = time.monotonic,
        thread_name: str = "event-loop-watchdog",
    ) -> None:
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        if check_interval <= 0:
            raise ValueError("check_interval must be positive")

        self._timeout = timeout
        self._check_interval = check_interval
        self._on_stall = on_stall
        self._on_recovered = on_recovered
        self._clock = clock
        self._thread_name = thread_name

        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._last_pulse = clock()
        self._stalled = False

    @property
    def is_stalled(self) -> bool:
        """Whether the latest observed transition is a stall."""
        with self._lock:
            return self._stalled

    def start(self) -> None:
        """Start the observer thread; repeated calls are harmless."""
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._last_pulse = self._clock()
            self._stalled = False
            self._stop = threading.Event()
            thread = threading.Thread(target=self._watch, name=self._thread_name, daemon=True)
            self._thread = thread
        thread.start()

    def pulse(self) -> None:
        """Record progress made by the observed event loop."""
        with self._lock:
            self._last_pulse = self._clock()

    def stop(self, *, join_timeout: float | None = None) -> None:
        """Stop the observer thread; repeated calls are harmless."""
        with self._lock:
            thread = self._thread
            self._thread = None
            self._stop.set()
        if thread is not None and thread is not threading.current_thread():
            thread.join(join_timeout)

    def _watch(self) -> None:
        while not self._stop.wait(self._check_interval):
            now = self._clock()
            callback: Callable[[], None] | None = None
            with self._lock:
                stalled_for = now - self._last_pulse
                if stalled_for > self._timeout and not self._stalled:
                    self._stalled = True
                    stall = LoopStall(stalled_for=stalled_for, last_pulse=self._last_pulse, detected_at=now)
                    callback = partial(self._notify_stall, stall)
                elif stalled_for <= self._timeout and self._stalled:
                    self._stalled = False
                    callback = self._on_recovered
            if callback is not None:
                try:
                    callback()
                except Exception:
                    logger.exception("event-loop watchdog callback failed")

    def _notify_stall(self, stall: LoopStall) -> None:
        if self._on_stall is not None:
            self._on_stall(stall)
