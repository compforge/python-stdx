import threading

import pytest

from python_stdx.asyncio import EventLoopWatchdog, LoopStall


def test_watchdog_detects_stall_on_an_independent_thread():
    caller_thread_id = threading.get_ident()
    detected = threading.Event()
    callback_thread_id: int | None = None
    observed: LoopStall | None = None

    def on_stall(stall: LoopStall) -> None:
        nonlocal callback_thread_id, observed
        callback_thread_id = threading.get_ident()
        observed = stall
        detected.set()

    watchdog = EventLoopWatchdog(timeout=0.03, check_interval=0.005, on_stall=on_stall)
    watchdog.start()
    try:
        assert detected.wait(1.0)
        assert watchdog.is_stalled
        assert callback_thread_id != caller_thread_id
        assert observed is not None
        assert observed.stalled_for > 0.03
    finally:
        watchdog.stop()


def test_watchdog_reports_recovery_after_a_new_pulse():
    detected = threading.Event()
    recovered = threading.Event()
    watchdog = EventLoopWatchdog(
        timeout=0.03,
        check_interval=0.005,
        on_stall=lambda _: detected.set(),
        on_recovered=recovered.set,
    )
    watchdog.start()
    try:
        assert detected.wait(1.0)
        watchdog.pulse()
        assert recovered.wait(1.0)
        assert not watchdog.is_stalled
    finally:
        watchdog.stop()


@pytest.mark.parametrize("timeout, check_interval", [(0, 1), (1, 0), (-1, 1), (1, -1)])
def test_watchdog_rejects_non_positive_timing(timeout: float, check_interval: float):
    with pytest.raises(ValueError):
        EventLoopWatchdog(timeout=timeout, check_interval=check_interval)
