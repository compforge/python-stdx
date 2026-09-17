"""Classify commands without replacing Redis argument validation."""

from collections.abc import Sequence
from math import isfinite


def tokens(args: Sequence[object]) -> tuple[object, ...]:
    if args and isinstance(args[0], (str, bytes)):
        return (*args[0].split(), *args[1:])
    return tuple(args)


def name(value: object) -> str:
    # Arbitrary bytes remain the server's responsibility, including invalid UTF-8.
    return value.decode("ascii", errors="replace").upper() if isinstance(value, bytes) else str(value).upper()


def blocking_timeout(args: Sequence[object]) -> float | None:
    """Return seconds (zero means forever), or None for an ordinary/invalid command."""
    parts = tokens(args)
    if not parts:
        return None
    command = name(parts[0])
    index, scale = -1, 1.0
    if command in {"BLPOP", "BRPOP", "BRPOPLPUSH", "BLMOVE", "BZPOPMIN", "BZPOPMAX"}:
        pass
    elif command in {"BLMPOP", "BZMPOP"}:
        index = 1
    elif command in {"WAIT", "WAITAOF"}:
        scale = 1000.0
    elif command in {"XREAD", "XREADGROUP"}:
        scale = 1000.0
        index = 4 if command == "XREADGROUP" else 1
        while index < len(parts):
            option = name(parts[index])
            if option == "BLOCK":
                index += 1
                break
            if option == "COUNT":
                index += 2
            elif option == "NOACK":
                index += 1
            else:
                return None
        else:
            return None
    else:
        return None
    try:
        value = parts[index]
        if isinstance(value, bool) or not isinstance(value, (str, bytes, int, float)):
            return None
        timeout = float(value)
    except (IndexError, ValueError, OverflowError):
        return None
    return timeout / scale if isfinite(timeout) and timeout >= 0 else None
