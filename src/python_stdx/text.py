"""Dependency-free helpers for text measurement and truncation."""

import re
from collections.abc import Callable


def byte_length(text: str, encoding: str = "utf-8") -> int:
    """Return the number of bytes needed to encode ``text``."""
    return len(text.encode(encoding))


def normalize_whitespace(text: str) -> str:
    """Collapse consecutive whitespace and trim both ends."""
    return re.sub(r"\s+", " ", text).strip()


def _ellipsis(removed: str, budget: int) -> str:
    return "…" if removed and budget > 0 else ""


def _fit(text: str, budget: int, encoding: str | None) -> str:
    if budget <= 0:
        return ""
    if encoding is None:
        return text[:budget]
    return text.encode(encoding)[:budget].decode(encoding, errors="ignore")


def truncate_middle(
    text: str,
    max_length: int | None,
    head: int = 256,
    tail: int | None = None,
    *,
    replacement: Callable[[str, int], str] = _ellipsis,
    encoding: str | None = None,
) -> str:
    """Truncate the middle while keeping the result within a character or byte budget.

    ``replacement`` receives the removed text and its available budget. When
    ``encoding`` is set, all size arguments are measured in encoded bytes.
    """
    if max_length is None:
        return text
    if max_length < 0:
        raise ValueError("max_length must be non-negative")
    if head < 0 or (tail is not None and tail < 0):
        raise ValueError("head and tail must be non-negative")
    if tail is None:
        tail = max(min(max_length - head - 3, head), 0)
    if head + tail > max_length:
        raise ValueError(f"head + tail ({head + tail}) must not exceed max_length ({max_length})")

    if encoding is None:
        if len(text) <= max_length:
            return text
        head_part = text[:head]
        tail_part = text[len(text) - tail :] if tail else ""
        measure: Callable[[str], int] = len
    else:
        encoded = text.encode(encoding)
        if len(encoded) <= max_length:
            return text
        head_part = encoded[:head].decode(encoding, errors="ignore")
        tail_part = encoded[len(encoded) - tail :].decode(encoding, errors="ignore") if tail else ""

        def measure(value: str) -> int:
            return len(value.encode(encoding))

    budget = max_length - measure(head_part) - measure(tail_part)
    removed = text[len(head_part) : len(text) - len(tail_part)]
    middle = replacement(removed, budget)
    if measure(middle) > budget:
        middle = _fit(middle, budget, encoding)
    return head_part + middle + tail_part
