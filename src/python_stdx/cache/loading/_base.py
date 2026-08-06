"""Contract for caches that coordinate value loading."""

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable, Hashable
from typing import Generic, TypeVar

Key = TypeVar("Key", bound=Hashable)
Value = TypeVar("Value")
Loader = Callable[[Key], Awaitable[Value | None]]


class LoadingCache(Generic[Key, Value], ABC):
    """An async cache that coordinates concurrent loads for the same key.

    ``None`` represents a cache miss and therefore is not cached.
    """

    @abstractmethod
    async def get(self, key: Key) -> Value | None:
        """Return the cached value, or ``None`` on a miss."""

    @abstractmethod
    async def set(self, key: Key, value: Value) -> None:
        """Store one value."""

    @abstractmethod
    async def get_or_load(self, key: Key, loader: Loader[Key, Value]) -> Value | None:
        """Return a cached value or coordinate one load for ``key``."""
