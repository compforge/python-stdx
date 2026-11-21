"""In-process loading cache for callers on a single asyncio event loop."""

import asyncio

from cachetools import TTLCache

from python_stdx.cache.loading._base import Key, Loader, LoadingCache, Value


class MemoryLoadingCache(LoadingCache[Key, Value]):
    """A bounded LRU cache with a fixed TTL and one active loader per key."""

    def __init__(self, *, max_size: int = 1000, ttl: float = 300.0) -> None:
        if max_size <= 0:
            raise ValueError("max_size must be positive")
        if ttl <= 0:
            raise ValueError("ttl must be positive")
        self._values: TTLCache[Key, Value] = TTLCache(maxsize=max_size, ttl=ttl)
        self._pending_loads: dict[Key, asyncio.Future[Value | None]] = {}

    async def get(self, key: Key) -> Value | None:
        return self._values.get(key)

    async def set(self, key: Key, value: Value) -> None:
        if value is None:
            raise ValueError("None cannot be stored because it represents a cache miss")
        self._values[key] = value

    async def get_or_load(self, key: Key, loader: Loader[Key, Value]) -> Value | None:
        while True:
            cached = self._values.get(key)
            if cached is not None:
                return cached

            pending = self._pending_loads.get(key)
            if pending is None:
                break
            # Cancelling one waiter must leave the shared result available to the others.
            loaded = await asyncio.shield(pending)
            if loaded is not None:
                return loaded

        # No await separates the miss check from registration on this event loop.
        pending = asyncio.get_running_loop().create_future()
        self._pending_loads[key] = pending
        try:
            value = await loader(key)
            if value is not None and key not in self._values:
                self._values[key] = value
            pending.set_result(value)
            return value
        finally:
            del self._pending_loads[key]
            if not pending.done():
                # The owner failed or was cancelled. A waiter can run its own loader.
                pending.set_result(None)
