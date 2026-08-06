"""In-process tagged cache backend."""

from collections.abc import Iterable
from typing import Generic

from cachetools import TTLCache

from python_stdx.cache.tagged._base import TaggedCache, Value


class MemoryTaggedCache(TaggedCache[Value], Generic[Value]):
    """A bounded LRU cache with fixed TTL and tag invalidation."""

    def __init__(self, *, max_size: int = 1000, ttl: float = 300.0) -> None:
        if max_size <= 0:
            raise ValueError("max_size must be positive")
        if ttl <= 0:
            raise ValueError("ttl must be positive")
        self._cache: TTLCache[str, Value] = TTLCache(maxsize=max_size, ttl=ttl)
        self._key_tags: dict[str, set[str]] = {}
        self._tag_keys: dict[str, set[str]] = {}

    async def get(self, key: str) -> Value | None:
        self._expire()
        return self._cache.get(key)

    async def set(self, key: str, value: Value, tags: Iterable[str] = ()) -> None:
        if value is None:
            raise ValueError("None cannot be stored because it represents a cache miss")
        self._expire()
        if key not in self._cache and len(self._cache) >= self._cache.maxsize:
            evicted_key, _ = self._cache.popitem()
            self._remove_tag_bindings(evicted_key)

        self._remove_tag_bindings(key)
        self._cache[key] = value
        normalized_tags = set(tags)
        if not normalized_tags:
            return
        self._key_tags[key] = normalized_tags
        for tag in normalized_tags:
            self._tag_keys.setdefault(tag, set()).add(key)

    async def delete(self, key: str) -> None:
        self._expire()
        self._cache.pop(key, None)
        self._remove_tag_bindings(key)

    async def invalidate_tag(self, tag: str) -> None:
        self._expire()
        for key in tuple(self._tag_keys.get(tag, set())):
            self._cache.pop(key, None)
            self._remove_tag_bindings(key)

    async def clear(self) -> None:
        self._cache.clear()
        self._key_tags.clear()
        self._tag_keys.clear()

    def _expire(self) -> None:
        for key, _ in self._cache.expire():
            self._remove_tag_bindings(key)

    def _remove_tag_bindings(self, key: str) -> None:
        for tag in self._key_tags.pop(key, set()):
            keys = self._tag_keys.get(tag)
            if keys is None:
                continue
            keys.discard(key)
            if not keys:
                self._tag_keys.pop(tag, None)
