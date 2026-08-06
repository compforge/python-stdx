"""Contract for caches that invalidate entries by tag."""

from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping
from typing import Generic, TypeVar

Value = TypeVar("Value")


class TaggedCache(Generic[Value], ABC):
    """An async cache where one entry can be invalidated through several tags.

    ``None`` represents a cache miss and therefore cannot be stored as a value.
    """

    @abstractmethod
    async def get(self, key: str) -> Value | None:
        """Return the cached value, or ``None`` on a miss."""

    @abstractmethod
    async def set(self, key: str, value: Value, tags: Iterable[str] = ()) -> None:
        """Store one value and replace its tag associations."""

    async def get_many(self, keys: Iterable[str]) -> dict[str, Value]:
        """Return only the entries that are currently cached."""
        values: dict[str, Value] = {}
        for key in keys:
            value = await self.get(key)
            if value is not None:
                values[key] = value
        return values

    async def set_many(self, values: Mapping[str, Value]) -> None:
        """Store several untagged values."""
        for key, value in values.items():
            await self.set(key, value)

    @abstractmethod
    async def delete(self, key: str) -> None:
        """Delete one entry."""

    async def delete_many(self, keys: Iterable[str]) -> None:
        """Delete several entries."""
        for key in keys:
            await self.delete(key)

    @abstractmethod
    async def invalidate_tag(self, tag: str) -> None:
        """Invalidate every entry associated with ``tag``."""

    async def invalidate_tags(self, tags: Iterable[str]) -> None:
        """Invalidate the union of entries associated with the supplied tags."""
        for tag in tags:
            await self.invalidate_tag(tag)

    @abstractmethod
    async def clear(self) -> None:
        """Invalidate all entries owned by this cache."""
