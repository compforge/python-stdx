"""Redis tagged cache using generation-based invalidation."""

import asyncio
import hashlib
import json
from collections.abc import Callable, Iterable
from typing import Generic, cast

from python_stdx.cache.tagged._base import TaggedCache, Value
from python_stdx.redis import RedisClient


def _json_dumps(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _json_loads(value: str) -> object:
    return json.loads(value)


def _text(value: str | bytes) -> str:
    return value.decode("utf-8") if isinstance(value, bytes) else value


class RedisTaggedCache(TaggedCache[Value], Generic[Value]):
    """A tagged cache compatible with standalone, Sentinel, and Cluster Redis.

    Tag invalidation increments a generation instead of maintaining reverse
    entry indexes. Stale values remain physically stored only until their TTL.
    """

    def __init__(
        self,
        client: RedisClient,
        *,
        namespace: str,
        ttl: int = 300,
        value_dumps: Callable[[Value], str] | None = None,
        value_loads: Callable[[str], Value] | None = None,
    ) -> None:
        if not namespace.strip():
            raise ValueError("namespace must not be empty")
        if ttl <= 0:
            raise ValueError("ttl must be positive")
        self._redis = client
        self._namespace = namespace.rstrip(":")
        self._ttl = ttl
        self._value_dumps = value_dumps or cast(Callable[[Value], str], _json_dumps)
        self._value_loads = value_loads or cast(Callable[[str], Value], _json_loads)

    async def get(self, key: str) -> Value | None:
        entry_key = self._entry_key(key)
        raw = await self._redis.get(entry_key)
        if raw is None:
            return None

        entry = json.loads(_text(raw))
        stored_namespace_generation = int(entry["namespace_generation"])
        if stored_namespace_generation != await self._generation(self._namespace_generation_key()):
            await self._redis.delete(entry_key)
            return None

        stored_tag_generations = cast(dict[str, int], entry["tag_generations"])
        current_tag_generations = await self._tag_generations(stored_tag_generations)
        if any(stored_tag_generations[tag] != current_tag_generations[tag] for tag in stored_tag_generations):
            await self._redis.delete(entry_key)
            return None
        return self._value_loads(entry["value"])

    async def set(self, key: str, value: Value, tags: Iterable[str] = ()) -> None:
        if value is None:
            raise ValueError("None cannot be stored because it represents a cache miss")
        normalized_tags = tuple(dict.fromkeys(tags))
        namespace_generation, tag_generations = await asyncio.gather(
            self._generation(self._namespace_generation_key()),
            self._tag_generations(normalized_tags),
        )
        entry = _json_dumps(
            {
                "namespace_generation": namespace_generation,
                "tag_generations": tag_generations,
                "value": self._value_dumps(value),
            }
        )
        await self._redis.set(self._entry_key(key), entry, ex=self._ttl)

    async def delete(self, key: str) -> None:
        await self._redis.delete(self._entry_key(key))

    async def invalidate_tag(self, tag: str) -> None:
        await self._redis.incr(self._tag_generation_key(tag))

    async def clear(self) -> None:
        await self._redis.incr(self._namespace_generation_key())

    async def _tag_generations(self, tags: Iterable[str]) -> dict[str, int]:
        tag_list = tuple(tags)
        generations = await asyncio.gather(*(self._generation(self._tag_generation_key(tag)) for tag in tag_list))
        return dict(zip(tag_list, generations))

    async def _generation(self, key: str) -> int:
        raw = await self._redis.get(key)
        return int(raw) if raw is not None else 0

    def _entry_key(self, key: str) -> str:
        return f"{self._namespace}:entry:{key}"

    def _tag_generation_key(self, tag: str) -> str:
        digest = hashlib.sha256(tag.encode("utf-8")).hexdigest()
        return f"{self._namespace}:tag-generation:{digest}"

    def _namespace_generation_key(self) -> str:
        return f"{self._namespace}:generation"
