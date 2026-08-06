"""Redis loading cache with distributed same-key load coordination."""

import asyncio
import hashlib
import json
import time
import uuid
from collections.abc import Callable
from typing import Generic, cast

from python_stdx.cache.loading._base import Key, Loader, LoadingCache, Value
from python_stdx.redis import RedisClient

_RELEASE_LEASE = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""

_PUBLISH_VALUE = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
    return 0
end
if redis.call('EXISTS', KEYS[2]) == 0 then
    redis.call('SET', KEYS[2], ARGV[2], 'EX', ARGV[3])
end
redis.call('DEL', KEYS[1])
return 1
"""


def _json_dumps(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _json_loads(value: str) -> object:
    return json.loads(value)


def _text(value: str | bytes) -> str:
    return value.decode("utf-8") if isinstance(value, bytes) else value


class RedisLoadingCache(LoadingCache[Key, Value], Generic[Key, Value]):
    """A Guava-style loading cache coordinated through Redis leases.

    Distinct keys load concurrently. Callers for the same missing key wait for
    the lease owner and reuse its value. A loader timeout releases the lease so
    another caller can retry.
    """

    def __init__(
        self,
        client: RedisClient,
        *,
        namespace: str,
        ttl: int = 300,
        load_timeout: float = 30.0,
        lease_ttl: float = 35.0,
        wait_timeout: float = 40.0,
        poll_interval: float = 0.2,
        key_dumps: Callable[[Key], str] = str,
        value_dumps: Callable[[Value], str] | None = None,
        value_loads: Callable[[str], Value] | None = None,
    ) -> None:
        if not namespace.strip():
            raise ValueError("namespace must not be empty")
        if ttl <= 0:
            raise ValueError("ttl must be positive")
        if load_timeout <= 0 or lease_ttl <= 0 or wait_timeout <= 0 or poll_interval <= 0:
            raise ValueError("loading timeouts and poll_interval must be positive")
        if lease_ttl <= load_timeout:
            raise ValueError("lease_ttl must be greater than load_timeout")
        self._redis = client
        self._namespace = namespace.rstrip(":")
        self._ttl = ttl
        self._load_timeout = load_timeout
        self._lease_ttl_ms = max(1, int(lease_ttl * 1000))
        self._wait_timeout = wait_timeout
        self._poll_interval = poll_interval
        self._key_dumps = key_dumps
        self._value_dumps = value_dumps or cast(Callable[[Value], str], _json_dumps)
        self._value_loads = value_loads or cast(Callable[[str], Value], _json_loads)

    async def get(self, key: Key) -> Value | None:
        value_key, _ = self._redis_keys(key)
        raw = await self._redis.get(value_key)
        return None if raw is None else self._value_loads(_text(raw))

    async def set(self, key: Key, value: Value) -> None:
        if value is None:
            raise ValueError("None cannot be stored because it represents a cache miss")
        value_key, _ = self._redis_keys(key)
        await self._redis.set(value_key, self._value_dumps(value), ex=self._ttl)

    async def get_or_load(self, key: Key, loader: Loader[Key, Value]) -> Value | None:
        deadline = time.monotonic() + self._wait_timeout
        value_key, lease_key = self._redis_keys(key)

        while True:
            cached = await self.get(key)
            if cached is not None:
                return cached

            token = uuid.uuid4().hex
            acquired = await self._redis.set(lease_key, token, nx=True, px=self._lease_ttl_ms)
            if acquired:
                # A writer may have populated the value between the miss and lease acquisition.
                cached = await self.get(key)
                if cached is not None:
                    await self._release_lease(lease_key, token)
                    return cached
                return await self._load(key, value_key, lease_key, token, loader)

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"timed out waiting for cache key {key!r}")
            await asyncio.sleep(min(self._poll_interval, remaining))

    async def _load(
        self,
        key: Key,
        value_key: str,
        lease_key: str,
        token: str,
        loader: Loader[Key, Value],
    ) -> Value | None:
        try:
            async with asyncio.timeout(self._load_timeout):
                value = await loader(key)
            if value is None:
                return None
            await self._redis.eval(_PUBLISH_VALUE, 2, lease_key, value_key, token, self._value_dumps(value), self._ttl)
            return value
        finally:
            await self._release_lease(lease_key, token)

    async def _release_lease(self, lease_key: str, token: str) -> None:
        await self._redis.eval(_RELEASE_LEASE, 1, lease_key, token)

    def _redis_keys(self, key: Key) -> tuple[str, str]:
        digest = hashlib.sha256(self._key_dumps(key).encode("utf-8")).hexdigest()
        base = f"{self._namespace}:{{{digest}}}"
        return f"{base}:value", f"{base}:lease"
