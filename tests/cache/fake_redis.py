import asyncio


class FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, str | int] = {}
        self.expirations: dict[str, dict[str, int]] = {}
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> str | int | None:
        async with self._lock:
            return self.values.get(key)

    async def set(
        self,
        key: str,
        value: str,
        *,
        nx: bool = False,
        ex: int | None = None,
        px: int | None = None,
    ) -> bool | None:
        async with self._lock:
            if nx and key in self.values:
                return None
            self.values[key] = value
            self.expirations[key] = {name: ttl for name, ttl in (("ex", ex), ("px", px)) if ttl is not None}
            return True

    async def delete(self, *keys: str) -> int:
        async with self._lock:
            deleted = 0
            for key in keys:
                if key in self.values:
                    deleted += 1
                self.values.pop(key, None)
                self.expirations.pop(key, None)
            return deleted

    async def incr(self, key: str) -> int:
        async with self._lock:
            value = int(self.values.get(key, 0)) + 1
            self.values[key] = value
            return value

    async def eval(self, script: str, numkeys: int, *arguments: object) -> int:
        del script
        async with self._lock:
            if numkeys == 1:
                lease_key, token = arguments
                if self.values.get(str(lease_key)) != token:
                    return 0
                self.values.pop(str(lease_key), None)
                return 1

            lease_key, value_key, token, value, ttl = arguments
            if self.values.get(str(lease_key)) != token:
                return 0
            if str(value_key) not in self.values:
                self.values[str(value_key)] = str(value)
                self.expirations[str(value_key)] = {"ex": int(ttl)}
            self.values.pop(str(lease_key), None)
            return 1
