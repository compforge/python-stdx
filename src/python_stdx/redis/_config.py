"""Topology-neutral Redis connection settings."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Literal


class RedisTopology(str, Enum):
    """Redis deployment shapes supported by :class:`RedisConnector`."""

    STANDALONE = "standalone"
    SENTINEL = "sentinel"
    CLUSTER = "cluster"


@dataclass(frozen=True, slots=True)
class RedisEndpoint:
    """One address used for direct access or topology discovery."""

    host: str
    port: int = 6379

    def __post_init__(self) -> None:
        if not self.host:
            raise ValueError("host must not be empty")
        if not 0 < self.port < 65536:
            raise ValueError("port must be between 1 and 65535")


@dataclass(frozen=True, slots=True)
class RedisConnectionConfig:
    """One configuration shape for standalone, Sentinel, and Cluster Redis."""

    topology: RedisTopology
    endpoints: tuple[RedisEndpoint, ...]
    username: str | None = None
    password: str | None = field(default=None, repr=False)
    database: int = 0
    tls: bool = False
    max_connections: int = 20
    connect_timeout: float = 5.0
    command_timeout: float = 5.0
    health_check_interval: int = 30
    decode_responses: bool = True
    protocol: Literal[2, 3] = 2
    client_name: str | None = None
    sentinel_service: str = "mymaster"
    sentinel_username: str | None = None
    sentinel_password: str | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if not self.endpoints:
            raise ValueError("endpoints must not be empty")
        if self.topology is RedisTopology.STANDALONE and len(self.endpoints) != 1:
            raise ValueError("standalone topology requires exactly one endpoint")
        if self.topology is RedisTopology.CLUSTER and self.database != 0:
            raise ValueError("cluster topology only supports database 0")
        if self.topology is RedisTopology.SENTINEL and not self.sentinel_service:
            raise ValueError("sentinel_service must not be empty")
        if self.database < 0:
            raise ValueError("database must be non-negative")
        if self.max_connections <= 0:
            raise ValueError("max_connections must be positive")
        for name in ("connect_timeout", "command_timeout", "health_check_interval"):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")
