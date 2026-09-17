import pytest

from python_stdx.redis import RedisConnectionConfig, RedisEndpoint, RedisTopology


def config(topology: RedisTopology, *ports: int, **updates: object) -> RedisConnectionConfig:
    values: dict[str, object] = {
        "topology": topology,
        "endpoints": tuple(RedisEndpoint("redis.local", port) for port in ports),
    }
    values.update(updates)
    return RedisConnectionConfig(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("topology", "ports", "updates", "message"),
    [
        (RedisTopology.CLUSTER, (7000,), {"database": 1}, "database 0"),
        (RedisTopology.STANDALONE, (6379, 6380), {}, "exactly one endpoint"),
        (RedisTopology.SENTINEL, (26379,), {"sentinel_service": ""}, "sentinel_service"),
    ],
)
def test_config_rejects_topology_specific_mistakes(
    topology: RedisTopology,
    ports: tuple[int, ...],
    updates: dict[str, object],
    message: str,
):
    with pytest.raises(ValueError, match=message):
        config(topology, *ports, **updates)


def test_config_hides_credentials_from_repr():
    redis_config = config(
        RedisTopology.STANDALONE,
        6379,
        password="data-password",
        sentinel_password="sentinel-password",
    )

    rendered = repr(redis_config)
    assert "data-password" not in rendered
    assert "sentinel-password" not in rendered


@pytest.mark.parametrize("capacity", [1, 20, 500])
def test_long_capacity_defaults_at_config_initialization(capacity):
    assert config(RedisTopology.STANDALONE, 6379, max_connections=capacity).max_long_connections == capacity
    assert (
        config(RedisTopology.STANDALONE, 6379, max_connections=capacity, max_long_connections=7).max_long_connections
        == 7
    )


@pytest.mark.parametrize("maximum", [0, -1])
def test_invalid_long_capacity(maximum):
    with pytest.raises(ValueError, match="max_long_connections"):
        config(RedisTopology.STANDALONE, 6379, max_long_connections=maximum)


@pytest.mark.parametrize("name", ["command_timeout", "connect_timeout", "health_check_interval"])
@pytest.mark.parametrize("value", [0, float("nan"), float("inf")])
def test_timeouts_are_finite_positive(name, value):
    with pytest.raises(ValueError, match=name):
        config(RedisTopology.STANDALONE, 6379, **{name: value})
