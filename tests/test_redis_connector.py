from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from python_stdx.redis import RedisConnectionConfig, RedisConnector, RedisEndpoint, RedisTopology


def config(topology: RedisTopology, *ports: int, **updates: object) -> RedisConnectionConfig:
    values: dict[str, object] = {
        "topology": topology,
        "endpoints": tuple(RedisEndpoint("redis.local", port) for port in ports),
    }
    values.update(updates)
    return RedisConnectionConfig(**values)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_connector_builds_and_reuses_standalone_client():
    client = MagicMock()
    client.ping = AsyncMock()
    client.aclose = AsyncMock()
    connector = RedisConnector(config(RedisTopology.STANDALONE, 6379, database=2))

    with patch("python_stdx.redis._connector.Redis", return_value=client) as redis_class:
        assert await connector.connect() is client
        assert await connector.connect() is client
        await connector.aclose()

    redis_class.assert_called_once()
    assert redis_class.call_args.kwargs["db"] == 2
    client.ping.assert_awaited_once()
    client.aclose.assert_awaited_once()


@pytest.mark.asyncio
async def test_connector_hides_cluster_initialization():
    client = MagicMock()
    client.initialize = AsyncMock()
    client.ping = AsyncMock()
    client.aclose = AsyncMock()
    connector = RedisConnector(config(RedisTopology.CLUSTER, 7000, 7001))

    with (
        patch("python_stdx.redis._connector.RedisCluster", return_value=client),
        patch("python_stdx.redis._connector.ClusterNode") as cluster_node,
    ):
        assert await connector.connect() is client

    assert cluster_node.call_count == 2
    client.initialize.assert_awaited_once()
    client.ping.assert_awaited_once()


@pytest.mark.asyncio
async def test_connector_hides_sentinel_discovery_and_closes_its_clients():
    client = MagicMock()
    client.ping = AsyncMock()
    client.aclose = AsyncMock()
    discovery_client = MagicMock()
    discovery_client.aclose = AsyncMock()
    sentinel = MagicMock()
    sentinel.master_for.return_value = client
    sentinel.sentinels = [discovery_client]
    connector = RedisConnector(
        config(
            RedisTopology.SENTINEL,
            26379,
            26380,
            sentinel_service="primary",
            username="data-user",
            password="data-password",
            sentinel_username="sentinel-user",
            sentinel_password="sentinel-password",
        )
    )

    with patch("python_stdx.redis._connector.Sentinel", return_value=sentinel):
        assert await connector.connect() is client
        await connector.aclose()

    sentinel.master_for.assert_called_once()
    assert sentinel.master_for.call_args.args == ("primary",)
    client.aclose.assert_awaited_once()
    discovery_client.aclose.assert_awaited_once()


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
