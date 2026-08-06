# python-stdx

`python-stdx` is a small collection of reusable Python infrastructure for services and libraries.

It is organized by capability instead of growing a generic `common` or `utils` package. A module belongs here only when its contract is independent from a specific product or agent runtime.

## Included capabilities

- `python_stdx.asyncio.EventLoopWatchdog`: observes event-loop progress from an independent OS thread and reports stalls and recovery.
- `python_stdx.redis.RedisConnector`: creates one native async client for standalone, Sentinel, or Cluster Redis.

## Install

```bash
python -m pip install python-stdx
```

## Event-loop watchdog

The event loop must call `pulse()` from work scheduled on that loop. The watchdog thread only observes pulse freshness; it never creates a healthy signal on behalf of a stalled loop.

```python
from python_stdx.asyncio import EventLoopWatchdog

watchdog = EventLoopWatchdog(timeout=30.0)
watchdog.start()

# Call this periodically from the event loop being observed.
watchdog.pulse()

watchdog.stop()
```

## Redis client

Redis support is optional:

```bash
python -m pip install "python-stdx[redis]"
```

`python-stdx` configures the official redis-py client instead of wrapping its command API. `RedisConnector` hides standalone, Sentinel, and Cluster construction behind one lifecycle and returns a native client with the full redis-py command surface.

```python
from python_stdx.redis import RedisConnectionConfig, RedisConnector, RedisEndpoint, RedisTopology

connector = RedisConnector(
    RedisConnectionConfig(
        topology=RedisTopology.STANDALONE,
        endpoints=(RedisEndpoint("localhost", 6379),),
        max_connections=20,
        connect_timeout=5.0,
        command_timeout=5.0,
    )
)

redis = await connector.connect()
await redis.ping()
await connector.aclose()
```
