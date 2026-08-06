# python-stdx

`python-stdx` is a small collection of reusable Python infrastructure for services and libraries.

It is organized by capability instead of growing a generic `common` or `utils` package. A module belongs here only when its contract is independent from a specific product or agent runtime.

## Included capabilities

- `python_stdx.iterables`, `mappings`, and `text`: focused, dependency-free helpers that complement the standard library.
- `python_stdx.asyncio`: manages async stream lifecycles and observes event-loop stalls from an independent OS thread.
- `python_stdx.redis.RedisConnector`: creates one native async client for standalone, Sentinel, or Cluster Redis.
- `python_stdx.cache`: tagged invalidation and coordinated loading with in-process and Redis backends.
- `python_stdx.database.Database`: owns a synchronous SQLAlchemy engine and explicit session/transaction lifecycles.
- `python_stdx.scheduler.TaskScheduler`: runs scheduled, one-shot, and triggered tasks through a pluggable distributed store.

## Install

```bash
python -m pip install python-stdx
```

## Utility functions

Utilities are grouped by the value they operate on instead of living in a generic `utils` package:

```python
from python_stdx.iterables import first_where, group_by, unique
from python_stdx.mappings import get_in, map_values, without_keys
from python_stdx.text import byte_length, normalize_whitespace, truncate_middle
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

## Async stream lifecycle

Use `scoped_stream` when an asynchronous iterator owns resources that must be closed deterministically. The default
policy closes the stream without consuming remaining items; `DRAIN` consumes them after a normal scope exit. Exceptions
and cancellation always close the stream without draining it.

```python
from python_stdx.asyncio import StreamExitPolicy, scoped_stream

async with scoped_stream(source, exit_policy=StreamExitPolicy.DRAIN) as stream:
    item = await anext(stream)
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

## Caches

The in-process tagged cache has one optional dependency:

```bash
python -m pip install "python-stdx[cache]"
```

```python
from python_stdx.cache.tagged.memory import MemoryTaggedCache

cache = MemoryTaggedCache[str](max_size=1_000, ttl=300)
await cache.set("user:42", "Ada", tags=["users"])
await cache.invalidate_tag("users")
```

Redis cache backends use the native client returned by `RedisConnector` and are installed through the existing `redis`
extra:

```python
from python_stdx.cache.loading.redis import RedisLoadingCache
from python_stdx.cache.tagged.redis import RedisTaggedCache

tagged = RedisTaggedCache(redis, namespace="profiles")
loading = RedisLoadingCache(redis, namespace="profile-loader")
profile = await loading.get_or_load("42", load_profile)
```

See [the cache model](docs/cache.md) for tagged invalidation and coordinated loading semantics.

## Database lifecycle

The database package is backed by SQLAlchemy and remains optional:

```bash
python -m pip install "python-stdx[database]"
```

```python
from python_stdx.database import Database

database = Database(
    "postgresql+psycopg://user:password@localhost/app",
    pool_size=20,
    max_overflow=10,
    pool_timeout=30,
)

with database.transaction() as session:
    session.execute(...)
```

See [the database lifecycle contract](docs/database.md) for session ownership and extension boundaries.

## Task scheduler

Install the scheduler with the storage capabilities you use:

```bash
python -m pip install "python-stdx[scheduler,database]"
# or: python -m pip install "python-stdx[scheduler,redis]"
```

```python
from python_stdx.scheduler import IntervalSchedule, TaskScheduler, get_schedule_defs, get_task_defs, schedule, task
from python_stdx.scheduler.store.sqlalchemy import SQLTaskStore
from python_stdx.database import Database


@schedule(IntervalSchedule(60))
@task(name="jobs.refresh", timeout=30)
async def refresh() -> None: ...


database = Database("sqlite:///tasks.db", pool_size=5, max_overflow=5)
store = SQLTaskStore(database, auto_migrate=True)
await store.init()

scheduler = TaskScheduler(store, get_task_defs(), get_schedule_defs())
await scheduler.start()
```

See [the scheduler design and lifecycle](docs/scheduler.md) for the three task modes, store contracts, and shutdown behavior.

## License

python-stdx is available under the [MIT License](LICENSE).
