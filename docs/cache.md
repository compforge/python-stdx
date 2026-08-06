# Cache models

## Concepts

`python_stdx.cache` separates two independent cache behaviors:

- A tagged cache associates entries with invalidation tags. Invalidating a tag makes every associated entry miss.
- A loading cache fills a missing value through a caller-supplied loader and coordinates concurrent loads for the same key.

Both contracts use `None` to represent a miss, so implementations do not store `None` as a value.

## Flow

The in-process tagged backend stores values in a bounded TTL cache and maintains tag associations in the same process.
The Redis backend records the tag generations observed when an entry is written. Reads compare those generations with
their current values; tag and namespace invalidation only increment a generation, while stale entries disappear through
their normal TTL.

The Redis loading backend stores values and short-lived load leases under keys in the same Redis Cluster slot. One caller
owns the lease and invokes the loader. Other callers wait for its value, while different keys continue loading
independently. Loader failure, cancellation, or timeout releases the lease so another caller can retry.

## Key design choices

### Cache semantics own their storage adapters

Redis cache implementations live below `python_stdx.cache`, while `python_stdx.redis` remains responsible only for client
construction and lifecycle. Cache backends accept a native async redis-py client and therefore do not branch on standalone,
Sentinel, or Cluster deployment details.

### Invalidation does not scan Redis

Generation-based invalidation avoids reverse indexes whose members outlive expired values. Clearing a namespace is also a
generation change rather than a key scan, so its latency does not grow with entry count.

### Loading is bounded

Redis commands use the timeouts configured on the shared client. Loading, lease, and follower waiting have separate bounds:
the lease must outlive one loader invocation, and a follower raises `TimeoutError` instead of starting unbounded duplicate
work when its wait budget expires.
