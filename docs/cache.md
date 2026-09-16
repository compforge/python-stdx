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
independently. This provides queued loading: a waiting caller reuses the owner's successful result, or competes to
become the next owner when the preceding load fails.

A lease holds `__IN_PROGRESS__:<owner token>` during loading. Loader failure or timeout atomically changes only that
owner's lease to `__FAILED__:<error data>` and re-raises the original exception to the owner. Waiting and new callers can
immediately replace a failed lease with their own in-progress token. The failure marker uses the existing lease TTL for
cleanup; callers never wait for that TTL before retrying. Cancellation or a normal `None` result releases the lease.
Failure data is coordination state, never a cached result. Each caller that becomes owner invokes its own loader once;
it does not retry its own loader indefinitely.

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

### Failure diagnostics are application-owned

By default, failed leases record JSON containing the exception class name as `code`, without copying potentially sensitive
exception text. Supply `error_dumps` to attach an application's safe business error code or message. The returned text is
stored after `__FAILED__:` and is diagnostic only; followers never reconstruct or raise another caller's exception from it.
Errors during failure publication are logged without exception payloads and do not replace the original loader exception.

### Owners cannot overwrite their successors

Lease acquisition, failure publication, and result publication use atomic Redis scripts. Publication checks the unique
owner token, so a loader that finishes after its lease expires cannot replace a successor's lease, failure marker, or
value. Values and coordination state use separate keys in the same Redis Cluster slot; payloads cannot collide with
reserved state prefixes. The cache borrows its client and leaves connection limits, command timeouts, and shutdown to
`RedisConnector` or the caller.
