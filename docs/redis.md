# Redis routing and lifecycle

## One client, two capacity budgets

`RedisConnector.connect()` validates connectivity once and returns a shared `RedisClient`. Services inject that
client into caches, schedulers, and their own functions. The connector's configuration selects standalone,
Sentinel, or Cluster; calling code does not branch on deployment shape or choose connection pools.

`max_connections` limits ordinary connections. `max_long_connections` defaults to that value at configuration
construction and limits concurrent blocking calls, blocking batches, and open subscriptions/monitors. Resource
construction and physical connections are lazy: ordinary commands never create the long client, and an unused
`pubsub()` object opens no connection. A running subscription keeps its permit until closed, including after
unsubscribe, because its connection remains reserved for reuse.

In Cluster, redis-py maintains physical pools per node. Ordinary limits are per node; the long-operation budget
is shared across nodes and protocols. Idle long sockets and protocol pools can remain cached on multiple nodes,
so the global operation budget is not a limit on the cluster's total physical socket count.

## Commands and batches

Command methods come from redis-py. The execution boundary classifies blocking list/sorted-set operations and
`XREAD`/`XREADGROUP` with `BLOCK`. Keys, group names, and payloads named `BLOCK` do not affect classification.
Missing/unparseable timeouts pass through for native validation. A business function can alternate blocking
reads and ordinary writes without holding an ordinary connection for the function's whole lifetime.

Pipelines preserve a batch's connection semantics:

- A transaction uses ordinary capacity: Redis does not block commands inside `MULTI`/`EXEC`.
- A nontransactional batch containing a blocking command uses long capacity for the whole batch, preserving
  per-node ordering. Redis Cluster still executes different nodes independently.
- `WATCH` retains its native connection until reset or execution. Blocking between `WATCH` and `MULTI` is rejected;
  `unwatch()` releases the reservation before a new batch is built.
- Cluster transactions and atomic multi-key commands require keys in the same hash slot. A shared hash tag such
  as `{account}:balance` and `{account}:events` expresses that relationship.

`scan_iter()` delegates cursor traversal to the native topology client. Registered scripts use native SHA caching
for individual calls and `EVAL` inside batches, where replay after `NOSCRIPT` could repeat completed writes.

This is a shared command API, not an escape hatch to raw sockets or every redis-py administrative extension.
Use `pubsub()` for regular channel/pattern subscriptions and `monitor()` to monitor one backend-selected server.
Raw subscription commands and replication acknowledgements (`WAIT`/`WAITAOF`) are rejected: acknowledgements
require explicit affinity to prior writes, which arbitrary pooled calls cannot promise. Sharded Pub/Sub and
low-level native-client/connection access are not exposed. Raw `execute_command()` uses the same routing rules;
unknown module commands are ordinary unless they are inside a batch with a recognized blocking command.

## Deadlines and cleanup

Connection establishment, authentication, health checks, and Cluster discovery keep their finite configured
timeouts. Blocking response reads have a command-derived deadline with transport slack; zero means wait until
completion or cancellation. A batch's finite deadline allows for its combined blocking waits. Ordinary commands
continue to use `command_timeout` even while other tasks are blocked.

No transport failure automatically replays a command: a lost `BLPOP` response does not prove that Redis left the
item in the list. Native topology redirects remain enabled because the redirect response says that the command
has not executed on that node. ASKING and its redirected command share one borrowed connection, following the
same affinity rule used by rueidis batches. Cancellation disconnects a pending response before the socket can be
reused, preventing the next caller from consuming an old reply.

Capacity exhaustion raises `RedisPoolExhaustedError`, outside redis-py's connection retry hierarchy. It does not
trigger discovery or reconnect storms. Applications may shed load or apply their own bounded admission policy.

Closing is terminal and idempotent. It first blocks new operations, cancels tracked I/O, releases protocols and
watched pipelines, and then closes data/discovery clients. Cancellation of a close waiter does not cancel shared
cleanup. Retained client, pipeline, and subscription handles cannot reopen sockets after shutdown.

## Implementation boundaries

The public command/lifecycle API owns classification, capacity admission, and shutdown. Topology backends own
native clients and discovery. The transport adapter owns command response deadlines and the small ASK affinity
bridge; RESP parsing, connection reuse, Sentinel discovery, slot routing, and native batch execution remain in
redis-py. There is no product-specific dependency or telemetry requirement in this module.

The supported dependency baseline is redis-py 8.1 through major version 8. Tests start isolated local Redis
processes for all three topologies and cover cancellation, exhaustion, lifecycle races, WATCH, scripts, and
MOVED/ASK behavior. Set `REDIS_SERVER` if the binary is not on PATH; these socket tests explicitly skip when the
binary is unavailable. Cache and scheduler tests remain part of `make test`.
