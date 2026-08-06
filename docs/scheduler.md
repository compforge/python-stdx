# Task scheduler

## Concepts

The scheduler separates task definitions, dispatch policy, execution mechanics, and persistence:

```text
TaskScheduler
  ├── ScheduledRunner
  ├── OneshotRunner
  ├── TriggeredRunner
  ├── Executor
  └── TaskStore
```

Tasks have three distinct identities and lifecycles:

| Mode | Trigger | Identity | Lifecycle |
|---|---|---|---|
| scheduled | interval or cron | task name | one long-lived row |
| oneshot | `submit_task()` | submission ID | one row per submission |
| triggered | `submit_task()` | task name + business key | one row reactivated by later submissions |

The scheduler provides at-least-once execution. Handlers must be idempotent because an expired lock can be acquired by another process after a crash or long stall.

## Flow

Decorators register task definitions before scheduler construction. Each scheduler tick asks the three runners to discover eligible rows and acquire their locks. A runner dispatches the handler into a background asyncio task and returns immediately; the shared executor applies parameter binding, timeout, retry, backoff, and OpenTelemetry spans.

`TaskScheduler.stop()` first stops new ticks, then gives in-flight tasks a grace period to finish. Tasks still running after the grace period are cancelled.

## Stores

`TaskStore` is the persistence contract. `NamespacedTaskStore` adds a task-name prefix at the storage boundary, allowing multiple environments to share one physical store without changing registered handler names.

Two adapters are included:

- `RedisTaskStore` accepts the native async redis-py client returned by `RedisConnector`. It supports scheduled and oneshot tasks and keeps only lightweight history.
- `SQLTaskStore` accepts `python_stdx.database.Database`. It supports all three task modes, persistent history, retention cleanup, and development-time schema creation.

Production SQL deployments should manage the task table through the application's migration system. `auto_migrate=True` is intended for development and tests.

## Key design choices

### Task modes remain separate

Scheduled, oneshot, and triggered tasks share locking and execution mechanics, but their identity and completion rules differ. Separate runners keep those state transitions out of the scheduler's control loop.

### Execution is independent from persistence

`Executor` does not hold a store. It returns an `ExecutionResult`; the runner decides which row to update and whether the row should be retained or deleted.

### Locks recover crashed workers

Every acquired lock has a TTL derived from the handler timeout. A row left in a running state becomes eligible again after that TTL, so another scheduler instance can recover it without a separate reaper process.

### Triggered submissions are coalesced

A triggered task has a stable `(task_name, biz_name)` identity. Submitting it again updates its request time instead of creating another row. A cooldown can delay the next eligible run while preserving the latest request.
