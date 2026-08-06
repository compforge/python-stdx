import asyncio

from python_stdx.scheduler.decorator import ScheduleDef, TaskDef, TriggerDef
from python_stdx.scheduler.schedule import IntervalSchedule
from python_stdx.scheduler.scheduler import TaskScheduler
from python_stdx.scheduler.types import TaskRetentionPolicy, constant_backoff


class FakeTaskStore:
    def __init__(self):
        self.existing = None
        self.submitted = []
        self.locked = []
        self.dynamic_locked = []
        self.released = []
        self.dynamic_released = []
        self.successes = []
        self.dynamic_successes = []
        self.failures = []
        self.dynamic_failures = []
        self.pending = []
        self.deleted = []
        self.prefixed_cleanup_calls = []

    async def get_oneshot_task_by_biz_name(self, name, biz_name):
        self.lookup = (name, biz_name)
        return self.existing

    async def get_last_run(self, name):
        return None

    async def acquire_lock(self, name, ttl):
        self.locked.append((name, ttl))
        return True

    async def release_lock(self, name):
        self.released.append(name)

    async def acquire_dynamic_task_lock(self, task_id, ttl):
        self.dynamic_locked.append((task_id, ttl))
        return True

    async def release_dynamic_task_lock(self, task_id):
        self.dynamic_released.append(task_id)

    async def record_success(self, name, ts, duration_ms, attempt=1, trace_id=None):
        self.successes.append((name, attempt, trace_id))

    async def record_failure(self, name, error, attempt, ts, trace_id=None):
        self.failures.append((name, error, attempt, trace_id))

    async def record_dynamic_success(self, task_id, ts, duration_ms, attempt=1, trace_id=None):
        self.dynamic_successes.append((task_id, attempt, trace_id))

    async def record_dynamic_failure(self, task_id, error, attempt, ts, trace_id=None):
        self.dynamic_failures.append((task_id, error, attempt, trace_id))

    async def list_pending_oneshot_tasks(self):
        return self.pending

    async def list_pending_triggered_tasks(self):
        return getattr(self, "pending_triggered", [])

    async def submit_oneshot_task(
        self,
        name,
        biz_name,
        schedule,
        timeout,
        max_runs,
        params=None,
    ):
        self.submitted.append((name, biz_name, timeout, max_runs, params))
        return "new-task-id"

    async def submit_triggered_task(
        self,
        name,
        biz_name,
        timeout,
        max_runs,
        params=None,
        *,
        cooldown_seconds=0,
    ):
        # 用单独列表记录 triggered submit，便于测试断言（不混入 oneshot 的 submitted）
        if not hasattr(self, "submitted_triggered"):
            self.submitted_triggered = []
        self.submitted_triggered.append((name, biz_name, timeout, max_runs, params, cooldown_seconds))
        return "new-trigger-task-id"

    async def delete_oneshot_task(self, task_id):
        self.deleted.append(task_id)

    def should_delete_successful_oneshot_task(self, retention_policy):
        return retention_policy.success_retention_seconds == 0

    def should_delete_failed_oneshot_task(self, retention_policy):
        return retention_policy.failure_retention_seconds == 0

    async def cleanup_expired_oneshot_tasks(self, retention_policy):
        self.cleanup_policy = retention_policy
        return 1

    async def cleanup_expired_oneshot_tasks_by_name_prefix(self, retention_policy, task_name_prefix):
        self.prefixed_cleanup_calls.append((retention_policy, task_name_prefix))
        return 2

    async def count_running_oneshot_tasks(self):
        # 测试用 fake：默认无 running，单测可手动 setattr 覆盖
        self.count_running_calls = getattr(self, "count_running_calls", 0) + 1
        return getattr(self, "running_counts", {})


async def noop_task():
    return None


def make_scheduler(store):
    return TaskScheduler(
        store=store,
        task_defs=[
            TaskDef(
                name="note.generate_name.executor",
                timeout=30,
                max_runs=1,
                backoff=constant_backoff,
                func=noop_task,
            )
        ],
        schedule_defs=[],
    )


def test_submit_task_returns_existing_task_for_same_biz_name():
    async def run():
        store = FakeTaskStore()
        store.existing = ("existing-task-id", "note.generate_name.executor", 30, {"note_id": "note-1"})
        scheduler = make_scheduler(store)

        task_id = await scheduler.submit_task(
            name="note.generate_name.executor",
            biz_name="note.generate_name:note-1",
            params={"note_id": "note-1"},
        )

        assert task_id == "existing-task-id"
        assert store.lookup == ("note.generate_name.executor", "note.generate_name:note-1")
        assert store.submitted == []

    asyncio.run(run())


def test_submit_task_with_biz_name_passes_dedupe_key_to_store():
    async def run():
        store = FakeTaskStore()
        scheduler = make_scheduler(store)

        task_id = await scheduler.submit_task(
            name="note.generate_name.executor",
            biz_name="note.generate_name:note-1",
            params={"note_id": "note-1"},
        )

        assert task_id == "new-task-id"
        assert store.submitted == [
            ("note.generate_name.executor", "note.generate_name:note-1", 30, 1, {"note_id": "note-1"})
        ]

    asyncio.run(run())


def test_submit_task_without_biz_name_allows_duplicate_handler_tasks():
    async def run():
        store = FakeTaskStore()
        scheduler = make_scheduler(store)

        task_id = await scheduler.submit_task(
            name="note.generate_name.executor",
            params={"note_id": "note-1"},
        )

        assert task_id == "new-task-id"
        assert store.submitted == [("note.generate_name.executor", None, 30, 1, {"note_id": "note-1"})]

    asyncio.run(run())


def test_interval_schedule_runs_on_first_tick_without_previous_run():
    executed = []

    async def scheduled_task():
        executed.append("ran")

    async def run():
        store = FakeTaskStore()
        scheduler = TaskScheduler(
            store=store,
            task_defs=[
                TaskDef(
                    name="scheduled.task",
                    timeout=30,
                    max_runs=1,
                    backoff=constant_backoff,
                    func=scheduled_task,
                )
            ],
            schedule_defs=[ScheduleDef(task_name="scheduled.task", schedule=IntervalSchedule(60))],
        )

        await scheduler._tick()
        # tick 现在 fire-and-forget,需要 drain 等 in-flight 后台 task 真正落副作用
        await scheduler._drain_in_flight()

        assert executed == ["ran"]
        assert store.locked == [("scheduled.task", 90)]
        assert store.released == ["scheduled.task"]
        assert store.successes == [("scheduled.task", 1, None)]

    asyncio.run(run())


def test_interval_schedule_without_previous_run_is_immediately_due():
    async def run():
        assert await IntervalSchedule(60).next_run(None) == 0.0

    asyncio.run(run())


def test_interval_schedule_jitter_zero_keeps_fixed_interval():
    async def run():
        schedule = IntervalSchedule(60, jitter=0)

        assert await schedule.next_run(100.0) == 160.0

    asyncio.run(run())


def test_interval_schedule_explicit_jitter_is_stable_and_bounded():
    async def run():
        schedule = IntervalSchedule(60, jitter=3)

        first = await schedule.next_run(100.0)
        second = await schedule.next_run(100.0)

        assert first == second
        assert 157.0 <= first < 163.0

    asyncio.run(run())


def test_interval_schedule_default_jitter_is_small_and_bounded():
    async def run():
        schedule = IntervalSchedule(10)

        next_run = await schedule.next_run(100.0)

        assert 108.0 <= next_run < 112.0

    asyncio.run(run())


def test_dynamic_task_uses_registered_name_and_records_by_task_id_without_deleting_retained_success():
    executed = []

    async def dynamic_task(note_id):
        executed.append(note_id)

    async def run():
        store = FakeTaskStore()
        store.pending = [("task-id", "note.generate_name.executor", 30, {"note_id": "note-1"})]
        scheduler = TaskScheduler(
            store=store,
            task_defs=[
                TaskDef(
                    name="note.generate_name.executor",
                    timeout=30,
                    max_runs=1,
                    backoff=constant_backoff,
                    func=dynamic_task,
                )
            ],
            schedule_defs=[],
        )

        await scheduler._tick()
        await scheduler._drain_in_flight()

        assert executed == ["note-1"]
        assert store.locked == []
        assert store.dynamic_locked == [("task-id", 90)]
        assert store.dynamic_successes == [("task-id", 1, None)]
        assert store.dynamic_released == ["task-id"]
        assert store.deleted == []

    asyncio.run(run())


def test_task_name_prefix_is_applied_when_submitting_dynamic_tasks():
    async def run():
        store = FakeTaskStore()
        scheduler = TaskScheduler(
            store=store,
            task_defs=[
                TaskDef(
                    name="note.generate_name.executor",
                    timeout=30,
                    max_runs=1,
                    backoff=constant_backoff,
                    func=noop_task,
                )
            ],
            schedule_defs=[],
            task_name_prefix="local.",
        )

        task_id = await scheduler.submit_task(
            name="note.generate_name.executor",
            params={"note_id": "note-1"},
        )

        assert task_id == "new-task-id"
        assert store.submitted == [("local.note.generate_name.executor", None, 30, 1, {"note_id": "note-1"})]

    asyncio.run(run())


def test_task_name_prefix_routes_pending_dynamic_tasks_to_registered_handler():
    executed = []

    async def dynamic_task(note_id):
        executed.append(note_id)

    async def run():
        store = FakeTaskStore()
        store.pending = [("task-id", "local.note.generate_name.executor", 30, {"note_id": "note-1"})]
        scheduler = TaskScheduler(
            store=store,
            task_defs=[
                TaskDef(
                    name="note.generate_name.executor",
                    timeout=30,
                    max_runs=1,
                    backoff=constant_backoff,
                    func=dynamic_task,
                )
            ],
            schedule_defs=[],
            task_name_prefix="local.",
        )

        await scheduler._tick()
        await scheduler._drain_in_flight()

        assert executed == ["note-1"]
        assert store.dynamic_locked == [("task-id", 90)]
        assert store.dynamic_successes == [("task-id", 1, None)]

    asyncio.run(run())


def test_task_name_prefix_ignores_unscoped_pending_dynamic_tasks():
    executed = []

    async def dynamic_task(note_id):
        executed.append(note_id)

    async def run():
        store = FakeTaskStore()
        store.pending = [
            ("foreign-task-id", "note.generate_name.executor", 30, {"note_id": "foreign"}),
            ("local-task-id", "local.note.generate_name.executor", 30, {"note_id": "local"}),
        ]
        scheduler = TaskScheduler(
            store=store,
            task_defs=[
                TaskDef(
                    name="note.generate_name.executor",
                    timeout=30,
                    max_runs=1,
                    backoff=constant_backoff,
                    func=dynamic_task,
                )
            ],
            schedule_defs=[],
            task_name_prefix="local.",
        )

        await scheduler._tick()
        await scheduler._drain_in_flight()

        assert executed == ["local"]
        assert store.dynamic_locked == [("local-task-id", 90)]
        assert store.dynamic_successes == [("local-task-id", 1, None)]
        assert store.dynamic_released == ["local-task-id"]

    asyncio.run(run())


def test_task_name_prefix_is_applied_when_submitting_triggered_tasks():
    async def run():
        store = FakeTaskStore()
        scheduler = TaskScheduler(
            store=store,
            task_defs=[
                TaskDef(
                    name="resource.summary.executor",
                    timeout=180,
                    max_runs=3,
                    backoff=constant_backoff,
                    func=noop_task,
                )
            ],
            schedule_defs=[],
            trigger_defs=[TriggerDef(task_name="resource.summary.executor", cooldown_seconds=600)],
            task_name_prefix="local.",
        )

        task_id = await scheduler.submit_task(
            name="resource.summary.executor",
            biz_name="resource.summary:res-1",
            params={"resource_id": "res-1"},
        )

        assert task_id == "new-trigger-task-id"
        assert store.submitted_triggered == [
            (
                "local.resource.summary.executor",
                "resource.summary:res-1",
                180,
                3,
                {"resource_id": "res-1"},
                600,
            )
        ]

    asyncio.run(run())


def test_task_name_prefix_routes_pending_triggered_tasks_to_registered_handler():
    executed = []

    async def triggered_task(resource_id):
        executed.append(resource_id)

    async def run():
        store = FakeTaskStore()
        store.pending_triggered = [
            ("foreign-trigger-id", "resource.summary.executor", 180, {"resource_id": "foreign"}),
            ("local-trigger-id", "local.resource.summary.executor", 180, {"resource_id": "local"}),
        ]
        scheduler = TaskScheduler(
            store=store,
            task_defs=[
                TaskDef(
                    name="resource.summary.executor",
                    timeout=180,
                    max_runs=3,
                    backoff=constant_backoff,
                    func=triggered_task,
                )
            ],
            schedule_defs=[],
            trigger_defs=[TriggerDef(task_name="resource.summary.executor", cooldown_seconds=0)],
            task_name_prefix="local.",
        )

        await scheduler._tick()
        await scheduler._drain_in_flight()

        assert executed == ["local"]
        assert store.dynamic_locked == [("local-trigger-id", 240)]
        assert store.dynamic_successes == [("local-trigger-id", 1, None)]
        assert store.dynamic_released == ["local-trigger-id"]

    asyncio.run(run())


def test_dynamic_task_deletes_success_when_success_retention_is_zero():
    async def dynamic_task(note_id):
        del note_id

    async def run():
        store = FakeTaskStore()
        store.pending = [("task-id", "note.generate_name.executor", 30, {"note_id": "note-1"})]
        scheduler = TaskScheduler(
            store=store,
            task_defs=[
                TaskDef(
                    name="note.generate_name.executor",
                    timeout=30,
                    max_runs=1,
                    backoff=constant_backoff,
                    func=dynamic_task,
                )
            ],
            schedule_defs=[],
            retention_policy=TaskRetentionPolicy(success_retention_seconds=0),
        )

        await scheduler._tick()
        await scheduler._drain_in_flight()

        assert store.deleted == ["task-id"]

    asyncio.run(run())


def test_cleanup_expired_tasks_passes_global_retention_policy():
    async def run():
        store = FakeTaskStore()
        scheduler = TaskScheduler(
            store=store,
            task_defs=[
                TaskDef(
                    name="note.generate_name.executor",
                    timeout=30,
                    max_runs=1,
                    backoff=constant_backoff,
                    func=noop_task,
                )
            ],
            schedule_defs=[],
            retention_policy=TaskRetentionPolicy(
                success_retention_seconds=60,
                failure_retention_seconds=120,
            ),
        )

        # cleanup_expired_tasks 遍历所有 Runner；scheduled/triggered no-op 返回 0，
        # 只有 OneshotRunner.cleanup 实际调 store。最终 deleted_count 来自 oneshot。
        deleted_count = await scheduler.cleanup_expired_tasks()

        assert deleted_count == 1
        assert store.cleanup_policy == TaskRetentionPolicy(
            success_retention_seconds=60,
            failure_retention_seconds=120,
        )

    asyncio.run(run())


def test_cleanup_expired_tasks_uses_task_name_prefix_when_configured():
    async def run():
        store = FakeTaskStore()
        retention_policy = TaskRetentionPolicy(
            success_retention_seconds=60,
            failure_retention_seconds=120,
        )
        scheduler = TaskScheduler(
            store=store,
            task_defs=[
                TaskDef(
                    name="note.generate_name.executor",
                    timeout=30,
                    max_runs=1,
                    backoff=constant_backoff,
                    func=noop_task,
                )
            ],
            schedule_defs=[],
            retention_policy=retention_policy,
            task_name_prefix="local.",
        )

        deleted_count = await scheduler.cleanup_expired_tasks()

        assert deleted_count == 2
        assert store.prefixed_cleanup_calls == [(retention_policy, "local.")]
        assert not hasattr(store, "cleanup_policy")

    asyncio.run(run())


def test_task_retention_policy_rejects_negative_values():
    from python_stdx.scheduler.types import TaskRetentionPolicy

    # success_retention_seconds < 0 应抛出 ValueError
    try:
        TaskRetentionPolicy(success_retention_seconds=-1)
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "success_retention_seconds" in str(e)

    # failure_retention_seconds < 0 应抛出 ValueError
    try:
        TaskRetentionPolicy(failure_retention_seconds=-1)
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "failure_retention_seconds" in str(e)


def test_dynamic_task_preserves_failed_task_by_default():
    """失败任务默认保留，不立即删除。"""

    async def run():
        store = FakeTaskStore()
        # 默认 retention_policy（成功 3 天，失败 10 天）
        scheduler = TaskScheduler(
            store=store,
            task_defs=[
                TaskDef(
                    name="note.generate_name.executor",
                    timeout=30,
                    max_runs=1,
                    backoff=constant_backoff,
                    func=noop_task,
                )
            ],
            schedule_defs=[],
        )

        # 模拟任务执行并返回失败
        failed_task_id = "failed-task-id"
        store.pending = [(failed_task_id, "note.generate_name.executor", 30, {"note_id": "note-1"})]

        # 让 OneshotRunner._run_one 返回 False（模拟失败），观察 retention 决策
        async def mock_run_one(*args, **kwargs):
            return False

        scheduler._oneshot._run_one = mock_run_one
        await scheduler._tick()
        await scheduler._drain_in_flight()

        # 失败任务不应被立即删除（默认 retention > 0）
        assert store.deleted == [], f"Failed task should not be deleted immediately, got {store.deleted}"

    asyncio.run(run())


def test_dynamic_task_deletes_failed_when_failure_retention_is_zero():
    """失败任务 failure_retention_seconds=0 时立即删除。"""

    async def run():
        store = FakeTaskStore()
        failed_task_id = "failed-task-id"
        store.pending = [(failed_task_id, "note.generate_name.executor", 30, {"note_id": "note-1"})]

        scheduler = TaskScheduler(
            store=store,
            task_defs=[
                TaskDef(
                    name="note.generate_name.executor",
                    timeout=30,
                    max_runs=1,
                    backoff=constant_backoff,
                    func=noop_task,
                )
            ],
            schedule_defs=[],
            retention_policy=TaskRetentionPolicy(failure_retention_seconds=0),
        )

        # 让 OneshotRunner._run_one 返回 False（模拟失败），观察 retention 决策
        async def mock_run_one(*args, **kwargs):
            return False

        scheduler._oneshot._run_one = mock_run_one
        await scheduler._tick()
        await scheduler._drain_in_flight()

        # 失败任务应被立即删除（failure_retention_seconds=0）
        assert store.deleted == [failed_task_id], f"Failed task should be deleted, got {store.deleted}"

    asyncio.run(run())


# ── triggered + cooldown 行为 ────────────────────────────────────────────────


def _make_triggered_scheduler(store, cooldown_seconds: int = 0) -> TaskScheduler:
    return TaskScheduler(
        store=store,
        task_defs=[
            TaskDef(
                name="resource.summary.executor",
                timeout=180,
                max_runs=3,
                backoff=constant_backoff,
                func=noop_task,
            )
        ],
        schedule_defs=[],
        trigger_defs=[TriggerDef(task_name="resource.summary.executor", cooldown_seconds=cooldown_seconds)],
    )


def test_submit_triggered_task_passes_cooldown_to_store():
    """scheduler 把 @trigger 上的 cooldown_seconds 透传给 store.submit_triggered_task。"""

    async def run():
        store = FakeTaskStore()
        scheduler = _make_triggered_scheduler(store, cooldown_seconds=600)

        task_id = await scheduler.submit_task(
            name="resource.summary.executor",
            biz_name="resource.summary:res-1",
            params={"resource_id": "res-1"},
        )

        assert task_id == "new-trigger-task-id"
        assert store.submitted_triggered == [
            (
                "resource.summary.executor",
                "resource.summary:res-1",
                180,
                3,
                {"resource_id": "res-1"},
                600,
            )
        ]

    asyncio.run(run())


def test_submit_triggered_task_defaults_cooldown_to_zero_when_not_declared():
    """未声明 cooldown_seconds 时按 0 透传，行为与历史 @trigger() 等价。"""

    async def run():
        store = FakeTaskStore()
        scheduler = _make_triggered_scheduler(store, cooldown_seconds=0)

        await scheduler.submit_task(
            name="resource.summary.executor",
            biz_name="resource.summary:res-1",
        )

        assert store.submitted_triggered[-1][-1] == 0

    asyncio.run(run())


def test_trigger_decorator_rejects_negative_cooldown():
    """cooldown_seconds < 0 直接在装饰器构造期抛错，避免运行期才发现配置错误。"""
    import pytest

    from python_stdx.scheduler.decorator import trigger

    with pytest.raises(ValueError, match="cooldown_seconds"):
        trigger(cooldown_seconds=-1)


# ── @oneshot(max_concurrency=N) ────────────────────────────────────────────


def _make_oneshot_scheduler(store, *, max_concurrency: int):
    """构造一个声明了 max_concurrency 的 oneshot 测试 scheduler。"""
    from python_stdx.scheduler.decorator import OneshotDef

    return TaskScheduler(
        store=store,
        task_defs=[
            TaskDef(
                name="resource.export.executor",
                timeout=30,
                max_runs=1,
                backoff=constant_backoff,
                func=noop_task,
            )
        ],
        schedule_defs=[],
        oneshot_defs=[OneshotDef(task_name="resource.export.executor", max_concurrency=max_concurrency)],
    )


def test_oneshot_max_concurrency_skips_pending_when_running_at_cap():
    """running_count >= cap 时本轮 tick 直接跳过该 handler 所有 pending 行。"""

    async def run():
        store = FakeTaskStore()
        store.pending = [
            ("task-1", "resource.export.executor", 30, None),
            ("task-2", "resource.export.executor", 30, None),
        ]
        store.running_counts = {"resource.export.executor": 2}
        scheduler = _make_oneshot_scheduler(store, max_concurrency=2)

        await scheduler._oneshot.tick()

        # cap 已满，不应有任何 acquire / 执行
        assert store.dynamic_locked == []
        assert store.dynamic_successes == []

    asyncio.run(run())


def test_oneshot_max_concurrency_local_inflight_throttles_within_single_tick():
    """fire-and-forget dispatch:同一 tick 内 local in-flight 计数也参与 cap 判定。

    旧串行实现下,本测试断言"3 行都 dispatch + 每行都查一次 cluster count"。
    新并发实现下,iter1/iter2 dispatch 后 in_flight 立即 +1,iter3 看到 local
    in_flight 已达 cap 就短路,不再问 cluster,也不 dispatch。这是必要变化:
    create_task 派发的 bg task 尚未真正写锁前,cluster SQL 计数还来不及上升,
    若不加本地视角,单 pod 单 tick 内会 dispatch 远超 cap。
    """

    async def run():
        store = FakeTaskStore()
        store.pending = [
            ("task-1", "resource.export.executor", 30, None),
            ("task-2", "resource.export.executor", 30, None),
            ("task-3", "resource.export.executor", 30, None),
        ]
        store.running_counts = {"resource.export.executor": 0}
        scheduler = _make_oneshot_scheduler(store, max_concurrency=2)

        await scheduler._oneshot.tick()
        # 派发到了 task-1 / task-2,task-3 被 local cap 挡住;还没 drain
        # bg task 之前,断言 dispatch 形态稳定
        assert [tid for tid, _ in store.dynamic_locked] == ["task-1", "task-2"]
        # iter1/iter2 各查一次 cluster,iter3 short-circuit 不查
        assert store.count_running_calls == 2

        await scheduler._oneshot.drain()

    asyncio.run(run())


def test_oneshot_without_max_concurrency_skips_count_query_and_dispatches_all():
    """未声明 max_concurrency 的 handler 完全不查 running_count，行为与历史一致。"""

    async def run():
        store = FakeTaskStore()
        store.pending = [
            ("task-1", "note.generate_name.executor", 30, None),
            ("task-2", "note.generate_name.executor", 30, None),
        ]
        # 即使 store 报告了 cluster 级 running，也不应被消费
        store.running_counts = {"note.generate_name.executor": 99}
        # _make_scheduler 不传 oneshot_defs，相当于该 handler 不限并发
        scheduler = make_scheduler(store)

        await scheduler._oneshot.tick()

        # 全部 dispatch；count 查询完全不被触发，避免给未声明 cap 的 handler 增加 DB 开销
        assert [tid for tid, _ in store.dynamic_locked] == ["task-1", "task-2"]
        assert getattr(store, "count_running_calls", 0) == 0

        await scheduler._oneshot.drain()

    asyncio.run(run())


def test_oneshot_decorator_registers_max_concurrency_and_rejects_invalid_value():
    import pytest

    from python_stdx.scheduler.decorator import _ONESHOT_DEFS, _TASK_DEFS, oneshot, task

    # 隔离全局注册表，避免污染其他测试
    saved_tasks = dict(_TASK_DEFS)
    saved_oneshots = list(_ONESHOT_DEFS)
    _TASK_DEFS.clear()
    _ONESHOT_DEFS.clear()
    try:

        @oneshot(max_concurrency=3)
        @task(name="t.unit.test_oneshot_register", timeout=10, max_runs=1)
        async def _handler():
            return None

        assert any(od.task_name == "t.unit.test_oneshot_register" and od.max_concurrency == 3 for od in _ONESHOT_DEFS)

        with pytest.raises(ValueError, match="max_concurrency"):
            oneshot(max_concurrency=0)
    finally:
        _TASK_DEFS.clear()
        _TASK_DEFS.update(saved_tasks)
        _ONESHOT_DEFS.clear()
        _ONESHOT_DEFS.extend(saved_oneshots)


def test_oneshot_decorator_conflicts_with_trigger_and_schedule():
    """同一个 task 不允许同时声明 @oneshot 与 @trigger / @schedule。"""
    import pytest

    from python_stdx.scheduler.decorator import (
        _ONESHOT_DEFS,
        _SCHEDULE_DEFS,
        _TASK_DEFS,
        _TRIGGER_DEFS,
        oneshot,
        schedule,
        task,
        trigger,
    )

    saved_tasks = dict(_TASK_DEFS)
    saved_oneshots = list(_ONESHOT_DEFS)
    saved_triggers = list(_TRIGGER_DEFS)
    saved_schedules = list(_SCHEDULE_DEFS)
    _TASK_DEFS.clear()
    _ONESHOT_DEFS.clear()
    _TRIGGER_DEFS.clear()
    _SCHEDULE_DEFS.clear()
    try:

        @trigger()
        @task(name="t.unit.conflict_trigger", timeout=10, max_runs=1)
        async def trigger_handler():
            return None

        with pytest.raises(ValueError, match="@oneshot"):
            oneshot(max_concurrency=2)(trigger_handler)

        @schedule(IntervalSchedule(60))
        @task(name="t.unit.conflict_schedule", timeout=10, max_runs=1)
        async def scheduled_handler():
            return None

        with pytest.raises(ValueError, match="@oneshot"):
            oneshot(max_concurrency=2)(scheduled_handler)
    finally:
        _TASK_DEFS.clear()
        _TASK_DEFS.update(saved_tasks)
        _ONESHOT_DEFS.clear()
        _ONESHOT_DEFS.extend(saved_oneshots)
        _TRIGGER_DEFS.clear()
        _TRIGGER_DEFS.extend(saved_triggers)
        _SCHEDULE_DEFS.clear()
        _SCHEDULE_DEFS.extend(saved_schedules)


# ── 并发派发与 shutdown ────────────────────────────────────────────────────


def test_oneshot_tick_dispatches_handlers_concurrently_not_serially():
    """5 个 oneshot handler 各 sleep 0.3s,tick + drain 总耗时应远小于串行 1.5s。

    fire-and-forget dispatch 的核心收益:慢 handler 不再阻塞 tick loop,也不再
    阻塞同一 tick 内其他行的 dispatch。
    """

    async def slow_handler():
        await asyncio.sleep(0.3)

    async def run():
        store = FakeTaskStore()
        store.pending = [(f"task-{i}", "slow.executor", 30, None) for i in range(5)]
        scheduler = TaskScheduler(
            store=store,
            task_defs=[
                TaskDef(
                    name="slow.executor",
                    timeout=30,
                    max_runs=1,
                    backoff=constant_backoff,
                    func=slow_handler,
                )
            ],
            schedule_defs=[],
        )

        import time as _time

        t0 = _time.monotonic()
        await scheduler._oneshot.tick()
        # tick 本身应秒级返回——只 dispatch 不等 handler
        tick_only = _time.monotonic() - t0
        assert tick_only < 0.2, f"tick should return fast, got {tick_only:.3f}s"

        # drain 等所有 5 个并发跑完;串行至少 1.5s,并发约 0.3s
        await scheduler._oneshot.drain()
        total = _time.monotonic() - t0
        assert total < 0.8, f"5 concurrent handlers should finish in ~0.3s, got {total:.3f}s"
        assert len(store.dynamic_successes) == 5

    asyncio.run(run())


def test_scheduler_stop_drains_in_flight_within_grace():
    """scheduler.stop() 在 grace 内等 in-flight handler 跑完,锁正常释放。"""

    finished = []

    async def short_handler(note_id):
        await asyncio.sleep(0.05)
        finished.append(note_id)

    async def run():
        store = FakeTaskStore()
        store.pending = [("task-id", "note.generate_name.executor", 30, {"note_id": "n1"})]
        scheduler = TaskScheduler(
            store=store,
            task_defs=[
                TaskDef(
                    name="note.generate_name.executor",
                    timeout=30,
                    max_runs=1,
                    backoff=constant_backoff,
                    func=short_handler,
                )
            ],
            schedule_defs=[],
        )
        # 手工触发一次 dispatch
        await scheduler._oneshot.tick()
        # 不等 drain,直接 shutdown,grace 足够
        await scheduler._oneshot.shutdown(grace_seconds=1.0)

        assert finished == ["n1"]
        assert store.dynamic_released == ["task-id"]

    asyncio.run(run())


def test_scheduler_stop_cancels_in_flight_when_grace_exceeded():
    """grace 超时后 in-flight handler 被 cancel,scheduler 不卡退出。"""

    cancelled_marker = []

    async def stuck_handler():
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            cancelled_marker.append("cancelled")
            raise

    async def run():
        store = FakeTaskStore()
        store.pending = [("task-id", "stuck.executor", 30, None)]
        scheduler = TaskScheduler(
            store=store,
            task_defs=[
                TaskDef(
                    name="stuck.executor",
                    timeout=60,
                    max_runs=1,
                    backoff=constant_backoff,
                    func=stuck_handler,
                )
            ],
            schedule_defs=[],
        )
        await scheduler._oneshot.tick()

        import time as _time

        t0 = _time.monotonic()
        await scheduler._oneshot.shutdown(grace_seconds=0.1)
        elapsed = _time.monotonic() - t0
        # grace 0.1s + cancel 兜底,总耗时应 < 1s,不会等 30s
        assert elapsed < 1.0, f"shutdown should not block past grace, got {elapsed:.3f}s"
        assert cancelled_marker == ["cancelled"]

    asyncio.run(run())


def test_oneshot_handler_exception_does_not_leak_to_create_task():
    """handler 抛异常被 _run_one_and_finalize 吞掉,锁照常释放,in_flight 清空。"""

    async def crashing_handler():
        raise RuntimeError("boom")

    async def run():
        store = FakeTaskStore()
        store.pending = [("task-id", "crash.executor", 30, None)]
        scheduler = TaskScheduler(
            store=store,
            task_defs=[
                TaskDef(
                    name="crash.executor",
                    timeout=30,
                    max_runs=1,
                    backoff=constant_backoff,
                    func=crashing_handler,
                )
            ],
            schedule_defs=[],
        )

        await scheduler._oneshot.tick()
        await scheduler._oneshot.drain()

        # Executor 把 RuntimeError 转成 failure 路径,store 写 dynamic_failure
        assert len(store.dynamic_failures) == 1
        # 锁释放
        assert store.dynamic_released == ["task-id"]
        # in-flight 已经被 done_callback 清空
        assert scheduler._oneshot.in_flight_count() == 0

    asyncio.run(run())


def test_oneshot_local_inflight_blocks_redispatch_same_tick():
    """同一 task_id 在 in-flight 中时,同 tick 不会被重复 dispatch。"""

    started = []

    async def blocking_handler():
        await asyncio.sleep(0.5)
        started.append("ran")

    async def run():
        store = FakeTaskStore()
        store.pending = [
            ("task-id", "block.executor", 30, None),
            ("task-id", "block.executor", 30, None),  # 同 id 被 list 返回两次(防御性测试)
        ]
        scheduler = TaskScheduler(
            store=store,
            task_defs=[
                TaskDef(
                    name="block.executor",
                    timeout=30,
                    max_runs=1,
                    backoff=constant_backoff,
                    func=blocking_handler,
                )
            ],
            schedule_defs=[],
        )

        await scheduler._oneshot.tick()
        # 同一 id 只 acquire 一次,第二次被 in_flight short-circuit
        assert store.dynamic_locked == [("task-id", 90)]

        await scheduler._oneshot.drain()

    asyncio.run(run())
