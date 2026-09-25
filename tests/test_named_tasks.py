import pytest

from tasktiger import Task, TaskDispatch, TaskImportError, TaskTiger, Worker
from tasktiger.executor import SyncExecutor
from tasktiger.runner import DefaultRunner

from .tasks import simple_task


def test_enqueue_dispatches_named_task(tiger):
    def handler(value, *, suffix):
        tiger.connection.set("named_task_result", value + suffix)

    tiger.set_dispatch(lambda name: handler if name == "example.named" else None)
    task = tiger.enqueue(
        "example.named", args=["hello"], kwargs={"suffix": " world"}, queue="named"
    )

    assert task.serialized_func == "example.named"
    assert task.data["args"] == ["hello"]
    assert task.data["kwargs"] == {"suffix": " world"}
    assert Task.from_id(tiger, "named", "queued", task.id).func is handler

    Worker(tiger, executor_class=SyncExecutor).run(once=True)
    assert tiger.connection.get("named_task_result") == "hello world"


def test_named_task_runs_in_default_worker(tiger):
    def handler(value):
        tiger.connection.set("forked_named_task_result", value)

    tiger.set_dispatch(lambda name: handler if name == "example.forked" else None)
    tiger.enqueue("example.forked", args=["done"])

    Worker(tiger).run(once=True)
    assert tiger.connection.get("forked_named_task_result") == "done"


def test_named_task_runs_eagerly(tiger):
    called = []
    tiger.config["ALWAYS_EAGER"] = True
    tiger.set_dispatch(lambda name: called.append if name == "example.eager" else None)

    tiger.enqueue("example.eager", args=["done"])

    assert called == ["done"]


def test_named_batch_dispatch(tiger):
    calls = []
    tiger.config["BATCH_QUEUES"]["named"] = 2

    def handler(params):
        calls.append(params)

    tiger.set_dispatch(
        lambda name: (
            TaskDispatch(handler, batch=True) if name == "example.batch" else None
        )
    )
    tiger.enqueue("example.batch", kwargs={"value": "one"}, queue="named")
    tiger.enqueue("example.batch", kwargs={"value": "two"}, queue="named")

    Worker(tiger, executor_class=SyncExecutor).run(once=True)

    assert len(calls) == 1
    assert sorted(call["kwargs"]["value"] for call in calls[0]) == ["one", "two"]


def test_named_batch_runs_eagerly_without_redis():
    calls = []
    tiger = TaskTiger(lazy_init=True)
    tiger.set_dispatch(
        lambda name: (
            TaskDispatch(calls.append, batch=True)
            if name == "example.eager_batch"
            else None
        )
    )

    task = Task(
        tiger,
        name="example.eager_batch",
        kwargs={"value": "one"},
        queue="named",
    )
    assert task.is_batch is True
    DefaultRunner(tiger).run_eager_task(task)

    assert calls == [[{"args": [], "kwargs": {"value": "one"}}]]


def test_dispatch_can_override_legacy_name(tiger):
    called = []

    def handler():
        called.append(True)

    tiger.set_dispatch(
        lambda name: handler if name == "tests.tasks:simple_task" else None
    )
    tiger.config["ALWAYS_EAGER"] = True

    tiger.delay(simple_task)

    assert called == [True]


def test_dispatch_miss_uses_legacy_import(tiger):
    looked_up = []

    def dispatch(name):
        looked_up.append(name)
        return None

    tiger.set_dispatch(dispatch)
    task = tiger.delay(simple_task)

    assert Task.from_id(tiger, "default", "queued", task.id).func is simple_task
    assert looked_up == ["tests.tasks:simple_task"]


def test_enqueue_importable_name_uses_legacy_import(tiger):
    tiger.set_dispatch(lambda name: None)
    task = tiger.enqueue("tests.tasks:simple_task")

    assert Task.from_id(tiger, "default", "queued", task.id).func is simple_task


def test_unrouted_name_uses_legacy_import(tiger):
    task = tiger.enqueue("not.an.import.path")

    with pytest.raises(TaskImportError):
        Task.from_id(tiger, "default", "queued", task.id).func


def test_named_task_unique_ids_use_name(tiger):
    first = tiger.enqueue("example.first", args=[1], unique=True)
    duplicate = tiger.enqueue("example.first", args=[1], unique=True)
    other = tiger.enqueue("example.other", args=[1], unique=True)

    assert first.id == duplicate.id
    assert first.id != other.id


def test_dispatch_errors_do_not_fall_back_to_import(tiger):
    def dispatch(name):
        raise RuntimeError("route error")

    tiger.set_dispatch(dispatch)
    task = tiger.enqueue("tests.tasks:simple_task")

    with pytest.raises(RuntimeError, match="route error"):
        Task.from_id(tiger, "default", "queued", task.id).func
