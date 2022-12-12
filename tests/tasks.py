import json
import time
from math import ceil

import redis

from tasktiger import RetryException, TaskTiger
from tasktiger.retry import fixed
from tasktiger.runner import BaseRunner, DefaultRunner

from .config import DELAY, REDIS_HOST, TEST_DB
from .utils import get_tiger

LONG_TASK_SIGNAL_KEY = "long_task_ok"

tiger = get_tiger()


def simple_task():
    pass


@tiger.task()
def decorated_task(*args, **kwargs):
    pass


# This decorator below must not contain parenthesis
@tiger.task
def decorated_task_simple_func(*args, **kwargs):
    pass


def exception_task():
    raise Exception("this failed")


@tiger.task(queue="other")
def task_on_other_queue():
    pass


def file_args_task(filename, *args, **kwargs):
    with open(filename, "w") as f:
        f.write(json.dumps({"args": args, "kwargs": kwargs}))


@tiger.task(hard_timeout=DELAY)
def long_task_killed():
    time.sleep(DELAY * 2)


@tiger.task(hard_timeout=DELAY * 2)
def long_task_ok():
    # Signal task has started
    with redis.Redis(
        host=REDIS_HOST, db=TEST_DB, decode_responses=True
    ) as conn:
        conn.lpush(LONG_TASK_SIGNAL_KEY, "1")
    time.sleep(DELAY)


def wait_for_long_task():
    """Waits for a long task to start."""
    with redis.Redis(
        host=REDIS_HOST, db=TEST_DB, decode_responses=True
    ) as conn:
        result = conn.blpop(LONG_TASK_SIGNAL_KEY, int(ceil(DELAY * 3)))
    assert result[1] == "1"


@tiger.task(unique=True)
def unique_task(value=None):
    with redis.Redis(
        host=REDIS_HOST, db=TEST_DB, decode_responses=True
    ) as conn:
        conn.lpush("unique_task", value)


@tiger.task(unique=True)
def unique_exception_task(value=None):
    raise Exception("this failed")


@tiger.task(unique_key=("a",))
def unique_key_task(a, b):
    pass


@tiger.task(lock=True)
def locked_task(key, other=None):
    with redis.Redis(
        host=REDIS_HOST, db=TEST_DB, decode_responses=True
    ) as conn:
        data = conn.getset(key, 1)
        if data is not None:
            raise Exception("task failed, key already set")
        time.sleep(DELAY)
        conn.delete(key)


@tiger.task(queue="batch", batch=True)
def batch_task(params):
    with redis.Redis(
        host=REDIS_HOST, db=TEST_DB, decode_responses=True
    ) as conn:
        try:
            conn.rpush("batch_task", json.dumps(params))
        except Exception:
            pass
    if any(p["args"][0] == 10 for p in params if p["args"]):
        raise Exception("exception")


@tiger.task(queue="batch")
def non_batch_task(arg):
    with redis.Redis(
        host=REDIS_HOST, db=TEST_DB, decode_responses=True
    ) as conn:
        conn.rpush("batch_task", arg)

    if arg == 10:
        raise Exception("exception")


def retry_task():
    raise RetryException()


def retry_task_2():
    raise RetryException(method=fixed(DELAY, 1), log_error=False)


def verify_current_task():
    with redis.Redis(
        host=REDIS_HOST, db=TEST_DB, decode_responses=True
    ) as conn:
        try:
            tiger.current_tasks
        except RuntimeError:
            # This is expected (we need to use current_task)
            task = tiger.current_task
            conn.set("task_id", task.id)


@tiger.task(batch=True, queue="batch")
def verify_current_tasks(tasks):
    with redis.Redis(
        host=REDIS_HOST, db=TEST_DB, decode_responses=True
    ) as conn:
        try:
            tasks = tiger.current_task
        except RuntimeError:
            # This is expected (we need to use current_tasks)

            tasks = tiger.current_tasks
            conn.rpush("task_ids", *[t.id for t in tasks])


@tiger.task()
def verify_tasktiger_instance():
    # Not necessarily the same object, but the same configuration.
    config_1 = dict(TaskTiger.current_instance.config)
    config_2 = dict(tiger.config)

    # Changed during the test case, so this may differ.
    config_1.pop("ALWAYS_EAGER")
    config_2.pop("ALWAYS_EAGER")

    assert config_1 == config_2


@tiger.task()
def sleep_task(delay=10):
    time.sleep(delay)


@tiger.task(hard_timeout=1)
def decorated_task_sleep_timeout(delay=10):
    time.sleep(delay)


@tiger.task(max_queue_size=1)
def decorated_task_max_queue_size(*args, **kwargs):
    pass


class StaticTask:
    @staticmethod
    def task():
        pass


class MyRunnerClass(BaseRunner):
    def run_single_task(self, task, hard_timeout):
        assert self.tiger.config == tiger.config
        assert hard_timeout == 300
        assert task.func is simple_task

        with redis.Redis(
            host=REDIS_HOST, db=TEST_DB, decode_responses=True
        ) as conn:
            conn.set("task_id", task.id)

    def run_batch_tasks(self, tasks, hard_timeout):
        assert self.tiger.config == tiger.config
        assert hard_timeout == 300
        assert len(tasks) == 2

        with redis.Redis(
            host=REDIS_HOST, db=TEST_DB, decode_responses=True
        ) as conn:
            conn.set("task_args", ",".join(str(t.args[0]) for t in tasks))

    def run_eager_task(self, task):
        return 123


class MyErrorRunnerClass(DefaultRunner):
    def on_permanent_error(self, task, execution):
        assert task.func is exception_task
        assert execution["exception_name"] == "builtins:Exception"
        with redis.Redis(
            host=REDIS_HOST, db=TEST_DB, decode_responses=True
        ) as conn:
            conn.set("task_id", task.id)
