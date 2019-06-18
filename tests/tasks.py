import json
from math import ceil
import time

import redis

from tasktiger import RetryException
from tasktiger.retry import fixed

from .config import DELAY, TEST_DB
from .utils import get_tiger


LONG_TASK_SIGNAL_KEY = 'long_task_ok'

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
    raise Exception('this failed')


@tiger.task(queue='other')
def task_on_other_queue():
    pass


def file_args_task(filename, *args, **kwargs):
    open(filename, 'w').write(json.dumps({
        'args': args,
        'kwargs': kwargs,
    }))


@tiger.task(hard_timeout=DELAY)
def long_task_killed():
    time.sleep(DELAY * 2)


@tiger.task(hard_timeout=DELAY * 2)
def long_task_ok():
    # Signal task has started
    conn = redis.Redis(db=TEST_DB, decode_responses=True)
    conn.lpush(LONG_TASK_SIGNAL_KEY, '1')

    time.sleep(DELAY)


def wait_for_long_task():
    """Waits for a long task to start."""
    conn = redis.Redis(db=TEST_DB, decode_responses=True)
    result = conn.blpop(LONG_TASK_SIGNAL_KEY, int(ceil(DELAY * 3)))
    assert result[1] == '1'


@tiger.task(unique=True)
def unique_task(value=None):
    conn = redis.Redis(db=TEST_DB, decode_responses=True)
    conn.lpush('unique_task', value)


@tiger.task(lock=True)
def locked_task(key, other=None):
    conn = redis.Redis(db=TEST_DB, decode_responses=True)
    data = conn.getset(key, 1)
    if data is not None:
        raise Exception('task failed, key already set')
    time.sleep(DELAY)
    conn.delete(key)


@tiger.task(queue='batch', batch=True)
def batch_task(params):
    conn = redis.Redis(db=TEST_DB, decode_responses=True)
    try:
        conn.rpush('batch_task', json.dumps(params))
    except Exception:
        pass
    if any(p['args'][0] == 10 for p in params if p['args']):
        raise Exception('exception')


@tiger.task(queue='batch')
def non_batch_task(arg):
    conn = redis.Redis(db=TEST_DB, decode_responses=True)
    conn.rpush('batch_task', arg)
    if arg == 10:
        raise Exception('exception')


def retry_task():
    raise RetryException()


def retry_task_2():
    raise RetryException(method=fixed(DELAY, 1),
                         log_error=False)


def verify_current_task():
    conn = redis.Redis(db=TEST_DB, decode_responses=True)

    try:
        tiger.current_tasks
    except RuntimeError:
        # This is expected (we need to use current_task)
        task = tiger.current_task
        conn.set('task_id', task.id)


@tiger.task(batch=True, queue='batch')
def verify_current_tasks(tasks):
    conn = redis.Redis(db=TEST_DB, decode_responses=True)

    try:
        tasks = tiger.current_task
    except RuntimeError:
        # This is expected (we need to use current_tasks)

        tasks = tiger.current_tasks
        conn.rpush('task_ids', *[t.id for t in tasks])


@tiger.task()
def sleep_task(delay=10):
    time.sleep(delay)


@tiger.task(max_queue_size=1)
def decorated_task_max_queue_size(*args, **kwargs):
    pass


class StaticTask(object):
    @staticmethod
    def task():
        pass
