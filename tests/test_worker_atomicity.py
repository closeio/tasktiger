"""Tests for the atomic expired-task cleanup in Worker._worker_queue_expired_tasks."""

import json
import os
import signal
import time
from multiprocessing import Process

from tasktiger import Task, Worker
from tasktiger._internal import ACTIVE

from .config import DELAY
from .tasks import sleep_task
from .utils import external_worker


def test_expiry_cleans_up_orphan_active_entry(tiger):
    """A worker is killed mid-task with its data already
    evicted from Redis. The next expiry pass should remove the orphan
    from the active queue."""
    redis = tiger.connection
    prefix = tiger.config["REDIS_PREFIX"]
    queue = "default"
    active_zset = f"{prefix}:{ACTIVE}:{queue}"

    task = Task(tiger, sleep_task, kwargs={"delay": 2 * DELAY})
    task.delay()

    worker_proc = Process(target=external_worker)
    worker_proc.start()
    time.sleep(DELAY)  # allow the worker to move the task to ACTIVE

    # Simulate an eviction
    assert redis.delete(f"{prefix}:task:{task.id}") == 1
    os.kill(worker_proc.pid, signal.SIGKILL)
    worker_proc.join(timeout=5)

    assert redis.zscore(active_zset, task.id) is not None
    assert redis.exists(f"{prefix}:task:{task.id}") == 0

    time.sleep(2 * DELAY)
    Worker(tiger).run(once=True, force_once=True)
    Worker(tiger).run(once=True, force_once=True)

    assert redis.zscore(active_zset, task.id) is None, (
        f"expiry failed to remove orphan ACTIVE entry for {task.id!r}"
    )
    assert redis.scard(f"{prefix}:{ACTIVE}") == 0


def test_handle_expired_task_lua_atomicity(tiger):
    """Test the HANDLE_EXPIRED_TASK Lua script cases:
    - the task data still exists (do nothing)
    - the data is gone but other tasks share the queue
    - the data is gone and there are no other tasks in the queue"""
    redis = tiger.connection
    prefix = tiger.config["REDIS_PREFIX"]
    queue = "default"
    task_key = f"{prefix}:task:t1"
    zset_key = f"{prefix}:{ACTIVE}:{queue}"
    state_key = f"{prefix}:{ACTIVE}"

    # task data still exists, do nothing
    redis.flushdb()
    redis.set(task_key, json.dumps({"id": "t1"}))
    redis.zadd(zset_key, {"t1": 1.0})
    redis.sadd(state_key, queue)

    result = tiger.scripts.handle_expired_task(
        key_task=task_key,
        key_from_state_queue=zset_key,
        key_from_state=state_key,
        id="t1",
        queue=queue,
    )
    assert result == 0
    assert redis.exists(task_key) == 1
    assert redis.zscore(zset_key, "t1") == 1.0
    assert redis.sismember(state_key, queue) == 1

    # data removed, another task shares the queue, queue tracker stays.
    redis.flushdb()
    redis.zadd(zset_key, {"t1": 1.0, "other": 2.0})
    redis.sadd(state_key, queue)

    result = tiger.scripts.handle_expired_task(
        key_task=task_key,
        key_from_state_queue=zset_key,
        key_from_state=state_key,
        id="t1",
        queue=queue,
    )
    assert result == 1
    assert redis.zscore(zset_key, "t1") is None
    assert redis.zscore(zset_key, "other") == 2.0
    assert redis.sismember(state_key, queue) == 1

    # data removed, no other tasks in the queue, tracker removed.
    redis.flushdb()
    redis.zadd(zset_key, {"t1": 1.0})
    redis.sadd(state_key, queue)

    result = tiger.scripts.handle_expired_task(
        key_task=task_key,
        key_from_state_queue=zset_key,
        key_from_state=state_key,
        id="t1",
        queue=queue,
    )
    assert result == 1
    assert redis.zscore(zset_key, "t1") is None
    assert redis.sismember(state_key, queue) == 0
