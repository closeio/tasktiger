"""Periodic test task."""

import redis

from tasktiger.schedule import periodic

from .config import REDIS_HOST, TEST_DB
from .utils import get_tiger

tiger = get_tiger()


@tiger.task(
    schedule=periodic(seconds=1), queue="periodic", retry_on=(ValueError,)
)
def periodic_task():
    """Periodic task."""
    conn = redis.Redis(host=REDIS_HOST, db=TEST_DB, decode_responses=True)
    conn.incr("period_count", 1)
    fail = conn.get("fail-periodic-task")
    if fail == "retriable":
        raise ValueError("retriable failure")
    elif fail == "permanent":
        raise Exception("permanent failure")


@tiger.task(schedule=periodic(seconds=1), queue="periodic_ignore")
def periodic_task_ignore():
    """
    Ignored periodic task.

    This task should never get queued.
    """
    pass
