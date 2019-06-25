"""Periodic test task."""

import redis

from tasktiger.schedule import periodic

from .config import TEST_DB
from .utils import get_tiger

tiger = get_tiger()


@tiger.task(schedule=periodic(seconds=1), queue='periodic')
def periodic_task():
    """Periodic task."""
    conn = redis.Redis(db=TEST_DB, decode_responses=True)
    conn.incr('period_count', 1)


@tiger.task(schedule=periodic(seconds=1), queue='periodic_ignore')
def periodic_task_ignore():
    """
    Ignored periodic task.

    This task should never get queued.
    """
    pass
