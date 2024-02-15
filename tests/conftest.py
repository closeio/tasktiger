import json

import pytest

from .utils import get_tiger


@pytest.fixture
def tiger():
    tiger = get_tiger()
    redis = tiger.connection
    redis.flushdb()

    yield tiger

    redis.flushdb()
    redis.close()
    # Force disconnect so we don't get Too many open files
    redis.connection_pool.disconnect()


@pytest.fixture
def redis(tiger):
    return tiger.connection


@pytest.fixture
def ensure_queues(redis):
    def _ensure_queues(queued=None, active=None, error=None, scheduled=None):
        expected_queues = {
            "queued": {name for name, n in (queued or {}).items() if n},
            "active": {name for name, n in (active or {}).items() if n},
            "error": {name for name, n in (error or {}).items() if n},
            "scheduled": {name for name, n in (scheduled or {}).items() if n},
        }
        actual_queues = {
            i: redis.smembers("t:{}".format(i))
            for i in ("queued", "active", "error", "scheduled")
        }
        assert expected_queues == actual_queues

        def _ensure_queue(typ, data):
            data = data or {}
            ret = {}
            for name, n in data.items():
                task_ids = redis.zrange("t:%s:%s" % (typ, name), 0, -1)
                assert len(task_ids) == n
                ret[name] = [
                    json.loads(redis.get("t:task:%s" % task_id))
                    for task_id in task_ids
                ]
                assert [task["id"] for task in ret[name]] == task_ids
            return ret

        return {
            "queued": _ensure_queue("queued", queued),
            "active": _ensure_queue("active", active),
            "error": _ensure_queue("error", error),
            "scheduled": _ensure_queue("scheduled", scheduled),
        }

    return _ensure_queues
