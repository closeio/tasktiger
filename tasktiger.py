if __name__ == '__main__':
    import gevent.monkey
    gevent.monkey.patch_all()

import random
import redis
import json
import time

import gevent
from gevent.queue import Queue, Full

from redis_scripts import RedisScripts

conn = redis.Redis()
scripts = RedisScripts(conn)

DEFAULT_QUEUE = 'default'
REDIS_PREFIX = 't'

"""
Redis keys:

Set of any active queues
SET <prefix>:<queues>

Serialized task for the given task ID.
STRING <prefix>:task:<task_id>

Queued task IDs for a specific queue, scored by the time the task was queued.
ZSET <prefix>:queue:<queue>

Channel that receives the queue name as a message whenever a task is queued.
CHANNEL <prefix>:activity
"""

def _gen_id():
    return open('/dev/urandom').read(32).encode('hex')

def _serialize_func_name(func):
    return func.__name__

def _func_from_serialized_name(serialized_func):
    raise NotImplementedError("TODO")

def _key(*parts):
    return ':'.join([REDIS_PREFIX] + list(parts))

def task():
    def _wrap(func):
        return func
    return _wrap

def delay(func, args=None, kwargs=None, queue=None):
    task_id = _gen_id()

    if queue is None:
        queue = DEFAULT_QUEUE

    now = time.time()
    task = {
        'id': task_id,
        'func': _serialize_func_name(func),
        'args': args,
        'kwargs': kwargs,
        'time_queued': now,
    }
    serialized_task = json.dumps(task)

    pipeline = conn.pipeline()
    pipeline.sadd(_key('queues'), queue)
    pipeline.set(_key('task', task_id), serialized_task)
    pipeline.zadd(_key('queue', queue), task_id, now)
    pipeline.publish(_key('activity'), queue)
    pipeline.execute()

def process(task_id):
    print 'PROCESSING', task_id
    pass

def process_from_queue(queue):
    now = time.time()

    # Move an item to the active queue, if available.
    tasks = scripts.zpoppush(
        _key('queue', queue),
        _key('active', queue),
        1,
        None,
        now,
    )

    assert len(tasks) < 2

    if tasks:
        task = tasks[0]

        process(task)

        return task

_worker_queue_set = set()
_worker_has_items = Queue(maxsize=1)
_worker_queues_scanned = set()

def _worker_pubsub(pubsub):
    global _worker_queue_set

    for msg in pubsub.listen():
        print 'MSG', msg
        if msg['type'] == 'message':
            _worker_queue_set.add(msg['data'])

            # Notify main greenlet that we have items.
            try:
                _worker_has_items.put(True, block=False)
            except Full:
                pass

def _worker_process_messages_run():
    global _worker_queue_set

    queue_set = _worker_queue_set
    _worker_queue_set = set()

    queues = list(queue_set)
    random.shuffle(queues)

    print 'QUEUES', queues
    for queue in queues:
        if process_from_queue(queue) is None:
            queue_set.remove(queue)

    _worker_queue_set |= queue_set


    """

    while _worker_queue_set:
        queues = list(_worker_queue_set)
        random.shuffle(queues)

        for queue in queues:
            if process_from_queue(queue) is None:
                _worker_queue_set.remove(queue)
    """

def worker():
    """
    Main worker entry point method.
    """

    global _worker_queue_set

    # TODO: Filter queue names. Also support wildcards in filter

    # First scan all the available queues for new items until they're empty.
    # Then, listen to the activity channel.
    # XXX: This can get inefficient when having lots of queues.

    pubsub = conn.pubsub()
    pubsub.subscribe(_key('activity'))

    _worker_queue_set = set(conn.smembers(_key('queues')))

    gevent.spawn(_worker_pubsub, pubsub)

    while _worker_queue_set or _worker_has_items.get():
        _worker_process_messages_run()

if __name__ == '__main__':
    worker()
