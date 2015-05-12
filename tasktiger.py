if __name__ == '__main__':
    import gevent.monkey
    gevent.monkey.patch_all()

import importlib
import random
import redis
import json
import time
import traceback

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

Task IDs waiting in the given queue to be processed, scored by the time the
task was queued.
ZSET <prefix>:queue:<queue>

Task IDs being processed in the specific queue, scored by the time processing
started.
ZSET <prefix>:active:<queue>

Channel that receives the queue name as a message whenever a task is queued.
CHANNEL <prefix>:activity
"""

# from rq
def import_attribute(name):
    """Return an attribute from a dotted path name (e.g. "path.to.func")."""
    module_name, attribute = name.rsplit('.', 1)
    module = importlib.import_module(module_name)
    return getattr(module, attribute)

def _gen_id():
    return open('/dev/urandom').read(32).encode('hex')

def _serialize_func_name(func):
    if func.__module__ == '__main__':
        raise ValueError('Functions from the __main__ module cannot be '
                         'processed by workers.')
    return '.'.join([func.__module__, func.__name__])

def _func_from_serialized_name(serialized_name):
    return import_attribute(serialized_name)

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
        'time_queued': now,
    }
    if args:
        task['args'] = args
    if kwargs:
        task['kwargs'] = kwargs
    serialized_task = json.dumps(task)

    pipeline = conn.pipeline()
    pipeline.sadd(_key('queues'), queue)
    pipeline.set(_key('task', task_id), serialized_task)
    pipeline.zadd(_key('queue', queue), task_id, now)
    pipeline.publish(_key('activity'), queue)
    pipeline.execute()

def execute(task_id):
    """
    Executes the task with the given ID. Returns a boolean indicating whether
    the task was executed succesfully.
    """

    serialized_task = conn.get(_key('task', task_id))
    if not serialized_task:
        print 'ERROR: could not find task', task_id
        return
    task = json.loads(serialized_task)
    print 'TASK', task

    success = False

    try:
        func = _func_from_serialized_name(task['func'])
    except (ValueError, ImportError, AttributeError):
        print 'ERROR', task, 'Could not import', task['func']
    else:
        args = task.get('args', [])
        kwargs = task.get('kwargs', {})
        now = time.time()
        task['time_started'] = now
        try:
            func(*args, **kwargs)
            success = True
        except:
            task['traceback'] = traceback.format_exc()

    now = time.time()
    if not success:
        task['time_failed'] = now
        serialized_task = json.dumps(task)
        conn.set(_key('task', task_id), serialized_task)

    return success

def process_from_queue(queue):
    now = time.time()

    # Move an item to the active queue, if available.
    task_ids = scripts.zpoppush(
        _key('queue', queue),
        _key('active', queue),
        1,
        None,
        now,
    )

    assert len(task_ids) < 2

    if task_ids:
        task_id = task_ids[0]

        success = execute(task_id)
        if success:
            # Remove the task from active queue
            pipeline = conn.pipeline()
            pipeline.zrem(_key('active', queue), task_id)
            pipeline.delete(_key('task', task_id))
            pipeline.execute()
            print 'DONE WITH TASK', task_id
            pass
        else:
            # TODO: Move task to the scheduled queue for retry,
            # or move to error queue if we don't want to retry.
            print 'ERROR WITH TASK', task_id
            now = time.time()
            pipeline = conn.pipeline()
            pipeline.zrem(_key('active', queue), task_id)
            pipeline.zadd(_key('error', queue), task_id, now)
            pipeline.execute()

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


def worker():
    """
    Main worker entry point method.
    """

    global _worker_queue_set

    # TODO: Filter queue names. Also support wildcards in filter
    # TODO: Safe shutdown

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
