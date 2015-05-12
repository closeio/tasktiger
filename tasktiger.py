import importlib
import random
import redis
import json
import os
import select
import signal
import time
import traceback

from redis_scripts import RedisScripts
from timeouts import UnixSignalDeathPenalty

conn = redis.Redis()
scripts = RedisScripts(conn)

DEFAULT_QUEUE = 'default'
DEFAULT_HARD_TIMEOUT = 2
REDIS_PREFIX = 't'

"""
Redis keys:

Set of any active queues
SET <prefix>:queues

Serialized task for the given task ID.
STRING <prefix>:task:<task_id>

Task IDs waiting in the given queue to be processed, scored by the time the
task was queued.
ZSET <prefix>:queued:<queue>

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
    pipeline.zadd(_key('queued', queue), task_id, now)
    pipeline.publish(_key('activity'), queue)
    pipeline.execute()

def _execute_forked(task_id):
    conn = redis.Redis()
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
            with UnixSignalDeathPenalty(DEFAULT_HARD_TIMEOUT):
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

def execute(task_id):
    """
    Executes the task with the given ID. Returns a boolean indicating whether
    the task was executed succesfully.
    """
    # Adapted from rq Worker.execute_job / Worker.main_work_horse
    child_pid = os.fork()
    if child_pid == 0:

        # We need to reinitialize Redis' connection pool, otherwise the parent
        # socket will be disconnected by the Redis library.
        pool = conn.connection_pool
        pool.__init__(pool.connection_class, pool.max_connections,
                      **pool.connection_kwargs)

        random.seed()
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        success = _execute_forked(task_id)
        os._exit(int(not success))
    else:
        # Main process
        while True:
            try:
                _, return_code = os.waitpid(child_pid, 0)
                break
            except OSError as e:
                if e.errno != errno.EINTR:
                    raise
        print 'RETURN CODE', return_code
        status = not return_code
        return status

def process_from_queue(queue):
    now = time.time()

    # Move an item to the active queue, if available.
    task_ids = scripts.zpoppush(
        _key('queued', queue),
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

def _worker_update_queue_set(pubsub, queue_set):
    """
    This method checks the activity channel for any new queues that have
    activities and updates the queue_set. If there are no queues in the
    queue_set, this method blocks until there is activity. Otherwise, this
    method returns as soon as all messages from the activity channel were read.
    """

    # Pubsub messages generator
    gen = pubsub.listen()
    while True:
        # Since Redis' listen method blocks, we use select to inspect the
        # underlying socket to see if there is activity.
        fileno = pubsub.connection._sock.fileno()
        r, w, x = select.select([fileno], [], [], 0)
        if fileno in r or not queue_set:
            message = gen.next()
            if message['type'] == 'message':
                queue_set.add(message['data'])
        else:
            break
    return queue_set

def _worker_process_messages_run(queue_set):
    """
    Performs one worker run, i.e. processes a set of messages from each queue
    and removes any empty queues from the working set.
    """

    queues = list(queue_set)
    random.shuffle(queues)

    for queue in queues:
        if process_from_queue(queue) is None:
            queue_set.remove(queue)

    return queue_set


def worker():
    """
    Main worker entry point method.
    """

    # TODO: Filter queue names. Also support wildcards in filter
    # TODO: Safe shutdown

    # First scan all the available queues for new items until they're empty.
    # Then, listen to the activity channel.
    # XXX: This can get inefficient when having lots of queues.

    pubsub = conn.pubsub()
    pubsub.subscribe(_key('activity'))

    queue_set = set(conn.smembers(_key('queues')))

    while True:
        if not queue_set:
            queue_set = _worker_update_queue_set(pubsub, queue_set)
        queue_set = _worker_process_messages_run(queue_set)

if __name__ == '__main__':
    worker()
