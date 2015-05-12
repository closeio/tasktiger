import errno
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
from timeouts import UnixSignalDeathPenalty, JobTimeoutException

conn = redis.Redis()
scripts = RedisScripts(conn)

_stop_requested = False

# Where to queue tasks that don't have an explicit queue
DEFAULT_QUEUE = 'default'

# After how many seconds a long-running task is killed. This can be overridden
# by the task or at queue time.
DEFAULT_HARD_TIMEOUT = 300

# The timer specifies how often the worker updates the task's timestamp in the
# active queue. Tasks exceeding the timeout value are requeued. Note that no
# delay is necessary before the retry since this condition happens when the
# worker crashes, and not when there is an exception in the task itself.
ACTIVE_TASK_UPDATE_TIMER = 10
ACTIVE_TASK_UPDATE_TIMEOUT = 60
ACTIVE_TASK_EXPIRED_BATCH_SIZE = 10

REDIS_PREFIX = 't'

"""
Redis keys:

Set of all queues that contain items in the given status.
SET <prefix>:queued
SET <prefix>:active
SET <prefix>:error

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

def task(hard_timeout=None, queue=None):
    def _wrap(func):
        if hard_timeout:
            func._task_hard_timeout = hard_timeout
        if queue:
            func._task_queue = queue
        return func
    return _wrap

def delay(func, args=None, kwargs=None, queue=None, hard_timeout=None):
    task_id = _gen_id()

    if queue is None:
        queue = getattr(func, '_task_queue', DEFAULT_QUEUE)

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
    if hard_timeout:
        task['hard_timeout'] = hard_timeout
    serialized_task = json.dumps(task)

    pipeline = conn.pipeline()
    pipeline.sadd(_key('queued'), queue)
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
            hard_timeout = task.get('hard_timeout', None) or \
                           getattr(func, '_task_hard_timeout', None) or \
                           DEFAULT_HARD_TIMEOUT
            with UnixSignalDeathPenalty(hard_timeout):
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

def _heartbeat(queue, task_id):
    now = time.time()
    conn.zadd(_key('active', queue), task_id, now)

def _execute(queue, task_id):
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
                with UnixSignalDeathPenalty(ACTIVE_TASK_UPDATE_TIMER):
                    _, return_code = os.waitpid(child_pid, 0)
                    break
            except OSError as e:
                if e.errno != errno.EINTR:
                    raise
            except JobTimeoutException:
                _heartbeat(queue, task_id)

        print 'RETURN CODE', return_code
        status = not return_code
        return status

def _process_from_queue(queue):
    now = time.time()

    # Move an item to the active queue, if available.
    task_ids = scripts.zpoppush(
        _key('queued', queue),
        _key('active', queue),
        1,
        None,
        now,
        on_success=('update_sets', _key('queued'), _key('active'), queue),
    )

    assert len(task_ids) < 2

    if task_ids:
        task_id = task_ids[0]

        success = _execute(queue, task_id)
        if success:
            # Remove the task from active queue
            pipeline = conn.pipeline()
            pipeline.zrem(_key('active', queue), task_id)
            pipeline.delete(_key('task', task_id))
            scripts.srem_if_not_exists(_key('active'), queue,
                    _key('active', queue), client=pipeline)
            pipeline.execute()
            print 'DONE WITH TASK', task_id
        else:
            # TODO: Move task to the scheduled queue for retry,
            # or move to error queue if we don't want to retry.
            print 'ERROR WITH TASK', task_id
            now = time.time()
            pipeline = conn.pipeline()
            pipeline.zrem(_key('active', queue), task_id)
            pipeline.zadd(_key('error', queue), task_id, now)
            scripts.srem_if_not_exists(_key('active'), queue,
                    _key('active', queue), client=pipeline)
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

def _worker_queue_expired_tasks():
    active_queues = conn.smembers(_key('active'))
    now = time.time()
    for queue in active_queues:
        result = scripts.zpoppush(
            _key('active', queue),
            _key('queued', queue),
            ACTIVE_TASK_EXPIRED_BATCH_SIZE,
            now - ACTIVE_TASK_UPDATE_TIMEOUT,
            now,
            on_success=('update_sets', _key('active'), _key('queued'), queue),
        )
        # XXX: Ideally this would be atomic with the operation above.
        if result:
            print 'QUEUING ERRORED TASKS:', result
            conn.publish(_key('activity'), queue)

def _worker_run(queue_set):
    """
    Performs one worker run:
    * Processes a set of messages from each queue and removes any empty queues
      from the working set.
    * Move any expired items from the active queue to the queued queue.
    """

    queues = list(queue_set)
    random.shuffle(queues)

    for queue in queues:
        if _process_from_queue(queue) is None:
            queue_set.remove(queue)
        if _stop_requested:
            break

    # XXX: If no tasks are queued, we don't reach this code.
    if not _stop_requested:
        _worker_queue_expired_tasks()

    return queue_set

def _install_signal_handlers():
    def request_stop(signum, frame):
        global _stop_requested
        _stop_requested = True
        print 'Task in progress. Stop requested.'
    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

def _uninstall_signal_handlers():
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)

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

    queue_set = set(conn.smembers(_key('queued')))

    try:
        while True:
            if not queue_set:
                queue_set = _worker_update_queue_set(pubsub, queue_set)
            _install_signal_handlers()
            queue_set = _worker_run(queue_set)
            _uninstall_signal_handlers()
            if _stop_requested:
                raise KeyboardInterrupt()
    except KeyboardInterrupt:
        print 'Done'

if __name__ == '__main__':
    worker()
