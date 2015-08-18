import calendar
import click
import datetime
import errno
import hashlib
import importlib
import random
import redis
import json
import logging
import os
import select
import signal
import structlog
import time
import traceback

from redis_scripts import RedisScripts
from redis_lock import Lock
from timeouts import UnixSignalDeathPenalty, JobTimeoutException

conn = redis.Redis()
scripts = RedisScripts(conn)

LOGGER_NAME = 'tasktiger'

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

# How many items to move at most from the scheduled queue to the active queue.
SCHEDULED_TASK_BATCH_SIZE = 1000

# After how many seconds time out on listening on the activity channel and
# check for scheduled or expired items.
SELECT_TIMEOUT = 1

# After how many seconds a task that can't require a lock is retried.
LOCK_RETRY = 1

REDIS_PREFIX = 't'

"""
Redis keys:

Set of all queues that contain items in the given status.
SET <prefix>:queued
SET <prefix>:active
SET <prefix>:error
SET <prefix>:scheduled

Serialized task for the given task ID.
STRING <prefix>:task:<task_id>

List of (failed) task executions
LIST <prefix>:task:<task_id>:executions

Task IDs waiting in the given queue to be processed, scored by the time the
task was queued.
ZSET <prefix>:queued:<queue>

Task IDs being processed in the specific queue, scored by the time processing
started.
ZSET <prefix>:active:<queue>

Task IDs that failed, scored by the time processing failed.
ZSET <prefix>:error:<queue>

Task IDs that are scheduled to be executed at a specific time, scored by the
time they should be executed.
ZSET <prefix>:scheduled:<queue>

Channel that receives the queue name as a message whenever a task is queued.
CHANNEL <prefix>:activity

Locks
STRING <prefix>:lock:<lock_hash>
"""

# TODO: migrate other config options to this (where it makes sense)
_config = {
    # If this is True, all tasks will be executed locally by blocking until the
    # task returns. This is useful for testing purposes.
    'ALWAYS_EAGER': False,
}

# from rq
def import_attribute(name):
    """Return an attribute from a dotted path name (e.g. "path.to.func")."""
    module_name, attribute = name.rsplit('.', 1)
    module = importlib.import_module(module_name)
    return getattr(module, attribute)

def _gen_id():
    return os.urandom(32).encode('hex')

def _gen_unique_id(serialized_name, args, kwargs):
    return hashlib.sha256(json.dumps({
        'func': serialized_name,
        'args': args,
        'kwargs': kwargs,
    }, sort_keys=True)).hexdigest()

def _serialize_func_name(func):
    if func.__module__ == '__main__':
        raise ValueError('Functions from the __main__ module cannot be '
                         'processed by workers.')
    return '.'.join([func.__module__, func.__name__])

def _func_from_serialized_name(serialized_name):
    return import_attribute(serialized_name)

def _key(*parts):
    return ':'.join([REDIS_PREFIX] + list(parts))

def task(queue=None, hard_timeout=None, unique=False, lock=False):
    def _wrap(func):
        if hard_timeout:
            func._task_hard_timeout = hard_timeout
        if queue:
            func._task_queue = queue
        if unique:
            func._task_unique = True
        if lock:
            func._task_lock = True
        return func
    return _wrap

def delay(func, args=None, kwargs=None, queue=None, hard_timeout=None,
          unique=None, lock=None, when=None):
    """
    Queues a task.
    * func
      Dotted path to the function that will be queued (e.g. module.func)

    * args
      List of arguments that will be passed to the function.

    * kwargs
      List of keyword arguments that will be passed to the function.

    * queue
      Name of the queue where the task will be queued.

    * hard_timeout
      If the task runs longer than the given number of seconds, it will be
      killed and marked as failed.

    * unique
      The task will only be queued if there is no similar task with the same
      function, arguments and keyword arguments in the queue. Note that
      multiple similar tasks may still be executed at the same time since the
      task will still be inserted into the queue if another one is being
      processed.

    * lock
      Hold a lock while the task is being executed (with the given args and
      kwargs). If a task with similar args/kwargs is queued and tries to
      acquire the lock, it will be retried later.

    * when
      Takes either a datetime (for an absolute date) or a timedelta (relative
      to now). If given, the task will be scheduled for the given time.
    """

    global _config

    if _config['ALWAYS_EAGER']:
        if not args:
            args = []
        if not kwargs:
            kwargs = {}
        return func(*args, **kwargs)

    serialized_name = _serialize_func_name(func)

    if unique is None:
        unique = getattr(func, '_task_unique', False)

    if lock is None:
        lock = getattr(func, '_task_lock', False)

    if unique:
        task_id = _gen_unique_id(serialized_name, args, kwargs)
    else:
        task_id = _gen_id()

    if queue is None:
        queue = getattr(func, '_task_queue', DEFAULT_QUEUE)

    # convert timedelta to datetime
    if isinstance(when, datetime.timedelta):
        when = datetime.datetime.utcnow() + when

    # convert to unixtime
    if isinstance(when, datetime.datetime):
        when = calendar.timegm(when.utctimetuple()) + when.microsecond/1.e6

    now = time.time()
    task = {
        'id': task_id,
        'func': serialized_name,
        'time_last_queued': now,
    }
    if unique:
        task['unique'] = True
    if lock:
        task['lock'] = True
    if args:
        task['args'] = args
    if kwargs:
        task['kwargs'] = kwargs
    if hard_timeout:
        task['hard_timeout'] = hard_timeout
    serialized_task = json.dumps(task)

    if when:
        queue_type = 'scheduled'
    else:
        queue_type = 'queued'

    pipeline = conn.pipeline()
    pipeline.sadd(_key(queue_type), queue)
    pipeline.set(_key('task', task_id), serialized_task)
    pipeline.zadd(_key(queue_type, queue), task_id, when or now)
    if queue_type == 'queued':
        pipeline.publish(_key('activity'), queue)
    pipeline.execute()

class Worker(object):
    def __init__(self, logger, queues):
        self.log = logger.bind(pid=os.getpid())

        # TODO: Also support wildcards in filter
        if queues:
            self.queue_filter = queues.split(',')
        else:
            self.queue_filter = None

        self._stop_requested = False

    def _install_signal_handlers(self):
        def request_stop(signum, frame):
            self._stop_requested = True
            self.log.info('stop requested, waiting for task to finish')
        signal.signal(signal.SIGINT, request_stop)
        signal.signal(signal.SIGTERM, request_stop)

    def _uninstall_signal_handlers(self):
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)

    def _filter_queues(self, queues):
        if self.queue_filter:
            return [q for q in queues if q in self.queue_filter]
        else:
            return queues

    def _worker_queue_scheduled_tasks(self):
        queues = set(self._filter_queues(conn.smembers(_key('scheduled'))))
        now = time.time()
        for queue in queues:
            # Move due items from scheduled queue to active queue. If items
            # were moved, remove the queue from the scheduled set if it is
            # empty, and add it to the active set so the task gets picked up.

            result = scripts.zpoppush(
                _key('scheduled', queue),
                _key('queued', queue),
                SCHEDULED_TASK_BATCH_SIZE,
                now,
                now,
                on_success=('update_sets', _key('scheduled'), _key('active'), queue),
            )

            # XXX: ideally this would be in the same pipeline, but we only want
            # to announce if there was a result.
            if result:
                conn.publish(_key('activity'), queue)

    def _update_queue_set(self, timeout=None):
        """
        This method checks the activity channel for any new queues that have
        activities and updates the queue_set. If there are no queues in the
        queue_set, this method blocks until there is activity or the timeout
        elapses. Otherwise, this method returns as soon as all messages from
        the activity channel were read.
        """

        # Pubsub messages generator
        gen = self._pubsub.listen()
        while True:
            # Since Redis' listen method blocks, we use select to inspect the
            # underlying socket to see if there is activity.
            fileno = self._pubsub.connection._sock.fileno()
            r, w, x = select.select([fileno], [], [],
                                    0 if self._queue_set else timeout)
            if fileno in r: # or not self._queue_set:
                message = gen.next()
                if message['type'] == 'message':
                    for queue in self._filter_queues([message['data']]):
                        self._queue_set.add(queue)
            else:
                break

    def _worker_queue_expired_tasks(self):
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
                self.log.info('queueing expired tasks', task_ids=result)
                conn.publish(_key('activity'), queue)

    def _execute_forked(self, task, log):
        success = False

        execution = {}

        try:
            func = _func_from_serialized_name(task['func'])
        except (ValueError, ImportError, AttributeError):
            log.error('could not import', func=task['func'])
        else:
            args = task.get('args', [])
            kwargs = task.get('kwargs', {})
            execution['time_started'] = time.time()
            try:
                hard_timeout = task.get('hard_timeout', None) or \
                               getattr(func, '_task_hard_timeout', None) or \
                               DEFAULT_HARD_TIMEOUT
                with UnixSignalDeathPenalty(hard_timeout):
                    func(*args, **kwargs)
            except:
                execution['traceback'] = traceback.format_exc()
                log.error(traceback=execution['traceback'])
                execution['time_failed'] = time.time()
            else:
                success = True

        if not success:
            # Currently we only log failed task executions to Redis.
            execution['success'] = success
            serialized_execution = json.dumps(execution)
            conn.rpush(_key('task', task['id'], 'executions'), serialized_execution)

        return success

    def _heartbeat(self, queue, task_id):
        now = time.time()
        conn.zadd(_key('active', queue), task_id, now)

    def _execute(self, queue, task, log, lock):
        """
        Executes the task with the given ID. Returns a boolean indicating whether
        the task was executed succesfully.
        """
        # Adapted from rq Worker.execute_job / Worker.main_work_horse
        child_pid = os.fork()
        if child_pid == 0:
            log = log.bind(child_pid=os.getpid())

            # We need to reinitialize Redis' connection pool, otherwise the parent
            # socket will be disconnected by the Redis library.
            # TODO: We might only need this if the task fails.
            pool = conn.connection_pool
            pool.__init__(pool.connection_class, pool.max_connections,
                          **pool.connection_kwargs)

            random.seed()
            signal.signal(signal.SIGINT, signal.SIG_IGN)
            success = self._execute_forked(task, log)
            os._exit(int(not success))
        else:
            log = log.bind(child_pid=child_pid)
            log.debug('processing')
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
                    self._heartbeat(queue, task['id'])
                    if lock:
                        lock.renew(ACTIVE_TASK_UPDATE_TIMEOUT)

            status = not return_code
            return status

    def _process_from_queue(self, queue):
        now = time.time()

        log = self.log.bind(queue=queue)

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

            log = log.bind(task_id=task_id)

            serialized_task = conn.get(_key('task', task_id))
            if not serialized_task:
                log.error('not found')
                # Return the task ID since there may be more tasks.
                return task_id

            task = json.loads(serialized_task)

            if task.get('lock', False):
                lock = Lock(conn, _key('lock', _gen_unique_id(
                    task['func'],
                    task.get('args', []),
                    task.get('kwargs', []),
                )), timeout=ACTIVE_TASK_UPDATE_TIMEOUT, blocking=False)
            else:
                lock = None

            if lock and not lock.acquire():
                log.info('could not acquire lock')

                # Reschedule the task
                now = time.time()
                when = now + LOCK_RETRY
                pipeline = conn.pipeline()
                pipeline.zrem(_key('active', queue), task_id)
                scripts.srem_if_not_exists(_key('active'), queue,
                        _key('active', queue), client=pipeline)
                pipeline.sadd(_key('scheduled'), queue)
                pipeline.zadd(_key('scheduled', queue), task_id, when)
                pipeline.execute()

                return task_id

            success = self._execute(queue, task, log, lock)

            if lock:
                lock.release()

            if success:
                # Remove the task from active queue
                pipeline = conn.pipeline()
                pipeline.zrem(_key('active', queue), task_id)
                if task.get('unique', False):
                    # Only delete if it's not in the error or queued queue.
                    scripts.delete_if_not_in_zsets(_key('task', task_id), task_id, [
                        _key('queued', queue),
                        _key('error', queue)
                    ], client=pipeline)
                else:
                    pipeline.delete(_key('task', task_id))
                scripts.srem_if_not_exists(_key('active'), queue,
                        _key('active', queue), client=pipeline)
                pipeline.execute()
                log.debug('done')
            else:
                # TODO: Move task to the scheduled queue for retry,
                # or move to error queue if we don't want to retry.
                now = time.time()
                pipeline = conn.pipeline()
                pipeline.zrem(_key('active', queue), task_id)
                pipeline.zadd(_key('error', queue), task_id, now)
                scripts.srem_if_not_exists(_key('active'), queue,
                        _key('active', queue), client=pipeline)
                pipeline.execute()
                log.error('error')

            return task_id

    def _worker_run(self):
        """
        Performs one worker run:
        * Processes a set of messages from each queue and removes any empty queues
          from the working set.
        * Move any expired items from the active queue to the queued queue.
        * Move any scheduled items from the scheduled queue to the queued queue.
        """

        queues = list(self._queue_set)
        random.shuffle(queues)

        for queue in queues:
            if self._process_from_queue(queue) is None:
                self._queue_set.remove(queue)
            if self._stop_requested:
                break

        if not self._stop_requested:
            self._worker_queue_scheduled_tasks()
            self._worker_queue_expired_tasks()

    def run(self):
        self.log.info('ready', queues=self.queue_filter)

        # First scan all the available queues for new items until they're empty.
        # Then, listen to the activity channel.
        # XXX: This can get inefficient when having lots of queues.

        self._pubsub = conn.pubsub()
        self._pubsub.subscribe(_key('activity'))

        self._queue_set = set(self._filter_queues(conn.smembers(_key('queued'))))

        try:
            while True:
                if not self._queue_set:
                    self._update_queue_set(timeout=SELECT_TIMEOUT)

                self._install_signal_handlers()
                queue_set = self._worker_run()
                self._uninstall_signal_handlers()
                #if not queue_set:
                #    break
                if self._stop_requested:
                    raise KeyboardInterrupt()
        except KeyboardInterrupt:
            pass
        self.log.info('done')

@click.command()
@click.option('-q', '--queues', help='If specified, only the given queue(s) '
                                     'are processed. Multiple queues can be '
                                     'separated by comma.')
@click.option('-m', '--module', help="Module(s) to import when launching the "
                                     "worker. This improves task performance "
                                     "since the module doesn't have to be "
                                     "reimported every time a task is forked. "
                                     "Multiple modules can be separated by "
                                     "comma.")
def run_worker(**kwargs):
    """
    Main worker entry point method.
    """

    logger = structlog.get_logger(
        LOGGER_NAME,
    )
    logger.setLevel(logging.DEBUG)
    logging.basicConfig(format='%(message)s')

    module_names = kwargs.pop('module') or ''
    for module_name in module_names.split(','):
        module_name = module_name.strip()
        if module_name:
            importlib.import_module(module_name)
            logger.debug('imported module', module_name=module_name)

    worker = Worker(logger=logger, **kwargs)
    worker.run()

def configure(**kwargs):
    """
    Configures tasktiger and returns the current (updated) configuration.
    """

    global _config
    _config.update(**kwargs)
    return _config


if __name__ == '__main__':
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.stdlib.filter_by_level,
            structlog.processors.TimeStamper(fmt='iso', utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    run_worker()
