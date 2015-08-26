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

from tasktiger.retry import *
from tasktiger.timeouts import UnixSignalDeathPenalty, JobTimeoutException

__all__ = ['task', 'TaskTiger', 'Worker',

           # Exceptions
           'JobTimeoutException', 'StopRetry',

           # Retry methods
           'fixed', 'linear', 'exponential']

conn = redis.Redis()
scripts = RedisScripts(conn)

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

class Worker(object):
    def __init__(self, tiger, queues=None):
        self.log = tiger.log.bind(pid=os.getpid())
        self.connection = tiger.connection
        self.scripts = tiger.scripts
        self.config = tiger.config

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
        queues = set(self._filter_queues(self.connection.smembers(
                _key('scheduled'))))
        now = time.time()
        for queue in queues:
            # Move due items from scheduled queue to active queue. If items
            # were moved, remove the queue from the scheduled set if it is
            # empty, and add it to the active set so the task gets picked up.

            result = self.scripts.zpoppush(
                _key('scheduled', queue),
                _key('queued', queue),
                SCHEDULED_TASK_BATCH_SIZE,
                now,
                now,
                on_success=('update_sets', _key('scheduled'), _key('queued'), queue),
            )

            # XXX: ideally this would be in the same pipeline, but we only want
            # to announce if there was a result.
            if result:
                self.connection.publish(_key('activity'), queue)

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
        active_queues = self.connection.smembers(_key('active'))
        now = time.time()
        for queue in active_queues:
            result = self.scripts.zpoppush(
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
                self.connection.publish(_key('activity'), queue)

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
            except Exception, exc:
                execution['traceback'] = traceback.format_exc()
                execution['exception_name'] = _serialize_func_name(exc.__class__)
                log.error(traceback=execution['traceback'])
                execution['time_failed'] = time.time()
            else:
                success = True

        if not success:
            # Currently we only log failed task executions to Redis.
            execution['success'] = success
            serialized_execution = json.dumps(execution)
            self.connection.rpush(_key('task', task['id'], 'executions'),
                                  serialized_execution)

        return success

    def _heartbeat(self, queue, task_id):
        now = time.time()
        self.connection.zadd(_key('active', queue), task_id, now)

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
            pool = self.connection.connection_pool
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
        task_ids = self.scripts.zpoppush(
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

            serialized_task = self.connection.get(_key('task', task_id))
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
                when = now + self.config['LOCK_RETRY']
                pipeline = self.connection.pipeline()
                pipeline.zrem(_key('active', queue), task_id)
                self.scripts.srem_if_not_exists(_key('active'), queue,
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
                pipeline = self.connection.pipeline()
                pipeline.zrem(_key('active', queue), task_id)
                if task.get('unique', False):
                    # Only delete if it's not in the error or queued queue.
                    self.scripts.delete_if_not_in_zsets(_key('task', task_id), task_id, [
                        _key('queued', queue),
                        _key('error', queue)
                    ], client=pipeline)
                else:
                    pipeline.delete(_key('task', task_id))
                self.scripts.srem_if_not_exists(_key('active'), queue,
                        _key('active', queue), client=pipeline)
                pipeline.execute()
                log.debug('done')
            else:
                should_retry = False
                # Get execution info
                if 'retry_method' in task:
                    if 'retry_on' in task:
                        execution = self.connection.lindex(
                            _key('task', task['id'], 'executions'), -1)
                        if execution:
                            execution = json.loads(execution)
                            exception_name = execution.get('exception_name')
                            exception_class = _func_from_serialized_name(exception_name)
                            for n in task['retry_on']:
                                if issubclass(exception_class, _func_from_serialized_name(n)):
                                    should_retry = True
                                    break
                    else:
                        should_retry = True

                queue_type = 'error'

                when = time.time()

                if should_retry:
                    retry_func, retry_args = task['retry_method']
                    retry_num = self.connection.llen(_key('task', task['id'], 'executions'))
                    try:
                        func = _func_from_serialized_name(retry_func)
                    except (ValueError, ImportError, AttributeError):
                        log.error('could not import retry function',
                                  func=retry_func)
                    else:
                        try:
                            when += func(retry_num, *retry_args)
                        except StopRetry:
                            pass
                        else:
                            queue_type = 'scheduled'

                # Move task to the scheduled queue for retry, or move to error
                # queue if we don't want to retry.
                pipeline = self.connection.pipeline()
                pipeline.zadd(_key(queue_type, queue), task_id, when)
                pipeline.sadd(_key(queue_type), queue)
                pipeline.zrem(_key('active', queue), task_id)
                self.scripts.srem_if_not_exists(_key('active'), queue,
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

    def run(self, once=False):
        """
        Main loop of the worker. Use once=True to execute any queued tasks and
        then exit.
        """

        self.log.info('ready', queues=self.queue_filter)

        # First scan all the available queues for new items until they're empty.
        # Then, listen to the activity channel.
        # XXX: This can get inefficient when having lots of queues.

        self._pubsub = self.connection.pubsub()
        self._pubsub.subscribe(_key('activity'))

        self._queue_set = set(self._filter_queues(
                self.connection.smembers(_key('queued'))))

        try:
            while True:
                if not self._queue_set:
                    self._update_queue_set(
                            timeout=self.config['SELECT_TIMEOUT'])

                self._install_signal_handlers()
                queue_set = self._worker_run()
                self._uninstall_signal_handlers()
                if once and not queue_set:
                    break
                if self._stop_requested:
                    raise KeyboardInterrupt()
        except KeyboardInterrupt:
            pass
        self.log.info('done')

class TaskTiger(object):
    def __init__(self, connection=None, config=None):
        """
        Initializes TaskTiger with the given Redis connection and config
        options.
        """

        # TODO: migrate other config options to this (where it makes sense)
        self.config = {
            # Name of the Python (structlog) logger
            'LOGGER_NAME': 'tasktiger',

            # After how many seconds time out on listening on the activity
            # channel and check for scheduled or expired items.
            'SELECT_TIMEOUT': 1,

            # If this is True, all tasks will be executed locally by blocking
            # until the task returns. This is useful for testing purposes.
            'ALWAYS_EAGER': False,

            # If retry is True but no retry_method is specified for a given
            # task, use the following default method.
            'DEFAULT_RETRY_METHOD': fixed(60, 3),

            # After how many seconds a task that can't require a lock is
            # retried.
            'LOCK_RETRY': 1,
        }
        if config:
            self.config.update(config)

        self.connection = connection or redis.Redis()
        self.scripts = RedisScripts(self.connection)
        self.log = structlog.get_logger(
            self.config['LOGGER_NAME'],
        ).bind()

    def task(self, queue=None, hard_timeout=None, unique=None, lock=None,
             retry=None, retry_on=None, retry_method=None):
        """
        Function decorator that defines the behavior of the function when it is
        used as a task. To use the default behavior, tasks don't need to be
        decorated. All the arguments are described in the delay() method.
        """

        def _wrap(func):
            if hard_timeout is not None:
                func._task_hard_timeout = hard_timeout
            if queue is not None:
                func._task_queue = queue
            if unique is not None:
                func._task_unique = True
            if lock is not None:
                func._task_lock = True
            if retry is not None:
                func._task_retry = retry
            if retry_on is not None:
                func._task_retry_on = retry_on
            if retry_method is not None:
                func._task_retry_method = retry_method
            return func
        return _wrap


    def run_worker(self, **kwargs):
        """
        Main worker entry point method.
        """

        self.log.setLevel(logging.DEBUG)
        logging.basicConfig(format='%(message)s')

        module_names = kwargs.pop('module') or ''
        for module_name in module_names.split(','):
            module_name = module_name.strip()
            if module_name:
                importlib.import_module(module_name)
                self.log.debug('imported module', module_name=module_name)

        worker = Worker(self, **kwargs)
        worker.run()

    def delay(self, func, args=None, kwargs=None, queue=None,
              hard_timeout=None, unique=None, lock=None, when=None,
              retry=None, retry_on=None, retry_method=None):
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
          The task will only be queued if there is no similar task with the
          same function, arguments and keyword arguments in the queue. Note
          that multiple similar tasks may still be executed at the same time
          since the task will still be inserted into the queue if another one
          is being processed.

        * lock
          Hold a lock while the task is being executed (with the given args and
          kwargs). If a task with similar args/kwargs is queued and tries to
          acquire the lock, it will be retried later.

        * when
          Takes either a datetime (for an absolute date) or a timedelta
          (relative to now). If given, the task will be scheduled for the given
          time.

        * retry
          Whether to retry a task when it fails (either because of an exception
          or because of a timeout). To restrict the list of failures, use
          retry_on. Unless retry_method is given, the configured
          DEFAULT_RETRY_METHOD is used.

        * retry_on
          If a list is given, it implies retry=True. Task will be only retried
          on the given exceptions (or its subclasses). To retry the task when a
          hard timeout occurs, use JobTimeoutException.

        * retry_method
          If given, implies retry=True. Pass either:

          * a function that takes the retry number as an argument, or,
          * a tuple (f, args), where f takes the retry number as the first
            argument, followed by the additional args.

          The function needs to return the desired retry interval in seconds,
          or raise StopRetry to stop retrying. The following built-in functions
          can be passed for common scenarios and return the appropriate tuple:

          * fixed(delay, max_retries)
            Returns a method that returns the given delay or raises StopRetry
            if the number of retries exceeds max_retries.

          * linear(delay, increment, max_retries)
            Like fixed, but starts off with the given delay and increments it
            by the given increment after every retry.

          * exponential(delay, factor, max_retries)
            Like fixed, but starts off with the given delay and multiplies it
            by the given factor after every retry.
        """

        if self.config['ALWAYS_EAGER']:
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

        if retry is None:
            retry = getattr(func, '_task_retry', False)

        if retry_on is None:
            retry_on = getattr(func, '_task_retry_on', None)

        if retry_method is None:
            retry_method = getattr(func, '_task_retry_method', None)

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
        if retry or retry_on or retry_method:
            if not retry_method:
                retry_method = self.config['DEFAULT_RETRY_METHOD']

            if callable(retry_method):
                retry_method = (_serialize_func_name(retry_method), ())
            else:
                retry_method = (_serialize_func_name(retry_method[0]),
                                retry_method[1])

            task['retry_method'] = retry_method
            if retry_on:
                task['retry_on'] = [_serialize_func_name(cls)
                                    for cls in retry_on]

        serialized_task = json.dumps(task)

        if when:
            queue_type = 'scheduled'
        else:
            queue_type = 'queued'

        pipeline = self.connection.pipeline()
        pipeline.sadd(_key(queue_type), queue)
        pipeline.set(_key('task', task_id), serialized_task)
        pipeline.zadd(_key(queue_type, queue), task_id, when or now)
        if queue_type == 'queued':
            pipeline.publish(_key('activity'), queue)
        pipeline.execute()


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
    # TODO: figure out a better way for this.
    TaskTiger().run_worker(**kwargs)

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
