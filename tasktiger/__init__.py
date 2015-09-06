import calendar
import click
from collections import defaultdict
import datetime
import importlib
import json
import logging
import redis
import structlog
import time

from redis_scripts import RedisScripts

from ._internal import *
from .retry import *
from .worker import Worker
from .exceptions import *

__all__ = ['TaskTiger', 'Worker',

           # Exceptions
           'TaskImportError', 'JobTimeoutException', 'StopRetry',

           # Retry methods
           'fixed', 'linear', 'exponential']

"""
Redis keys:

Set of all queues that contain items in the given state.
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

class TaskTiger(object):
    def __init__(self, connection=None, config=None):
        """
        Initializes TaskTiger with the given Redis connection and config
        options.
        """

        self.config = {
            # String that is used to prefix all Redis keys
            'REDIS_PREFIX': 't',

            # Name of the Python (structlog) logger
            'LOGGER_NAME': 'tasktiger',

            # Where to queue tasks that don't have an explicit queue
            'DEFAULT_QUEUE': 'default',

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

            # How many items to move at most from the scheduled queue to the
            # active queue.
            'SCHEDULED_TASK_BATCH_SIZE': 1000,

            # After how many seconds a long-running task is killed. This can be
            # overridden by the task or at queue time.
            'DEFAULT_HARD_TIMEOUT': 300,

            # The timer specifies how often the worker updates the task's
            # timestamp in the active queue. Tasks exceeding the timeout value
            # are requeued. Note that no delay is necessary before the retry
            # since this condition happens when the worker crashes, and not
            # when there is an exception in the task itself.
            'ACTIVE_TASK_UPDATE_TIMER': 10,
            'ACTIVE_TASK_UPDATE_TIMEOUT': 60,
            'ACTIVE_TASK_EXPIRED_BATCH_SIZE': 10,

            # Set up queues that will be processed in batch, i.e. multiple jobs
            # are taken out of the queue at the same time and passed as a list
            # to the worker method. Takes a dict where the key represents the
            # queue name and the value represents the batch size. Note that the
            # task needs to be declared as batch=True.
            'BATCH_QUEUES': {},
        }
        if config:
            self.config.update(config)

        self.connection = connection or redis.Redis()
        self.scripts = RedisScripts(self.connection)
        self.log = structlog.get_logger(
            self.config['LOGGER_NAME'],
        ).bind()

    def _key(self, *parts):
        """
        Internal helper to get a Redis key, taking the REDIS_PREFIX into
        account. Parts are delimited with a colon. Individual parts shouldn't
        contain colons since we don't escape them.
        """
        return ':'.join([self.config['REDIS_PREFIX']] + list(parts))

    def _redis_move_task(self, queue, task_id, from_state, to_state=None,
                         when=None, remove_task=None):
        """
        Internal helper to move a task from one state another (e.g. from QUEUED
        to DELAYED). The "when" argument indicates the timestamp of the task in
        the new state. If no to_state is specified, the task will be simply
        removed from the from_state. In this case remove_task can be specified,
        which can have one of the following values:
          * 'always': Remove the task object from Redis.
          * 'check': Only remove the task object from Redis if the task doesn't
            exist in the QUEUED or ERROR queue. This is useful for unique
            tasks, which can have multiple instances in different states with
            the same ID.
        """
        pipeline = self.connection.pipeline()
        if to_state:
            if not when:
                when = time.time()
            pipeline.zadd(self._key(to_state, queue), task_id, when)
            pipeline.sadd(self._key(to_state), queue)
        pipeline.zrem(self._key(from_state, queue), task_id)
        if remove_task == 'always':
            pipeline.delete(self._key('task', task_id))
        elif remove_task == 'check':
            # Only delete if it's not in the error or queued queue.
            self.scripts.delete_if_not_in_zsets(self._key('task', task_id),
                                                task_id, [
                self._key(QUEUED, queue),
                self._key(ERROR, queue)
            ], client=pipeline)
        self.scripts.srem_if_not_exists(self._key(from_state), queue,
                self._key(from_state, queue), client=pipeline)
        if to_state == QUEUED:
            pipeline.publish(self._key('activity'), queue)
        pipeline.execute()

    def task(self, queue=None, hard_timeout=None, unique=None, lock=None,
             lock_key=None, retry=None, retry_on=None, retry_method=None,
             batch=False):
        """
        Function decorator that defines the behavior of the function when it is
        used as a task. To use the default behavior, tasks don't need to be
        decorated. Arguments not listed below are described in the delay()
        method.

        * batch
          If set to True, the task will receive a list of dicts with args and
          kwargs and can process multiple tasks of the same type at once.
          Example: [{"args": [1], "kwargs": {}}, {"args": [2], "kwargs": {}}]
          Note that the list will only contain multiple items if the worker
          has set up BATCH_QUEUES for the specific queue.
        """

        def _delay(func):
            def _delay_inner(*args, **kwargs):
                self.delay(func, args=args, kwargs=kwargs)
            return _delay_inner

        def _wrap(func):
            if hard_timeout is not None:
                func._task_hard_timeout = hard_timeout
            if queue is not None:
                func._task_queue = queue
            if unique is not None:
                func._task_unique = unique
            if lock is not None:
                func._task_lock = lock
            if lock_key is not None:
                func._task_lock_key = lock_key
            if retry is not None:
                func._task_retry = retry
            if retry_on is not None:
                func._task_retry_on = retry_on
            if retry_method is not None:
                func._task_retry_method = retry_method
            if batch is not None:
                func._task_batch = batch

            func.delay = _delay(func)

            return func

        return _wrap

    def run_worker_with_args(self, args):
        """
        Runs a worker with the given command line args. The use case is running
        a worker from a custom manage script.
        """
        run_worker(args=args, obj=self)

    def run_worker(self, queues=None, module=None):
        """
        Main worker entry point method.

        The arguments are explained in the module-level run_worker() method's
        click options.
        """

        module_names = module or ''
        for module_name in module_names.split(','):
            module_name = module_name.strip()
            if module_name:
                importlib.import_module(module_name)
                self.log.debug('imported module', module_name=module_name)

        worker = Worker(self, queues)
        worker.run()

    def delay(self, func, args=None, kwargs=None, queue=None,
              hard_timeout=None, unique=None, lock=None, lock_key=None,
              when=None, retry=None, retry_on=None, retry_method=None):
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

        * lock_key
          If set, this implies lock=True and specifies the list of kwargs to
          use to construct the lock key. By default, all args and kwargs are
          serialized and hashed.

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
            if getattr(func, '_task_batch', False):
                return func([{'args': args, 'kwargs': kwargs}])
            else:
                return func(*args, **kwargs)

        serialized_name = serialize_func_name(func)

        if unique is None:
            unique = getattr(func, '_task_unique', False)

        if lock is None:
            lock = getattr(func, '_task_lock', False)

        if lock_key is None:
            lock_key = getattr(func, '_task_lock_key', None)

        if retry is None:
            retry = getattr(func, '_task_retry', False)

        if retry_on is None:
            retry_on = getattr(func, '_task_retry_on', None)

        if retry_method is None:
            retry_method = getattr(func, '_task_retry_method', None)

        if unique:
            task_id = gen_unique_id(serialized_name, args, kwargs)
        else:
            task_id = gen_id()

        if queue is None:
            queue = getattr(func, '_task_queue', self.config['DEFAULT_QUEUE'])

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
        if lock or lock_key:
            task['lock'] = True
            if lock_key:
                task['lock_key'] = lock_key
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
                retry_method = (serialize_func_name(retry_method), ())
            else:
                retry_method = (serialize_func_name(retry_method[0]),
                                retry_method[1])

            task['retry_method'] = retry_method
            if retry_on:
                task['retry_on'] = [serialize_func_name(cls)
                                    for cls in retry_on]

        serialized_task = json.dumps(task)

        if when:
            state = SCHEDULED
        else:
            state = QUEUED

        pipeline = self.connection.pipeline()
        pipeline.sadd(self._key(state), queue)
        pipeline.set(self._key('task', task_id), serialized_task)
        pipeline.zadd(self._key(state, queue), task_id, when or now)
        if state == QUEUED:
            pipeline.publish(self._key('activity'), queue)
        pipeline.execute()

    def get_queue_stats(self):
        """
        Returns a dict with stats about all the queues. The keys are the queue
        names, the values are dicts representing how many tasks are in a given
        status ("queued", "active", "error" or "scheduled").

        Example return value:
        { "default": { "queued": 1, "error": 2 } }
        """

        states = (QUEUED, ACTIVE, SCHEDULED, ERROR)

        pipeline = self.connection.pipeline()
        for state in states:
            pipeline.smembers(self._key(state))
        queue_results = pipeline.execute()

        pipeline = self.connection.pipeline()
        for state, result in zip(states, queue_results):
            for queue in result:
                pipeline.zcard(self._key(state, queue))
        card_results = pipeline.execute()

        queue_stats = defaultdict(dict)
        for state, result in zip(states, queue_results):
            for queue in result:
                queue_stats[queue][state] = card_results.pop(0)

        return queue_stats

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
@click.pass_context
def run_worker(context, **kwargs):
    # TODO: Make Redis settings configurable via click.
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
    tiger = context.obj or TaskTiger()
    tiger.log.setLevel(logging.DEBUG)
    logging.basicConfig(format='%(message)s')
    tiger.run_worker(**kwargs)
