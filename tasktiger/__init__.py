import calendar
import click
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
from .timeouts import JobTimeoutException

__all__ = ['TaskTiger', 'Worker',

           # Exceptions
           'JobTimeoutException', 'StopRetry',

           # Retry methods
           'fixed', 'linear', 'exponential']

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

class TaskTiger(object):
    def __init__(self, connection=None, config=None):
        """
        Initializes TaskTiger with the given Redis connection and config
        options.
        """

        # TODO: migrate other config options to this (where it makes sense)
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
        }
        if config:
            self.config.update(config)

        self.connection = connection or redis.Redis()
        self.scripts = RedisScripts(self.connection)
        self.log = structlog.get_logger(
            self.config['LOGGER_NAME'],
        ).bind()

    def _key(self, *parts):
        return ':'.join([self.config['REDIS_PREFIX']] + list(parts))

    @classmethod
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

    def run_worker_with_args(self, args):
        run_worker(args=args, obj=self)

    def run_worker(self, queues=None, module=None):
        """
        Main worker entry point method.
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

        serialized_name = serialize_func_name(func)

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
            queue_type = SCHEDULED
        else:
            queue_type = QUEUED

        pipeline = self.connection.pipeline()
        pipeline.sadd(self._key(queue_type), queue)
        pipeline.set(self._key('task', task_id), serialized_task)
        pipeline.zadd(self._key(queue_type, queue), task_id, when or now)
        if queue_type == QUEUED:
            pipeline.publish(self._key('activity'), queue)
        pipeline.execute()

# Currently it's not necessary to have a TaskTiger instance to define a task.
task = TaskTiger.task

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
