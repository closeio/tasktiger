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

from .redis_scripts import RedisScripts

from ._internal import *
from .exceptions import *
from .retry import *
from .task import Task
from .worker import Worker

__all__ = ['TaskTiger', 'Worker', 'Task',

           # Exceptions
           'JobTimeoutException', 'RetryException', 'StopRetry',
           'TaskImportError', 'TaskNotFound',

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
    def __init__(self, connection=None, config=None, setup_structlog=False):
        """
        Initializes TaskTiger with the given Redis connection and config
        options. Optionally sets up structlog.
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

            # If this is True, all tasks except future tasks (when=a future
            # time) will be executed locally by blocking until the task
            # returns. This is useful for testing purposes.
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
            # timestamp in the active queue (in seconds). Tasks exceeding the
            # timeout value are requeued periodically. This may happen when a
            # worker crashes or is killed.
            'ACTIVE_TASK_UPDATE_TIMER': 10,
            'ACTIVE_TASK_UPDATE_TIMEOUT': 60,

            # How often we requeue expired tasks (in seconds), and how many
            # expired tasks we requeue at a time. The interval also determines
            # the lock timeout, i.e. it should be large enough to have enough
            # time to requeue a batch of tasks.
            'REQUEUE_EXPIRED_TASKS_INTERVAL': 30,
            'REQUEUE_EXPIRED_TASKS_BATCH_SIZE': 10,

            # Set up queues that will be processed in batch, i.e. multiple jobs
            # are taken out of the queue at the same time and passed as a list
            # to the worker method. Takes a dict where the key represents the
            # queue name and the value represents the batch size. Note that the
            # task needs to be declared as batch=True. Also note that any
            # subqueues will be automatically treated as batch queues, and the
            # batch value of the most specific subqueue name takes precedence.
            'BATCH_QUEUES': {},

            # How often to print stats.
            'STATS_INTERVAL': 60,

            # The following settings are only considered if no explicit queues
            # are passed in the command line (or to the queues argument in the
            # run_worker() method).

            # If non-empty, a worker only processeses the given queues.
            'ONLY_QUEUES': [],

            # If non-empty, a worker excludes the given queues from processing.
            'EXCLUDE_QUEUES': [],
        }
        if config:
            self.config.update(config)

        self.connection = connection or redis.Redis(decode_responses=True)
        self.scripts = RedisScripts(self.connection)

        if setup_structlog:
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

        self.log = structlog.get_logger(
            self.config['LOGGER_NAME'],
        ).bind()

        if setup_structlog:
            self.log.setLevel(logging.DEBUG)
            logging.basicConfig(format='%(message)s')

    def _get_current_task(self):
        if g['current_tasks'] is None:
            raise RuntimeError('Must be accessed from within a task')
        if g['current_task_is_batch']:
            raise RuntimeError('Must use current_tasks in a batch task.')
        return g['current_tasks'][0]

    def _get_current_tasks(self):
        if g['current_tasks'] is None:
            raise RuntimeError('Must be accessed from within a task')
        if not g['current_task_is_batch']:
            raise RuntimeError('Must use current_task in a non-batch task.')
        return g['current_tasks']

    """
    Properties to access the currently processing task (or tasks, in case of a
    batch task) from within the task. They must be invoked from within a task.
    """
    current_task = property(_get_current_task)
    current_tasks = property(_get_current_tasks)

    def _key(self, *parts):
        """
        Internal helper to get a Redis key, taking the REDIS_PREFIX into
        account. Parts are delimited with a colon. Individual parts shouldn't
        contain colons since we don't escape them.
        """
        return ':'.join([self.config['REDIS_PREFIX']] + list(parts))

    def task(self, queue=None, hard_timeout=None, unique=None, lock=None,
             lock_key=None, retry=None, retry_on=None, retry_method=None,
             batch=False):
        """
        Function decorator that defines the behavior of the function when it is
        used as a task. To use the default behavior, tasks don't need to be
        decorated.

        See README.rst for an explanation of the options.
        """

        def _delay(func):
            def _delay_inner(*args, **kwargs):
                return self.delay(func, args=args, kwargs=kwargs)
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

    def run_worker(self, queues=None, module=None, exclude_queues=None):
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

        worker = Worker(self,
                        queues.split(',') if queues else None,
                        exclude_queues.split(',') if exclude_queues else None)
        worker.run()

    def delay(self, func, args=None, kwargs=None, queue=None,
              hard_timeout=None, unique=None, lock=None, lock_key=None,
              when=None, retry=None, retry_on=None, retry_method=None):
        """
        Queues a task. See README.rst for an explanation of the options.
        """

        task = Task(self, func, args=args, kwargs=kwargs, queue=queue,
                    hard_timeout=hard_timeout, unique=unique,
                    lock=lock, lock_key=lock_key,
                    retry=retry, retry_on=retry_on, retry_method=retry_method)

        task.delay(when=when)

        return task

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
@click.option('-e', '--exclude-queues', help='If specified, exclude the given '
                                             'queue(s) from processing. '
                                             'Multiple queues can be '
                                             'separated by comma.')
@click.option('-h', '--host', help='Redis server hostname')
@click.option('-p', '--port', help='Redis server port')
@click.option('-a', '--password', help='Redis password')
@click.option('-n', '--db', help='Redis database number')
@click.pass_context
def run_worker(context, host, port, db, password, **kwargs):
    conn = redis.Redis(host, int(port or 6379), int(db or 0), password, decode_responses=True)
    tiger = context.obj or TaskTiger(setup_structlog=True, connection=conn)
    tiger.run_worker(**kwargs)

if __name__ == '__main__':
    run_worker()
