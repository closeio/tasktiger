from __future__ import absolute_import

import datetime
import importlib
import logging
from collections import defaultdict

import click
import redis
import structlog

from ._internal import (
    ACTIVE,
    ERROR,
    QUEUED,
    SCHEDULED,
    classproperty,
    g,
    queue_matches,
    serialize_func_name,
)
from .redis_scripts import RedisScripts
from .redis_semaphore import Semaphore
from .retry import fixed
from .task import Task
from .worker import LOCK_REDIS_KEY, Worker

__all__ = ["TaskTiger"]


"""
Redis keys:

Set of all queues that contain items in the given state.
SET <prefix>:queued
SET <prefix>:active
SET <prefix>:error
SET <prefix>:scheduled

Serialized task for the given task ID.
STRING <prefix>:task:<task_id>

List of (failed) task executions. The oldest entries
could be truncated depending on the task parameters.
LIST <prefix>:task:<task_id>:executions

The total number of times a task was executed.
STRING <prefix>:task:<task_id>:executions_count

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

Task locks
STRING <prefix>:lock:<lock_hash>

Queue periodic tasks lock
STRING <prefix>:queue_periodic_tasks_lock

Queue locks scored by timeout
ZSET <prefix>:qslock:<queue>
STRING <prefix>:qlock:<queue> (Legacy queue locks that are no longer used)
"""


class TaskTiger:
    def __init__(
        self,
        connection=None,
        config=None,
        setup_structlog=False,
        lazy_init=False,
    ):
        """
        Initializes TaskTiger with the given Redis connection and config
        options. Optionally sets up structlog.

        Lazy initialization can be used to create a TaskTiger instance
        and import decorated tasks before Redis connection and configuration
        is available.

        You will have to provide Redis and config before scheduling any tasks
        using init method.
        """

        self.config = None

        # List of task functions that are executed periodically.
        self.periodic_task_funcs = {}

        if lazy_init:
            assert (
                connection is None
                and config is None
                and setup_structlog is False
            )
        else:
            self.init(
                connection=connection,
                config=config,
                setup_structlog=setup_structlog,
            )

    def init(self, connection=None, config=None, setup_structlog=False):
        """Provide Redis connection and config when lazy initialization is used."""

        if self.config is not None:
            raise RuntimeError("TaskTiger was already initialized")

        self.config = {
            # String that is used to prefix all Redis keys
            "REDIS_PREFIX": "t",
            # Name of the Python (structlog) logger
            "LOGGER_NAME": "tasktiger",
            # Where to queue tasks that don't have an explicit queue
            "DEFAULT_QUEUE": "default",
            # After how many seconds time out on listening on the activity
            # channel and check for scheduled or expired items.  The batch
            # timeout will delay the specified seconds after the first message
            # to wait for additional messages, useful for very active systems.
            # Appropriate values: 0 <= SELECT_BATCH_TIMEOUT <= SELECT_TIMEOUT
            "SELECT_TIMEOUT": 1,
            "SELECT_BATCH_TIMEOUT": 0,
            # If this is True, all tasks except future tasks (when=a future
            # time) will be executed locally by blocking until the task
            # returns. This is useful for testing purposes.
            "ALWAYS_EAGER": False,
            # If retry is True but no retry_method is specified for a given
            # task, use the following default method.
            "DEFAULT_RETRY_METHOD": fixed(60, 3),
            # After how many seconds a task that can't acquire a lock is
            # retried.
            "LOCK_RETRY": 1,
            # How many items to move at most from the scheduled queue to the
            # active queue.
            "SCHEDULED_TASK_BATCH_SIZE": 1000,
            # After how many seconds a long-running task is killed. This can be
            # overridden by the task or at queue time.
            "DEFAULT_HARD_TIMEOUT": 300,
            # The timer specifies how often the worker updates the task's
            # timestamp in the active queue (in seconds). Tasks exceeding the
            # timeout value are requeued periodically. This may happen when a
            # worker crashes or is killed.
            "ACTIVE_TASK_UPDATE_TIMER": 10,
            "ACTIVE_TASK_UPDATE_TIMEOUT": 60,
            # How often we requeue expired tasks (in seconds), and how many
            # expired tasks we requeue at a time. The interval also determines
            # the lock timeout, i.e. it should be large enough to have enough
            # time to requeue a batch of tasks.
            "REQUEUE_EXPIRED_TASKS_INTERVAL": 30,
            "REQUEUE_EXPIRED_TASKS_BATCH_SIZE": 10,
            # Time the scheduled tasks queue lock is held (in seconds). Other
            # workers processing the same queues won't be scheduling tasks as
            # long as the lock is held to prevent unnecessary load on Redis.
            "QUEUE_SCHEDULED_TASKS_TIME": 1,
            # Set up queues that will be processed in batch, i.e. multiple jobs
            # are taken out of the queue at the same time and passed as a list
            # to the worker method. Takes a dict where the key represents the
            # queue name and the value represents the batch size. Note that the
            # task needs to be declared as batch=True. Also note that any
            # subqueues will be automatically treated as batch queues, and the
            # batch value of the most specific subqueue name takes precedence.
            "BATCH_QUEUES": {},
            # How often to print stats.
            "STATS_INTERVAL": 60,
            # Single worker queues can reduce redis activity in some use cases
            # by locking at the queue level instead of just at the task or task
            # group level. These queues will only allow a single worker to
            # access the queue at a time.  This can be useful in environments
            # with large queues and many worker processes that need aggressive
            # locking techniques.
            "SINGLE_WORKER_QUEUES": [],
            # The following settings are only considered if no explicit queues
            # are passed in the command line (or to the queues argument in the
            # run_worker() method).
            # If non-empty, a worker only processes the given queues.
            "ONLY_QUEUES": [],
            # If non-empty, a worker excludes the given queues from processing.
            "EXCLUDE_QUEUES": [],
            # List of context manager instances that will be called in each
            # forked child process. Useful to do things like close file handles
            # or reinitialize crypto libraries.
            "CHILD_CONTEXT_MANAGERS": [],
            # Store traceback in execution history for failed tasks. This can
            # increase Redis storage requirements and therefore can be disabled
            # if that is a concern.
            "STORE_TRACEBACKS": True,
            # Set to > 0 to poll periodically for queues with tasks. Otherwise
            # subscribe to the activity channel. Use for more efficient task
            # processing with a large amount of workers.
            "POLL_TASK_QUEUES_INTERVAL": 0,
            # Whether to publish new tasks to the activity channel. Only set to
            # False if all the workers are polling queues.
            "PUBLISH_QUEUED_TASKS": True,
        }
        if config:
            self.config.update(config)

        if setup_structlog:
            structlog.configure(
                processors=[
                    structlog.stdlib.add_log_level,
                    structlog.stdlib.filter_by_level,
                    structlog.processors.TimeStamper(fmt="iso", utc=True),
                    structlog.processors.StackInfoRenderer(),
                    structlog.processors.format_exc_info,
                    structlog.processors.JSONRenderer(),
                ],
                context_class=dict,
                logger_factory=structlog.stdlib.LoggerFactory(),
                wrapper_class=structlog.stdlib.BoundLogger,
                cache_logger_on_first_use=True,
            )

        self.log = structlog.get_logger(self.config["LOGGER_NAME"]).bind()

        if setup_structlog:
            self.log.setLevel(logging.DEBUG)
            logging.basicConfig(format="%(message)s")

        self.connection = connection or redis.Redis(decode_responses=True)
        self.scripts = RedisScripts(self.connection)

    def _get_current_task(self):
        if g["current_tasks"] is None:
            raise RuntimeError("Must be accessed from within a task.")
        if g["current_task_is_batch"]:
            raise RuntimeError("Must use current_tasks in a batch task.")
        return g["current_tasks"][0]

    def _get_current_tasks(self):
        if g["current_tasks"] is None:
            raise RuntimeError("Must be accessed from within a task.")
        if not g["current_task_is_batch"]:
            raise RuntimeError("Must use current_task in a non-batch task.")
        return g["current_tasks"]

    """
    Properties to access the currently processing task (or tasks, in case of a
    batch task) from within the task. They must be invoked from within a task.
    """
    current_task = property(_get_current_task)
    current_tasks = property(_get_current_tasks)

    @classproperty
    def current_instance(self):
        """
        Access the current TaskTiger instance from within a task.
        """
        if g["tiger"] is None:
            raise RuntimeError("Must be accessed from within a task.")
        return g["tiger"]

    def _key(self, *parts):
        """
        Internal helper to get a Redis key, taking the REDIS_PREFIX into
        account. Parts are delimited with a colon. Individual parts shouldn't
        contain colons since we don't escape them.
        """
        return ":".join([self.config["REDIS_PREFIX"]] + list(parts))

    def task(
        self,
        _fn=None,
        queue=None,
        hard_timeout=None,
        unique=None,
        unique_key=None,
        lock=None,
        lock_key=None,
        retry=None,
        retry_on=None,
        retry_method=None,
        schedule=None,
        batch=False,
        max_queue_size=None,
        max_stored_executions=None,
        runner_class=None,
    ):
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

        # Periodic tasks are unique.
        if schedule is not None:
            unique = True

        def _wrap(func):
            if hard_timeout is not None:
                func._task_hard_timeout = hard_timeout
            if queue is not None:
                func._task_queue = queue
            if unique is not None:
                func._task_unique = unique
            if unique_key is not None:
                func._task_unique_key = unique_key
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
            if schedule is not None:
                func._task_schedule = schedule
            if max_queue_size is not None:
                func._task_max_queue_size = max_queue_size
            if max_stored_executions is not None:
                func._task_max_stored_executions = max_stored_executions
            if runner_class is not None:
                func._task_runner_class = runner_class

            func.delay = _delay(func)

            if schedule is not None:
                serialized_func = serialize_func_name(func)
                assert (
                    serialized_func not in self.periodic_task_funcs
                ), "attempted duplicate registration of periodic task"
                self.periodic_task_funcs[serialized_func] = func

            return func

        return _wrap if _fn is None else _wrap(_fn)

    def run_worker_with_args(self, args):
        """
        Runs a worker with the given command line args. The use case is running
        a worker from a custom manage script.
        """
        run_worker(args=args, obj=self)

    def run_worker(
        self,
        queues=None,
        module=None,
        exclude_queues=None,
        max_workers_per_queue=None,
        store_tracebacks=None,
    ):
        """
        Main worker entry point method.

        The arguments are explained in the module-level run_worker() method's
        click options.
        """

        try:
            module_names = module or ""
            for module_name in module_names.split(","):
                module_name = module_name.strip()
                if module_name:
                    importlib.import_module(module_name)
                    self.log.debug("imported module", module_name=module_name)

            worker = Worker(
                self,
                queues.split(",") if queues else None,
                exclude_queues.split(",") if exclude_queues else None,
                max_workers_per_queue=max_workers_per_queue,
                store_tracebacks=store_tracebacks,
            )
            worker.run()
        except Exception:
            self.log.exception("Unhandled exception")
            raise

    def delay(
        self,
        func,
        args=None,
        kwargs=None,
        queue=None,
        hard_timeout=None,
        unique=None,
        unique_key=None,
        lock=None,
        lock_key=None,
        when=None,
        retry=None,
        retry_on=None,
        retry_method=None,
        max_queue_size=None,
        max_stored_executions=None,
        runner_class=None,
    ):
        """
        Queues a task. See README.rst for an explanation of the options.
        """

        task = Task(
            self,
            func,
            args=args,
            kwargs=kwargs,
            queue=queue,
            hard_timeout=hard_timeout,
            unique=unique,
            unique_key=unique_key,
            lock=lock,
            lock_key=lock_key,
            retry=retry,
            retry_on=retry_on,
            retry_method=retry_method,
            max_stored_executions=max_stored_executions,
            runner_class=runner_class,
        )

        task.delay(when=when, max_queue_size=max_queue_size)

        return task

    def get_queue_sizes(self, queue):
        """
        Get the queue's number of tasks in each state.

        Returns dict with queue size for the QUEUED, SCHEDULED, and ACTIVE
        states. Does not include size of error queue.
        """

        states = [QUEUED, SCHEDULED, ACTIVE]
        pipeline = self.connection.pipeline()
        for state in states:
            pipeline.zcard(self._key(state, queue))
        results = pipeline.execute()
        return dict(zip(states, results))

    def get_total_queue_size(self, queue):
        """Get total queue size for QUEUED, SCHEDULED, and ACTIVE states."""

        return sum(self.get_queue_sizes(queue).values())

    def get_queue_system_lock(self, queue):
        """
        Get system lock timeout

        Returns time system lock expires or None if lock does not exist
        """

        key = self._key(LOCK_REDIS_KEY, queue)
        return Semaphore.get_system_lock(self.connection, key)

    def set_queue_system_lock(self, queue, timeout):
        """
        Set system lock on a queue.

        Max workers for this queue must be used for this to have any effect.

        This will keep workers from processing tasks for this queue until
        the timeout has expired. Active tasks will continue processing their
        current task.

        timeout is number of seconds to hold the lock
        """

        key = self._key(LOCK_REDIS_KEY, queue)
        Semaphore.set_system_lock(self.connection, key, timeout)

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

    def purge_errored_tasks(
        self,
        queues=None,
        exclude_queues=None,
        last_execution_before=None,
        limit=5000,
    ):
        """Purge failed tasks left in the ERROR state

        Example usage::

            limit = 500
            n_processed = limit
            while n_processed == limit:
                n_processed = tiger.purge_errored_tasks(
                    queues=['my-queue'],
                    exclude_queues=['other-queue'],
                    last_execution_before=(
                        datetime.datetime.utcnow()
                        - datetime.timedelta(days=14)
                    ),
                    limit=limit,
                ):
                time.sleep(.1)  # don't overload redis

            # or to purge all in one go
            tiger.purge_errored_tasks(limit=None)

        :param iterable(str) queues: Queues to include
        :param iterable(str) exclude_queues: Queues to exclude
        :param datetime last_execution_before: If provided, only deletes tasks
            older than this.
        :param int limit: Stops after purging ``limit`` tasks. Can be
            explicitly set to None to remove limit and purge all at once.

        :returns: The total number of tasks purged
        """
        # some sanity checking and kind error messages for arguments
        if last_execution_before:
            assert isinstance(last_execution_before, datetime.datetime)
        if limit is not None:
            assert limit > 0, "If specified, limit must be greater than zero"

        only_queues = set(queues or self.config["ONLY_QUEUES"] or [])
        exclude_queues = set(
            exclude_queues or self.config["EXCLUDE_QUEUES"] or []
        )

        def errored_tasks():
            queues_with_errors = self.connection.smembers(self._key(ERROR))
            for queue in queues_with_errors:
                if not queue_matches(
                    queue,
                    only_queues=only_queues,
                    exclude_queues=exclude_queues,
                ):
                    continue

                skip = 0
                total_tasks = None
                task_limit = 5000
                while total_tasks is None or skip < total_tasks:
                    total_tasks, tasks = Task.tasks_from_queue(
                        self, queue, ERROR, skip=skip, limit=task_limit
                    )
                    for task in tasks:
                        if (
                            last_execution_before
                            and task.ts
                            and task.ts > last_execution_before
                        ):
                            continue
                        yield task
                    skip += task_limit

        total_processed = 0
        for idx, task in enumerate(errored_tasks()):
            task.delete()

            total_processed = idx + 1
            if limit and total_processed >= limit:
                break

        return total_processed

    def would_process_configured_queue(self, queue_name):
        """
        Return if a queue_name would be processed by this tasktiger instance. It does not consider explicit
        --queue param.
        """
        return queue_matches(
            queue_name,
            self.config["ONLY_QUEUES"],
            self.config["EXCLUDE_QUEUES"],
        )


@click.command()
@click.option(
    "-q",
    "--queues",
    help=(
        "If specified, only the given queue(s) are processed. Multiple queues "
        "can be separated by comma."
    ),
)
@click.option(
    "-m",
    "--module",
    help=(
        "Module(s) to import when launching the worker. This improves task "
        "performance since the module doesn't have to be reimported every "
        "time a task is forked. Multiple modules can be separated by comma."
    ),
)
@click.option(
    "-e",
    "--exclude-queues",
    help=(
        "If specified, exclude the given queue(s) from processing. Multiple "
        "queues can be separated by comma."
    ),
)
@click.option(
    "-M",
    "--max-workers-per-queue",
    help="Maximum workers allowed to process a queue",
    type=int,
)
@click.option(
    "--store-tracebacks/--no-store-tracebacks",
    help="Store tracebacks with execution history",
    default=None,
)
@click.option("-h", "--host", help="Redis server hostname")
@click.option("-p", "--port", help="Redis server port")
@click.option("-a", "--password", help="Redis password")
@click.option("-n", "--db", help="Redis database number")
@click.pass_context
def run_worker(context, host, port, db, password, **kwargs):
    conn = redis.Redis(
        host, int(port or 6379), int(db or 0), password, decode_responses=True
    )
    tiger = context.obj or TaskTiger(setup_structlog=True, connection=conn)
    tiger.run_worker(**kwargs)
