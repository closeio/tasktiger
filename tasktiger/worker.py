from collections import OrderedDict
import errno
import json
import os
import random
import select
import signal
import socket
import sys
import time
import traceback

from .redis_lock import Lock

from ._internal import *
from .exceptions import RetryException
from .retry import *
from .stats import StatsThread
from .task import Task
from .timeouts import UnixSignalDeathPenalty, JobTimeoutException

__all__ = ['Worker']

class Worker(object):
    def __init__(self, tiger, queues=None):
        """
        Internal method to initialize a worker.
        """

        self.tiger = tiger
        self.log = tiger.log.bind(pid=os.getpid())
        self.connection = tiger.connection
        self.scripts = tiger.scripts
        self.config = tiger.config
        self._key = tiger._key

        self.stats_thread = None

        if queues:
            self.queue_filter = queues
        elif self.config['ONLY_QUEUES']:
            self.queue_filter = self.config['ONLY_QUEUES']
        else:
            self.queue_filter = None

        self._stop_requested = False

    def _install_signal_handlers(self):
        """
        Sets up signal handlers for safely stopping the worker.
        """
        def request_stop(signum, frame):
            self._stop_requested = True
            self.log.info('stop requested, waiting for task to finish')
        signal.signal(signal.SIGINT, request_stop)
        signal.signal(signal.SIGTERM, request_stop)

    def _uninstall_signal_handlers(self):
        """
        Restores default signal handlers.
        """
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)

    def _filter_queues(self, queues):
        """
        Applies the queue filter to the given list of queues and returns the
        queues that match. Note that a queue name matches any subqueues
        starting with the name, followed by a date. For example, "foo" will
        match both "foo" and "foo.bar".
        """

        def match(queue):
            """
            Checks if any of the parts of the queue name match the filter.
            """
            for part in dotted_parts(queue):
                if part in self.queue_filter:
                    return True
            return False

        if self.queue_filter:
            return [q for q in queues if match(q)]
        else:
            return queues

    def _worker_queue_scheduled_tasks(self):
        """
        Helper method that takes due tasks from the SCHEDULED queue and puts
        them in the QUEUED queue for execution. This should be called
        periodically.
        """
        queues = set(self._filter_queues(self.connection.smembers(
                self._key(SCHEDULED))))
        now = time.time()
        for queue in queues:
            # Move due items from the SCHEDULED queue to the QUEUED queue. If
            # items were moved, remove the queue from the scheduled set if it
            # is empty, and add it to the queued set so the task gets picked
            # up. If any unique tasks are already queued, don't update their
            # queue time (because the new queue time would be later).
            result = self.scripts.zpoppush(
                self._key(SCHEDULED, queue),
                self._key(QUEUED, queue),
                self.config['SCHEDULED_TASK_BATCH_SIZE'],
                now,
                now,
                if_exists=('noupdate',),
                on_success=('update_sets', queue,
                            self._key(SCHEDULED), self._key(QUEUED)),
            )

            # XXX: ideally this would be in the same pipeline, but we only want
            # to announce if there was a result.
            if result:
                self.connection.publish(self._key('activity'), queue)

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
                message = next(gen)
                if message['type'] == 'message':
                    for queue in self._filter_queues([message['data']]):
                        self._queue_set.add(queue)
            else:
                break

    def _worker_queue_expired_tasks(self):
        """
        Helper method that takes expired tasks (where we didn't get a
        heartbeat until we reached a timeout) and puts them back into the
        QUEUED queue for re-execution.
        """
        active_queues = self.connection.smembers(self._key(ACTIVE))
        now = time.time()
        for queue in active_queues:
            # Move expired items from the ACTIVE queue to the QUEUED queue and
            # update the active and queued sets (remove the affected queue from
            # the ACTIVE set if there are no more items, and add it to the
            # QUEUED set if any items were moved). If items already exist in
            # the QUEUED queue, don't change their queue time.
            result = self.scripts.zpoppush(
                self._key(ACTIVE, queue),
                self._key(QUEUED, queue),
                self.config['ACTIVE_TASK_EXPIRED_BATCH_SIZE'],
                now - self.config['ACTIVE_TASK_UPDATE_TIMEOUT'],
                now,
                if_exists=('noupdate',),
                on_success=('update_sets', queue,
                            self._key(ACTIVE), self._key(QUEUED)),
            )
            # XXX: Ideally this would be atomic with the operation above.
            if result:
                self.log.info('queueing expired tasks', task_ids=result)
                self.connection.publish(self._key('activity'), queue)

    def _execute_forked(self, tasks, log):
        """
        Executes the tasks in the forked process. Multiple tasks can be passed
        for batch processing. However, they must all use the same function and
        will share the execution entry.
        """
        success = False

        execution = {}

        assert len(tasks)
        task_func = tasks[0].serialized_func
        assert all([task_func == task.serialized_func for task in tasks[1:]])

        execution['time_started'] = time.time()

        exc = None
        exc_info = None

        try:
            func = tasks[0].func

            is_batch_func = getattr(func, '_task_batch', False)
            g['current_task_is_batch'] = is_batch_func

            if is_batch_func:
                # Batch process if the task supports it.
                params = [{
                    'args': task.args,
                    'kwargs': task.kwargs,
                } for task in tasks]
                task_timeouts = [task.hard_timeout for task in tasks if task.hard_timeout is not None]
                hard_timeout = ((max(task_timeouts) if task_timeouts else None)
                                or
                                getattr(func, '_task_hard_timeout', None) or
                                self.config['DEFAULT_HARD_TIMEOUT'])

                g['current_tasks'] = tasks
                with UnixSignalDeathPenalty(hard_timeout):
                    func(params)

            else:
                # Process sequentially.
                for task in tasks:
                    hard_timeout = (task.hard_timeout or
                                    getattr(func, '_task_hard_timeout', None) or
                                    self.config['DEFAULT_HARD_TIMEOUT'])

                    g['current_tasks'] = [task]
                    with UnixSignalDeathPenalty(hard_timeout):
                        func(*task.args, **task.kwargs)

        except RetryException as exc:
            execution['retry'] = True
            if exc.method:
                execution['retry_method'] = serialize_retry_method(exc.method)
            execution['log_error'] = exc.log_error
            execution['exception_name'] = serialize_func_name(exc.__class__)
            exc_info = exc.exc_info
        except Exception as exc:
            execution['exception_name'] = serialize_func_name(exc.__class__)
        else:
            success = True

        if not success:
            execution['time_failed'] = time.time()
            if not exc_info:
                exc_info = sys.exc_info()
            # Currently we only log failed task executions to Redis.
            execution['traceback'] = \
                    ''.join(traceback.format_exception(*exc_info)) if exc_info != (None, None, None) else None
            execution['success'] = success
            execution['host'] = socket.gethostname()
            serialized_execution = json.dumps(execution)
            for task in tasks:
                self.connection.rpush(self._key('task', task.id, 'executions'),
                                      serialized_execution)

        return success

    def _heartbeat(self, queue, task_ids):
        """
        Updates the heartbeat for the given task IDs to prevent them from
        timing out and being requeued.
        """
        now = time.time()
        self.connection.zadd(self._key(ACTIVE, queue),
                             **{task_id: now for task_id in task_ids})

    def _execute(self, queue, tasks, log, locks, all_task_ids):
        """
        Executes the given tasks. Returns a boolean indicating whether
        the tasks were executed succesfully.
        """

        # The tasks must use the same function.
        assert len(tasks)
        task_func = tasks[0].serialized_func
        assert all([task_func == task.serialized_func for task in tasks[1:]])

        # Adapted from rq Worker.execute_job / Worker.main_work_horse
        child_pid = os.fork()
        if child_pid == 0:
            # Child process
            log = log.bind(child_pid=os.getpid())

            # We need to reinitialize Redis' connection pool, otherwise the
            # parent socket will be disconnected by the Redis library.
            # TODO: We might only need this if the task fails.
            pool = self.connection.connection_pool
            pool.__init__(pool.connection_class, pool.max_connections,
                          **pool.connection_kwargs)

            random.seed()
            signal.signal(signal.SIGINT, signal.SIG_IGN)
            success = self._execute_forked(tasks, log)
            os._exit(int(not success))
        else:
            # Main process
            log = log.bind(child_pid=child_pid)
            log.debug('processing', func=task_func, params=[{
                    'task_id': task.id,
                    'args': task.args,
                    'kwargs': task.kwargs,
            } for task in tasks])

            while True:
                try:
                    with UnixSignalDeathPenalty(self.config['ACTIVE_TASK_UPDATE_TIMER']):
                        _, return_code = os.waitpid(child_pid, 0)
                        break
                except OSError as e:
                    if e.errno != errno.EINTR:
                        raise
                except JobTimeoutException:
                    self._heartbeat(queue, all_task_ids)
                    for lock in locks:
                        lock.renew(self.config['ACTIVE_TASK_UPDATE_TIMEOUT'])

            status = not return_code
            return status

    def _process_from_queue(self, queue):
        """
        Internal method to processes a task batch from the given queue. Returns
        the task IDs that were processed (even if there was an error so that
        client code can assume the queue is empty if nothing was returned).
        """
        now = time.time()

        log = self.log.bind(queue=queue)

        # Fetch one item unless this is a batch queue.
        # XXX: It would be more efficient to loop in reverse order and break.
        batch_queues = self.config['BATCH_QUEUES']
        batch_size = 1
        for part in dotted_parts(queue):
            if part in batch_queues:
                batch_size = batch_queues[part]

        # Move an item to the active queue, if available.
        # We need to be careful when moving unique tasks: We currently don't
        # support concurrent processing of multiple unique tasks. If the task
        # is already in the ACTIVE queue, we need to execute the queued task
        # later, i.e. move it to the SCHEDULED queue (prefer the earliest
        # time if it's already scheduled). We want to make sure that the last
        # queued instance of the task always gets executed no earlier than it
        # was queued.
        later = time.time() + self.config['LOCK_RETRY']
        task_ids = self.scripts.zpoppush(
            self._key(QUEUED, queue),
            self._key(ACTIVE, queue),
            batch_size,
            None,
            now,
            if_exists=('add', self._key(SCHEDULED, queue), later, 'min'),
            on_success=('update_sets', queue, self._key(QUEUED),
                        self._key(ACTIVE), self._key(SCHEDULED))
        )

        if task_ids:
            # Get all tasks
            serialized_tasks = self.connection.mget([
                self._key('task', task_id) for task_id in task_ids
            ])

            # Parse tasks
            tasks = []
            for task_id, serialized_task in zip(task_ids, serialized_tasks):
                if serialized_task:
                    task_data = json.loads(serialized_task)
                else:
                    task_data = {}
                task = Task(self.tiger, queue=queue, _data=task_data,
                            _state=ACTIVE, _ts=now)
                if not task_data:
                    log.error('not found', task_id=task_id)
                    # Remove task
                    task._move()
                elif task.id != task_id:
                    log.error('task ID mismatch', task_id=task_id)
                    # Remove task
                    task._move()
                else:
                    tasks.append(task)

            # List of task IDs that exist and we will update the heartbeat on.
            valid_task_ids = set(task.id for task in tasks)

            # Group by task func
            tasks_by_func = OrderedDict()
            for task in tasks:
                func = task.serialized_func
                if func in tasks_by_func:
                    tasks_by_func[func].append(task)
                else:
                    tasks_by_func[func] = [task]

            # Execute tasks for each task func
            for tasks in tasks_by_func.values():
                success, processed_tasks = self._execute_task_group(queue,
                        tasks, valid_task_ids)
                for task in processed_tasks:
                    self._finish_task_processing(queue, task, success)

        return task_ids

    def _execute_task_group(self, queue, tasks, all_task_ids):
        """
        Executes the given tasks in the queue. Updates the heartbeat for task
        IDs passed in all_task_ids. This internal method is only meant to be
        called from within _process_from_queue.
        """
        log = self.log.bind(queue=queue)

        locks = []
        # Keep track of the acquired locks: If two tasks in the list require
        # the same lock we only acquire it once.
        lock_ids = set()

        ready_tasks = []
        for task in tasks:
            if task.lock:
                if task.lock_key:
                    kwargs = task.kwargs
                    lock_id = gen_unique_id(
                        task.serialized_func,
                        None,
                        {key: kwargs.get(key) for key in task.lock_key},
                    )
                else:
                    lock_id = gen_unique_id(
                        task.serialized_func,
                        task.args,
                        task.kwargs,
                    )

                if lock_id not in lock_ids:
                    lock = Lock(self.connection, self._key('lock', lock_id), timeout=self.config['ACTIVE_TASK_UPDATE_TIMEOUT'])

                    acquired = lock.acquire(blocking=False)
                    if acquired:
                        lock_ids.add(lock_id)
                        locks.append(lock)
                    else:
                        log.info('could not acquire lock', task_id=task.id)

                        # Reschedule the task (but if the task is already
                        # scheduled in case of a unique task, don't prolong
                        # the schedule date).
                        when = time.time() + self.config['LOCK_RETRY']
                        task._move(from_state=ACTIVE, to_state=SCHEDULED,
                                   when=when, mode='min')
                        # Make sure to remove it from this list so we don't
                        # re-add to the ACTIVE queue by updating the heartbeat.
                        all_task_ids.remove(task.id)
                        continue

            ready_tasks.append(task)

        if not ready_tasks:
            return True, []

        if self.stats_thread:
            self.stats_thread.report_task_start()
        success = self._execute(queue, ready_tasks, log, locks, all_task_ids)
        if self.stats_thread:
            self.stats_thread.report_task_end()

        for lock in locks:
            lock.release()

        return success, ready_tasks

    def _finish_task_processing(self, queue, task, success):
        """
        After a task is executed, this method is calling and ensures that
        the task gets properly removed from the ACTIVE queue and, in case of an
        error, retried or marked as failed.
        """
        log = self.log.bind(queue=queue, task_id=task.id)

        def _mark_done():
            # Remove the task from active queue
            if task.unique:
                # For unique tasks we need to check if they're in use in
                # another queue since they share the task ID.
                remove_task = 'check'
            else:
                remove_task = 'always'
            task._move(from_state=ACTIVE)
            log.debug('done')

        if success:
            _mark_done()
        else:
            should_retry = False
            should_log_error = True
            # Get execution info (for logging and retry purposes)
            execution = self.connection.lindex(
                self._key('task', task.id, 'executions'), -1)

            if execution:
                execution = json.loads(execution)

            if execution.get('retry'):
                if 'retry_method' in execution:
                    retry_func, retry_args = execution['retry_method']
                else:
                    # We expect the serialized method here.
                    retry_func, retry_args = serialize_retry_method( \
                            self.config['DEFAULT_RETRY_METHOD'])
                should_log_error = execution['log_error']
                should_retry = True

            if task.retry_method and not should_retry:
                retry_func, retry_args = task.retry_method
                if task.retry_on:
                    if execution:
                        exception_name = execution.get('exception_name')
                        try:
                            exception_class = import_attribute(exception_name)
                        except TaskImportError:
                            log.error('could not import exception',
                                      exception_name=exception_name)
                        else:
                            for n in task.retry_on:
                                if issubclass(exception_class, import_attribute(n)):
                                    should_retry = True
                                    break
                else:
                    should_retry = True

            state = ERROR

            when = time.time()

            log_context = {}

            if should_retry:
                retry_num = task.n_executions()
                log_context['retry_func'] = retry_func
                log_context['retry_num'] = retry_num

                try:
                    func = import_attribute(retry_func)
                except TaskImportError:
                    log.error('could not import retry function',
                              func=retry_func)
                else:
                    try:
                        retry_delay = func(retry_num, *retry_args)
                        log_context['retry_delay'] = retry_delay
                        when += retry_delay
                    except StopRetry:
                        pass
                    else:
                        state = SCHEDULED

            if state == ERROR and should_log_error:
                log_func = log.error
            else:
                log_func = log.warning

            log_func(func=task.serialized_func,
                     time_failed=execution.get('time_failed'),
                     traceback=execution.get('traceback'),
                     exception_name=execution.get('exception_name'),
                     **log_context)

            # Move task to the scheduled queue for retry, or move to error
            # queue if we don't want to retry.
            if state == ERROR and not should_log_error:
                _mark_done()
            else:
                task._move(from_state=ACTIVE, to_state=state, when=when)

    def _worker_run(self):
        """
        Performs one worker run:
        * Processes a set of messages from each queue and removes any empty
          queues from the working set.
        * Move any expired items from the active queue to the queued queue.
        * Move any scheduled items from the scheduled queue to the queued
          queue.
        """

        queues = list(self._queue_set)
        random.shuffle(queues)

        for queue in queues:
            if not self._process_from_queue(queue):
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

        if self.config['STATS_INTERVAL']:
            self.stats_thread = StatsThread(self)
            self.stats_thread.start()

        # First scan all the available queues for new items until they're empty.
        # Then, listen to the activity channel.
        # XXX: This can get inefficient when having lots of queues.

        self._pubsub = self.connection.pubsub()
        self._pubsub.subscribe(self._key('activity'))

        self._queue_set = set(self._filter_queues(
                self.connection.smembers(self._key(QUEUED))))

        try:
            while True:
                # Update the queue set on every iteration so we don't get stuck
                # on processing a specific queue.
                self._update_queue_set(timeout=self.config['SELECT_TIMEOUT'])

                self._install_signal_handlers()
                self._worker_run()
                self._uninstall_signal_handlers()
                if once and not self._queue_set:
                    break
                if self._stop_requested:
                    raise KeyboardInterrupt()
        except KeyboardInterrupt:
            pass

        except Exception as e:
            self.log.exception()
            raise

        finally:
            if self.stats_thread:
                self.stats_thread.stop()
                self.stats_thread = None

            self.log.info('done')
