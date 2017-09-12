from collections import OrderedDict
import errno
import fcntl
import json
import os
import random
import select
import signal
import socket
import sys
import threading
import time
import traceback

from .redis_lock import Lock

from ._internal import *
from .exceptions import RetryException, TaskNotFound
from .retry import *
from .stats import StatsThread
from .task import Task
from .timeouts import UnixSignalDeathPenalty, JobTimeoutException

__all__ = ['Worker']

def sigchld_handler(*args):
    # Nothing to do here. This is just a dummy handler that we set up to catch
    # the child process exiting.
    pass

class Worker(object):
    def __init__(self, tiger, queues=None, exclude_queues=None,
                 single_worker_queues=None):
        """
        Internal method to initialize a worker.
        """

        self.tiger = tiger
        self.log = tiger.log.bind(pid=os.getpid())
        self.connection = tiger.connection
        self.scripts = tiger.scripts
        self.config = tiger.config
        self._key = tiger._key
        self._did_work = True
        self._last_task_check = 0
        self.stats_thread = None

        if queues:
            self.only_queues = set(queues)
        elif self.config['ONLY_QUEUES']:
            self.only_queues = set(self.config['ONLY_QUEUES'])
        else:
            self.only_queues = set()

        if exclude_queues:
            self.exclude_queues = set(exclude_queues)
        elif self.config['EXCLUDE_QUEUES']:
            self.exclude_queues = set(self.config['EXCLUDE_QUEUES'])
        else:
            self.exclude_queues = set()

        if single_worker_queues:
            self.single_worker_queues = set(single_worker_queues)
        elif self.config['SINGLE_WORKER_QUEUES']:
            self.single_worker_queues = set(self.config['SINGLE_WORKER_QUEUES'])
        else:
            self.single_worker_queues = set()

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
            Returns whether the given queue should be included by checking each
            part of the queue name.
            """
            for part in reversed_dotted_parts(queue):
                if part in self.exclude_queues:
                    return False
                if part in self.only_queues:
                    return True
            return not self.only_queues

        return [q for q in queues if match(q)]

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
            self.log.debug('scheduled tasks', queue=queue, qty=len(result))
            # XXX: ideally this would be in the same pipeline, but we only want
            # to announce if there was a result.
            if result:
                self.connection.publish(self._key('activity'), queue)
                self._did_work = True

    def _wait_for_new_tasks(self, timeout=0, batch_timeout=0):
        """
        Check activity channel and wait as necessary.

        This method is also used to slow down the main processing loop to reduce
        the effects of rapidly sending Redis commands.  This method will exit
        for any of these conditions:
           1. _did_work is True, suggests there could be more work pending
           2. Found new queue and after batch timeout. Note batch timeout
              can be zero so it will exit immediately.
           3. Timeout seconds have passed, this is the maximum time to stay in
              this method
        """

        new_queue_found = False
        start_time = batch_exit = time.time()
        while True:
            # Check to see if batch_exit has been updated
            if batch_exit > start_time:
                pubsub_sleep = batch_exit - time.time()
            else:
                pubsub_sleep = start_time + timeout - time.time()
            message = self._pubsub.get_message(timeout=0 if pubsub_sleep < 0 or
                                               self._did_work
                                               else pubsub_sleep)

            # Pull remaining messages off of channel
            while message:
                if message['type'] == 'message':
                    new_queue_found, batch_exit = self._process_queue_message(
                        message['data'], new_queue_found, batch_exit,
                        start_time, timeout, batch_timeout
                    )

                message = self._pubsub.get_message()

            if self._did_work:
                break   # Exit immediately if we did work during the last
                        # execution loop because there might be more work to do
            elif time.time() >= batch_exit and new_queue_found:
                break   # After finding a new queue we can wait until the
                        # batch timeout expires
            elif time.time() - start_time > timeout:
                break   # Always exit after our maximum wait time


    def _worker_queue_expired_tasks(self):
        """
        Helper method that takes expired tasks (where we didn't get a
        heartbeat until we reached a timeout) and puts them back into the
        QUEUED queue for re-execution if they're idempotent, i.e. retriable
        on JobTimeoutException. Otherwise, tasks are moved to the ERROR queue
        and an exception is logged.
        """

        # Note that we use the lock both to unnecessarily prevent multiple
        # workers from requeueing expired tasks, as well as to space out
        # this task (i.e. we don't release the lock unless we've exhausted our
        # batch size, which will hopefully never happen)
        lock = Lock(self.connection, self._key('lock', 'queue_expired_tasks'),
                    timeout=self.config['REQUEUE_EXPIRED_TASKS_INTERVAL'])

        acquired = lock.acquire(blocking=False)
        if not acquired:
            return

        now = time.time()

        # Get a batch of expired tasks.
        task_data = self.scripts.get_expired_tasks(
            self.config['REDIS_PREFIX'],
            now - self.config['ACTIVE_TASK_UPDATE_TIMEOUT'],
            self.config['REQUEUE_EXPIRED_TASKS_BATCH_SIZE']
        )

        for (queue, task_id) in task_data:
            self.log.debug('expiring task', queue=queue, task_id=task_id)
            self._did_work = True
            try:
                task = Task.from_id(self.tiger, queue, ACTIVE, task_id)
                if task.should_retry_on(JobTimeoutException):
                    self.log.info('queueing expired task',
                                  queue=queue, task_id=task_id)

                    # Task is idempotent and can be requeued. If the task
                    # already exists in the QUEUED queue, don't change its
                    # time.
                    task._move(from_state=ACTIVE,
                               to_state=QUEUED,
                               when=now,
                               mode='nx')
                else:
                    self.log.error('failing expired task',
                                   queue=queue, task_id=task_id)

                    # Assume the task can't be retried and move it to the error
                    # queue.
                    task._move(from_state=ACTIVE, to_state=ERROR, when=now)
            except TaskNotFound:
                # Either the task was requeued by another worker, or we
                # have a task without a task object.

                # XXX: Ideally, the following block should be atomic.
                if not self.connection.get(self._key('task', task_id)):
                    self.log.error('not found', queue=queue, task_id=task_id)
                    task = Task(self.tiger, queue=queue,
                                _data={'id': task_id}, _state=ACTIVE)
                    task._move()

        # Release the lock immediately if we processed a full batch. This way,
        # another process will be able to pick up another batch immediately
        # without waiting for the lock to time out.
        if len(task_data) == self.config['REQUEUE_EXPIRED_TASKS_BATCH_SIZE']:
            lock.release()

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
            exc_info = exc.exc_info or sys.exc_info()
        except (JobTimeoutException, Exception) as exc:
            execution['exception_name'] = serialize_func_name(exc.__class__)
            exc_info = sys.exc_info()
        else:
            success = True

        if not success:
            execution['time_failed'] = time.time()
            # Currently we only log failed task executions to Redis.
            execution['traceback'] = \
                    ''.join(traceback.format_exception(*exc_info))
            execution['success'] = success
            execution['host'] = socket.gethostname()
            serialized_execution = json.dumps(execution)
            for task in tasks:
                self.connection.rpush(self._key('task', task.id, 'executions'),
                                      serialized_execution)

        return success

    def _get_queue_batch_size(self, queue):
        """Get queue batch size."""

        # Fetch one item unless this is a batch queue.
        # XXX: It would be more efficient to loop in reverse order and break.
        batch_queues = self.config['BATCH_QUEUES']
        batch_size = 1
        for part in dotted_parts(queue):
            if part in batch_queues:
                batch_size = batch_queues[part]

        return batch_size

    def _get_queue_lock(self, queue, log):
        """Get queue lock for single worker queues.

        For single worker queues it returns a Lock if acquired and whether
        it failed to acquire the lock.
        """

        # Check if this is single worker queue
        single_worker_queue = False
        for part in dotted_parts(queue):
            if part in self.single_worker_queues:
                log.debug('single worker queue')
                single_worker_queue = True
                break

        # Single worker queues require us to get a queue lock before
        # moving tasks
        if single_worker_queue:
            queue_lock = Lock(self.connection, self._key('qlock', queue),
                              timeout=self.config['ACTIVE_TASK_UPDATE_TIMEOUT'])
            acquired = queue_lock.acquire(blocking=False)
            if not acquired:
                return None, True
            log.debug('acquired swq lock')
        else:
            queue_lock = None

        return queue_lock, False

    def _heartbeat(self, queue, task_ids):
        """
        Updates the heartbeat for the given task IDs to prevent them from
        timing out and being requeued.
        """
        now = time.time()
        self.connection.zadd(self._key(ACTIVE, queue),
                             **{task_id: now for task_id in task_ids})

    def _execute(self, queue, tasks, log, locks, queue_lock, all_task_ids):
        """
        Executes the given tasks. Returns a boolean indicating whether
        the tasks were executed successfully.
        """

        # The tasks must use the same function.
        assert len(tasks)
        task_func = tasks[0].serialized_func
        assert all([task_func == task.serialized_func for task in tasks[1:]])

        # Before executing periodic tasks, queue them for the next period.
        if task_func in self.tiger.periodic_task_funcs:
            tasks[0]._queue_for_next_period()

        with g_fork_lock:
            child_pid = os.fork()

        if child_pid == 0:
            # Child process
            log = log.bind(child_pid=os.getpid())

            # Disconnect the Redis connection inherited from the main process.
            # Note that this doesn't disconnect the socket in the main process.
            self.connection.connection_pool.disconnect()

            random.seed()

            # Ignore Ctrl+C in the child so we don't abort the job -- the main
            # process already takes care of a graceful shutdown.
            signal.signal(signal.SIGINT, signal.SIG_IGN)

            success = self._execute_forked(tasks, log)

            # Wait for any threads that might be running in the child, just
            # like sys.exit() would. Note we don't call sys.exit() directly
            # because it would perform additional cleanup (e.g. calling atexit
            # handlers twice). See also: https://bugs.python.org/issue18966
            threading._shutdown()

            os._exit(int(not success))
        else:
            # Main process
            log = log.bind(child_pid=child_pid)
            for task in tasks:
                log.info('processing', func=task_func, task_id=task.id,
                         params={'args': task.args, 'kwargs': task.kwargs})

            # Attach a signal handler to SIGCHLD (sent when the child process
            # exits) so we can capture it.
            signal.signal(signal.SIGCHLD, sigchld_handler)

            # Since newer Python versions retry interrupted system calls we can't
            # rely on the fact that select() is interrupted with EINTR. Instead,
            # we'll set up a wake-up file descriptor below.

            # Create a new pipe and apply the non-blocking flag (required for
            # set_wakeup_fd).
            pipe_r, pipe_w = os.pipe()
            flags = fcntl.fcntl(pipe_w, fcntl.F_GETFL, 0)
            flags = flags | os.O_NONBLOCK
            fcntl.fcntl(pipe_w, fcntl.F_SETFL, flags)

            # A byte will be written to pipe_w if a signal occurs (and can be
            # read from pipe_r).
            old_wakeup_fd = signal.set_wakeup_fd(pipe_w)

            def check_child_exit():
                """
                Do a non-blocking check to see if the child process exited.
                Returns None if the process is still running, or the exit code
                value of the child process.
                """
                try:
                    pid, return_code = os.waitpid(child_pid, os.WNOHANG)
                    if pid != 0: # The child process is done.
                        return return_code
                except OSError as e:
                    # Of course EINTR can happen if the child process exits
                    # while we're checking whether it exited. In this case it
                    # should be safe to retry.
                    if e.errno == errno.EINTR:
                        return check_child_exit()
                    else:
                        raise

            # Wait for the child to exit and perform a periodic heartbeat.
            # We check for the child twice in this loop so that we avoid
            # unnecessary waiting if the child exited just before entering
            # the while loop or while renewing heartbeat/locks.
            while True:
                return_code = check_child_exit()
                if return_code is not None:
                    break

                # Wait until the timeout or a signal / child exit occurs.
                try:
                    select.select([pipe_r], [], [],
                                  self.config['ACTIVE_TASK_UPDATE_TIMER'])
                except select.error as e:
                    if e.args[0] != errno.EINTR:
                        raise

                return_code = check_child_exit()
                if return_code is not None:
                    break

                try:
                    self._heartbeat(queue, all_task_ids)
                    for lock in locks:
                        lock.renew(self.config['ACTIVE_TASK_UPDATE_TIMEOUT'])
                    if queue_lock:
                        queue_lock.renew(self.config['ACTIVE_TASK_UPDATE_TIMEOUT'])
                except OSError as e:
                    # EINTR happens if the task completed. Since we're just
                    # renewing locks/heartbeat it's okay if we get interrupted.
                    if e.errno != errno.EINTR:
                        raise

            # Restore signals / clean up
            signal.signal(signal.SIGCHLD, signal.SIG_DFL)
            signal.set_wakeup_fd(old_wakeup_fd)
            os.close(pipe_r)
            os.close(pipe_w)

            success = (return_code == 0)
            return success

    def _process_queue_message(self, message_queue, new_queue_found, batch_exit,
                               start_time, timeout, batch_timeout):
        """Process a queue message from activity channel."""

        for queue in self._filter_queues([message_queue]):
            if queue not in self._queue_set:
                if not new_queue_found:
                    new_queue_found = True
                    batch_exit = time.time() + batch_timeout
                    # Limit batch_exit to max timeout
                    if batch_exit > start_time + timeout:
                        batch_exit = start_time + timeout
                self._queue_set.add(queue)
                self.log.debug('new queue', queue=queue)

        return new_queue_found, batch_exit

    def _process_queue_tasks(self, queue, queue_lock, task_ids, now, log):
        """Process tasks in queue."""

        processed_count = 0

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
                # In the rare case where we don't find the task which is
                # queued (see ReliabilityTestCase.test_task_disappears),
                # we log an error and remove the task below. We need to
                # at least initialize the Task object with an ID so we can
                # remove it.
                task_data = {'id': task_id}

            task = Task(self.tiger, queue=queue, _data=task_data,
                        _state=ACTIVE, _ts=now)

            if not serialized_task:
                # Remove task as per comment above
                log.error('not found', task_id=task_id)
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
                    tasks, valid_task_ids, queue_lock)
            processed_count = processed_count + len(processed_tasks)
            log.debug('processed', attempted=len(tasks),
                      processed=processed_count)
            for task in processed_tasks:
                self._finish_task_processing(queue, task, success)

        return processed_count

    def _process_from_queue(self, queue):
        """
        Internal method to process a task batch from the given queue.

        Args:
            queue: Queue name to be processed

        Returns:
            Task IDs:   List of tasks that were processed (even if there was an
                        error so that client code can assume the queue is empty
                        if nothing was returned)
            Count:      The number of tasks that were attempted to be executed or
                        -1 if the queue lock couldn't be acquired.
        """
        now = time.time()

        log = self.log.bind(queue=queue)

        batch_size = self._get_queue_batch_size(queue)

        queue_lock, failed_to_acquire = self._get_queue_lock(queue, log)
        if failed_to_acquire:
            return [], -1

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
        log.debug('moved tasks', src_queue=QUEUED, dest_queue=ACTIVE,
                  qty=len(task_ids))

        processed_count = 0
        if task_ids:
            processed_count = self._process_queue_tasks(queue, queue_lock,
                                                        task_ids, now, log)

        if queue_lock:
            queue_lock.release()
            log.debug('released swq lock')

        return task_ids, processed_count

    def _execute_task_group(self, queue, tasks, all_task_ids, queue_lock):
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
        success = self._execute(queue, ready_tasks, log, locks, queue_lock, all_task_ids)
        if self.stats_thread:
            self.stats_thread.report_task_end()

        for lock in locks:
            lock.release()

        return success, ready_tasks

    def _finish_task_processing(self, queue, task, success):
        """
        After a task is executed, this method is called and ensures that
        the task gets properly removed from the ACTIVE queue and, in case of an
        error, retried or marked as failed.
        """
        log = self.log.bind(queue=queue, task_id=task.id)

        def _mark_done():
            # Remove the task from active queue
            task._move(from_state=ACTIVE)
            log.info('done')

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

            if execution and execution.get('retry'):
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
                            if task.should_retry_on(exception_class):
                                should_retry = True
                else:
                    should_retry = True

            state = ERROR

            when = time.time()

            log_context = {
                'func': task.serialized_func
            }

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

            if execution:
                if state == ERROR and should_log_error:
                    log_func = log.error
                else:
                    log_func = log.warning

                log_context.update({
                    'time_failed': execution.get('time_failed'),
                    'traceback': execution.get('traceback'),
                    'exception_name': execution.get('exception_name'),
                })

                log_func('task error', **log_context)
            else:
                log.error('execution not found', **log_context)

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
            task_ids, processed_count = self._process_from_queue(queue)
            # Remove queue if queue was processed and was empty
            if not task_ids and processed_count != -1:
                self._queue_set.remove(queue)

            if self._stop_requested:
                break
            if processed_count > 0:
                self._did_work = True

        if time.time() - self._last_task_check > self.config['SELECT_TIMEOUT'] and \
           not self._stop_requested:
            self._worker_queue_scheduled_tasks()
            self._worker_queue_expired_tasks()
            self._last_task_check = time.time()

    def _queue_periodic_tasks(self):
        # If we can acquire the lock, queue any periodic tasks that are not
        # queued yet. Otherwise, assume another worker is doing this.
        funcs = self.tiger.periodic_task_funcs.values()

        if not funcs:
            return

        lock = Lock(
            self.connection,
            self._key('queue_periodic_tasks_lock'),
            timeout=self.config['QUEUE_PERIODIC_TASKS_LOCK_TIMEOUT'])
        acquired = lock.acquire(blocking=False)
        if not acquired:
            self.log.info('could not acquire lock to queue periodic tasks '
                          ' (assuming other process will)')
            return

        try:
            for func in funcs:
                # Check if task is queued (in scheduled/active queues).
                task = Task(self.tiger, func)

                # Since periodic tasks are unique, we can use the ID to look up
                # the task.
                pipeline = self.tiger.connection.pipeline()
                for state in [QUEUED, ACTIVE, SCHEDULED]:
                    pipeline.zscore(self.tiger._key(state, task.queue), task.id)
                results = pipeline.execute()

                # Task is already queued, scheduled, or running.
                if any(results):
                    self.log.info('periodic task already in queue',
                                  func=task.serialized_func,
                                  result=results)
                    continue

                # We can safely queue the task here since we have a lock (the
                # uniqueness of the task also prevents it from being scheduled
                # multiple times)
                when = task._queue_for_next_period()
                self.log.info('queued periodic task',
                              func=task.serialized_func,
                              when=when)
        finally:
            lock.release()

    def run(self, once=False, force_once=False):
        """
        Main loop of the worker.

        Use once=True to execute any queued tasks and then exit.
        Use force_once=True with once=True to always exit after one processing
        loop even if tasks remain queued.
        """

        self.log.info('ready', queues=sorted(self.only_queues),
                               exclude_queues=sorted(self.exclude_queues),
                               single_worker_queues=sorted(self.single_worker_queues))

        if self.config['STATS_INTERVAL']:
            self.stats_thread = StatsThread(self)
            self.stats_thread.start()

        # Queue any periodic tasks that are not queued yet.
        self._queue_periodic_tasks()

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
                self._wait_for_new_tasks(timeout=self.config['SELECT_TIMEOUT'],
                            batch_timeout=self.config['SELECT_BATCH_TIMEOUT'])

                self._install_signal_handlers()
                self._did_work = False
                self._worker_run()
                self._uninstall_signal_handlers()
                if once and (not self._queue_set or force_once):
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

            # Free up Redis connection
            self._pubsub.reset()
            self.log.info('done')
