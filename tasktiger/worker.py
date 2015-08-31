import errno
import json
import os
import random
import select
import signal
import time
import traceback

from redis_lock import Lock

from ._internal import *
from .retry import *
from .timeouts import UnixSignalDeathPenalty, JobTimeoutException

__all__ = ['Worker']

class Worker(object):
    def __init__(self, tiger, queues=None):
        self.log = tiger.log.bind(pid=os.getpid())
        self.connection = tiger.connection
        self.scripts = tiger.scripts
        self.config = tiger.config
        self._key = tiger._key

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
                self._key(SCHEDULED))))
        now = time.time()
        for queue in queues:
            # Move due items from scheduled queue to active queue. If items
            # were moved, remove the queue from the scheduled set if it is
            # empty, and add it to the active set so the task gets picked up.

            result = self.scripts.zpoppush(
                self._key(SCHEDULED, queue),
                self._key(QUEUED, queue),
                self.config['SCHEDULED_TASK_BATCH_SIZE'],
                now,
                now,
                on_success=('update_sets', self._key(SCHEDULED), self._key(QUEUED), queue),
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
                message = gen.next()
                if message['type'] == 'message':
                    for queue in self._filter_queues([message['data']]):
                        self._queue_set.add(queue)
            else:
                break

    def _worker_queue_expired_tasks(self):
        active_queues = self.connection.smembers(self._key(ACTIVE))
        now = time.time()
        for queue in active_queues:
            result = self.scripts.zpoppush(
                self._key(ACTIVE, queue),
                self._key(QUEUED, queue),
                self.config['ACTIVE_TASK_EXPIRED_BATCH_SIZE'],
                now - self.config['ACTIVE_TASK_UPDATE_TIMEOUT'],
                now,
                on_success=('update_sets', self._key(ACTIVE), self._key(QUEUED), queue),
            )
            # XXX: Ideally this would be atomic with the operation above.
            if result:
                self.log.info('queueing expired tasks', task_ids=result)
                self.connection.publish(self._key('activity'), queue)

    def _execute_forked(self, task, log):
        success = False

        execution = {}

        try:
            func = import_attribute(task['func'])
        except (ValueError, ImportError, AttributeError):
            log.error('could not import', func=task['func'])
        else:
            args = task.get('args', [])
            kwargs = task.get('kwargs', {})
            execution['time_started'] = time.time()
            try:
                hard_timeout = task.get('hard_timeout', None) or \
                               getattr(func, '_task_hard_timeout', None) or \
                               self.config['DEFAULT_HARD_TIMEOUT']
                with UnixSignalDeathPenalty(hard_timeout):
                    func(*args, **kwargs)
            except Exception, exc:
                execution['traceback'] = traceback.format_exc()
                execution['exception_name'] = serialize_func_name(exc.__class__)
                execution['time_failed'] = time.time()
            else:
                success = True

        if not success:
            # Currently we only log failed task executions to Redis.
            execution['success'] = success
            serialized_execution = json.dumps(execution)
            self.connection.rpush(self._key('task', task['id'], 'executions'),
                                  serialized_execution)

        return success

    def _heartbeat(self, queue, task_id):
        now = time.time()
        self.connection.zadd(self._key(ACTIVE, queue), task_id, now)

    def _execute(self, queue, task, log, lock):
        """
        Executes the task with the given ID. Returns a boolean indicating whether
        the task was executed succesfully.
        """
        # Adapted from rq Worker.execute_job / Worker.main_work_horse
        child_pid = os.fork()
        if child_pid == 0:
            # Child process
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
            # Main process
            log = log.bind(child_pid=child_pid)
            log.debug('processing')
            while True:
                try:
                    with UnixSignalDeathPenalty(self.config['ACTIVE_TASK_UPDATE_TIMER']):
                        _, return_code = os.waitpid(child_pid, 0)
                        break
                except OSError as e:
                    if e.errno != errno.EINTR:
                        raise
                except JobTimeoutException:
                    self._heartbeat(queue, task['id'])
                    if lock:
                        lock.renew(self.config['ACTIVE_TASK_UPDATE_TIMEOUT'])

            status = not return_code
            return status

    def _process_from_queue(self, queue):
        now = time.time()

        log = self.log.bind(queue=queue)

        # Move an item to the active queue, if available.
        task_ids = self.scripts.zpoppush(
            self._key(QUEUED, queue),
            self._key(ACTIVE, queue),
            1,
            None,
            now,
            on_success=('update_sets', self._key(QUEUED), self._key(ACTIVE), queue),
        )

        assert len(task_ids) < 2

        if task_ids:
            task_id = task_ids[0]

            log = log.bind(task_id=task_id)

            serialized_task = self.connection.get(self._key('task', task_id))
            if not serialized_task:
                log.error('not found')
                # Return the task ID since there may be more tasks.
                return task_id

            task = json.loads(serialized_task)

            if task.get('lock', False):
                lock = Lock(self.connection, self._key('lock', gen_unique_id(
                    task['func'],
                    task.get('args', []),
                    task.get('kwargs', []),
                )), timeout=self.config['ACTIVE_TASK_UPDATE_TIMEOUT'], blocking=False)
            else:
                lock = None

            if lock and not lock.acquire():
                log.info('could not acquire lock')

                # Reschedule the task
                now = time.time()
                when = now + self.config['LOCK_RETRY']
                pipeline = self.connection.pipeline()
                pipeline.zrem(self._key(ACTIVE, queue), task_id)
                self.scripts.srem_if_not_exists(self._key(ACTIVE), queue,
                        self._key(ACTIVE, queue), client=pipeline)
                pipeline.sadd(self._key(SCHEDULED), queue)
                pipeline.zadd(self._key(SCHEDULED, queue), task_id, when)
                pipeline.execute()

                return task_id

            success = self._execute(queue, task, log, lock)

            if lock:
                lock.release()

            if success:
                # Remove the task from active queue
                pipeline = self.connection.pipeline()
                pipeline.zrem(self._key(ACTIVE, queue), task_id)
                if task.get('unique', False):
                    # Only delete if it's not in the error or queued queue.
                    self.scripts.delete_if_not_in_zsets(self._key('task', task_id), task_id, [
                        self._key(QUEUED, queue),
                        self._key(ERROR, queue)
                    ], client=pipeline)
                else:
                    pipeline.delete(self._key('task', task_id))
                self.scripts.srem_if_not_exists(self._key(ACTIVE), queue,
                        self._key(ACTIVE, queue), client=pipeline)
                pipeline.execute()
                log.debug('done')
            else:
                should_retry = False
                # Get execution info (for logging and retry purposes)
                execution = self.connection.lindex(
                    self._key('task', task['id'], 'executions'), -1)
                if execution:
                    execution = json.loads(execution)
                if 'retry_method' in task:
                    if 'retry_on' in task:
                        if execution:
                            exception_name = execution.get('exception_name')
                            exception_class = import_attribute(exception_name)
                            for n in task['retry_on']:
                                if issubclass(exception_class, import_attribute(n)):
                                    should_retry = True
                                    break
                    else:
                        should_retry = True

                queue_type = ERROR

                when = time.time()

                log_context = {}

                if should_retry:
                    retry_func, retry_args = task['retry_method']
                    retry_num = self.connection.llen(self._key('task', task['id'], 'executions'))
                    log_context['retry_func'] = retry_func
                    log_context['retry_num'] = retry_num

                    try:
                        func = import_attribute(retry_func)
                    except (ValueError, ImportError, AttributeError):
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
                            queue_type = SCHEDULED

                if queue_type == ERROR:
                    log_func = log.error
                else:
                    log_func = log.warning

                log_func(func=task['func'],
                         time_failed=execution['time_failed'],
                         traceback=execution['traceback'],
                         exception_name=execution['exception_name'],
                         **log_context)

                # Move task to the scheduled queue for retry, or move to error
                # queue if we don't want to retry.
                pipeline = self.connection.pipeline()
                pipeline.zadd(self._key(queue_type, queue), task_id, when)
                pipeline.sadd(self._key(queue_type), queue)
                pipeline.zrem(self._key(ACTIVE, queue), task_id)
                self.scripts.srem_if_not_exists(self._key(ACTIVE), queue,
                        self._key(ACTIVE, queue), client=pipeline)
                pipeline.execute()

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
        self._pubsub.subscribe(self._key('activity'))

        self._queue_set = set(self._filter_queues(
                self.connection.smembers(self._key(QUEUED))))

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
