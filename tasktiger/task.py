import datetime
import json
import redis
import time

from ._internal import *
from .exceptions import TaskNotFound

__all__ = ['Task']

class Task(object):
    def __init__(self, tiger, func=None, args=None, kwargs=None, queue=None,
                 hard_timeout=None, unique=None, lock=None, lock_key=None,
                 retry=None, retry_on=None, retry_method=None,

                 # internal variables
                 _data=None, _state=None, _ts=None, _executions=None):
        """
        Queues a task. See README.rst for an explanation of the options.
        """

        if func and queue is None:
            queue = getattr(func, '_task_queue', tiger.config['DEFAULT_QUEUE'])

        self.tiger = tiger
        self._func = func
        self._queue = queue
        self._state = _state
        self._ts = _ts
        self._executions = _executions or []

        # Internal initialization based on raw data.
        if _data is not None:
            self._data = _data
            return

        assert func

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

        task = {
            'id': task_id,
            'func': serialized_name,
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
                retry_method = tiger.config['DEFAULT_RETRY_METHOD']

            retry_method = serialize_retry_method(retry_method)

            task['retry_method'] = retry_method
            if retry_on:
                task['retry_on'] = [serialize_func_name(cls)
                                    for cls in retry_on]

        self._data = task

    @property
    def id(self):
        return self._data['id']

    @property
    def data(self):
        return self._data

    @property
    def state(self):
        return self._state

    @property
    def queue(self):
        return self._queue

    @property
    def serialized_func(self):
        return self._data['func']

    @property
    def lock(self):
        return self._data.get('lock', False)

    @property
    def lock_key(self):
        return self._data.get('lock_key')

    @property
    def args(self):
        return self._data.get('args', [])

    @property
    def kwargs(self):
        return self._data.get('kwargs', {})

    @property
    def hard_timeout(self):
        return self._data.get('hard_timeout', None)

    @property
    def unique(self):
        return self._data.get('unique', False)

    @property
    def retry_method(self):
        if 'retry_method' in self._data:
            retry_func, retry_args = self._data['retry_method']
            return retry_func, retry_args
        else:
            return None

    @property
    def retry_on(self):
        return self._data.get('retry_on')

    def should_retry_on(self, exception_class):
        """
        Whether this task should be retried when the given exception occurs.
        """
        for n in (self.retry_on or []):
            if issubclass(exception_class, import_attribute(n)):
                return True
        return False

    @property
    def func(self):
        if not self._func:
            self._func = import_attribute(self.serialized_func)
        return self._func

    @property
    def ts(self):
        """
        The timestamp (datetime) of the task in the queue, or None, if the task
        hasn't been queued.
        """
        return self._ts

    @property
    def executions(self):
        return self._executions

    def _move(self, from_state=None, to_state=None, when=None, mode=None):
        """
        Internal helper to move a task from one state to another (e.g. from
        QUEUED to DELAYED). The "when" argument indicates the timestamp of the
        task in the new state. If no to_state is specified, the task will be
        simply removed from the original state.

        The "mode" param can be specified to define how the timestamp in the
        new state should be updated and is passed to the ZADD Redis script (see
        its documentation for details).

        Raises TaskNotFound if the task is not in the expected state or not in
        the expected queue.
        """

        pipeline = self.tiger.connection.pipeline()
        scripts = self.tiger.scripts
        _key = self.tiger._key

        from_state = from_state or self.state
        queue = self.queue

        assert from_state
        assert queue

        scripts.fail_if_not_in_zset(_key(from_state, queue), self.id,
                                    client=pipeline)
        if to_state:
            if not when:
                when = time.time()
            if mode:
                scripts.zadd(_key(to_state, queue), when, self.id,
                                  mode, client=pipeline)
            else:
                pipeline.zadd(_key(to_state, queue), self.id, when)
            pipeline.sadd(_key(to_state), queue)
        pipeline.zrem(_key(from_state, queue), self.id)

        if not to_state: # Remove the task if necessary
            if self.unique:
                # Only delete if it's not in any other queue
                check_states = set([ACTIVE, QUEUED, ERROR, SCHEDULED])
                check_states.remove(from_state)
                # TODO: Do the following two in one call.
                scripts.delete_if_not_in_zsets(_key('task', self.id, 'executions'),
                                                    self.id, [
                    _key(state, queue) for state in check_states
                ], client=pipeline)
                scripts.delete_if_not_in_zsets(_key('task', self.id),
                                                    self.id, [
                    _key(state, queue) for state in check_states
                ], client=pipeline)
            else:
                # Safe to remove
                pipeline.delete(_key('task', self.id, 'executions'))
                pipeline.delete(_key('task', self.id))

        scripts.srem_if_not_exists(_key(from_state), queue,
                _key(from_state, queue), client=pipeline)

        if to_state == QUEUED:
            pipeline.publish(_key('activity'), queue)

        try:
            scripts.execute_pipeline(pipeline)
        except redis.ResponseError as e:
            if '<FAIL_IF_NOT_IN_ZSET>' in e.args[0]:
                raise TaskNotFound('Task {} not found in queue "{}" in state "{}".'.format(
                    self.id, queue, from_state
                ))
            raise
        else:
            self._state = to_state

    def execute(self):
        func = self.func
        is_batch_func = getattr(func, '_task_batch', False)

        g['current_task_is_batch'] = is_batch_func
        g['current_tasks'] = [self]

        try:
            if is_batch_func:
                return func([{'args': self.args, 'kwargs': self.kwargs}])
            else:
                return func(*self.args, **self.kwargs)
        finally:
            g['current_task_is_batch'] = None
            g['current_tasks'] = None

    def delay(self, when=None):
        tiger = self.tiger

        ts = get_timestamp(when)

        now = time.time()
        self._data['time_last_queued'] = now

        if not ts or ts <= now:
            # Immediately queue if the timestamp is in the past.
            ts = now
            state = QUEUED
        else:
            state = SCHEDULED

        # When using ALWAYS_EAGER, make sure we have serialized the task to
        # ensure there are no serialization errors.
        serialized_task = json.dumps(self._data)

        if tiger.config['ALWAYS_EAGER'] and state == QUEUED:
            return self.execute()

        pipeline = tiger.connection.pipeline()
        pipeline.sadd(tiger._key(state), self.queue)
        pipeline.set(tiger._key('task', self.id), serialized_task)
        # In case of unique tasks, don't update the score.
        tiger.scripts.zadd(tiger._key(state, self.queue), ts, self.id,
                          mode='nx', client=pipeline)
        if state == QUEUED:
            pipeline.publish(tiger._key('activity'), self.queue)
        pipeline.execute()

        self._state = state
        self._ts = ts

    def update_scheduled_time(self, when):
        """
        Updates a scheduled task's date to the given date. If the task is not
        scheduled, a TaskNotFound exception is raised.
        """
        tiger = self.tiger

        ts = get_timestamp(when)
        assert ts

        pipeline = tiger.connection.pipeline()
        key = tiger._key(SCHEDULED, self.queue)
        tiger.scripts.zadd(key, ts, self.id, mode='xx', client=pipeline)
        pipeline.zscore(key, self.id)
        _, score = pipeline.execute()
        if not score:
            raise TaskNotFound('Task {} not found in queue "{}" in state "{}".'.format(
                self.id, self.queue, SCHEDULED
            ))

        self._ts = ts

    def __repr__(self):
        return u'<Task %s>' % self.func

    @classmethod
    def from_id(self, tiger, queue, state, task_id, load_executions=0):
        """
        Loads a task with the given ID from the given queue in the given
        state. An integer may be passed in the load_executions parameter
        to indicate how many executions should be loaded (starting from the
        latest). If the task doesn't exist, None is returned.
        """
        if load_executions:
            pipeline = tiger.connection.pipeline()
            pipeline.get(tiger._key('task', task_id))
            pipeline.lrange(tiger._key('task', task_id, 'executions'), -load_executions, -1)
            serialized_data, serialized_executions = pipeline.execute()
        else:
            serialized_data = tiger.connection.get(tiger._key('task', task_id))
            serialized_executions = []
        # XXX: No timestamp for now
        if serialized_data:
            data = json.loads(serialized_data)
            executions = [json.loads(e) for e in serialized_executions if e]
            return Task(tiger, queue=queue, _data=data, _state=state,
                        _executions=executions)
        else:
            raise TaskNotFound('Task {} not found.'.format(
                task_id
            ))

    @classmethod
    def tasks_from_queue(self, tiger, queue, state, skip=0, limit=1000,
                   load_executions=0):
        """
        Returns a tuple with the following information:
        * total items in the queue
        * tasks from the given queue in the given state, latest first.

        An integer may be passed in the load_executions parameter to indicate
        how many executions should be loaded (starting from the latest).
        """

        key = tiger._key(state, queue)
        pipeline = tiger.connection.pipeline()
        pipeline.zcard(key)
        pipeline.zrange(key, -limit-skip, -1-skip, withscores=True)
        n, items = pipeline.execute()

        tasks = []

        if items:
            tss = [datetime.datetime.utcfromtimestamp(item[1]) for item in items]
            if load_executions:
                pipeline = tiger.connection.pipeline()
                pipeline.mget([tiger._key('task', item[0]) for item in items])
                for item in items:
                    pipeline.lrange(tiger._key('task', item[0], 'executions'), -load_executions, -1)
                results = pipeline.execute()

                for serialized_data, serialized_executions, ts in zip(results[0], results[1:], tss):
                    data = json.loads(serialized_data)
                    executions = [json.loads(e) for e in serialized_executions if e]

                    task = Task(tiger, queue=queue, _data=data, _state=state,
                                _ts=ts, _executions=executions)

                    tasks.append(task)
            else:
                data = tiger.connection.mget([tiger._key('task', item[0]) for item in items])
                for serialized_data, ts in zip(data, tss):
                    data = json.loads(serialized_data)
                    task = Task(tiger, queue=queue, _data=data, _state=state,
                                _ts=ts)
                    tasks.append(task)

        return n, tasks

    def n_executions(self):
        """
        Queries and returns the number of past task executions.
        """
        pipeline = self.tiger.connection.pipeline()
        pipeline.exists(self.tiger._key('task', self.id))
        pipeline.llen(self.tiger._key('task', self.id, 'executions'))
        exists, n_executions = pipeline.execute()
        if not exists:
            raise TaskNotFound('Task {} not found.'.format(
                self.id
            ))
        return n_executions

    def retry(self):
        """
        Retries a task that's in the error queue.

        Raises TaskNotFound if the task could not be found in the ERROR
        queue.
        """
        self._move(from_state=ERROR, to_state=QUEUED)

    def cancel(self):
        """
        Cancels a task that is queued in the SCHEDULED queue.

        Raises TaskNotFound if the task could not be found in the SCHEDULED
        queue.
        """
        self._move(from_state=SCHEDULED)

    def delete(self):
        """
        Removes a task that's in the error queue.

        Raises TaskNotFound if the task could not be found in the ERROR
        queue.
        """
        self._move(from_state=ERROR)

    def _queue_for_next_period(self):
        now = datetime.datetime.utcnow()
        schedule = self.func._task_schedule
        if callable(schedule):
            schedule_func = schedule
            schedule_args = ()
        else:
            schedule_func, schedule_args = schedule
        when = schedule_func(now, *schedule_args)
        if when:
            self.delay(when=when)
        return when
