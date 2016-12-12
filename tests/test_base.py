import datetime
import json
import os
import pytest
import signal
import tempfile
import time
import unittest
from multiprocessing import Pool, Process

from tasktiger import (StopRetry, Task, TaskNotFound, Worker, exponential,
                       fixed, linear)
from tasktiger._internal import serialize_func_name

from .config import DELAY
from .tasks import (batch_task, decorated_task, exception_task, file_args_task,
                    locked_task, long_task_killed, long_task_ok,
                    non_batch_task, retry_task, retry_task_2, simple_task,
                    sleep_task, task_on_other_queue, unique_task,
                    verify_current_task, verify_current_tasks)
from .utils import Patch, external_worker, get_tiger


class BaseTestCase(unittest.TestCase):
    def setUp(self):
        self.tiger = get_tiger()
        self.conn = self.tiger.connection
        self.conn.flushdb()

    def tearDown(self):
        self.conn.flushdb()

    def _ensure_queues(self, queued=None, active=None, error=None,
                       scheduled=None):

        def _ensure_queue(typ, data):
            data = data or {}
            data_names = set(name for name, n in data.items() if n)
            assert self.conn.smembers('t:%s' % typ) == data_names
            ret = {}
            for name, n in data.items():
                task_ids = self.conn.zrange('t:%s:%s' % (typ, name), 0, -1)
                assert len(task_ids) == n
                ret[name] = [json.loads(self.conn.get('t:task:%s' % task_id))
                             for task_id in task_ids]
                assert list(task['id'] for task in ret[name]) == task_ids
            return ret

        return {
            'queued': _ensure_queue('queued', queued),
            'active': _ensure_queue('active', active),
            'error': _ensure_queue('error', error),
            'scheduled': _ensure_queue('scheduled', scheduled),
        }


class TestCase(BaseTestCase):
    """
    TaskTiger main test cases.

    Run a single test like this:
    python -m unittest tests.TestCase.test_unique_task
    """

    def test_simple_task(self):
        self.tiger.delay(simple_task)
        queues = self._ensure_queues(queued={'default': 1})
        task = queues['queued']['default'][0]
        assert task['func'] == 'tests.tasks.simple_task'

        Worker(self.tiger).run(once=True)
        self._ensure_queues(queued={'default': 0})
        assert not self.conn.exists('t:task:%s' % task['id'])

    def test_task_delay(self):
        decorated_task.delay(1, 2, a=3, b=4)
        queues = self._ensure_queues(queued={'default': 1})
        task = queues['queued']['default'][0]
        assert task['func'] == 'tests.tasks.decorated_task'
        assert task['args'] == [1, 2]
        assert task['kwargs'] == {'a': 3, 'b': 4}

    def test_file_args_task(self):
        # Use a temp file to communicate since we're forking.
        tmpfile = tempfile.NamedTemporaryFile()
        worker = Worker(self.tiger)

        self.tiger.delay(file_args_task, args=(tmpfile.name,))
        queues = self._ensure_queues(queued={'default': 1})
        task = queues['queued']['default'][0]
        assert task['func'] == 'tests.tasks.file_args_task'

        worker.run(once=True)
        self._ensure_queues(queued={'default': 0})

        json_data = tmpfile.read().decode('utf8')
        assert json.loads(json_data) == {
            'args': [],
            'kwargs': {}
        }

        tmpfile.seek(0)

        self.tiger.delay(file_args_task, args=(tmpfile.name, 123, 'args'),
                         kwargs={'more': [1, 2, 3]})
        self._ensure_queues(queued={'default': 1})

        worker.run(once=True)
        self._ensure_queues(queued={'default': 0})
        json_data = tmpfile.read().decode('utf8')
        assert json.loads(json_data) == {
            'args': [123, 'args'],
            'kwargs': {'more': [1, 2, 3]}
        }

    def test_queue(self):
        self.tiger.delay(simple_task, queue='a')
        self._ensure_queues(queued={'a': 1, 'b': 0, 'c': 0})

        self.tiger.delay(simple_task, queue='b')
        self._ensure_queues(queued={'a': 1, 'b': 1, 'c': 0})

        self.tiger.delay(simple_task, queue='c')
        self._ensure_queues(queued={'a': 1, 'b': 1, 'c': 1})

        Worker(self.tiger, queues=['a', 'b']).run(once=True)
        self._ensure_queues(queued={'a': 0, 'b': 0, 'c': 1})

        Worker(self.tiger).run(once=True)
        self._ensure_queues(queued={'a': 0, 'b': 0, 'c': 0})

    def test_nested_queue(self):
        self.tiger.delay(simple_task, queue='x')
        self.tiger.delay(simple_task, queue='a')
        self.tiger.delay(simple_task, queue='a.b')
        self.tiger.delay(simple_task, queue='a.b.c')
        self._ensure_queues(queued={'a': 1, 'a.b': 1, 'a.b.c': 1, 'x': 1})

        Worker(self.tiger, queues=['a', 'b']).run(once=True)
        self._ensure_queues(queued={'a': 0, 'a.b': 0, 'a.b.c': 0, 'x': 1})

    def test_nested_queue_2(self):
        self.tiger.delay(simple_task, queue='x')
        self.tiger.delay(simple_task, queue='a')
        self.tiger.delay(simple_task, queue='a.b')
        self.tiger.delay(simple_task, queue='a.b.c')
        self._ensure_queues(queued={'a': 1, 'a.b': 1, 'a.b.c': 1, 'x': 1})

        Worker(self.tiger, queues=['a.b', 'b']).run(once=True)
        self._ensure_queues(queued={'a': 1, 'a.b': 0, 'a.b.c': 0, 'x': 1})

    def test_task_on_other_queue(self):
        self.tiger.delay(task_on_other_queue)
        self._ensure_queues(queued={'other': 1})

        Worker(self.tiger).run(once=True)
        self._ensure_queues(queued={'other': 0})

    def test_when(self):
        self.tiger.delay(simple_task, when=datetime.timedelta(seconds=DELAY))
        self._ensure_queues(queued={'default': 0}, scheduled={'default': 1})

        Worker(self.tiger).run(once=True)
        self._ensure_queues(queued={'default': 0}, scheduled={'default': 1})

        time.sleep(DELAY)

        # Two runs: The first one picks the task up from the "scheduled" queue,
        # the second one processes it.
        Worker(self.tiger).run(once=True)
        self._ensure_queues(queued={'default': 1}, scheduled={'default': 0})

        Worker(self.tiger).run(once=True)
        self._ensure_queues(queued={'default': 0}, scheduled={'default': 0})

    def test_exception_task(self):
        self.tiger.delay(exception_task)

        Worker(self.tiger).run(once=True)
        queues = self._ensure_queues(queued={'default': 0},
                                     error={'default': 1})

        task = queues['error']['default'][0]
        assert task['func'] == 'tests.tasks.exception_task'

        executions = self.conn.lrange('t:task:%s:executions' % task['id'], 0, -1)
        assert len(executions) == 1
        execution = json.loads(executions[0])
        assert execution['exception_name'] == serialize_func_name(Exception)
        assert not execution['success']
        assert execution['traceback'].startswith(
            'Traceback (most recent call last):'
        )

    def test_long_task_ok(self):
        self.tiger.delay(long_task_ok)
        Worker(self.tiger).run(once=True)
        self._ensure_queues(queued={'default': 0}, error={'default': 0})

    def test_long_task_killed(self):
        self.tiger.delay(long_task_killed)
        Worker(self.tiger).run(once=True)
        queues = self._ensure_queues(
            queued={'default': 0},
            error={'default': 1},
        )

        task = queues['error']['default'][0]
        assert task['func'] == 'tests.tasks.long_task_killed'

        executions = self.conn.lrange('t:task:%s:executions' % task['id'], 0, -1)
        assert len(executions) == 1
        execution = json.loads(executions[0])
        exception_name = execution['exception_name']
        assert exception_name == 'tasktiger.exceptions.JobTimeoutException'
        assert not execution['success']

    def test_unique_task(self):
        self.tiger.delay(unique_task, kwargs={'value': 1})
        self.tiger.delay(unique_task, kwargs={'value': 2})
        self.tiger.delay(unique_task, kwargs={'value': 2})

        queues = self._ensure_queues(queued={'default': 2},
                                     error={'default': 0})

        task_1, task_2 = queues['queued']['default']

        assert task_1['func'] == 'tests.tasks.unique_task'
        assert task_1['kwargs'] == {'value': 1}

        assert task_2['func'] == 'tests.tasks.unique_task'
        assert task_2['kwargs'] == {'value': 2}

        Pool(3).map(external_worker, range(3))

        results = self.conn.lrange('unique_task', 0, -1)
        assert len(results) == 2
        assert set(results) == set(['1', '2'])

    def test_unique_task_2(self):
        self.tiger.delay(unique_task)
        self.tiger.delay(unique_task)
        self.tiger.delay(unique_task)

        self._ensure_queues(queued={'default': 1}, error={'default': 0})

    def test_locked_task(self):
        self.tiger.delay(locked_task, kwargs={'key': '1'})
        self.tiger.delay(locked_task, kwargs={'key': '2'})
        self.tiger.delay(locked_task, kwargs={'key': '2'})

        self._ensure_queues(queued={'default': 3},
                            scheduled={'default': 0},
                            error={'default': 0})

        Pool(3).map(external_worker, range(3))

        # One task with keys 1 and 2 executed, but one is scheduled because
        # it hit a lock.
        self._ensure_queues(queued={'default': 0},
                            scheduled={'default': 1},
                            error={'default': 0})

        time.sleep(DELAY)

        # Two runs: The first one picks the task up from the "scheduled" queue,
        # the second one processes it.
        Worker(self.tiger).run(once=True)
        self._ensure_queues(queued={'default': 1},
                            scheduled={'default': 0},
                            error={'default': 0})

        Worker(self.tiger).run(once=True)
        self._ensure_queues(queued={'default': 0},
                            scheduled={'default': 0},
                            error={'default': 0})

    def test_lock_key(self):
        self.tiger.delay(locked_task, kwargs={'key': '1', 'other': 1},
                         lock_key=('key',))
        self.tiger.delay(locked_task, kwargs={'key': '2', 'other': 2},
                         lock_key=('key',))
        self.tiger.delay(locked_task, kwargs={'key': '2', 'other': 3},
                         lock_key=('key',))

        self._ensure_queues(queued={'default': 3},
                            scheduled={'default': 0},
                            error={'default': 0})

        Pool(3).map(external_worker, range(3))

        # One task with keys 1 and 2 executed, but one is scheduled because
        # it hit a lock.
        self._ensure_queues(queued={'default': 0},
                            scheduled={'default': 1},
                            error={'default': 0})

        time.sleep(DELAY)

        # Two runs: The first one picks the task up from the "scheduled" queue,
        # the second one processes it.
        Worker(self.tiger).run(once=True)
        self._ensure_queues(queued={'default': 1},
                            scheduled={'default': 0},
                            error={'default': 0})

        Worker(self.tiger).run(once=True)
        self._ensure_queues(queued={'default': 0},
                            scheduled={'default': 0},
                            error={'default': 0})

    def test_retry(self):
        # Use the default retry method we configured.
        task = self.tiger.delay(exception_task, retry=True)
        self._ensure_queues(queued={'default': 1},
                            scheduled={'default': 0},
                            error={'default': 0})

        # First run
        Worker(self.tiger).run(once=True)
        assert task.n_executions() == 1
        self._ensure_queues(queued={'default': 0},
                            scheduled={'default': 1},
                            error={'default': 0})

        # The task is scheduled, so nothing happens here.
        Worker(self.tiger).run(once=True)
        assert task.n_executions() == 1

        self._ensure_queues(queued={'default': 0},
                            scheduled={'default': 1},
                            error={'default': 0})

        time.sleep(DELAY)

        # Second run (run twice to move from scheduled to queued)
        Worker(self.tiger).run(once=True)
        Worker(self.tiger).run(once=True)
        assert task.n_executions() == 2
        self._ensure_queues(queued={'default': 0},
                            scheduled={'default': 1},
                            error={'default': 0})

        time.sleep(DELAY)

        # Third run will fail permanently.
        Worker(self.tiger).run(once=True)
        Worker(self.tiger).run(once=True)
        assert task.n_executions() == 3
        self._ensure_queues(queued={'default': 0},
                            scheduled={'default': 0},
                            error={'default': 1})

    def test_retry_on_1(self):
        # Fails immediately
        self.tiger.delay(exception_task, retry_on=[ValueError, IndexError])
        Worker(self.tiger).run(once=True)
        self._ensure_queues(queued={'default': 0},
                            scheduled={'default': 0},
                            error={'default': 1})

    def test_retry_on_2(self):
        # Will be retried
        self.tiger.delay(exception_task, retry_on=[ValueError, Exception])
        Worker(self.tiger).run(once=True)
        self._ensure_queues(queued={'default': 0},
                            scheduled={'default': 1},
                            error={'default': 0})

    def test_retry_on_3(self):
        # Make sure we catch superclasses.
        self.tiger.delay(exception_task, retry_on=[Exception])
        Worker(self.tiger).run(once=True)
        self._ensure_queues(queued={'default': 0},
                            scheduled={'default': 1},
                            error={'default': 0})

    def test_retry_method(self):
        task = self.tiger.delay(exception_task,
                                retry_method=linear(DELAY, DELAY, 3))

        def _run(n_executions):
            Worker(self.tiger).run(once=True)
            Worker(self.tiger).run(once=True)
            assert task.n_executions() == n_executions

        _run(1)

        # Retry in 1*DELAY
        time.sleep(DELAY)
        _run(2)

        # Retry in 2*DELAY
        time.sleep(DELAY)
        _run(2)
        time.sleep(DELAY)
        _run(3)

        # Retry in 3*DELAY
        time.sleep(DELAY)
        _run(3)
        time.sleep(DELAY)
        _run(3)
        time.sleep(DELAY)
        _run(4)

        self._ensure_queues(error={'default': 1})

    def test_retry_method_fixed(self):
        f = fixed(2, 3)
        assert f[0](1, *f[1]) == 2
        assert f[0](2, *f[1]) == 2
        assert f[0](3, *f[1]) == 2
        pytest.raises(StopRetry, f[0], 4, *f[1])

    def test_retry_method_linear(self):
        f = linear(1, 2, 3)
        assert f[0](1, *f[1]), 1
        assert f[0](2, *f[1]), 3
        assert f[0](3, *f[1]), 5
        pytest.raises(StopRetry, f[0], 4, *f[1])

    def test_retry_method_exponential(self):
        f = exponential(1, 2, 4)
        assert f[0](1, *f[1]), 1
        assert f[0](2, *f[1]), 2
        assert f[0](3, *f[1]), 4
        assert f[0](4, *f[1]), 8
        pytest.raises(StopRetry, f[0], 5, *f[1])

    def test_retry_exception_1(self):
        self.tiger.delay(retry_task)
        self._ensure_queues(queued={'default': 1})

        Worker(self.tiger).run(once=True)
        self._ensure_queues(scheduled={'default': 1})

        time.sleep(DELAY)

        Worker(self.tiger).run(once=True)
        Worker(self.tiger).run(once=True)
        self._ensure_queues(scheduled={'default': 1})

        time.sleep(DELAY)

        Worker(self.tiger).run(once=True)
        Worker(self.tiger).run(once=True)
        self._ensure_queues(error={'default': 1})

    def test_retry_exception_2(self):
        task = self.tiger.delay(retry_task_2)
        self._ensure_queues(queued={'default': 1})
        assert task.n_executions() == 0

        Worker(self.tiger).run(once=True)
        self._ensure_queues(scheduled={'default': 1})
        assert task.n_executions() == 1

        time.sleep(DELAY)

        Worker(self.tiger).run(once=True)
        Worker(self.tiger).run(once=True)
        self._ensure_queues()

        pytest.raises(TaskNotFound, task.n_executions)

    def test_batch_1(self):
        self.tiger.delay(batch_task, args=[1])
        self.tiger.delay(batch_task, args=[2])
        self.tiger.delay(batch_task, args=[3])
        self.tiger.delay(batch_task, args=[4])
        self._ensure_queues(queued={'batch': 4})
        Worker(self.tiger).run(once=True)
        self._ensure_queues(queued={'batch': 0})
        data = [json.loads(d) for d in self.conn.lrange('batch_task', 0, -1)]
        assert data == [
            [
                {'args': [1], 'kwargs': {}},
                {'args': [2], 'kwargs': {}},
                {'args': [3], 'kwargs': {}},
            ], [
                {'args': [4], 'kwargs': {}},
            ],
        ]

    def test_batch_2(self):
        self.tiger.delay(batch_task, args=[1])
        self.tiger.delay(non_batch_task, args=[5])
        self.tiger.delay(batch_task, args=[2])
        self.tiger.delay(batch_task, args=[3])
        self.tiger.delay(batch_task, args=[4])
        self.tiger.delay(non_batch_task, args=[6])
        self.tiger.delay(non_batch_task, args=[7])
        self._ensure_queues(queued={'batch': 7})
        Worker(self.tiger).run(once=True)
        self._ensure_queues(queued={'batch': 0})
        data = [json.loads(d) for d in self.conn.lrange('batch_task', 0, -1)]
        assert data == [
            [
                {'args': [1], 'kwargs': {}},
                {'args': [2], 'kwargs': {}},
            ], 5, [
                {'args': [3], 'kwargs': {}},
                {'args': [4], 'kwargs': {}},
            ], 6, 7,
        ]

    def test_batch_3(self):
        self.tiger.delay(batch_task, queue='default', args=[1])
        self.tiger.delay(batch_task, queue='default', args=[2])
        self.tiger.delay(batch_task, queue='default', args=[3])
        self.tiger.delay(batch_task, queue='default', args=[4])
        self._ensure_queues(queued={'default': 4})
        Worker(self.tiger).run(once=True)
        self._ensure_queues(queued={'default': 0})
        data = [json.loads(d) for d in self.conn.lrange('batch_task', 0, -1)]
        assert data == [
            [
                {'args': [1], 'kwargs': {}},
            ], [
                {'args': [2], 'kwargs': {}},
            ], [
                {'args': [3], 'kwargs': {}},
            ], [
                {'args': [4], 'kwargs': {}},
            ],
        ]

    def test_batch_4(self):
        self.tiger.delay(batch_task, queue='batch.sub', args=[1])
        self.tiger.delay(batch_task, queue='batch.sub', args=[2])
        self.tiger.delay(batch_task, queue='batch.sub', args=[3])
        self.tiger.delay(batch_task, queue='batch.sub', args=[4])
        self._ensure_queues(queued={'batch.sub': 4})
        Worker(self.tiger).run(once=True)
        self._ensure_queues(queued={'batch.sub': 0})
        data = [json.loads(d) for d in self.conn.lrange('batch_task', 0, -1)]
        assert data == [
            [
                {'args': [1], 'kwargs': {}},
                {'args': [2], 'kwargs': {}},
                {'args': [3], 'kwargs': {}},
            ], [
                {'args': [4], 'kwargs': {}},
            ],
        ]

    def test_batch_exception_1(self):
        self.tiger.delay(batch_task, args=[1])
        self.tiger.delay(batch_task, args=[10])
        self.tiger.delay(batch_task, args=[2])
        self.tiger.delay(batch_task, args=[3])
        self._ensure_queues(queued={'batch': 4})
        Worker(self.tiger).run(once=True)
        self._ensure_queues(queued={'batch': 0}, error={'batch': 3})

    def test_batch_exception_2(self):
        # If we queue non-batch tasks into a batch queue, we currently fail
        # the entire batch for a specific task.
        self.tiger.delay(non_batch_task, args=[1])
        self.tiger.delay(non_batch_task, args=[10])
        self.tiger.delay(non_batch_task, args=[2])
        self.tiger.delay(non_batch_task, args=[3])
        self._ensure_queues(queued={'batch': 4})
        Worker(self.tiger).run(once=True)
        self._ensure_queues(queued={'batch': 0}, error={'batch': 3})

    def test_batch_exception_3(self):
        self.tiger.delay(batch_task, args=[1])
        self.tiger.delay(non_batch_task, args=[2])
        self.tiger.delay(batch_task, args=[10])
        self._ensure_queues(queued={'batch': 3})
        Worker(self.tiger).run(once=True)
        self._ensure_queues(queued={'batch': 0}, error={'batch': 2})

    def test_batch_lock_key(self):
        self.tiger.delay(batch_task, kwargs={'key': '1', 'other': 1},
                         lock_key=('key,'))
        self.tiger.delay(batch_task, kwargs={'key': '2', 'other': 2},
                         lock_key=('key,'))
        self.tiger.delay(batch_task, kwargs={'key': '2', 'other': 3},
                         lock_key=('key,'))

        self._ensure_queues(queued={'batch': 3})
        Worker(self.tiger).run(once=True)
        self._ensure_queues(queued={'batch': 0})

    def test_only_queues(self):
        self.tiger.delay(simple_task, queue='a')
        self.tiger.delay(simple_task, queue='a.a')
        self.tiger.delay(simple_task, queue='b')
        self.tiger.delay(simple_task, queue='b.a')

        self._ensure_queues(queued={'a': 1, 'a.a': 1, 'b': 1, 'b.a': 1})

        self.tiger.config['ONLY_QUEUES'] = ['a']

        Worker(self.tiger).run(once=True)

        self._ensure_queues(queued={'b': 1, 'b.a': 1})


class TaskTestCase(BaseTestCase):
    """
    Task class test cases.
    """
    def test_delay(self):
        task = Task(self.tiger, simple_task)
        self._ensure_queues()
        task.delay()
        self._ensure_queues(queued={'default': 1})

        # Canceling only works for scheduled tasks.
        pytest.raises(TaskNotFound, task.cancel)

    def test_delay_scheduled(self):
        task = Task(self.tiger, simple_task, queue='a')
        task.delay(when=datetime.timedelta(minutes=5))
        self._ensure_queues(scheduled={'a': 1})

        # Test canceling a scheduled task.
        task.cancel()
        self._ensure_queues()

        # Canceling again raises an error
        pytest.raises(TaskNotFound, task.cancel)

    def test_delay_scheduled_2(self):
        task = Task(self.tiger, simple_task, queue='a')
        task.delay(when=datetime.timedelta(minutes=5))
        self._ensure_queues(scheduled={'a': 1})

        task_id = task.id

        # We can't look up a non-unique task by recreating it.
        task = Task(self.tiger, simple_task, queue='a')
        pytest.raises(TaskNotFound, task.cancel)

        # We can look up a task by its ID.
        fetch_task = lambda: Task.from_id(self.tiger, 'a', 'scheduled', task_id)

        task = fetch_task()
        task.cancel()
        self._ensure_queues()

        # Task.from_id raises if it doesn't exist.
        pytest.raises(TaskNotFound, fetch_task)

    def test_delay_scheduled_3(self):
        task = Task(self.tiger, simple_task, unique=True)
        task.delay(when=datetime.timedelta(minutes=5))
        self._ensure_queues(scheduled={'default': 1})

        # We can look up a unique task by recreating it.
        task = Task(self.tiger, simple_task, unique=True)
        task.cancel()
        self._ensure_queues()

    def test_update_scheduled_time(self):
        task = Task(self.tiger, simple_task, unique=True)
        task.delay(when=datetime.timedelta(minutes=5))
        self._ensure_queues(scheduled={'default': 1})
        old_score = self.conn.zscore('t:scheduled:default', task.id)

        task.update_scheduled_time(when=datetime.timedelta(minutes=6))
        self._ensure_queues(scheduled={'default': 1})
        new_score = self.conn.zscore('t:scheduled:default', task.id)

        # The difference can be slightly over 60 due to processing time, but
        # shouldn't be much higher.
        self.assertTrue(60 <= new_score - old_score < 61)

    def test_execute(self):
        task = Task(self.tiger, exception_task)
        pytest.raises(Exception, task.execute)

    def test_tasks_from_queue(self):
        task0 = Task(self.tiger, simple_task)
        task1 = Task(self.tiger, exception_task)
        task2 = Task(self.tiger, simple_task, queue='other')

        task0.delay()
        task1.delay()
        task2.delay()

        n, tasks = Task.tasks_from_queue(self.tiger, 'default', 'queued')
        assert n == 2
        assert task0.id == tasks[0].id
        assert task0.func == simple_task
        assert task0.func == tasks[0].func
        assert task0.serialized_func == 'tests.tasks.simple_task'
        assert task0.serialized_func == tasks[0].serialized_func
        assert task0.state == tasks[0].state
        assert task0.state == 'queued'
        assert task0.queue == tasks[0].queue
        assert task0.queue == 'default'

    def test_eager(self):
        self.tiger.config['ALWAYS_EAGER'] = True

        # Ensure task is immediately executed.
        task = Task(self.tiger, simple_task)
        task.delay()
        self._ensure_queues()

        # Ensure task is immediately executed.
        task = Task(self.tiger, exception_task)
        pytest.raises(Exception, task.delay)
        self._ensure_queues()

        # Even when we specify "when" in the past.
        task = Task(self.tiger, simple_task)
        task.delay(when=datetime.timedelta(seconds=-5))
        self._ensure_queues()

        # Ensure there is an exception if we can't serialize the task.
        task = Task(self.tiger, decorated_task,
                    args=[object()])
        pytest.raises(TypeError, task.delay)
        self._ensure_queues()

        # Ensure task is not executed if it's scheduled in the future.
        task = Task(self.tiger, simple_task)
        task.delay(when=datetime.timedelta(seconds=5))
        self._ensure_queues(scheduled={'default': 1})


class CurrentTaskTestCase(BaseTestCase):
    def test_current_task(self):
        task = Task(self.tiger, verify_current_task)
        task.delay()
        Worker(self.tiger).run(once=True)
        assert not self.conn.exists('runtime_error')
        assert self.conn.get('task_id') == task.id

    def test_current_tasks(self):
        task1 = Task(self.tiger, verify_current_tasks)
        task1.delay()
        task2 = Task(self.tiger, verify_current_tasks)
        task2.delay()
        Worker(self.tiger).run(once=True)
        assert self.conn.lrange('task_ids', 0, -1) == [task1.id, task2.id]

    def test_current_task_eager(self):
        self.tiger.config['ALWAYS_EAGER'] = True

        task = Task(self.tiger, verify_current_task)
        task.delay()
        assert not self.conn.exists('runtime_error')
        assert self.conn.get('task_id') == task.id

    def test_current_tasks_eager(self):
        self.tiger.config['ALWAYS_EAGER'] = True

        task = Task(self.tiger, verify_current_tasks)
        task.delay()
        assert not self.conn.exists('runtime_error')
        assert self.conn.lrange('task_ids', 0, -1) == [task.id]


class ReliabilityTestCase(BaseTestCase):
    """ Test behavior if things go wrong. """

    def test_killed_child_process(self):
        """
        Ensure that TaskTiger completes gracefully if the child process
        disappears and there is no execution object.
        """
        import psutil

        sleep_task.delay()
        self._ensure_queues(queued={'default': 1})

        # Start a worker and wait until it starts processing.
        worker = Process(target=external_worker)
        worker.start()
        time.sleep(DELAY)

        # Get the PID of the worker subprocess actually executing the task
        current_process = psutil.Process(pid=worker.pid)
        current_children = current_process.children()
        assert len(current_children) == 1

        # Kill the worker subprocess that is executing the task.
        current_children[0].kill()

        # Make sure the worker still terminates gracefully.
        worker.join()
        assert worker.exitcode == 0

        # Make sure the task is in the error queue.
        self._ensure_queues(error={'default': 1})

    def test_task_disappears(self):
        """
        Ensure that a task object that disappears while the task is processing
        is handled properly. This could happen when a worker processes a task,
        then hangs for a long time, causing another worker to pick up and finish
        the task. Then, when the original worker resumes, the task object will
        be gone. Make sure we log a "not found" error and move on.
        """

        task = Task(self.tiger, sleep_task, kwargs={'delay': 2 * DELAY})
        task.delay()
        self._ensure_queues(queued={'default': 1})

        # Start a worker and wait until it starts processing.
        worker = Process(target=external_worker)
        worker.start()
        time.sleep(DELAY)

        # Remove the task object while the task is processing.
        assert self.conn.delete('t:task:{}'.format(task.id)) == 1

        # Kill the worker while it's still processing the task.
        os.kill(worker.pid, signal.SIGKILL)

        # _ensure_queues() breaks here because it can't find the task
        assert self.conn.scard('t:queued') == 0
        assert self.conn.scard('t:active') == 1
        assert self.conn.scard('t:error') == 0
        assert self.conn.scard('t:scheduled') == 0

        # Capture logger
        errors = []

        def fake_error(msg):
            errors.append(msg)

        with Patch(self.tiger.log._logger, 'error', fake_error):
            # Since ACTIVE_TASK_UPDATE_TIMEOUT hasn't elapsed yet, re-running
            # the worker at this time won't change anything. (run twice to move
            # from scheduled to queued)
            Worker(self.tiger).run(once=True)
            Worker(self.tiger).run(once=True)

            assert len(errors) == 0
            assert self.conn.scard('t:queued') == 0
            assert self.conn.scard('t:active') == 1
            assert self.conn.scard('t:error') == 0
            assert self.conn.scard('t:scheduled') == 0

            # After waiting and re-running the worker, queues will clear.
            time.sleep(DELAY)
            Worker(self.tiger).run(once=True)
            Worker(self.tiger).run(once=True)

            self._ensure_queues()
            assert len(errors) == 1
            assert "event='not found'" in errors[0]
