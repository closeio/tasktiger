import datetime
import json
from multiprocessing import Pool
from tasktiger import *
import tempfile
import time
import unittest

from .config import *
from .tasks import *
from .utils import *

class TestCase(unittest.TestCase):
    """
    TaskTiger test cases.

    Run a single test like this:
    python -m unittest tests.TestCase.test_unique_task
    """

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
            self.assertEqual(self.conn.smembers('t:%s' % typ),
                             set(name for name, n in data.items() if n))
            ret = {}
            for name, n in data.items():
                task_ids = self.conn.zrange('t:%s:%s' % (typ, name), 0, -1)
                self.assertEqual(len(task_ids), n)
                ret[name] = [json.loads(self.conn.get('t:task:%s' % task_id))
                             for task_id in task_ids]
                self.assertEqual(list(task['id'] for task in ret[name]),
                                 task_ids)
            return ret


        return {
            'queued': _ensure_queue('queued', queued),
            'active': _ensure_queue('active', active),
            'error': _ensure_queue('error', error),
            'scheduled': _ensure_queue('scheduled', scheduled),
        }

    def test_simple_task(self):
        self.tiger.delay(simple_task)
        queues = self._ensure_queues(queued={'default': 1})
        task = queues['queued']['default'][0]
        self.assertEqual(task['func'], 'tests.tasks.simple_task')

        Worker(self.tiger).run(once=True)
        self._ensure_queues(queued={'default': 0})
        self.assertFalse(self.conn.exists('t:task:%s' % task['id']))

    def test_task_delay(self):
        decorated_task.delay(1, 2, a=3, b=4)
        queues = self._ensure_queues(queued={'default': 1})
        task = queues['queued']['default'][0]
        self.assertEqual(task['func'], 'tests.tasks.decorated_task')
        self.assertEqual(task['args'], [1, 2])
        self.assertEqual(task['kwargs'], {'a': 3, 'b': 4})

    def test_file_args_task(self):
        # Use a temp file to communicate since we're forking.
        tmpfile = tempfile.NamedTemporaryFile()
        worker = Worker(self.tiger)

        self.tiger.delay(file_args_task, args=(tmpfile.name,))
        queues = self._ensure_queues(queued={'default': 1})
        task = queues['queued']['default'][0]
        self.assertEqual(task['func'], 'tests.tasks.file_args_task')

        worker.run(once=True)
        self._ensure_queues(queued={'default': 0})

        self.assertEqual(json.loads(tmpfile.read()), {
            'args': [],
            'kwargs': {}
        })

        tmpfile.seek(0)

        self.tiger.delay(file_args_task, args=(tmpfile.name, 123, 'args'),
                         kwargs={'more': [1, 2, 3]})
        self._ensure_queues(queued={'default': 1})

        worker.run(once=True)
        self._ensure_queues(queued={'default': 0})

        self.assertEqual(json.loads(tmpfile.read()), {
            'args': [123, 'args'],
            'kwargs': {'more': [1, 2, 3]}
        })

    def test_queue(self):
        self.tiger.delay(simple_task, queue='a')
        self._ensure_queues(queued={'a': 1, 'b': 0, 'c': 0})

        self.tiger.delay(simple_task, queue='b')
        self._ensure_queues(queued={'a': 1, 'b': 1, 'c': 0})

        self.tiger.delay(simple_task, queue='c')
        self._ensure_queues(queued={'a': 1, 'b': 1, 'c': 1})

        Worker(self.tiger, queues='a,b').run(once=True)
        self._ensure_queues(queued={'a': 0, 'b': 0, 'c': 1})

        Worker(self.tiger).run(once=True)
        self._ensure_queues(queued={'a': 0, 'b': 0, 'c': 0})

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
        self.assertEqual(task['func'], 'tests.tasks.exception_task')

        executions = self.conn.lrange('t:task:%s:executions'%task['id'], 0, -1)
        self.assertEqual(len(executions), 1)
        execution = json.loads(executions[0])
        self.assertEqual(execution['exception_name'],
                         'exceptions.StandardError')
        self.assertEqual(execution['success'], False)

    def test_long_task_ok(self):
        self.tiger.delay(long_task_ok)
        Worker(self.tiger).run(once=True)
        self._ensure_queues(queued={'default': 0}, error={'default': 0})

    def test_long_task_killed(self):
        self.tiger.delay(long_task_killed)
        Worker(self.tiger).run(once=True)
        queues = self._ensure_queues(queued={'default': 0},
                                     error={'default': 1})

        task = queues['error']['default'][0]
        self.assertEqual(task['func'], 'tests.tasks.long_task_killed')

        executions = self.conn.lrange('t:task:%s:executions'%task['id'], 0, -1)
        self.assertEqual(len(executions), 1)
        execution = json.loads(executions[0])
        self.assertEqual(execution['exception_name'],
                         'tasktiger.timeouts.JobTimeoutException')
        self.assertEqual(execution['success'], False)

    def test_unique_task(self):
        self.tiger.delay(unique_task, kwargs={'value': 1})
        self.tiger.delay(unique_task, kwargs={'value': 2})
        self.tiger.delay(unique_task, kwargs={'value': 2})

        queues = self._ensure_queues(queued={'default': 2},
                                     error={'default': 0})

        task_1, task_2 = queues['queued']['default']

        self.assertEqual(task_1['func'], 'tests.tasks.unique_task')
        self.assertEqual(task_1['kwargs'], {'value': 1})

        self.assertEqual(task_2['func'], 'tests.tasks.unique_task')
        self.assertEqual(task_2['kwargs'], {'value': 2})

        Pool(3).map(external_worker, range(3))

        results = self.conn.lrange('unique_task', 0, -1)
        self.assertEqual(len(results), 2)
        self.assertEqual(set(results), set(['1', '2']))

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

    def test_retry(self):
        # Use the default retry method we configured.
        self.tiger.delay(exception_task, retry=True)
        queues = self._ensure_queues(queued={'default': 1},
                                     scheduled={'default': 0},
                                     error={'default': 0})
        task = queues['queued']['default'][0]

        # First run
        Worker(self.tiger).run(once=True)
        self.assertEqual(self.conn.llen('t:task:%s:executions' % task['id']), 1)
        self._ensure_queues(queued={'default': 0},
                            scheduled={'default': 1},
                            error={'default': 0})

        # The task is scheduled, so nothing happens here.
        Worker(self.tiger).run(once=True)
        self.assertEqual(self.conn.llen('t:task:%s:executions' % task['id']), 1)

        self._ensure_queues(queued={'default': 0},
                            scheduled={'default': 1},
                            error={'default': 0})

        time.sleep(DELAY)

        # Second run (run twice to move from scheduled to queued)
        Worker(self.tiger).run(once=True)
        Worker(self.tiger).run(once=True)
        self.assertEqual(self.conn.llen('t:task:%s:executions' % task['id']), 2)
        self._ensure_queues(queued={'default': 0},
                            scheduled={'default': 1},
                            error={'default': 0})

        time.sleep(DELAY)

        # Third run will fail permanently.
        Worker(self.tiger).run(once=True)
        Worker(self.tiger).run(once=True)
        self.assertEqual(self.conn.llen('t:task:%s:executions' % task['id']), 3)
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
        self.tiger.delay(exception_task, retry_on=[ValueError, StandardError])
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
        self.tiger.delay(exception_task, retry_method=linear(DELAY, DELAY, 3))

        queues = self._ensure_queues(queued={'default': 1})
        task = queues['queued']['default'][0]

        def _run(n_executions):
            Worker(self.tiger).run(once=True)
            Worker(self.tiger).run(once=True)
            self.assertEqual(self.conn.llen('t:task:%s:executions'%task['id']),
                    n_executions)

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
        self.assertEqual(f[0](1, *f[1]), 2)
        self.assertEqual(f[0](2, *f[1]), 2)
        self.assertEqual(f[0](3, *f[1]), 2)
        self.assertRaises(StopRetry, f[0], 4, *f[1])

    def test_retry_method_linear(self):
        f = linear(1, 2, 3)
        self.assertEqual(f[0](1, *f[1]), 1)
        self.assertEqual(f[0](2, *f[1]), 3)
        self.assertEqual(f[0](3, *f[1]), 5)
        self.assertRaises(StopRetry, f[0], 4, *f[1])

    def test_retry_method_exponential(self):
        f = exponential(1, 2, 4)
        self.assertEqual(f[0](1, *f[1]), 1)
        self.assertEqual(f[0](2, *f[1]), 2)
        self.assertEqual(f[0](3, *f[1]), 4)
        self.assertEqual(f[0](4, *f[1]), 8)
        self.assertRaises(StopRetry, f[0], 5, *f[1])

    def test_batch_1(self):
        self.tiger.delay(batch_task, args=[1])
        self.tiger.delay(batch_task, args=[2])
        self.tiger.delay(batch_task, args=[3])
        self.tiger.delay(batch_task, args=[4])
        self._ensure_queues(queued={'batch': 4})
        Worker(self.tiger).run(once=True)
        self._ensure_queues(queued={'batch': 0})
        data = [json.loads(d) for d in self.conn.lrange('batch_task', 0, -1)]
        self.assertEqual(data, [
            [
                {'args': [1], 'kwargs': {}},
                {'args': [2], 'kwargs': {}},
                {'args': [3], 'kwargs': {}},
            ], [
                {'args': [4], 'kwargs': {}},
            ]
        ])

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
        self.assertEqual(data, [
            [
                {'args': [1], 'kwargs': {}},
                {'args': [2], 'kwargs': {}},
            ], 5, [
                {'args': [3], 'kwargs': {}},
                {'args': [4], 'kwargs': {}},
            ], 6, 7
        ])

    def test_batch_3(self):
        self.tiger.delay(batch_task, queue='default', args=[1])
        self.tiger.delay(batch_task, queue='default', args=[2])
        self.tiger.delay(batch_task, queue='default', args=[3])
        self.tiger.delay(batch_task, queue='default', args=[4])
        self._ensure_queues(queued={'default': 4})
        Worker(self.tiger).run(once=True)
        self._ensure_queues(queued={'default': 0})
        data = [json.loads(d) for d in self.conn.lrange('batch_task', 0, -1)]
        self.assertEqual(data, [
            [
                {'args': [1], 'kwargs': {}},
            ], [
                {'args': [2], 'kwargs': {}},
            ], [
                {'args': [3], 'kwargs': {}},
            ], [
                {'args': [4], 'kwargs': {}},
            ]
        ])

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

if __name__ == '__main__':
    unittest.main()
