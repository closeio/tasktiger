"""Child context manager tests."""
import redis

from tasktiger import Worker

from .tasks import exception_task, simple_task
from .test_base import BaseTestCase
from .config import TEST_DB


class ContextManagerTester(object):
    """
    Dummy context manager class.

    Uses Redis to track number of enter/exit calls
    """
    def __init__(self, name):
        self.name = name
        self.conn = redis.Redis(db=TEST_DB, decode_responses=True)
        self.conn.set('cm:{}:enter'.format(self.name), 0)
        self.conn.set('cm:{}:exit'.format(self.name), 0)

    def __enter__(self):
        self.conn.incr('cm:{}:enter'.format(self.name))

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.incr('cm:{}:exit'.format(self.name))


class TestChildContextManagers(BaseTestCase):
    """Child context manager tests."""

    def _get_context_managers(self, number):
        return [ContextManagerTester('cm' + str(i)) for i in range(number)]

    def _test_context_managers(self, num, task):
        cms = self._get_context_managers(num)

        self.tiger.config['CHILD_CONTEXT_MANAGERS'] = cms
        self.tiger.delay(task)
        Worker(self.tiger).run(once=True)

        for i in range(num):
            assert self.conn.get('cm:{}:enter'.format(cms[i].name)) == '1'
            assert self.conn.get('cm:{}:exit'.format(cms[i].name)) == '1'

    def test_single_context_manager(self):
        self._test_context_managers(1, simple_task)
        self._test_context_managers(1, exception_task)

    def test_multiple_context_managers(self):
        self._test_context_managers(10, simple_task)
        self._test_context_managers(10, exception_task)
