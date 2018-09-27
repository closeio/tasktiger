import datetime

import pytest
import redis

from freezefrog import FreezeTime

from tasktiger.redis_semaphore import Semaphore
from .utils import get_tiger


class TestSemaphore:
    def setup_method(self, method):
        self.tiger = get_tiger()
        self.conn = self.tiger.connection
        self.conn.flushdb()

    def teardown_method(self, method):
        self.conn.flushdb()

    def test_semaphore(self):
        semaphore1 = Semaphore(self.conn, 'test_key',
                                    'id_1', max=1,
                                    timeout=10)
        semaphore2 = Semaphore(self.conn, 'test_key',
                               'id_2', max=1,
                               timeout=10)

        # Get lock and then release
        with FreezeTime(datetime.datetime(2014, 1, 1)):
            locks = semaphore1.acquire()
        assert locks == 1
        semaphore1.release()

        # Get lock
        with FreezeTime(datetime.datetime(2014, 1, 1)):
            locks = semaphore2.acquire()
        assert locks == 1

        # Fail getting lock
        with FreezeTime(datetime.datetime(2014, 1, 1, 0, 0, 9)):
            locks = semaphore1.acquire()
        assert locks == -1

        # Successful getting lock after semaphore2 times out
        with FreezeTime(datetime.datetime(2014, 1, 1, 0, 0, 10)):
            locks = semaphore1.acquire()
        assert locks == 1

    def test_semaphores_multiple(self):
        semaphore1 = Semaphore(self.conn, 'test_key',
                               'id_1', max=2,
                               timeout=10)
        semaphore2 = Semaphore(self.conn, 'test_key',
                               'id_2', max=2,
                               timeout=10)
        semaphore3 = Semaphore(self.conn, 'test_key',
                               'id_3', max=2,
                               timeout=10)

        with FreezeTime(datetime.datetime(2014, 1, 1)):
            locks = semaphore1.acquire()
        assert locks == 1

        with FreezeTime(datetime.datetime(2014, 1, 1, 0, 0, 4)):
            locks = semaphore2.acquire()
        assert locks == 2

        with FreezeTime(datetime.datetime(2014, 1, 1, 0, 0, 6)):
            locks = semaphore3.acquire()
        assert locks == -1

        semaphore2.release()

        with FreezeTime(datetime.datetime(2014, 1, 1, 0, 0, 9)):
            locks = semaphore3.acquire()
        assert locks == 2

    def test_semaphores_renew(self):
        semaphore1 = Semaphore(self.conn, 'test_key',
                               'id_1', max=1,
                               timeout=10)
        semaphore2 = Semaphore(self.conn, 'test_key',
                               'id_2', max=1,
                               timeout=10)

        with FreezeTime(datetime.datetime(2014, 1, 1)):
            locks = semaphore1.acquire()
        assert locks == 1

        # Renew 5 seconds into lock timeout window
        with FreezeTime(datetime.datetime(2014, 1, 1, 0, 0, 5)):
            locks = semaphore1.renew()
        assert locks == 1

        # Fail getting a lock
        with FreezeTime(datetime.datetime(2014, 1, 1, 0, 0, 14)):
            locks = semaphore2.acquire()
        assert locks == -1

        # Successful getting lock after renewed timeout window passes
        with FreezeTime(datetime.datetime(2014, 1, 1, 0, 0, 15)):
            locks = semaphore2.acquire()
        assert locks == 1

        # Fail renewing
        with FreezeTime(datetime.datetime(2014, 1, 1, 0, 0, 15)):
            locks = semaphore1.renew()
        assert locks == -1
