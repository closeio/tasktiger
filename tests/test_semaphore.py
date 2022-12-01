"""Test Redis Semaphore lock."""
import datetime
import time

import pytest
from freezefrog import FreezeTime

from tasktiger.redis_semaphore import Semaphore

from .utils import get_tiger


class TestSemaphore:
    """Test Redis Semaphores."""

    def setup_method(self, method):
        """Test setup."""

        self.tiger = get_tiger()
        self.conn = self.tiger.connection
        self.conn.flushdb()

    def teardown_method(self, method):
        """Test teardown."""

        self.conn.flushdb()
        self.conn.close()
        # Force disconnect so we don't get Too many open files
        self.conn.connection_pool.disconnect()

    def test_simple_semaphore(self):
        """Test semaphore."""

        semaphore1 = Semaphore(
            self.conn, "test_key", "id_1", max_locks=1, timeout=10
        )
        semaphore2 = Semaphore(
            self.conn, "test_key", "id_2", max_locks=1, timeout=10
        )

        # Get lock and then release
        with FreezeTime(datetime.datetime(2014, 1, 1)):
            acquired, locks = semaphore1.acquire()
        assert acquired
        assert locks == 1
        semaphore1.release()

        # Get a new lock after releasing old
        with FreezeTime(datetime.datetime(2014, 1, 1)):
            acquired, locks = semaphore2.acquire()
        assert acquired
        assert locks == 1

        # Fail getting second lock while still inside time out period
        with FreezeTime(datetime.datetime(2014, 1, 1, 0, 0, 9)):
            acquired, locks = semaphore1.acquire()
        assert not acquired
        assert locks == 1

        # Successful getting lock after semaphore2 times out
        with FreezeTime(datetime.datetime(2014, 1, 1, 0, 0, 10)):
            acquired, locks = semaphore1.acquire()
        assert acquired
        assert locks == 1

    def test_multiple_locks(self):
        semaphore1 = Semaphore(
            self.conn, "test_key", "id_1", max_locks=2, timeout=10
        )
        semaphore2 = Semaphore(
            self.conn, "test_key", "id_2", max_locks=2, timeout=10
        )
        semaphore3 = Semaphore(
            self.conn, "test_key", "id_3", max_locks=2, timeout=10
        )

        # First two locks should be acquired
        with FreezeTime(datetime.datetime(2014, 1, 1)):
            acquired, locks = semaphore1.acquire()
        assert acquired
        assert locks == 1
        with FreezeTime(datetime.datetime(2014, 1, 1, 0, 0, 4)):
            acquired, locks = semaphore2.acquire()
        assert acquired
        assert locks == 2

        # Third lock should fail
        with FreezeTime(datetime.datetime(2014, 1, 1, 0, 0, 6)):
            acquired, locks = semaphore3.acquire()
        assert not acquired
        assert locks == 2

        semaphore2.release()

        # Releasing one of the existing locks should let a new lock succeed
        with FreezeTime(datetime.datetime(2014, 1, 1, 0, 0, 9)):
            acquired, locks = semaphore3.acquire()
        assert acquired
        assert locks == 2

    def test_semaphores_renew(self):
        semaphore1 = Semaphore(
            self.conn, "test_key", "id_1", max_locks=1, timeout=10
        )
        semaphore2 = Semaphore(
            self.conn, "test_key", "id_2", max_locks=1, timeout=10
        )

        with FreezeTime(datetime.datetime(2014, 1, 1)):
            acquired, locks = semaphore1.acquire()
        assert acquired
        assert locks == 1

        # Renew 5 seconds into lock timeout window
        with FreezeTime(datetime.datetime(2014, 1, 1, 0, 0, 5)):
            acquired, locks = semaphore1.renew()
        assert acquired
        assert locks == 1

        # Fail getting a lock
        with FreezeTime(datetime.datetime(2014, 1, 1, 0, 0, 14)):
            acquired, locks = semaphore2.acquire()
        assert not acquired
        assert locks == 1

        # Successful getting lock after renewed timeout window passes
        with FreezeTime(datetime.datetime(2014, 1, 1, 0, 0, 15)):
            acquired, locks = semaphore2.acquire()
        assert acquired
        assert locks == 1

        # Fail renewing
        with FreezeTime(datetime.datetime(2014, 1, 1, 0, 0, 15)):
            acquired, locks = semaphore1.renew()
        assert not acquired
        assert locks == 1

    # Test system lock shorter and longer than regular lock timeout
    @pytest.mark.parametrize("timeout", [8, 30])
    def test_system_lock(self, timeout):
        semaphore1 = Semaphore(
            self.conn, "test_key", "id_1", max_locks=10, timeout=10
        )

        with FreezeTime(datetime.datetime(2014, 1, 1)):
            Semaphore.set_system_lock(self.conn, "test_key", timeout)
            ttl = Semaphore.get_system_lock(self.conn, "test_key")
            assert ttl == time.time() + timeout

            # Should be blocked by system lock
            acquired, locks = semaphore1.acquire()
            assert not acquired
            assert locks == -1

        # System lock should still block other locks 1 second before it expires
        with FreezeTime(datetime.datetime(2014, 1, 1, 0, 0, timeout - 1)):
            acquired, locks = semaphore1.acquire()
            assert not acquired
            assert locks == -1

        # Wait for system lock to expire
        with FreezeTime(datetime.datetime(2014, 1, 1, 0, 0, timeout)):
            acquired, locks = semaphore1.acquire()
            assert acquired
            assert locks == 1
