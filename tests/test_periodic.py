"""Periodic task tests."""

import datetime
import time

from tasktiger import (Worker, periodic)

from .tasks_periodic import tiger
from .test_base import BaseTestCase


class TestPeriodicTasks(BaseTestCase):
    def test_periodic_schedule(self):
        """
        Test the periodic() schedule function.
        """
        dt = datetime.datetime(2010, 1, 1)

        f = periodic(seconds=1)
        assert f[0](dt, *f[1]) == datetime.datetime(2010, 1, 1, 0, 0, 1)

        f = periodic(minutes=1)
        assert f[0](dt, *f[1]) == datetime.datetime(2010, 1, 1, 0, 1)

        f = periodic(hours=1)
        assert f[0](dt, *f[1]) == datetime.datetime(2010, 1, 1, 1)

        f = periodic(days=1)
        assert f[0](dt, *f[1]) == datetime.datetime(2010, 1, 2)

        f = periodic(weeks=1)
        # 2010-01-02 is a Saturday
        assert f[0](dt, *f[1]) == datetime.datetime(2010, 1, 2)

        f = periodic(weeks=1, start_date=datetime.datetime(2000, 1, 2))
        # 2000-01-02 and 2010-01-02 are Sundays
        assert f[0](dt, *f[1]) == datetime.datetime(2010, 1, 3)

        f = periodic(seconds=1, minutes=2, hours=3, start_date=dt)
        assert f[0](dt, *f[1]) == datetime.datetime(2010, 1, 1, 3, 2, 1)
        # Make sure we return the start_date if the current date is earlier.
        assert f[0](datetime.datetime(1990, 1, 1), *f[1]) == dt

        f = periodic(minutes=1, end_date=dt)
        assert (f[0](datetime.datetime(2009, 12, 31, 23, 58), *f[1]) ==
                              datetime.datetime(2009, 12, 31, 23, 59))

        f = periodic(minutes=1, end_date=dt)
        assert (f[0](datetime.datetime(2009, 12, 31, 23, 59), *f[1]) ==
                              datetime.datetime(2010, 1, 1, 0, 0))

        f = periodic(minutes=1, end_date=dt)
        assert f[0](datetime.datetime(2010, 1, 1, 0, 0), *f[1]) == None

        f = periodic(minutes=1, end_date=dt)
        assert f[0](datetime.datetime(2010, 1, 1, 0, 1), *f[1]) == None

    def test_periodic_execution(self):
        """
        Test periodic task execution.

        Test periodic_task() runs as expected and periodic_task_ignore()
        is not queued.
        """

        # After the first worker run, the periodic task will be queued.
        # Note that since periodic tasks register with the Tiger instance, it
        # must be the same instance that was used to decorate the task. We
        # therefore use `tiger` from the tasks module instead of `self.tiger`.
        self._ensure_queues()
        Worker(tiger).run(once=True)

        # NOTE: When the worker is started just before the second elapses,
        # it's possible that the periodic task is in "queued" state instead
        # of "scheduled" to ensure immediate execution. We capture this
        # condition by running the task, and retry.
        try:
            self._ensure_queues(scheduled={'periodic': 1})
        except AssertionError:
            Worker(tiger).run(once=True)
            self._ensure_queues(scheduled={'periodic': 1})
            assert int(self.conn.get('period_count')) == 1
            self.conn.delete('period_count')

        def ensure_run(n):
            # Run worker twice (once to move from scheduled to queued, and once
            # to execute the task)
            Worker(tiger).run(once=True)
            self._ensure_queues(queued={'periodic': 1})
            Worker(tiger).run(once=True)
            self._ensure_queues(scheduled={'periodic': 1})

            assert int(self.conn.get('period_count')) == n

            # The task is requeued for the next period
            self._ensure_queues(scheduled={'periodic': 1})

        # Sleep until the next second
        now = datetime.datetime.utcnow()
        time.sleep(1 - now.microsecond / 10.**6)

        ensure_run(1)

        # Within less than a second, the task will be processed again.
        time.sleep(1)

        ensure_run(2)
