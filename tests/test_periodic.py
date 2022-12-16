"""Periodic task tests."""

import datetime
import time

from tasktiger import Task, Worker, periodic
from tasktiger._internal import (
    QUEUED,
    SCHEDULED,
    gen_unique_id,
    serialize_func_name,
)

from .tasks_periodic import periodic_task, tiger
from .test_base import BaseTestCase
from .utils import sleep_until_next_second


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
        assert f[0](
            datetime.datetime(2009, 12, 31, 23, 58), *f[1]
        ) == datetime.datetime(2009, 12, 31, 23, 59)

        f = periodic(minutes=1, end_date=dt)
        assert f[0](
            datetime.datetime(2009, 12, 31, 23, 59), *f[1]
        ) == datetime.datetime(2010, 1, 1, 0, 0)

        f = periodic(minutes=1, end_date=dt)
        assert f[0](datetime.datetime(2010, 1, 1, 0, 0), *f[1]) is None

        f = periodic(minutes=1, end_date=dt)
        assert f[0](datetime.datetime(2010, 1, 1, 0, 1), *f[1]) is None

    def test_periodic_execution(self):
        """
        Test periodic task execution.

        Test periodic_task() runs as expected and periodic_task_ignore()
        is not queued.
        """
        # Sleep until the next second to ensure we have enough time to start
        # the worker and get the periodic task queued before the following
        # second starts.
        sleep_until_next_second()

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
            self._ensure_queues(scheduled={"periodic": 1})
        except AssertionError:
            Worker(tiger).run(once=True)
            self._ensure_queues(scheduled={"periodic": 1})
            assert int(self.conn.get("period_count")) == 1
            self.conn.delete("period_count")

        def ensure_run(n):
            # Run worker twice (once to move from scheduled to queued, and once
            # to execute the task)
            Worker(tiger).run(once=True)
            self._ensure_queues(queued={"periodic": 1})
            Worker(tiger).run(once=True)
            self._ensure_queues(scheduled={"periodic": 1})

            assert int(self.conn.get("period_count")) == n

            # The task is requeued for the next period
            self._ensure_queues(scheduled={"periodic": 1})

        # Sleep until the next second
        sleep_until_next_second()

        ensure_run(1)

        # Within less than a second, the task will be processed again.
        time.sleep(1)

        ensure_run(2)

    def test_periodic_execution_unique_ids(self):
        """
        Test that periodic tasks generate the same unique ids

        When a periodic task is scheduled initially as part of worker startup
        vs re-scheduled from within python the unique id generated should be
        the same. If they aren't it could result in duplicate tasks.
        """
        # Sleep until the next second
        sleep_until_next_second()

        # After the first worker run, the periodic task will be queued.
        # Note that since periodic tasks register with the Tiger instance, it
        # must be the same instance that was used to decorate the task. We
        # therefore use `tiger` from the tasks module instead of `self.tiger`.
        self._ensure_queues()
        Worker(tiger).run(once=True)
        self._ensure_queues(scheduled={"periodic": 1})
        time.sleep(1)
        Worker(tiger).run(once=True)
        self._ensure_queues(queued={"periodic": 1})

        # generate the expected unique id
        expected_unique_id = gen_unique_id(
            serialize_func_name(periodic_task), [], {}
        )

        # pull task out of the queue by id. If found, then the id is correct
        task = Task.from_id(tiger, "periodic", QUEUED, expected_unique_id)
        assert task is not None

        # execute and reschedule the task
        self._ensure_queues(queued={"periodic": 1})
        Worker(tiger).run(once=True)
        self._ensure_queues(scheduled={"periodic": 1})

        # wait for the task to need to be queued
        time.sleep(1)
        Worker(tiger).run(once=True)
        self._ensure_queues(queued={"periodic": 1})

        # The unique id shouldn't change between executions. Try finding the
        # task by id again
        task = Task.from_id(tiger, "periodic", QUEUED, expected_unique_id)
        assert task is not None

    def test_periodic_execution_unique_ids_manual_scheduling(self):
        """
        Periodic tasks should have the same unique ids when manually scheduled

        When a periodic task is scheduled initially as part of worker startup
        vs ``.delay``'d manually, the unique id generated should be the same.
        If they aren't it could result in duplicate tasks.
        """
        # Sleep until the next second
        sleep_until_next_second()

        # After the first worker run, the periodic task will be queued.
        # Note that since periodic tasks register with the Tiger instance, it
        # must be the same instance that was used to decorate the task. We
        # therefore use `tiger` from the tasks module instead of `self.tiger`.
        self._ensure_queues()
        Worker(tiger).run(once=True)
        self._ensure_queues(scheduled={"periodic": 1})
        time.sleep(1)
        Worker(tiger).run(once=True)
        self._ensure_queues(queued={"periodic": 1})

        # schedule the task manually
        periodic_task.delay()

        # make sure a duplicate wasn't scheduled
        self._ensure_queues(queued={"periodic": 1})

    def test_periodic_execution_unique_ids_self_correct(self):
        """
        Test that periodic tasks will self-correct unique ids
        """
        # Sleep until the next second
        sleep_until_next_second()

        # generate the ids
        correct_unique_id = gen_unique_id(
            serialize_func_name(periodic_task), [], {}
        )
        malformed_unique_id = gen_unique_id(
            serialize_func_name(periodic_task), None, None
        )

        task = Task(tiger, func=periodic_task)

        # patch the id to something slightly wrong
        assert task.id == correct_unique_id
        task._data["id"] = malformed_unique_id
        assert task.id == malformed_unique_id

        # schedule the task
        task.delay()
        self._ensure_queues(queued={"periodic": 1})

        # pull task out of the queue by the malformed id
        task = Task.from_id(tiger, "periodic", QUEUED, malformed_unique_id)
        assert task is not None

        Worker(tiger).run(once=True)
        self._ensure_queues(scheduled={"periodic": 1})

        # pull task out of the queue by the self-corrected id
        task = Task.from_id(tiger, "periodic", SCHEDULED, correct_unique_id)
        assert task is not None

    def test_successful_execution_clears_executions_from_retries(self):
        """
        Ensure previous executions from retries are cleared after a successful
        execution.
        """
        sleep_until_next_second()

        # Queue the periodic task.
        self._ensure_queues()
        Worker(tiger).run(once=True)

        # Prepare to execute the periodic task (as retriable failure).
        tiger.connection.set("fail-periodic-task", "retriable")
        n_total, tasks = Task.tasks_from_queue(tiger, "periodic", SCHEDULED)
        task_id = tasks[0].id
        time.sleep(1)

        # Queue the periodic task.
        self._ensure_queues(scheduled={"periodic": 1})
        Worker(tiger).run(once=True)

        # Run the failing periodic task.
        self._ensure_queues(queued={"periodic": 1})
        Worker(tiger).run(once=True)

        task = Task.from_id(
            tiger, "periodic", SCHEDULED, task_id, load_executions=10
        )
        assert len(task.executions) == 1

        tiger.connection.delete("fail-periodic-task")
        time.sleep(1)

        # Queue the periodic task.
        self._ensure_queues(scheduled={"periodic": 1})
        Worker(tiger).run(once=True)

        # Run the successful periodic task.
        self._ensure_queues(queued={"periodic": 1})
        Worker(tiger).run(once=True)

        # Ensure we cleared any previous executions.
        task = Task.from_id(
            tiger, "periodic", SCHEDULED, task_id, load_executions=10
        )
        assert len(task.executions) == 0

    def test_successful_execution_doesnt_clear_previous_errors(self):
        """
        Ensure previous executions are not cleared if we have had non-retriable
        errors.
        """
        sleep_until_next_second()

        # Queue the periodic task.
        self._ensure_queues()
        Worker(tiger).run(once=True)

        # Prepare to execute the periodic task (as permanent failure).
        tiger.connection.set("fail-periodic-task", "permanent")
        n_total, tasks = Task.tasks_from_queue(tiger, "periodic", SCHEDULED)
        task_id = tasks[0].id
        time.sleep(1)

        # Queue the periodic task.
        self._ensure_queues(scheduled={"periodic": 1})
        Worker(tiger).run(once=True)

        # Run the failing periodic task.
        self._ensure_queues(queued={"periodic": 1})
        Worker(tiger).run(once=True)

        task = Task.from_id(
            tiger, "periodic", SCHEDULED, task_id, load_executions=10
        )
        assert len(task.executions) == 1

        tiger.connection.delete("fail-periodic-task")
        time.sleep(1)

        # Queue the periodic task.
        self._ensure_queues(scheduled={"periodic": 1}, error={"periodic": 1})
        Worker(tiger).run(once=True)

        # Run the successful periodic task.
        self._ensure_queues(queued={"periodic": 1}, error={"periodic": 1})
        Worker(tiger).run(once=True)

        # Ensure we didn't clear previous executions.
        task = Task.from_id(
            tiger, "periodic", SCHEDULED, task_id, load_executions=10
        )
        assert len(task.executions) == 1
