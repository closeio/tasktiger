"""Test max queue size limits."""

import datetime
import os
import signal
import time
from multiprocessing import Process

import pytest

from tasktiger import Task, Worker
from tasktiger.exceptions import QueueFullException

from .config import DELAY
from .tasks import decorated_task_max_queue_size, simple_task, sleep_task
from .test_base import BaseTestCase
from .utils import external_worker


class TestMaxQueue(BaseTestCase):
    """TaskTiger test max queue size."""

    def test_task_simple_delay(self):
        """Test enforcing max queue size using delay function."""

        self.tiger.delay(simple_task, queue="a", max_queue_size=1)
        self._ensure_queues(queued={"a": 1})

        # Queue size would be 2 so it should fail
        with pytest.raises(QueueFullException):
            self.tiger.delay(simple_task, queue="a", max_queue_size=1)

        # Process first task and then queuing a second should succeed
        Worker(self.tiger).run(once=True, force_once=True)
        self.tiger.delay(simple_task, queue="a", max_queue_size=1)
        self._ensure_queues(queued={"a": 1})

    def test_task_decorated(self):
        """Test max queue size with decorator."""

        decorated_task_max_queue_size.delay()
        self._ensure_queues(queued={"default": 1})

        with pytest.raises(QueueFullException):
            decorated_task_max_queue_size.delay()

    def test_task_all_states(self):
        """Test max queue size with tasks in all three states."""

        # Active
        task = Task(self.tiger, sleep_task, queue="a")
        task.delay()
        self._ensure_queues(queued={"a": 1})

        # Start a worker and wait until it starts processing.
        worker = Process(target=external_worker)
        worker.start()
        time.sleep(DELAY)

        # Kill the worker while it's still processing the task.
        os.kill(worker.pid, signal.SIGKILL)
        self._ensure_queues(active={"a": 1})

        # Scheduled
        self.tiger.delay(
            simple_task,
            queue="a",
            max_queue_size=3,
            when=datetime.timedelta(seconds=10),
        )

        # Queued
        self.tiger.delay(simple_task, queue="a", max_queue_size=3)

        self._ensure_queues(
            active={"a": 1}, queued={"a": 1}, scheduled={"a": 1}
        )

        # Should fail to queue task to run immediately
        with pytest.raises(QueueFullException):
            self.tiger.delay(simple_task, queue="a", max_queue_size=3)

        # Should fail to queue task to run in the future
        with pytest.raises(QueueFullException):
            self.tiger.delay(
                simple_task,
                queue="a",
                max_queue_size=3,
                when=datetime.timedelta(seconds=10),
            )


class TestQueueSizes:
    @pytest.fixture
    def queue_sample_tasks(self, tiger):
        tiger.delay(simple_task)
        tiger.delay(simple_task)
        tiger.delay(simple_task, queue="other")
        tiger.delay(simple_task, when=datetime.timedelta(seconds=60))

    def test_get_total_queue_size(self, tiger, queue_sample_tasks):
        assert tiger.get_total_queue_size("other") == 1
        assert tiger.get_total_queue_size("default") == 3

    def test_get_queue_sizes(self, tiger, queue_sample_tasks):
        assert tiger.get_queue_sizes("default") == {
            "active": 0,
            "queued": 2,
            "scheduled": 1,
        }
        assert tiger.get_queue_sizes("other") == {
            "active": 0,
            "queued": 1,
            "scheduled": 0,
        }

    def test_get_sizes_for_queues_and_states(self, tiger, queue_sample_tasks):
        assert tiger.get_sizes_for_queues_and_states(
            [
                ("default", "queued"),
                ("default", "scheduled"),
                ("other", "queued"),
                ("other", "scheduled"),
            ]
        ) == [2, 1, 1, 0]
