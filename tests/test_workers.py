"""Test workers."""

import datetime
import time
from multiprocessing import Process

import pytest
from freezefrog import FreezeTime

from tasktiger import Task, Worker
from tasktiger._internal import ACTIVE
from tasktiger.executor import SyncExecutor
from tasktiger.worker import LOCK_REDIS_KEY

from .config import DELAY
from .tasks import (
    exception_task,
    long_task_killed,
    long_task_ok,
    simple_task,
    sleep_task,
    system_exit_task,
    wait_for_long_task,
)
from .test_base import BaseTestCase
from .utils import external_worker


class TestMaxWorkers(BaseTestCase):
    """Single Worker Queue tests."""

    def test_max_workers(self):
        """Test Single Worker Queue."""

        # Queue three tasks
        for i in range(0, 3):
            task = Task(self.tiger, long_task_ok, queue="a")
            task.delay()
        self._ensure_queues(queued={"a": 3})

        # Start two workers and wait until they start processing.
        worker1 = Process(
            target=external_worker,
            kwargs={"worker_kwargs": {"max_workers_per_queue": 2}},
        )
        worker2 = Process(
            target=external_worker,
            kwargs={"worker_kwargs": {"max_workers_per_queue": 2}},
        )
        worker1.start()
        worker2.start()

        # Wait for both tasks to start
        wait_for_long_task()
        wait_for_long_task()

        # Verify they both are active
        self._ensure_queues(active={"a": 2}, queued={"a": 1})

        # This worker should fail to get the queue lock and exit immediately
        worker = Worker(self.tiger)
        worker.max_workers_per_queue = 2
        worker.run(once=True, force_once=True)
        self._ensure_queues(active={"a": 2}, queued={"a": 1})
        # Wait for external workers
        worker1.join()
        worker2.join()

    def test_single_worker_queue(self):
        """
        Test Single Worker Queue.

        Single worker queues are the same as running with MAX_WORKERS_PER_QUEUE
        set to 1.
        """

        # Queue two tasks
        task = Task(self.tiger, long_task_ok, queue="swq")
        task.delay()
        task = Task(self.tiger, long_task_ok, queue="swq")
        task.delay()
        self._ensure_queues(queued={"swq": 2})

        # Start a worker and wait until it starts processing.
        # It should start processing one task and hold a lock on the queue
        worker = Process(target=external_worker)
        worker.start()

        # Wait for task to start
        wait_for_long_task()

        # This worker should fail to get the queue lock and exit immediately
        Worker(self.tiger).run(once=True, force_once=True)
        self._ensure_queues(active={"swq": 1}, queued={"swq": 1})
        # Wait for external worker
        worker.join()

        # Clear out second task
        Worker(self.tiger).run(once=True, force_once=True)
        self.conn.delete("long_task_ok")

        # Retest using a non-single worker queue
        # Queue two tasks
        task = Task(self.tiger, long_task_ok, queue="not_swq")
        task.delay()
        task = Task(self.tiger, long_task_ok, queue="not_swq")
        task.delay()
        self._ensure_queues(queued={"not_swq": 2})

        # Start a worker and wait until it starts processing.
        # It should start processing one task
        worker = Process(target=external_worker)
        worker.start()

        # Wait for task to start processing
        wait_for_long_task()

        # This worker should process the second task
        Worker(self.tiger).run(once=True, force_once=True)

        # Queues should be empty since the first task will have to
        # have finished before the second task finishes.
        self._ensure_queues()

        worker.join()

    def test_queue_system_lock(self):
        """Test queue system lock."""

        with FreezeTime(datetime.datetime(2014, 1, 1)):
            # Queue three tasks
            for i in range(0, 3):
                task = Task(self.tiger, long_task_ok, queue="a")
                task.delay()
            self._ensure_queues(queued={"a": 3})

            # Ensure we can process one
            worker = Worker(self.tiger)
            worker.max_workers_per_queue = 2
            worker.run(once=True, force_once=True)
            self._ensure_queues(queued={"a": 2})

            # Set system lock so no processing should occur for 10 seconds
            self.tiger.set_queue_system_lock("a", 10)

            lock_timeout = self.tiger.get_queue_system_lock("a")
            assert lock_timeout == time.time() + 10

        # Confirm tasks don't get processed within the system lock timeout
        with FreezeTime(datetime.datetime(2014, 1, 1, 0, 0, 9)):
            worker = Worker(self.tiger)
            worker.max_workers_per_queue = 2
            worker.run(once=True, force_once=True)
            self._ensure_queues(queued={"a": 2})

        # 10 seconds in the future the lock should have expired
        with FreezeTime(datetime.datetime(2014, 1, 1, 0, 0, 10)):
            worker = Worker(self.tiger)
            worker.max_workers_per_queue = 2
            worker.run(once=True, force_once=True)
            self._ensure_queues(queued={"a": 1})


class TestSyncExecutorWorker:
    def test_success(self, tiger, ensure_queues):
        worker = Worker(tiger, executor_class=SyncExecutor)
        worker.run(once=True, force_once=True)

        task = Task(tiger, simple_task)
        task.delay()
        task = Task(tiger, simple_task)
        task.delay()
        ensure_queues(queued={"default": 2})

        worker.run(once=True)
        ensure_queues()

    def test_handles_exception(self, tiger, ensure_queues):
        Task(tiger, exception_task).delay()
        worker = Worker(tiger, executor_class=SyncExecutor)
        worker.run(once=True, force_once=True)
        ensure_queues(error={"default": 1})

    def test_handles_timeout(self, tiger, ensure_queues):
        Task(tiger, long_task_killed).delay()
        worker = Worker(tiger, executor_class=SyncExecutor)
        # Worker should exit to avoid any inconsistencies.
        with pytest.raises(SystemExit):
            worker.run(once=True, force_once=True)
        ensure_queues(error={"default": 1})

    def test_heartbeat(self, tiger):
        # Test both task heartbeat and lock renewal.
        # We set unique=True so the task ID matches the lock key.
        task = Task(tiger, sleep_task, lock=True, unique=True)
        task.delay()

        # Start a worker and wait until it starts processing.
        worker = Process(
            target=external_worker,
            kwargs={
                "patch_config": {"ACTIVE_TASK_UPDATE_TIMER": DELAY / 2},
                "worker_kwargs": {
                    # Test queue lock.
                    "max_workers_per_queue": 1,
                    "executor_class": SyncExecutor,
                },
            },
        )
        worker.start()

        time.sleep(DELAY)

        queue_key = tiger._key(ACTIVE, "default")
        queue_lock_key = tiger._key(LOCK_REDIS_KEY, "default")
        task_lock_key = tiger._key("lockv2", task.id)

        conn = tiger.connection

        heartbeat_1 = conn.zscore(queue_key, task.id)
        queue_lock_1 = conn.zrange(queue_lock_key, 0, -1, withscores=True)[0][
            1
        ]
        task_lock_1 = conn.pttl(task_lock_key)

        time.sleep(DELAY / 2)

        heartbeat_2 = conn.zscore(queue_key, task.id)
        queue_lock_2 = conn.zrange(queue_lock_key, 0, -1, withscores=True)[0][
            1
        ]
        task_lock_2 = conn.pttl(task_lock_key)

        assert heartbeat_2 > heartbeat_1 > 0
        assert queue_lock_2 > queue_lock_1 > 0

        # Active task update timeout is 2 * DELAY and we renew every DELAY / 2.
        assert task_lock_1 > DELAY
        assert task_lock_2 > DELAY

        worker.kill()

    def test_stop_heartbeat_thread_on_unhandled_exception(
        self, tiger, ensure_queues
    ):
        task = Task(tiger, system_exit_task)
        task.delay()

        # Start a worker and wait until it starts processing.
        worker = Process(
            target=external_worker,
            kwargs={
                "worker_kwargs": {
                    "executor_class": SyncExecutor,
                },
            },
        )
        worker.start()

        # Ensure process exits and does not hang here.
        worker.join()

        # Since SystemExit derives from BaseException and is therefore not
        # handled by the executor, the task is still active until it times out
        # and gets requeued by another worker.
        ensure_queues(active={"default": 1})
