import threading
import time
from typing import Any, Optional

from ._internal import g_fork_lock


class StatsConsumer:
    """Receives worker measurements; configure via STATS_CONSUMERS.

    The worker calls start()/stop() around its run loop and the paired
    on_*_start()/on_*_end() hooks around each task and idle wait. Hooks
    must not raise — that disrupts task processing. One instance is reused
    across every worker and run of a TaskTiger.
    """

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass

    def on_task_start(self) -> None:
        pass

    def on_task_end(self) -> None:
        pass

    def on_idle_start(self) -> None:
        pass

    def on_idle_end(self) -> None:
        pass


class StatsThread(threading.Thread, StatsConsumer):
    """Periodically logs a "stats" event with worker time accounting.

    Construct with a logger and interval (seconds) and add to
    STATS_CONSUMERS; the worker starts and stops it. As a thread it can
    only start once — use a fresh instance per worker run.
    """

    def __init__(self, log: Any, interval: float) -> None:
        super(StatsThread, self).__init__()
        self.log = log
        self.interval = interval
        self._stop_event = threading.Event()

        self._task_running = False
        self._time_start = time.monotonic()
        self._time_busy: float = 0.0
        self._task_start_time: Optional[float] = None
        self._time_idle: float = 0.0
        self._idle_start_time: Optional[float] = None
        self.daemon = True  # Exit process if main thread exits unexpectedly

        # Serializes stats computations: the on_*() hooks must not
        # interleave with compute_stats(), or state goes inconsistent.
        # Timestamps are read under the lock so a span can't straddle a
        # window boundary.
        self._computation_lock = threading.Lock()

    def on_task_start(self) -> None:
        with self._computation_lock:
            assert self._task_start_time is None
            self._task_start_time = time.monotonic()
            self._task_running = True

    def on_task_end(self) -> None:
        with self._computation_lock:
            assert self._task_start_time is not None
            self._time_busy += time.monotonic() - self._task_start_time
            self._task_running = False
            self._task_start_time = None

    def on_idle_start(self) -> None:
        with self._computation_lock:
            assert self._idle_start_time is None
            self._idle_start_time = time.monotonic()

    def on_idle_end(self) -> None:
        with self._computation_lock:
            assert self._idle_start_time is not None
            self._time_idle += time.monotonic() - self._idle_start_time
            self._idle_start_time = None

    def compute_stats(self) -> None:
        with self._computation_lock:
            now = time.monotonic()
            time_total = now - self._time_start
            time_busy = self._time_busy
            time_idle = self._time_idle
            self._time_start = now
            self._time_busy = 0
            self._time_idle = 0
            if self._task_running:
                assert self._task_start_time is not None
                time_busy += now - self._task_start_time
                self._task_start_time = now
            else:
                self._task_start_time = None
            if self._idle_start_time is not None:
                time_idle += now - self._idle_start_time
                self._idle_start_time = now

        if time_total:
            # busy: in task code. idle: blocking waits for work.
            # overhead: the rest (dequeue, scan, locks, maintenance).
            time_overhead = time_total - time_busy - time_idle
            # A worker saturated with short tasks can show low utilization;
            # for load/autoscaling decisions use occupancy.
            utilization = 100.0 / time_total * time_busy
            occupancy = 100.0 * (time_total - time_idle) / time_total
            with g_fork_lock:
                self.log.info(
                    "stats",
                    time_total=time_total,
                    time_busy=time_busy,
                    time_idle=time_idle,
                    time_overhead=time_overhead,
                    utilization=utilization,
                    occupancy=occupancy,
                )

    def run(self) -> None:
        while not self._stop_event.wait(self.interval):
            self.compute_stats()

    def stop(self) -> None:
        self._stop_event.set()
