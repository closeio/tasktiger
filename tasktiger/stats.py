import threading
import time
from typing import TYPE_CHECKING, Callable, Optional

from ._internal import g_fork_lock, import_attribute

if TYPE_CHECKING:
    from .tasktiger import TaskTiger


class Stats:
    def __init__(
        self,
        tiger: "TaskTiger",
        callback: Optional[Callable[[float], None]] = None,
    ) -> None:
        super().__init__()

        self.tiger = tiger

        self._logging_thread: Optional[StatsLoggingThread] = None

        self._time_start = time.monotonic()
        self._time_busy = 0.0
        self._task_start_time: Optional[float] = None

        # Lock that protects stats computations from interleaving. For example,
        # we don't want report_task_start() to run at the same time as
        # log(), as it might result in an inconsistent state.
        self._lock = threading.Lock()

        # Callback to receive the duration of each completed task.
        self._callback = (
            import_attribute(callback)
            if (callback := self.tiger.config["STATS_CALLBACK"])
            else None
        )

    def report_task_start(self) -> None:
        now = time.monotonic()
        with self._lock:
            self._task_start_time = now

    def report_task_end(self) -> None:
        assert self._task_start_time
        now = time.monotonic()

        if self._callback:
            self._callback(now - self._task_start_time)

        with self._lock:
            self._record_time_busy(now)
            self._task_start_time = None

    def log(self) -> None:
        now = time.monotonic()

        with self._lock:
            time_total = now - self._time_start
            time_busy = self._time_busy

            if self._task_start_time is not None:
                # Add busy time for the currently running task
                self._record_time_busy(now)

            time_busy = self._time_busy

            self._time_start = now
            self._time_busy = 0

        if time_total:
            utilization = 100.0 / time_total * time_busy
            with g_fork_lock:
                self.tiger.log.info(
                    "stats",
                    time_total=time_total,
                    time_busy=time_busy,
                    utilization=utilization,
                )

    def start_logging_thread(self) -> None:
        if not self._logging_thread:
            self._logging_thread = StatsLoggingThread(self)
            self._logging_thread.start()

    def stop_logging_thread(self) -> None:
        if self._logging_thread:
            self._logging_thread.stop()
            self._logging_thread = None

    def _record_time_busy(self, now: float) -> None:
        assert self._task_start_time
        self._time_busy += now - max(self._task_start_time, self._time_start)


class StatsLoggingThread(threading.Thread):
    def __init__(self, stats: Stats) -> None:
        super().__init__()

        self.tiger = stats.tiger
        self._stats: Stats = stats
        self._stop_event = threading.Event()

        self.daemon = True  # Exit process if main thread exits unexpectedly

    def run(self) -> None:
        while not self._stop_event.wait(self.tiger.config["STATS_INTERVAL"]):
            self._stats.log()

    def stop(self) -> None:
        self._stop_event.set()
