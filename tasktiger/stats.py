import threading
import time

from ._internal import g_fork_lock


class StatsThread(threading.Thread):
    def __init__(self, tiger):
        super(StatsThread, self).__init__()
        self.tiger = tiger
        self._stop_event = threading.Event()

        self._task_running = False
        self._time_start = time.time()
        self._time_busy = 0
        self._task_start_time = None
        self.daemon = True  # Exit process if main thread exits unexpectedly

        # Lock that protects stats computations from interleaving. For example,
        # we don't want report_task_start() to run at the same time as
        # compute_stats(), as it might result in an inconsistent state.
        self._computation_lock = threading.Lock()

    def report_task_start(self):
        now = time.time()
        with self._computation_lock:
            self._task_start_time = now
            self._task_running = True

    def report_task_end(self):
        now = time.time()
        with self._computation_lock:
            self._time_busy += now - self._task_start_time
            self._task_running = False
            self._task_start_time = None

    def compute_stats(self):
        now = time.time()

        with self._computation_lock:
            time_total = now - self._time_start
            time_busy = self._time_busy
            self._time_start = now
            self._time_busy = 0
            if self._task_running:
                time_busy += now - self._task_start_time
                self._task_start_time = now
            else:
                self._task_start_time = None

        if time_total:
            utilization = 100.0 / time_total * time_busy
            with g_fork_lock:
                self.tiger.log.info(
                    "stats",
                    time_total=time_total,
                    time_busy=time_busy,
                    utilization=utilization,
                )

    def run(self):
        while True:
            self._stop_event.wait(self.tiger.config["STATS_INTERVAL"])
            if self._stop_event.isSet():
                break
            self.compute_stats()

    def stop(self):
        self._stop_event.set()
