import threading
import time

class StatsThread(threading.Thread):
    def __init__(self, tiger):
        super(StatsThread, self).__init__()
        self.tiger = tiger
        self._stop_event = threading.Event()

        self._task_running = False
        self._time_start = time.time()
        self._time_busy = 0
        self._task_start_time = None

    def report_task_start(self):
        self._task_start_time = time.time()
        self._task_running = True

    def report_task_end(self):
        now = time.time()
        self._time_busy += now - self._task_start_time
        self._task_running = False
        self._task_start_time = None

    def compute_stats(self):
        now = time.time()
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
            utilization = 100. / time_total * time_busy
            self.tiger.log.info('stats', time_total=time_total, time_busy=time_busy, utilization=utilization)

    def run(self):
        while True:
            self._stop_event.wait(self.tiger.config['STATS_INTERVAL'])
            if self._stop_event.isSet():
                break
            self.compute_stats()

    def stop(self):
        self._stop_event.set()
