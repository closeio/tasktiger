import time
from unittest import mock

import pytest

from tasktiger import Worker
from tasktiger.stats import StatsThread

from tests.utils import get_tiger


@pytest.fixture
def tiger():
    t = get_tiger()
    t.config["STATS_INTERVAL"] = 0.07
    return t


def test_start_and_stop(tiger):
    stats = StatsThread(tiger)
    stats.compute_stats = mock.Mock()
    stats.start()

    time.sleep(0.22)
    stats.stop()

    assert len(stats.compute_stats.mock_calls) == 3

    # Stats are no longer being collected
    time.sleep(0.22)
    assert len(stats.compute_stats.mock_calls) == 3


@pytest.mark.parametrize(
    "task_state,time_busy", [("idle", 0), ("running", 9), ("finished", 2)]
)
def test_maintenance_does_not_affect_task_stats(tiger, task_state, time_busy):
    worker = Worker(tiger)
    worker.log = mock.Mock()
    with mock.patch("tasktiger.stats.time.monotonic", return_value=0) as clock:
        stats = StatsThread(worker)
        clock.return_value = 1
        if task_state != "idle":
            stats.report_task_start()
        clock.return_value = 3
        if task_state == "finished":
            stats.report_task_end()
        clock.return_value = 4
        stats.report_maintenance_start()
        clock.return_value = 6
        stats.report_maintenance_end()
        clock.return_value = 10
        stats.compute_stats()

    worker.log.info.assert_called_once_with(
        "stats", time_total=10, time_busy=time_busy, utilization=time_busy * 10
    )
