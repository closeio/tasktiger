import threading
import time
from unittest import mock

import pytest

from tasktiger.stats import Stats

from tests.utils import get_tiger


@pytest.fixture
def tiger():
    t = get_tiger()
    t.config["STATS_INTERVAL"] = 0.001
    return t


@pytest.fixture
def time_monotonic_mock():
    with mock.patch("time.monotonic") as m:
        yield m


def test_stats_logging_thread_start_and_stop(tiger):
    semaphore = threading.Semaphore(0)

    stats = Stats(tiger)
    stats.log = mock.Mock(side_effect=semaphore.acquire)
    stats.start_logging_thread()

    thread = stats._logging_thread

    time.sleep(0.05)
    assert len(stats.log.mock_calls) == 1
    assert thread.is_alive()

    for __ in range(17):
        semaphore.release()

    while semaphore._value:
        pass

    time.sleep(0.02)
    assert len(stats.log.mock_calls) == 18
    assert thread.is_alive()

    for __ in range(24):
        semaphore.release()

    while semaphore._value:
        pass

    time.sleep(0.02)
    assert len(stats.log.mock_calls) == 42
    assert thread.is_alive()

    stats.stop_logging_thread()

    for __ in range(20):
        semaphore.release()

    thread.join()
    assert len(stats.log.mock_calls) == 42
    assert not thread.is_alive()


def test_stats_without_callback(tiger, time_monotonic_mock):
    time_monotonic_mock.return_value = 376263.4195456

    stats = Stats(tiger)
    assert stats._callback is None

    stats.report_task_start()

    time_monotonic_mock.return_value += 48.34
    stats.report_task_end()


def test_stats_with_callback(tiger, time_monotonic_mock):
    time_monotonic_mock.return_value = 376263.4195456

    stats = Stats(tiger)
    stats._callback = mock.Mock()

    stats.report_task_start()
    assert stats._callback.mock_calls == []

    time_monotonic_mock.return_value = 376375.4737221
    stats.report_task_end()

    assert stats._callback.mock_calls == [mock.call(112.05417650000891)]

    time_monotonic_mock.return_value += 34.22
    stats.report_task_start()

    time_monotonic_mock.return_value += 185.03
    stats.report_task_end()

    assert stats._callback.mock_calls == [
        mock.call(112.05417650000891),
        mock.call(185.03000000002794),
    ]

    time_monotonic_mock.return_value += 73.26
    stats.report_task_start()

    # Test that calling stats.log during the task's execution is not
    # breaking the calculation of the runtime for the callback
    time_monotonic_mock.return_value += 217.9
    stats.log()

    time_monotonic_mock.return_value += 130.5
    stats.report_task_end()

    assert stats._callback.mock_calls == [
        mock.call(112.05417650000891),
        mock.call(185.03000000002794),
        mock.call(348.4000000000233),
    ]


def test_stats_logging_in_between_task_runs(tiger, time_monotonic_mock):
    time_monotonic_mock.return_value = 376203.2371821

    stats = Stats(tiger)
    tiger.log = mock.Mock()

    time_monotonic_mock.return_value += 65
    stats.report_task_start()

    time_monotonic_mock.return_value += 120.7
    stats.report_task_end()

    stats.log()
    assert tiger.log.mock_calls == [
        mock.call.info(
            "stats",
            time_total=185.70000000001164,
            time_busy=120.70000000001164,
            utilization=64.99730748519337,
        )
    ]

    time_monotonic_mock.return_value += 0.7
    stats.report_task_start()

    time_monotonic_mock.return_value += 50.6
    stats.report_task_end()

    time_monotonic_mock.return_value += 10
    stats.report_task_start()

    time_monotonic_mock.return_value += 40
    stats.report_task_end()

    stats.log()
    assert tiger.log.mock_calls == [
        mock.call.info(
            "stats",
            time_total=185.70000000001164,
            time_busy=120.70000000001164,
            utilization=64.99730748519337,
        ),
        mock.call.info(
            "stats",
            time_total=101.29999999998836,
            time_busy=90.59999999997672,
            utilization=89.43731490620644,
        ),
    ]


def test_stats_logging_during_task_run(tiger, time_monotonic_mock):
    time_monotonic_mock.return_value = 376203.2371821

    stats = Stats(tiger)
    tiger.log = mock.Mock()

    time_monotonic_mock.return_value += 65
    stats.report_task_start()

    time_monotonic_mock.return_value += 120.7

    stats.log()
    assert tiger.log.mock_calls == [
        mock.call.info(
            "stats",
            time_total=185.70000000001164,
            # Busy time for unfinished task is measured
            time_busy=120.70000000001164,
            utilization=64.99730748519337,
        )
    ]

    time_monotonic_mock.return_value += 350.5
    stats.report_task_end()

    time_monotonic_mock.return_value += 33.1
    stats.report_task_start()

    time_monotonic_mock.return_value += 17

    stats.log()
    assert tiger.log.mock_calls == [
        mock.call.info(
            "stats",
            time_total=185.70000000001164,
            time_busy=120.70000000001164,
            utilization=64.99730748519337,
        ),
        mock.call.info(
            "stats",
            time_total=400.5999999999767,
            # Sum of the remainder of the previous task's runtime
            # and the current unfinished task's runtime
            time_busy=367.5,
            utilization=91.73739390914163,
        ),
    ]
