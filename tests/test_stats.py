import time
from unittest import mock

from tasktiger.stats import StatsConsumer, StatsThread


def test_start_and_stop():
    stats = StatsThread(mock.Mock(), interval=0.07)
    stats.compute_stats = mock.Mock()
    stats.start()

    time.sleep(0.22)
    stats.stop()

    assert len(stats.compute_stats.mock_calls) == 3

    # Stats are no longer being collected
    time.sleep(0.22)
    assert len(stats.compute_stats.mock_calls) == 3


def test_utilization_and_occupancy():
    log = mock.Mock()
    with mock.patch(
        "tasktiger.stats.time.monotonic",
        autospec=True,
        side_effect=[1000.0, 1003.0, 1005.0, 1012.0, 1018.0, 1032.0],
    ):
        stats = StatsThread(log, interval=60)
        stats.on_idle_start()
        stats.on_idle_end()
        stats.on_task_start()
        stats.on_task_end()

        stats.compute_stats()

    assert log.info.mock_calls == [
        mock.call(
            "stats",
            time_total=32.0,
            time_busy=6.0,
            time_idle=2.0,
            time_overhead=24.0,
            utilization=18.75,
            occupancy=75.0,
        )
    ]


def test_in_progress_idle_time_spans_stats_windows():
    log = mock.Mock()
    with mock.patch(
        "tasktiger.stats.time.monotonic",
        autospec=True,
        side_effect=[500.0, 506.0, 520.0, 524.0, 540.0],
    ):
        stats = StatsThread(log, interval=60)
        stats.on_idle_start()

        # Window ends mid-idle: partial idle attributed here, the rest
        # to the next window.
        stats.compute_stats()

        stats.on_idle_end()
        stats.compute_stats()

    assert log.info.mock_calls == [
        mock.call(
            "stats",
            time_total=20.0,
            time_busy=0.0,
            time_idle=14.0,
            time_overhead=6.0,
            utilization=0.0,
            occupancy=0.0,
        ),
        mock.call(
            "stats",
            time_total=20.0,
            time_busy=0.0,
            time_idle=4.0,
            time_overhead=16.0,
            utilization=0.0,
            occupancy=0.0,
        ),
    ]


def test_in_progress_task_time_spans_stats_windows():
    log = mock.Mock()
    with mock.patch(
        "tasktiger.stats.time.monotonic",
        autospec=True,
        side_effect=[800.0, 809.0, 830.0, 838.0, 850.0],
    ):
        stats = StatsThread(log, interval=60)
        stats.on_task_start()

        # Window ends mid-task: partial busy attributed here, the rest
        # to the next window.
        stats.compute_stats()

        stats.on_task_end()
        stats.compute_stats()

    assert log.info.mock_calls == [
        mock.call(
            "stats",
            time_total=30.0,
            time_busy=21.0,
            time_idle=0.0,
            time_overhead=9.0,
            utilization=70.0,
            occupancy=100.0,
        ),
        mock.call(
            "stats",
            time_total=20.0,
            time_busy=8.0,
            time_idle=0.0,
            time_overhead=12.0,
            utilization=40.0,
            occupancy=100.0,
        ),
    ]


def test_occupancy_is_zero_without_tasks_or_waits():
    log = mock.Mock()
    with mock.patch(
        "tasktiger.stats.time.monotonic",
        autospec=True,
        side_effect=[500.0, 520.0],
    ):
        stats = StatsThread(log, interval=60)

        stats.compute_stats()

    assert log.info.mock_calls == [
        mock.call(
            "stats",
            time_total=20.0,
            time_busy=0.0,
            time_idle=0.0,
            time_overhead=20.0,
            utilization=0.0,
            occupancy=0.0,
        )
    ]


def test_stats_consumer_defaults_are_noops():
    consumer = StatsConsumer()

    consumer.start()
    consumer.on_task_start()
    consumer.on_task_end()
    consumer.on_idle_start()
    consumer.on_idle_end()
    consumer.stop()
