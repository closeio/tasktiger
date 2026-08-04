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
        side_effect=[1000.0, 1003.0, 1011.0, 1012.0, 1018.0, 1032.0],
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
            time_idle=8.0,
            time_overhead=18.0,
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
            occupancy=30.0,
        ),
        mock.call(
            "stats",
            time_total=20.0,
            time_busy=0.0,
            time_idle=4.0,
            time_overhead=16.0,
            utilization=0.0,
            occupancy=80.0,
        ),
    ]


def test_stats_consumer_defaults_are_noops():
    consumer = StatsConsumer()

    consumer.start()
    consumer.on_task_start()
    consumer.on_task_end()
    consumer.on_idle_start()
    consumer.on_idle_end()
    consumer.stop()
