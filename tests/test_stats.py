import time
from unittest import mock

import pytest

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
