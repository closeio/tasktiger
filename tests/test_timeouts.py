import signal
from unittest.mock import Mock

import pytest

from tasktiger.exceptions import JobTimeoutException
from tasktiger.timeouts import UnixSignalDeathPenalty


def test_unix_signal_death_penalty_retries_before_exiting() -> None:
    death_penalty = UnixSignalDeathPenalty(timeout=10)

    # Raise the regular timeout on the initial delivery and the first two
    # interval retries.
    for expected_retries in range(1, 4):
        with pytest.raises(JobTimeoutException):
            death_penalty.handle_death_penalty(signal.SIGALRM, None)
        assert death_penalty.retries == expected_retries

    # If all three deliveries failed to unwind the job, force the process to
    # exit when the interval timer delivers the third retry.
    with pytest.raises(SystemExit) as exc_info:
        death_penalty.handle_death_penalty(signal.SIGALRM, None)

    assert exc_info.value.code == 1


def test_unix_signal_death_penalty_uses_interval_timer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    signal_mock = Mock()
    setitimer_mock = Mock()
    monkeypatch.setattr(signal, "signal", signal_mock)
    monkeypatch.setattr(signal, "setitimer", setitimer_mock)

    death_penalty = UnixSignalDeathPenalty(timeout=10)
    death_penalty.setup_death_penalty()

    signal_mock.assert_called_once_with(
        signal.SIGALRM, death_penalty.handle_death_penalty
    )
    setitimer_mock.assert_called_once_with(signal.ITIMER_REAL, 10, 5)
