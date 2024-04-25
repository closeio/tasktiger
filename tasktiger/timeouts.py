import signal
from types import TracebackType
from typing import Any, Literal, Optional, Type

from .exceptions import JobTimeoutException


class BaseDeathPenalty:
    """Base class to setup job timeouts."""

    def __init__(self, timeout: float) -> None:
        self._timeout = timeout

    def __enter__(self) -> None:
        self.setup_death_penalty()

    def __exit__(
        self,
        type: Optional[Type[BaseException]],
        value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[False]:
        # Always cancel immediately, since we're done
        try:
            self.cancel_death_penalty()
        except JobTimeoutException:
            # Weird case: we're done with the with body, but now the alarm is
            # fired.  We may safely ignore this situation and consider the
            # body done.
            pass

        # __exit__ may return True to suppress further exception handling.  We
        # don't want to suppress any exceptions here, since all errors should
        # just pass through, JobTimeoutException being handled normally to the
        # invoking context.
        return False

    def setup_death_penalty(self) -> None:
        raise NotImplementedError()

    def cancel_death_penalty(self) -> None:
        raise NotImplementedError()


class UnixSignalDeathPenalty(BaseDeathPenalty):
    def handle_death_penalty(self, signum: int, frame: Any) -> None:
        raise JobTimeoutException(
            "Job exceeded maximum timeout "
            "value (%d seconds)." % self._timeout
        )

    def setup_death_penalty(self) -> None:
        """Sets up an alarm signal and a signal handler that raises
        a JobTimeoutException after the timeout amount (expressed in
        seconds).
        """
        signal.signal(signal.SIGALRM, self.handle_death_penalty)
        signal.setitimer(signal.ITIMER_REAL, self._timeout)

    def cancel_death_penalty(self) -> None:
        """Removes the death penalty alarm and puts back the system into
        default signal handling.
        """
        signal.alarm(0)
        signal.signal(signal.SIGALRM, signal.SIG_DFL)
