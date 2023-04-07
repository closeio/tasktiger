# The retry logic is documented in the README.
from .exceptions import StopRetry
from .types import RetryStrategy


def _fixed(retry: int, delay: float, max_retries: int) -> float:
    if retry > max_retries:
        raise StopRetry()
    return delay


def fixed(delay: float, max_retries: int) -> RetryStrategy:
    return (_fixed, (delay, max_retries))


def _linear(
    retry: int, delay: float, increment: float, max_retries: int
) -> float:
    if retry > max_retries:
        raise StopRetry()
    return delay + increment * (retry - 1)


def linear(delay: float, increment: float, max_retries: int) -> RetryStrategy:
    return (_linear, (delay, increment, max_retries))


def _exponential(
    retry: int, delay: float, factor: float, max_retries: int
) -> float:
    if retry > max_retries:
        raise StopRetry()
    return delay * factor ** (retry - 1)


def exponential(
    delay: float, factor: float, max_retries: int
) -> RetryStrategy:
    return (_exponential, (delay, factor, max_retries))
