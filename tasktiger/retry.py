# The retry logic is documented in the delay() method.
from .exceptions import StopRetry

def _fixed(retry, delay, max_retries):
    if retry > max_retries:
        raise StopRetry()
    return delay

def fixed(delay, max_retries):
    return (_fixed, (delay, max_retries))

def _linear(retry, delay, increment, max_retries):
    if retry > max_retries:
        raise StopRetry()
    return delay + increment*(retry-1)

def linear(delay, increment, max_retries):
    return (_linear, (delay, increment, max_retries))

def _exponential(retry, delay, factor, max_retries):
    if retry > max_retries:
        raise StopRetry()
    return delay * factor**(retry-1)

def exponential(delay, factor, max_retries):
    return (_exponential, (delay, factor, max_retries))
