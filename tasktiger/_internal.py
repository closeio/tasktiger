import binascii
import calendar
import datetime
import hashlib
import importlib
import json
import operator
import os
import threading

from .exceptions import TaskImportError

# Task states (represented by different queues)
# Note some client code may rely on the string values (e.g. get_queue_stats).
QUEUED = "queued"
ACTIVE = "active"
SCHEDULED = "scheduled"
ERROR = "error"

# This lock is acquired in the main process when forking, and must be acquired
# in any thread of the main process when performing an operation that triggers a
# lock that a child process might want to acquire.
#
# Specifically, we use this lock when logging in the StatsThread to prevent a
# deadlock in the child process when the child is forked while the stats thread
# is logging a message. This issue happens because Python acquires a lock while
# logging, so a child process could be stuck forever trying to acquire that
# lock. See http://bugs.python.org/issue6721 for more details.
g_fork_lock = threading.Lock()

# Global task context. We store this globally (and not on the TaskTiger
# instance) for consistent results just in case the user has multiple TaskTiger
# instances.
g = {"tiger": None, "current_task_is_batch": None, "current_tasks": None}


# from rq
def import_attribute(name):
    """Return an attribute from a dotted path name (e.g. "path.to.func")."""
    try:
        sep = ":" if ":" in name else "."  # For backwards compatibility
        module_name, attribute = name.rsplit(sep, 1)
        module = importlib.import_module(module_name)
        return operator.attrgetter(attribute)(module)
    except (ValueError, ImportError, AttributeError) as e:
        raise TaskImportError(e)


def gen_id():
    """
    Generates and returns a random hex-encoded 256-bit unique ID.
    """
    return binascii.b2a_hex(os.urandom(32)).decode("utf8")


def gen_unique_id(serialized_name, args, kwargs):
    """
    Generates and returns a hex-encoded 256-bit ID for the given task name and
    args. Used to generate IDs for unique tasks or for task locks.
    """
    return hashlib.sha256(
        json.dumps(
            {"func": serialized_name, "args": args, "kwargs": kwargs},
            sort_keys=True,
        ).encode("utf8")
    ).hexdigest()


def serialize_func_name(func):
    """
    Returns the dotted serialized path to the passed function.
    """
    if func.__module__ == "__main__":
        raise ValueError(
            "Functions from the __main__ module cannot be processed by "
            "workers."
        )
    try:
        # This will only work on Python 3.3 or above, but it will allow us to use static/classmethods
        func_name = func.__qualname__
    except AttributeError:
        func_name = func.__name__
    return ":".join([func.__module__, func_name])


def dotted_parts(s):
    """
    For a string "a.b.c", yields "a", "a.b", "a.b.c".
    """
    idx = -1
    while s:
        idx = s.find(".", idx + 1)
        if idx == -1:
            yield s
            break
        yield s[:idx]


def reversed_dotted_parts(s):
    """
    For a string "a.b.c", yields "a.b.c", "a.b", "a".
    """
    idx = -1
    if s:
        yield s
    while s:
        idx = s.rfind(".", 0, idx)
        if idx == -1:
            break
        yield s[:idx]


def serialize_retry_method(retry_method):
    if callable(retry_method):
        return (serialize_func_name(retry_method), ())
    else:
        return (serialize_func_name(retry_method[0]), retry_method[1])


def get_timestamp(when):
    # convert timedelta to datetime
    if isinstance(when, datetime.timedelta):
        when = datetime.datetime.utcnow() + when

    if when:
        # Convert to unixtime: utctimetuple drops microseconds so we add
        # them manually.
        return calendar.timegm(when.utctimetuple()) + when.microsecond / 1.0e6


def queue_matches(queue, only_queues=None, exclude_queues=None):
    """Checks if the given queue matches against only/exclude constraints

    Returns whether the given queue should be included by checking each part of
    the queue name.

    :param str queue: The queue name to check
    :param iterable(str) only_queues: Limit to only these queues
    :param iterable(str) exclude_queues: Specifically excluded queues

    :returns: A boolean indicating whether this queue matches against the given
        ``only`` and ``excludes`` constraints
    """
    # Check arguments to prevent a common footgun of passing 'my_queue' instead
    # of ``['my_queue']``
    error_template = (
        "{kwarg} should be an iterable of strings, not a string directly. "
        "Did you mean `{kwarg}=['{val}']`?"
    )
    assert not isinstance(only_queues, str), error_template.format(
        kwarg="queues", val=only_queues
    )
    assert not isinstance(exclude_queues, str), error_template.format(
        kwarg="exclude_queues", val=exclude_queues
    )

    only_queues = only_queues or []
    exclude_queues = exclude_queues or []
    for part in reversed_dotted_parts(queue):
        if part in exclude_queues:
            return False
        if part in only_queues:
            return True
    return not only_queues


class classproperty(property):
    """
    Simple class property implementation.

    Works like @property but on classes.
    """

    def __get__(desc, self, cls):
        return desc.fget(cls)
