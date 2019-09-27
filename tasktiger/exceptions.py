import sys


class TaskImportError(ImportError):
    """
    Raised when a task could not be imported.
    """


class JobTimeoutException(BaseException):
    """
    Raised when a job takes longer to complete than the allowed maximum timeout
    value.
    """


class QueueFullException(BaseException):
    """
    Raised when a task is attempted to be queued using max_queue_size and the
    total queue size (QUEUED + SCHEDULED + ACTIVE) is greater than or equal to
    max_queue_size.
    """


class StopRetry(Exception):
    """
    Raised by a retry function to indicate that the task shouldn't be retried.
    """


class RetryException(BaseException):
    """
    Alternative to retry_on for retrying a task. If raised within a task, the
    task will be retried as long as the retry method permits. The default retry
    method (specified in the task or in DEFAULT_RETRY_METHOD) may be overridden
    using the method argument. If original_traceback is True and RetryException
    is raised from within an except block, the original traceback will be
    logged. If log_error is set to False and the task fails permanently, a
    warning will be logged instead of an error, and the task will be removed
    from Redis when it completes.
    """

    def __init__(self, method=None, original_traceback=False, log_error=True):
        self.method = method
        self.exc_info = sys.exc_info() if original_traceback else None
        self.log_error = log_error


class TaskNotFound(Exception):
    """
    The task was not found or does not exist in the given queue/state.
    """
