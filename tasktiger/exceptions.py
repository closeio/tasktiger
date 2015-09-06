class TaskImportError(ImportError):
    """
    Raised when a task could not be imported.
    """

class JobTimeoutException(Exception):
    """
    Raised when a job takes longer to complete than the allowed maximum timeout
    value.
    """

class StopRetry(Exception):
    """
    Raised by a retry function to indicate that the task shouldn't be retried.
    """
