from typing import TYPE_CHECKING, Any, Dict, List, Type

from structlog.stdlib import BoundLogger

from ._internal import import_attribute
from .exceptions import TaskImportError
from .timeouts import UnixSignalDeathPenalty

if TYPE_CHECKING:
    from . import Task, TaskTiger


class BaseRunner:
    """
    Base implementation of the task runner.
    """

    def __init__(self, tiger: "TaskTiger"):
        self.tiger = tiger

    def run_single_task(self, task: "Task", hard_timeout: float) -> None:
        """
        Run the given task using the hard timeout in seconds.

        This is called inside of the forked process.
        """
        raise NotImplementedError("Single tasks are not supported.")

    def run_batch_tasks(
        self, tasks: List["Task"], hard_timeout: float
    ) -> None:
        """
        Run the given tasks using the hard timeout in seconds.

        This is called inside of the forked process.
        """
        raise NotImplementedError("Batch tasks are not supported.")

    def run_eager_task(self, task: "Task") -> None:
        """
        Run the task eagerly and return the value.

        Note that the task function could be a batch function.
        """
        raise NotImplementedError("Eager tasks are not supported.")

    def on_permanent_error(
        self, task: "Task", execution: Dict[str, Any]
    ) -> None:
        """
        Called if the task fails permanently.

        A task fails permanently if its status is set to ERROR and it is no
        longer retried.

        This is called in the main worker process.
        """


class DefaultRunner(BaseRunner):
    """
    Default implementation of the task runner.
    """

    def run_single_task(self, task: "Task", hard_timeout: float) -> None:
        with UnixSignalDeathPenalty(hard_timeout):
            task.func(*task.args, **task.kwargs)

    def run_batch_tasks(
        self, tasks: List["Task"], hard_timeout: float
    ) -> None:
        params = [{"args": task.args, "kwargs": task.kwargs} for task in tasks]
        func = tasks[0].func
        with UnixSignalDeathPenalty(hard_timeout):
            func(params)

    def run_eager_task(self, task: "Task") -> None:
        func = task.func
        is_batch_func = getattr(func, "_task_batch", False)

        if is_batch_func:
            return func([{"args": task.args, "kwargs": task.kwargs}])
        else:
            return func(*task.args, **task.kwargs)


def get_runner_class(
    log: BoundLogger, tasks: List["Task"]
) -> Type[BaseRunner]:
    runner_class_paths = {task.serialized_runner_class for task in tasks}
    if len(runner_class_paths) > 1:
        log.error(
            "cannot mix multiple runner classes",
            runner_class_paths=", ".join(str(p) for p in runner_class_paths),
        )
        raise ValueError("Found multiple runner classes in batch task.")

    runner_class_path = runner_class_paths.pop()
    if runner_class_path:
        try:
            return import_attribute(runner_class_path)
        except TaskImportError:
            log.error(
                "could not import runner class",
                runner_class_path=runner_class_path,
            )
            raise
    return DefaultRunner
