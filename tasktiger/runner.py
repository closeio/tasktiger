from ._internal import import_attribute
from .exceptions import TaskImportError


class BaseRunner:
    def __init__(self, tiger):
        self.tiger = tiger

    def run_single_task(self, task):
        raise NotImplementedError("Single tasks are not supported.")

    def run_batch_tasks(self, tasks):
        raise NotImplementedError("Batch tasks are not supported.")


class DefaultRunner(BaseRunner):
    def run_single_task(self, task):
        task.func(*task.args, **task.kwargs)

    def run_batch_tasks(self, tasks):
        params = [{'args': task.args, 'kwargs': task.kwargs} for task in tasks]
        func = tasks[0].func
        func(params)


def get_runner_class(log, tasks):
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
            runner_class = import_attribute(runner_class_path)
        except TaskImportError:
            log.error('could not import runner class', func=retry_func)
        return runner_class
    return DefaultRunner
