from .task import Task
from .worker import Worker

__all__ = ["TaskTigerTestMixin"]


class TaskTigerTestMixin:
    """
    Unit test mixin for tests that use TaskTiger.
    """

    def run_worker(self, tiger, raise_on_errors=True, **kwargs):
        # A worker run processes queued tasks, and then queues scheduled tasks.
        # We therefore need to run the worker twice to execute due scheduled
        # tasks.
        Worker(tiger, **kwargs).run(once=True)
        Worker(tiger, **kwargs).run(once=True)

        # Print any TaskTiger failures for debugging purposes.
        prefix = tiger.config["REDIS_PREFIX"]
        state = "error"
        has_errors = False
        for queue in tiger.connection.smembers("{}:{}".format(prefix, state)):
            n_tasks, tasks = Task.tasks_from_queue(
                tiger, queue, state, load_executions=1
            )
            for task in tasks:
                print("")
                print(task, "failed:")
                print(task.executions[0]["traceback"])
                has_errors = True
        if has_errors and raise_on_errors:
            raise Exception("One or more tasks have failed.")
