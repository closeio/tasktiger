import pytest

from tasktiger import Task, TaskNotFound

from .tasks import simple_task
from .utils import get_tiger


@pytest.fixture
def tiger():
    return get_tiger()


class TestTaskFromId:
    @pytest.fixture
    def queued_task(self, tiger):
        return tiger.delay(simple_task)

    def test_task_found(self, tiger, queued_task):
        task = Task.from_id(tiger, "default", "queued", queued_task.id)
        assert queued_task.id == task.id

    def test_task_wrong_state(self, tiger, queued_task):
        with pytest.raises(TaskNotFound):
            Task.from_id(tiger, "default", "active", queued_task.id)

    def test_task_wrong_queue(self, tiger, queued_task):
        with pytest.raises(TaskNotFound):
            Task.from_id(tiger, "other", "active", queued_task.id)


class TestTaskMaxTrackedExecutions:
    def test_max_stored_executions_passed_to_tiger_delay(self, tiger):
        task = tiger.delay(simple_task, max_stored_executions=17)
        assert task.max_stored_executions == 17

    def test_max_stored_executions_passed_to_decorator(self, tiger):
        @tiger.task(max_stored_executions=17)
        def some_task():
            pass

        task = some_task.delay()
        assert task.max_stored_executions == 17

    def test_max_stored_executions_overridden_in_tiger_delay(self, tiger):
        @tiger.task(max_stored_executions=17)
        def some_task():
            pass

        task = tiger.delay(some_task, max_stored_executions=11)
        assert task.max_stored_executions == 11
