import pytest

from tasktiger import Task, TaskNotFound

from .tasks import simple_task
from .utils import get_tiger


class TestTaskFromId:
    @pytest.fixture
    def tiger(self):
        return get_tiger()

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
