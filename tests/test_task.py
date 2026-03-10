import datetime

import pytest
from freezefrog import FreezeTime

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


class TestScheduledAt:
    FROZEN_NOW = datetime.datetime(2024, 1, 1, 12, 0, 0)

    def test_immediate_task_scheduled_at_equals_queue_time(self, tiger):
        with FreezeTime(self.FROZEN_NOW):
            task = tiger.delay(simple_task)
        assert task.scheduled_at == self.FROZEN_NOW

    def test_future_task_scheduled_at_equals_when(self, tiger):
        future = datetime.timedelta(minutes=5)
        with FreezeTime(self.FROZEN_NOW):
            task = tiger.delay(simple_task, when=future)
        assert task.scheduled_at == self.FROZEN_NOW + future

    def test_scheduled_at_survives_scheduled_to_queued_transition(self, tiger):
        future = datetime.timedelta(minutes=5)
        with FreezeTime(self.FROZEN_NOW):
            task = tiger.delay(simple_task, when=future)
        expected = self.FROZEN_NOW + future

        task._move(from_state="scheduled", to_state="queued")
        reloaded = Task.from_id(tiger, task.queue, "queued", task.id)
        assert reloaded.scheduled_at == expected

    def test_scheduled_at_persists_after_reload(self, tiger):
        with FreezeTime(self.FROZEN_NOW):
            task = tiger.delay(simple_task)
        reloaded = Task.from_id(tiger, task.queue, "queued", task.id)
        assert reloaded.scheduled_at == self.FROZEN_NOW

    def test_scheduled_at_none_for_unqueued_task(self, tiger):
        task = Task(tiger, simple_task)
        assert task.scheduled_at is None

    def test_update_scheduled_time_updates_scheduled_at(self, tiger):
        future = datetime.timedelta(minutes=5)
        later = datetime.timedelta(minutes=10)
        with FreezeTime(self.FROZEN_NOW):
            task = tiger.delay(simple_task, when=future)
        assert task.scheduled_at == self.FROZEN_NOW + future

        new_when = self.FROZEN_NOW + later
        task.update_scheduled_time(when=new_when)

        reloaded = Task.from_id(tiger, task.queue, "scheduled", task.id)
        assert reloaded.scheduled_at == new_when
