import uuid

import pytest

from tasktiger.constants import (
    EXECUTIONS,
    EXECUTIONS_COUNT,
    REDIS_PREFIX,
    TASK,
)
from tasktiger.migrations import migrate_executions_count

from .test_base import BaseTestCase


class TestMigrateExecutionsCount(BaseTestCase):
    def test_migrate_nothing(self):
        migrate_executions_count(self.tiger)
        assert self.conn.keys() == []

    @pytest.mark.parametrize(
        "key",
        [
            f"foot:{TASK}:{uuid.uuid4()}:{EXECUTIONS}",
            f"foo:{REDIS_PREFIX}:{TASK}:{uuid.uuid4()}:{EXECUTIONS}",
            f"{REDIS_PREFIX}:{TASK}:{uuid.uuid4()}:executionsfoo",
            f"{REDIS_PREFIX}:{TASK}:{uuid.uuid4()}:{EXECUTIONS}:foo",
            f"{REDIS_PREFIX}:{TASK}:{uuid.uuid4()}",
        ],
    )
    def test_migrate_ignores_irrelevant_keys(self, key):
        self.conn.rpush(key, "{}")
        migrate_executions_count(self.tiger)

        assert self.conn.keys() == [key]

    def test_migrate(self):
        task_id_1 = uuid.uuid4()
        task_id_2 = uuid.uuid4()

        for __ in range(73):
            self.conn.rpush(
                f"{REDIS_PREFIX}:{TASK}:{task_id_1}:{EXECUTIONS}", "{}"
            )

        for __ in range(35):
            self.conn.rpush(
                f"{REDIS_PREFIX}:{TASK}:{task_id_2}:{EXECUTIONS}", "{}"
            )

        migrate_executions_count(self.tiger)
        assert (
            self.conn.get(
                f"{REDIS_PREFIX}:{TASK}:{task_id_1}:{EXECUTIONS_COUNT}"
            )
            == "73"
        )
        assert (
            self.conn.get(
                f"{REDIS_PREFIX}:{TASK}:{task_id_2}:{EXECUTIONS_COUNT}"
            )
            == "35"
        )

    def test_migrate_when_some_tasks_already_migrated(self):
        task_id_1 = uuid.uuid4()
        task_id_2 = uuid.uuid4()

        for __ in range(73):
            self.conn.rpush(
                f"{REDIS_PREFIX}:{TASK}:{task_id_1}:{EXECUTIONS}", "{}"
            )

        self.conn.set(
            f"{REDIS_PREFIX}:{TASK}:{task_id_1}:{EXECUTIONS_COUNT}", 91
        )

        for __ in range(35):
            self.conn.rpush(
                f"{REDIS_PREFIX}:{TASK}:{task_id_2}:{EXECUTIONS}", "{}"
            )

        migrate_executions_count(self.tiger)
        assert (
            self.conn.get(
                f"{REDIS_PREFIX}:{TASK}:{task_id_2}:{EXECUTIONS_COUNT}"
            )
            == "35"
        )

        # looks migrated already - left untouched
        assert (
            self.conn.get(
                f"{REDIS_PREFIX}:{TASK}:{task_id_1}:{EXECUTIONS_COUNT}"
            )
            == "91"
        )

    def test_migrate_when_counter_is_behind(self):
        task_id_1 = uuid.uuid4()
        task_id_2 = uuid.uuid4()

        for __ in range(73):
            self.conn.rpush(
                f"{REDIS_PREFIX}:{TASK}:{task_id_1}:{EXECUTIONS}", "{}"
            )

        self.conn.set(
            f"{REDIS_PREFIX}:{TASK}:{task_id_1}:{EXECUTIONS_COUNT}", 10
        )

        for __ in range(35):
            self.conn.rpush(
                f"{REDIS_PREFIX}:{TASK}:{task_id_2}:{EXECUTIONS}", "{}"
            )

        migrate_executions_count(self.tiger)
        assert (
            self.conn.get(
                f"{REDIS_PREFIX}:{TASK}:{task_id_2}:{EXECUTIONS_COUNT}"
            )
            == "35"
        )

        # updated because the counter value was less than the actual count
        assert (
            self.conn.get(
                f"{REDIS_PREFIX}:{TASK}:{task_id_1}:{EXECUTIONS_COUNT}"
            )
            == "73"
        )
