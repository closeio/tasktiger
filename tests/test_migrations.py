import uuid

import pytest

from tasktiger.migrations import migrate_executions_count

from .test_base import BaseTestCase


class TestMigrateExecutionsCount(BaseTestCase):
    def test_migrate_nothing(self):
        migrate_executions_count(self.tiger)
        assert self.conn.keys() == []

    @pytest.mark.parametrize(
        "key",
        [
            f"foot:task:{uuid.uuid4()}:executions",
            f"foo:t:task:{uuid.uuid4()}:executions",
            f"t:task:{uuid.uuid4()}:executionsfoo",
            f"t:task:{uuid.uuid4()}:executions:foo",
            f"t:task:{uuid.uuid4()}",
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
            self.conn.rpush(f"t:task:{task_id_1}:executions", "{}")

        for __ in range(35):
            self.conn.rpush(f"t:task:{task_id_2}:executions", "{}")

        migrate_executions_count(self.tiger)
        assert self.conn.get(f"t:task:{task_id_1}:executions_count") == "73"
        assert self.conn.get(f"t:task:{task_id_2}:executions_count") == "35"

    def test_migrate_when_some_tasks_already_migrated(self):
        task_id_1 = uuid.uuid4()
        task_id_2 = uuid.uuid4()

        for __ in range(73):
            self.conn.rpush(f"t:task:{task_id_1}:executions", "{}")

        self.conn.set(f"t:task:{task_id_1}:executions_count", 91)

        for __ in range(35):
            self.conn.rpush(f"t:task:{task_id_2}:executions", "{}")

        migrate_executions_count(self.tiger)
        assert self.conn.get(f"t:task:{task_id_2}:executions_count") == "35"

        # looks migrated already - left untouched
        assert self.conn.get(f"t:task:{task_id_1}:executions_count") == "91"

    def test_migrate_when_counter_is_behind(self):
        task_id_1 = uuid.uuid4()
        task_id_2 = uuid.uuid4()

        for __ in range(73):
            self.conn.rpush(f"t:task:{task_id_1}:executions", "{}")

        self.conn.set(f"t:task:{task_id_1}:executions_count", 10)

        for __ in range(35):
            self.conn.rpush(f"t:task:{task_id_2}:executions", "{}")

        migrate_executions_count(self.tiger)
        assert self.conn.get(f"t:task:{task_id_2}:executions_count") == "35"

        # updated because the counter value was less than the actual count
        assert self.conn.get(f"t:task:{task_id_1}:executions_count") == "73"
