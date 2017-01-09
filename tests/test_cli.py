import json
import shutil
import sqlite3
import tempfile

import click.testing
import tasktiger.cli

from .test_base import BaseTestCase
from .tasks import simple_task


class TestCLI(BaseTestCase):
    """Test TaskTiger CLI commands."""

    def test_queue_stats(self):
        runner = click.testing.CliRunner()
        runner.invoke(tasktiger.cli.cli, args=['queue_stats'], obj=self.tiger)

    def test_dump_queue(self):
        db_file = '%s/clitest.db' % self.test_dir

        # Queue task
        task = self.tiger.delay(simple_task, queue='cli')

        self._ensure_queues(queued={'cli': 1})

        # Dump queue to sqlite3 file
        runner = click.testing.CliRunner()
        runner.invoke(tasktiger.cli.cli, args=['dump_queue', '-f', db_file, '-q', 'cli', '-s', 'queued'],
                      obj=self.tiger)

        # Verify contents of tasks table
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor().execute('select id, data from tasks where id=?', (task.id, ))
        row = cursor.fetchone()
        assert row is not None
        task_json = json.loads(row[1])

        assert row[0] == task.id
        assert task_json['func'] == 'tests.tasks.simple_task'
        self._ensure_queues(queued={'cli': 0})

        cursor.close()
        conn.close()

    def test_sample_queue(self):
        db_file = '%s/clitest.db' % self.test_dir

        # Queue task
        task = self.tiger.delay(simple_task, queue='clisample')

        # Dump queue to sqlite3 file
        runner = click.testing.CliRunner()
        runner.invoke(tasktiger.cli.cli, args=['sample_queue', '-f', db_file, '-q', 'clisample', '-s', 'queued'],
                      obj=self.tiger)
        # Verify contents of tasks table
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor().execute('select id, data from tasks where id=?', (task.id, ))
        row = cursor.fetchone()
        assert row is not None
        task_json = json.loads(row[1])

        assert row[0] == task.id
        assert task_json['func'] == 'tests.tasks.simple_task'

        # This is the important one to confirm the task was not removed from the queue for a sample
        self._ensure_queues(queued={'clisample': 1})

        cursor.close()
        conn.close()
