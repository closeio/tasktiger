import json
import shutil
import sqlite3
import tempfile

from .test_base import BaseTestCase
from tasktiger.cli import TaskTigerCLI

from .tasks import simple_task


class TestCLI(BaseTestCase):
    """Test TaskTiger CLI commands."""

    def setUp(self):
        super(TestCLI, self).setUp()
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        super(TestCLI, self).tearDown()
        shutil.rmtree(self.test_dir)

    def test_queue_stats(self):
        cli = TaskTigerCLI(self.tiger, ['queue_stats'])
        cli.run()

    def test_dump_queue(self):
        db_file = '%s/clitest.db' % self.test_dir

        # Queue task
        task = self.tiger.delay(simple_task, queue='cli')

        self._ensure_queues(queued={'cli': 1})

        # Dump queue to sqlite3 file
        cli = TaskTigerCLI(self.tiger, ['dump_queue', '-f', db_file, '-q', 'cli', '-s', 'queued'])
        cli.run()

        # Verify contents of tasks table
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor().execute('select id, data from tasks where id=?', (task.id, ))
        row = cursor.fetchone()
        self.assertIsNotNone(row)
        task_json = json.loads(row[1])

        self.assertEqual(row[0], task.id)
        self.assertEqual(task_json['func'], 'tests.tasks.simple_task')
        self._ensure_queues(queued={'cli': 0})

        cursor.close()
        conn.close()
