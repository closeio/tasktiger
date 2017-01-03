"""TaskTiger CLI."""

import argparse
import datetime
import json
import sqlite3
import sys

from ._internal import QUEUED, ACTIVE, SCHEDULED, ERROR
from .task import Task

MAX_ERRORS = 1000

class TaskTigerCLI(object):
    """TaskTiger CLI class."""

    def __init__(self, tiger, args):
        self.tiger = tiger
        self._parse_args(args)

    def _parse_args(self, args):
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers(dest='command')

        common_parser = argparse.ArgumentParser(add_help=False)

        # Queue stats command
        subparsers.add_parser('queue_stats', help='Print queue statistics.',
                              parents=[common_parser])

        # Dump queue command
        dq_parser = subparsers.add_parser('dump_queue', help='Dump tasks to sqlite database and clear queue.',
                                          parents=[common_parser])
        dq_parser.add_argument('-q', '--queue', help='Queue name', required=True)
        dq_parser.add_argument('-f', '--file', help='Sqlite3 file name', required=True)
        dq_parser.add_argument('-s', '--state', help='Task state (default=queued)',
                               required=False,  choices=[QUEUED, SCHEDULED], default='queued')
        dq_parser.add_argument('-b', '--batches', help='Number of batches to dump, defaults to all tasks',
                               required=False, default=-1)

        # Sample queue command
        sq_parser = subparsers.add_parser('sample_queue',
                                          help='Dump 1000 tasks to sqlite database. Do not delete any tasks.',
                                          parents=[common_parser])
        sq_parser.add_argument('-q', '--queue', help='Queue name', required=True)
        sq_parser.add_argument('-f', '--file', help='Sqlite3 file name', required=True)
        sq_parser.add_argument('-s', '--state', help='Task state (default=queued)',
                               required=False, choices=[QUEUED, SCHEDULED, ACTIVE, ERROR], default='queued')

        # Load queue command
        sq_parser = subparsers.add_parser('load_queue',
                                          help='Load queue from sqlite database.', parents=[common_parser])
        sq_parser.add_argument('-q', '--queue', help='Queue name', required=True)
        sq_parser.add_argument('-f', '--file', help='Sqlite3 file name', required=True)

        # Parse arguments
        self.args = parser.parse_args(args)
        self.command = self.args.command

    def _queue_stats(self):
        """Get queue statistics."""

        queue_stats = self.tiger.get_queue_stats()

        print('{:^64} {:^8s} {:^8s} {:^8s} {:^8s}'.format('Name', QUEUED, ACTIVE, SCHEDULED, ERROR))
        for queue in sorted(queue_stats):
            print('{:>64} {:8d} {:8d} {:8d} {:8d}'.format(queue,
                                                          queue_stats[queue].get(QUEUED) or 0,
                                                          queue_stats[queue].get(ACTIVE) or 0,
                                                          queue_stats[queue].get(SCHEDULED) or 0,
                                                          queue_stats[queue].get(ERROR) or 0))

    def dump_queue(self, sample=False):
        """Dump tasks from queue to sqlite3 database

        Sample mode will dump 1 batch of Tasks but not delete any of them.
        """

        print('%s queue:%s to file:%s' % ('Sampling' if sample else 'Dumping', self.args.queue, self.args.file))

        conn = sqlite3.connect(self.args.file)
        conn.execute('CREATE TABLE tasks (id text, data text)')
        conn.commit()

        dump_count = 0
        batch = 0
        error_count = 0

        # Keep processing batches of tasks
        n_tasks, tasks = Task.tasks_from_queue(self.tiger, self.args.queue, self.args.state, limit=1000)
        while n_tasks > 0 and error_count < MAX_ERRORS:
            for task in tasks:
                task_json = json.dumps(task.data)
                try:
                    conn.execute('INSERT INTO tasks (id, data) values (?,?)', (task.id, task_json))
                    # Delete task
                    if not sample:
                        task._move()
                    conn.commit()
                    dump_count += 1
                except Exception as exception:
                    error_count += 1
                    print('Error dumping task %s:%s' % (task.id, exception))
                    conn.rollback()

            batch += 1
            print('%s Dumped %d tasks' % (datetime.datetime.now(), dump_count))
            if sample or batch == int(self.args.batches):
                break
            n_tasks, tasks = Task.tasks_from_queue(self.tiger, self.args.queue, self.args.state, limit=1000)

        conn.close()

    def load_queue(self):
        """Load queue from db file."""
        conn = sqlite3.connect(self.args.file)

        cursor = conn.cursor().execute('select count(*) from tasks')
        row = cursor.fetchone()

        print('Loading %d tasks' % row[0])

        cursor = conn.cursor().execute('select ROWID, id, data from tasks order by ROWID')

        row = cursor.fetchone()
        while row:
            print(row[1])
            # TODO: Load task back into queue
            row = cursor.fetchone()
        cursor.close()
        conn.close()

    def run(self):
        """Run CLI command."""

        if self.command == 'queue_stats':
            self._queue_stats()
        elif self.command == 'dump_queue':
            self.dump_queue()
        elif self.command == 'sample_queue':
            self.dump_queue(sample=True)
        elif self.command == 'load_queue':
            self.load_queue()
