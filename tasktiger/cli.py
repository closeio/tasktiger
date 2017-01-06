"""TaskTiger CLI."""

import argparse
import datetime
import json
import signal
import sqlite3

from ._internal import QUEUED, ACTIVE, SCHEDULED, ERROR
from .exceptions import TaskNotFound
from .task import Task


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
        dq_parser.add_argument('-s', '--state', help='Task state (default=%s)' % QUEUED,
                               required=False, choices=[QUEUED, SCHEDULED], default=QUEUED)
        dq_parser.add_argument('-b', '--batches', help='Number of batches to dump, defaults to all tasks',
                               required=False, default=-1)

        purge_group = dq_parser.add_mutually_exclusive_group(required=True)
        purge_group.add_argument('-f', '--file', help='Sqlite file name')
        purge_group.add_argument('-p', '--purge', help='Purge tasks without saving them to Sqlite database',
                                 action='store_true')

        # Sample queue command
        sq_parser = subparsers.add_parser('sample_queue',
                                          help='Dump one batch of tasks to sqlite database. Do not delete any tasks from queue.',
                                          parents=[common_parser])
        sq_parser.add_argument('-q', '--queue', help='Queue name', required=True)
        sq_parser.add_argument('-f', '--file', help='Sqlite file name', required=True)
        sq_parser.add_argument('-s', '--state', help='Task state (default=%s)' % QUEUED,
                               required=False, choices=[QUEUED, SCHEDULED, ACTIVE, ERROR], default=QUEUED)

        # Load queue command
        sq_parser = subparsers.add_parser('load_queue',
                                          help='Load queue from sqlite database.', parents=[common_parser])
        sq_parser.add_argument('-q', '--queue', help='Queue name', required=True)
        sq_parser.add_argument('-f', '--file', help='Sqlite file name', required=True)

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
            queue_dumper = QueueDumper(self.tiger, self.args.queue, self.args.state)
            if self.args.purge:
                queue_dumper.dump_queue(None, batches=self.args.batches)
            else:
                queue_dumper.dump_queue(self.args.file, batches=self.args.batches)
        elif self.command == 'sample_queue':
            queue_dumper = QueueDumper(self.tiger, self.args.queue, self.args.state)
            queue_dumper.dump_queue(self.args.file, sample=True)
        elif self.command == 'load_queue':
            self.load_queue()


class QueueDumper(object):
    """Dump tasks from tasktiger queue."""

    def __init__(self, tiger, queue, state):
        self.tiger = tiger
        self.queue = queue
        self.state = state

        self.db_filename = None
        self.conn = None
        self.sample = False
        self.batches = -1
        self.exit_dump = False   # Used to handle SIGINT
        self.tasks_not_found = 0

    def _close_db(self):
        """Write stats and close db."""

        if self.db_filename:
            self.conn.execute('INSERT INTO stats (info, data) values (?,?)', ('stop_time', datetime.datetime.utcnow()))
            if self.exit_dump:
                self.conn.execute('INSERT INTO stats (info, data) values (?,?)', ('broke', 'true'))
            self.conn.commit()
            self.conn.close()

    def _create_database(self):
        """Create Sqlite database."""

        if self.db_filename:
            self.conn = sqlite3.connect(self.db_filename)
            self.conn.execute('CREATE TABLE tasks (id text, data text)')

            self.conn.execute('CREATE TABLE stats (info text, data text)')
            self.conn.execute('INSERT INTO stats (info, data) values (?,?)', ('start_time', datetime.datetime.utcnow()))
            self.conn.execute('INSERT INTO stats (info, data) values (?,?)', ('queue', self.queue))
            self.conn.execute('INSERT INTO stats (info, data) values (?,?)', ('state', self.state))

            self.conn.commit()

    def _delete_task(self, task):
        """Delete task from queue returning True is successful."""

        # Try to delete task if this is not a sample run
        if not self.sample:
            try:
                task.delete(None)
            except TaskNotFound:
                # Assuming tasks are concurrently being processed they may disappear
                self.tasks_not_found += 1
                return False
        return True

    def _dump_batch(self, tasks):
        """Dump a batch of tasks."""

        dump_count = 0
        for task in tasks:
            task_json = json.dumps(task.data)
            try:
                if self.db_filename:
                    self.conn.execute('INSERT INTO tasks (id, data) values (?,?)', (task.id, task_json))

                tasked_deleted = self._delete_task(task)

                if tasked_deleted:
                    dump_count += 1
                    if self.db_filename:
                        self.conn.commit()
                elif self.db_filename:
                    # Don't save to sqlite if task was not deleted
                    self.conn.rollback()

            except Exception as exception:
                print('Error dumping task %s:%s' % (task.id, exception))
                if self.db_filename:
                    self.conn.rollback()
                raise

            # Clean exit on ctrl-c
            if self.exit_dump:
                break

        return dump_count

    def _signal_handler(self, signum, frame):
        self.exit_dump = True
        print("Caught ctrl-c, finishing up.")

    def dump_queue(self, db_filename, sample=False, batches=-1):
        """Dump tasks from queue

        Sample mode will dump 1 batch of Tasks but not delete any of them.
        """

        print('%s queue %s:%s to file %s' % ('Sampling' if sample else 'Dumping', self.queue, self.state, db_filename))

        self.db_filename = db_filename
        self.sample = sample
        self.batches = batches

        # Catch ctrl-c
        signal.signal(signal.SIGINT, self._signal_handler)

        self._create_database()

        dump_count = 0
        batch = 0

        # Keep processing batches of tasks
        n_tasks, tasks = Task.tasks_from_queue(self.tiger, self.queue, self.state, limit=1000)
        while n_tasks > 0:
            dump_count += self._dump_batch(tasks)
            batch += 1
            print('%s Dumped %d tasks' % (datetime.datetime.now(), dump_count))
            if self.sample or batch == int(self.batches) or self.exit_dump:
                break
            n_tasks, tasks = Task.tasks_from_queue(self.tiger, self.queue, self.state, limit=1000)

        self._close_db()

        print('Total tasks dumped:%d' % dump_count)
        print('Total tasks not found:%d' % self.tasks_not_found)
