"""TaskTiger CLI."""

import datetime
import json
import os.path
import signal
import sqlite3

import click
from ._internal import QUEUED, ACTIVE, SCHEDULED, ERROR
from .exceptions import TaskNotFound
from .task import Task
from .click_helper import MutuallyExclusiveOption


class QueueLoader(object):
    def __init__(self, tiger, queue):
        self.tiger = tiger
        self.queue = queue

    def load_queue(self, filename):
        """Load queue from db file."""
        conn = sqlite3.connect(filename)

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
            self.conn.commit()

            # Add dump info to stats table
            self.conn.execute('CREATE TABLE stats (info text, data text)')
            self.conn.execute('INSERT INTO stats (info, data) values (?,?)', ('start_time', datetime.datetime.utcnow()))
            self.conn.execute('INSERT INTO stats (info, data) values (?,?)', ('queue', self.queue))
            self.conn.execute('INSERT INTO stats (info, data) values (?,?)', ('state', self.state))
            self.conn.execute('INSERT INTO stats (info, data) values (?,?)', ('sample', str(self.sample)))
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

        if self.db_filename and os.path.exists(self.db_filename):
            raise ValueError('File %s already exists' % self.db_filename)

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


def run_with_args(tiger, args):
    cli(args=args, obj=tiger)


@click.group()
def cli():
    pass


@click.command()
@click.pass_context
def queue_stats(context):
    queue_stats_array = context.obj.get_queue_stats()

    print('{:^64} {:^8s} {:^8s} {:^8s} {:^8s}'.format('Name', QUEUED, ACTIVE, SCHEDULED, ERROR))
    for queue in sorted(queue_stats_array):
        print('{:>64} {:8d} {:8d} {:8d} {:8d}'.format(queue,
                                                      queue_stats_array[queue].get(QUEUED) or 0,
                                                      queue_stats_array[queue].get(ACTIVE) or 0,
                                                      queue_stats_array[queue].get(SCHEDULED) or 0,
                                                      queue_stats_array[queue].get(ERROR) or 0))


dump_mutex_group = ["filename", "purge"]
@click.command()
@click.option('--queue', '-q', help='Queue name', required=True)
@click.option('--state', '-s', help='Task state (default=%s)' % QUEUED,
              required=False, type=click.Choice([QUEUED, SCHEDULED, ERROR]), default=QUEUED)
@click.option('--batches', '-b', help='Number of batches to dump, defaults to all tasks',
              required=False, default=-1)
@click.option('-f', '--filename', cls=MutuallyExclusiveOption, mutex_group=dump_mutex_group,
              help='Sqlite file name')
@click.option('-p', '--purge', cls=MutuallyExclusiveOption, mutex_group=dump_mutex_group,
              help='Purge tasks without saving them to Sqlite database', is_flag=True)
@click.pass_context
def dump_queue(context, queue, state, batches, filename, purge):
    queue_dumper = QueueDumper(context.obj, queue, state)
    if purge:
        queue_dumper.dump_queue(None, batches=batches)
    else:
        queue_dumper.dump_queue(filename, batches=batches)


@click.command()
@click.option('--queue', '-q', help='Queue name', required=True)
@click.option('--state', '-s', help='Task state (default=%s)' % QUEUED,
              required=False, type=click.Choice([QUEUED, SCHEDULED, ACTIVE, ERROR]), default=QUEUED)
@click.option('--filename', '-f', help='Sqlite file name', required=True)
@click.pass_context
def sample_queue(context, queue, state, filename):
    queue_dumper = QueueDumper(context.obj, queue, state)
    queue_dumper.dump_queue(filename, sample=True)


@click.command()
#TODO: Should probably default to loading back into same queue stored in stats table
@click.option('--queue', '-q', help='Queue name', required=True)
@click.option('--filename', '-f', help='Sqlite file name', required=True)
@click.pass_context
def load_queue(context, queue, filename):
    queue_loader = QueueLoader(context.obj, queue)
    queue_loader.load_queue(filename)


cli.add_command(queue_stats)
cli.add_command(dump_queue)
cli.add_command(load_queue)
cli.add_command(sample_queue)
