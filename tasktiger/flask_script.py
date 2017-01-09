from __future__ import absolute_import

import argparse
from flask.ext.script import Command

import tasktiger.cli

class TaskTigerCommand(Command):
    capture_all_args = True
    help = 'Run a TaskTiger worker'

    def __init__(self, tiger):
        super(TaskTigerCommand, self).__init__()
        self.tiger = tiger

    def create_parser(self, *args, **kwargs):
        # Override the default parser so we can pass all arguments to the
        # TaskTiger parser.
        func_stack = kwargs.pop('func_stack',())
        parent = kwargs.pop('parent', None)
        parser = argparse.ArgumentParser(*args, add_help=False, **kwargs)
        parser.set_defaults(func_stack=func_stack+(self,))
        self.parser = parser
        self.parent = parent
        return parser

    def setup(self):
        """
        Override this method to implement custom setup (e.g. logging) before
        running the worker.
        """

    def run(self, args):
        self.setup()
        self.tiger.run_worker_with_args(args)


class TaskTigerCLICommand(TaskTigerCommand):
    """
    This command can be used to manage TaskTiger queues.

    Examples:
        Print queue statistics:
            manage.py tasktigercli queue_stats

        Dump all tasks to /tmp/tasks.db file and clear queue:
            manage.py tasktigercli dump_queue -f /tmp/tasks.db -q default -s queued

        Dump all tasks in queue and do not save them to sqlite3 database:
            manage.py tasktigercli dump_queue -p -q default -s queued

        Dump a batch of tasks to /tmp/tasks.db but leave them on queue:
            manage.py tasktigercli sample_queue -f /tmp/tasks.db -q default -s queued

        Count number of tasks dumped to sqlite3 database:
            sqlite3 /tmp/tasks.db 'select count(*) from tasks;'

        View dump info in db file:
            sqlite3 /tmp/tasks.db 'select * from stats;'
    """

    capture_all_args = True
    help = 'Run a TaskTiger CLI command'

    def run(self, args):
        self.setup()
        tasktiger.cli.run_with_args(self.tiger, args)
