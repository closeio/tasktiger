from __future__ import absolute_import

import argparse

from flask_script import Command


class TaskTigerCommand(Command):
    """
    This class is deprecated and may be removed in future versions.
    That is because Flask-Script is no longer supported (since 2017).
    """

    capture_all_args = True
    help = "Run a TaskTiger worker"

    def __init__(self, tiger):
        super(TaskTigerCommand, self).__init__()
        self.tiger = tiger

    def create_parser(self, *args, **kwargs):
        # Override the default parser so we can pass all arguments to the
        # TaskTiger parser.
        func_stack = kwargs.pop("func_stack", ())
        parent = kwargs.pop("parent", None)
        parser = argparse.ArgumentParser(*args, add_help=False, **kwargs)
        parser.set_defaults(func_stack=func_stack + (self,))
        self.parser = parser
        self.parent = parent
        return parser

    def setup(self):
        """
        Override this method to implement custom setup (e.g. logging) before
        running the worker.
        """

    def run(self, args):
        # Allow passing a callable that returns the TaskTiger instance.
        if callable(self.tiger):
            self.tiger = self.tiger()
        self.setup()
        self.tiger.run_worker_with_args(args)
