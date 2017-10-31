import json
import datetime
import decimal

from .task import Task
from .worker import Worker


__all__ = ['TaskTigerTestMixin']


class TaskTigerTestMixin(object):
    """
    Unit test mixin for tests that use TaskTiger.
    """

    def run_worker(self, tiger, raise_on_errors=True, **kwargs):
        # A worker run processes queued tasks, and then queues scheduled tasks.
        # We therefore need to run the worker twice to execute due scheduled
        # tasks.
        Worker(tiger, **kwargs).run(once=True)
        Worker(tiger, **kwargs).run(once=True)

        # Print any TaskTiger failures for debugging purposes.
        prefix = tiger.config['REDIS_PREFIX']
        state = 'error'
        has_errors = False
        for queue in tiger.connection.smembers('{}:{}'.format(prefix, state)):
            n_tasks, tasks = Task.tasks_from_queue(tiger, queue, state, load_executions=1)
            for task in tasks:
                print('')
                print(task, 'failed:')
                print(task.executions[0]['traceback'])
                has_errors = True
        if has_errors and raise_on_errors:
            raise Exception('One or more tasks have failed.')


class CustomJSONEncoder(json.JSONEncoder):
    """
    A JSON encoder that allows for more common Python data types.

    In addition to the defaults handled by ``json``, this also supports:

        * ``datetime.datetime``
        * ``datetime.date``
        * ``datetime.time``
        * ``decimal.Decimal``

    """
    def default(self, data):
        if isinstance(data, (datetime.datetime, datetime.date, datetime.time)):
            return data.isoformat()
        elif isinstance(data, decimal.Decimal):
            return str(data)
        else:
            return super(CustomJSONEncoder, self).default(data)


def custom_serializer(obj):
    return json.dumps(obj, cls=CustomJSONEncoder)
