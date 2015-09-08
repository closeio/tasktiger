import datetime
import json
from ._internal import ERROR, QUEUED

# This module contains helper methods to inspect and requeue tasks from queues.
# TODO: Use task objects wherever we can.

class Task(object):
    def __init__(self, tiger, queue, state, _data, _ts=None, _executions=None):
        """
        Initializes a Task from the internal structure. Use tasks_from_queue to
        retrieve tasks from a given queue. Don't call this directly.
        """
        self.tiger = tiger
        self.queue = queue
        self.state = state
        self.data = json.loads(_data)
        if _executions:
            self.executions = [json.loads(e) for e in _executions if e]
        else:
            self.executions = []
        self.id = self.data['id']
        self.ts = _ts
        self.func = self.data['func']
        self.args = self.data.get('args', [])
        self.kwargs = self.data.get('kwargs', {})
        self.unique = self.data.get('unique', False)

    def __repr__(self):
        return u'<Task %s>' % self.func

    @classmethod
    def from_id(self, tiger, queue, state, task_id, load_executions=0):
        """
        Loads a task with the given ID from the given queue in the given
        state. An integer may be passed in the load_executions parameter
        to indicate how many executions should be loaded (starting from the
        latest). If the task doesn't exist, None is returned.
        """
        if load_executions:
            pipeline = tiger.connection.pipeline()
            pipeline.get(tiger._key('task', task_id))
            pipeline.lrange(tiger._key('task', task_id, 'executions'), -load_executions, -1)
            task, executions = pipeline.execute()
        else:
            task = tiger.connection.get(tiger._key('task', task_id))
            executions = []
        # XXX: No timestamp for now
        if task:
            return Task(tiger, queue, state, task, None, executions)

    @classmethod
    def tasks_from_queue(self, tiger, queue, state, skip=0, limit=1000,
                   load_executions=0):
        """
        Returns a tuple with the following information:
        * total items in the queue
        * tasks from the given queue in the given state, latest first.

        An integer may be passed in the load_executions parameter to indicate
        how many executions should be loaded (starting from the latest).
        """

        key = tiger._key(state, queue)
        pipeline = tiger.connection.pipeline()
        pipeline.zcard(key)
        pipeline.zrange(key, -limit-skip, -1-skip, withscores=True)
        n, items = pipeline.execute()

        if items:
            tss = [datetime.datetime.utcfromtimestamp(item[1]) for item in items]
            if load_executions:
                pipeline = tiger.connection.pipeline()
                pipeline.mget(['t:task:%s' % item[0] for item in items])
                for item in items:
                    pipeline.lrange(tiger._key('task', item[0], 'executions'), -load_executions, -1)
                results = pipeline.execute()

                tasks = [Task(tiger, queue, state, task, ts, executions)
                    for task, executions, ts in zip(results[0], results[1:], tss)]
            else:
                data = tiger.connection.mget([tiger._key('task', item[0]) for item in items])
                tasks = [Task(tiger, queue, state, task, ts)
                         for task, ts in zip(data, tss)]
        else:
            tasks = []

        return n, tasks

    def retry(self):
        """
        Retries a task that's in the error queue.
        """
        assert self.state == ERROR
        # TODO: Only allow this if the task is still in ERROR state
        self.tiger._redis_move_task(self.queue, self.id, ERROR, QUEUED)

    def delete(self):
        """
        Removes a task that's in the error queue.
        """
        assert self.state == ERROR
        if self.unique:
            remove_task = 'check'
        else:
            remove_task = 'always'
        # TODO: Only allow this if the task is still in ERROR state
        self.tiger._redis_move_task(self.queue, self.id, ERROR,
                                    remove_task=remove_task)
