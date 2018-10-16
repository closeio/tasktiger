"""Copy TaskTiger keys from one redis instance to another."""
import redis

import click

FROM_REDIS = 'redis://127.0.0.1:26379/0'
TO_REDIS = 'redis://127.0.0.1:36379/20'

PREFIX = 't'


def move_tasks(source, dest, state, queue, apply):
    """Move tasks in a queue."""
    queue_key = '{}:{}:{}'.format(PREFIX, state, queue)
    tasks = source.zrange(queue_key, 0, -1, withscores=True)
    if apply:
        print '  Moving tasks {}'.format(queue_key)

    for task in tasks:
        key = '{}:task:{}'.format(PREFIX, task[0])
        task_data = source.get(key)
        key_executions = '{}:executions'.format(key)
        executions_data = source.lrange(key_executions, 0, -1)
        print '  task', queue, task
        if apply:
            print '    Moving'
            dest.set(key, task_data)
            dest.zadd(queue_key, task[0], task[1])
        if executions_data is not None and apply:
            print '    Moving executions {}'.format(key_executions)
            for execution in executions_data:
                dest.rpush(key_executions, execution)


def move_queues_in_state(source, dest, state, apply):
    """Move queues in a specific state."""
    print 'queues', state
    key = '{}:{}'.format(PREFIX, state)

    queues = source.smembers(key)
    for queue in queues:
        move_tasks(source, dest, state, queue, apply)
        if apply:
            print 'Moving queue {} {}'.format(key, queue)
            dest.sadd(key, queue)


@click.command()
@click.option('--apply', is_flag=True, help='Apply changes')
def run(apply=False):
    """Run."""
    source = redis.Redis.from_url(FROM_REDIS)
    dest = redis.Redis.from_url(TO_REDIS)
    move_queues_in_state(source, dest, 'queued', apply)
    move_queues_in_state(source, dest, 'active', apply)
    move_queues_in_state(source, dest, 'scheduled', apply)
    move_queues_in_state(source, dest, 'error', apply)


if __name__ == '__main__':
    run()
