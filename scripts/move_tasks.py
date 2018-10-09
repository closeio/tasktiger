"""Copy TaskTiger keys from one redis instance to another."""
import redis

import click

FROM_REDIS = 'redis://127.0.0.1:26379/0'
TO_REDIS = 'redis://127.0.0.1:36379/20'

PREFIX = 't'


def move_tasks(source, dest, state, queue, apply):
    """Move tasks in a queue."""
    key = '{}:{}:{}'.format(PREFIX, state, queue)
    tasks = source.zrange(key, 0, -1)
    dump = source.dump(key)
    if apply:
        print '  Moving tasks'
        dest.restore(key, 0, dump)

    for task in tasks:
        key = '{}:task:{}'.format(PREFIX, task)
        dump = source.dump(key)
        key_executions = '{}:executions'.format(key)
        dump_executions = source.dump(key_executions)
        print '  task', queue, task
        if apply:
            print '    Moving'
            dest.restore(key, 0, dump)
        if dump_executions is not None and apply:
            print '    Moving executions'
            dest.restore(key_executions, 0, dump_executions)


def move_queues_in_state(source, dest, state, apply):
    """Move queues in a specific state."""
    print 'queues', state
    key = '{}:{}'.format(PREFIX, state)
    dump = source.dump(key)
    if dump is not None and apply:
        print 'Moving queue'
        dest.restore(key, 0, dump)

    queues = source.smembers(key)
    for queue in queues:
        move_tasks(source, dest, state, queue, apply)


@click.command()
@click.option('--apply', is_flag=True, help='Apply changes')
def run(apply=False):
    """Run."""
    source = redis.Redis.from_url(FROM_REDIS)
    dest = redis.Redis.from_url(TO_REDIS)
    move_queues_in_state(source, dest, 'queued', apply)
    move_queues_in_state(source, dest, 'active', apply)
    move_queues_in_state(source, dest, 'scheduled', apply)
    # move_queues_in_state(source, dest, 'error', apply)


if __name__ == '__main__':
    run()
