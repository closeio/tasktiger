from tasktiger import task


"""
import redis
conn = redis.Redis()
print 'ERROR QUEUE'
print conn.zrange('t:error:default', 0, -1, withscores=True)
conn.delete('t:error:default')
"""


#@task()
def sample_task(a, b):
    print 'Task', a, b

@task(queue='other')
def task_on_other_queue():
    print 'Other task'

def sample_exception():
    raise StandardError('this failed')

def long_task():
    import time
    time.sleep(5)

@task(hard_timeout=1)
def long_task_killed():
    import time
    time.sleep(2)

@task(hard_timeout=3)
def long_task_ok():
    import time
    time.sleep(2)

@task(unique=True)
def unique_task(a=None):
    import time
    time.sleep(1)
    print 'UNIQUE TASK', a

@task(unique=True)
def unique_task_failure():
    import time
    time.sleep(2)
    raise StandardError('failed')
