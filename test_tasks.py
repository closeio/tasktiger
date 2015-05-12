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

def sample_exception():
    raise StandardError('this failed')

def long_task():
    import time
    time.sleep(5)
