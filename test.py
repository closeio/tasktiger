from tasktiger import task, delay


@task()
def sample_task(a, b):
    print 'Task', a, b

delay(sample_task, args=(1, 2))
delay(sample_task, args=(1, 2))
