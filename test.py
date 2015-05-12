from tasktiger import delay

from test_tasks import *

delay(sample_task, args=(1, 2))
delay(sample_task, args=(1, 2), queue='a')
delay(sample_task, args=(1, 2), queue='b')
delay(task_on_other_queue)
delay(sample_exception)
delay(long_task)
delay(long_task_ok)
delay(long_task_killed, hard_timeout=3)
delay(long_task_ok, hard_timeout=1)
