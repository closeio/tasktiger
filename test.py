from tasktiger import delay

from test_tasks import *

delay(sample_task, args=(1, 2))
delay(sample_exception)
delay(long_task_killed)
delay(long_task_ok)
delay(long_task_killed, hard_timeout=3)
delay(long_task_ok, hard_timeout=1)
