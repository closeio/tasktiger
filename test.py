from tasktiger import delay

from test_tasks import *

delay(sample_task, args=(1, 2))
delay(sample_exception)
delay(long_task)
