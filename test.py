import datetime

from tasktiger import delay

from test_tasks import *

delay(sample_task, args=(1, 2))
delay(sample_task, args=(1, 3), queue='a')
delay(sample_task, args=(1, 4), queue='b')
delay(sample_task, args=(5, 6), when=datetime.timedelta(seconds=2))
delay(sample_task, args=(5, 6), when=datetime.timedelta(seconds=4))
delay(task_on_other_queue)
delay(sample_exception)
delay(long_task)
delay(long_task_ok)
delay(long_task_killed, hard_timeout=3)
delay(long_task_ok, hard_timeout=1)

delay(unique_task)
delay(unique_task)
delay(unique_task)

delay(unique_task, kwargs={'a': 1})
delay(unique_task, kwargs={'a': 2})
delay(unique_task, kwargs={'a': 2})

delay(unique_task_failure)
delay(unique_task_failure)
delay(unique_task_failure)

delay(locked_task, kwargs={'n': 1})
delay(locked_task, kwargs={'n': 2})
delay(locked_task, kwargs={'n': 1})
