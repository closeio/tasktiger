=========
tasktiger
=========

*tasktiger* is a Python task queue.

Example
-------

.. code:: python

  # tasks.py
  def task():
      print 'hello'

.. code:: python

  In [1]: import tasktiger, tasks
  In [2]: tiger = tasktiger.TaskTiger()
  In [3]: tiger.delay(tasks.task)

.. code:: bash

  % tasktiger
  {"timestamp": "2015-08-27T21:00:09.135344Z", "queues": null, "pid": 69840, "event": "ready", "level": "info"}
  {"task_id": "6fa07a91642363593cddef7a9e0c70ae3480921231710aa7648b467e637baa79", "level": "debug", "timestamp": "2015-08-27T21:03:56.727051Z", "pid": 69840, "queue": "default", "child_pid": 70171, "event": "processing"}
  hello
  {"task_id": "6fa07a91642363593cddef7a9e0c70ae3480921231710aa7648b467e637baa79", "level": "debug", "timestamp": "2015-08-27T21:03:56.732457Z", "pid": 69840, "queue": "default", "event": "done"}
