from __future__ import absolute_import

import logging
import tempfile

from tasktiger import TaskTiger, Worker

from tests.utils import TEST_TIGER_CONFIG, get_redis, setup_structlog

tiger = TaskTiger(lazy_init=True)


@tiger.task
def lazy_task(filename):
    with open(filename, "w") as f:
        f.write("ok")


def test_lazy_init():
    setup_structlog()
    tiger.init(connection=get_redis(), config=TEST_TIGER_CONFIG)
    tiger.log.setLevel(logging.CRITICAL)
    with tempfile.NamedTemporaryFile() as f:
        lazy_task.delay(f.name)
        Worker(tiger).run(once=True)
        assert f.read().decode("utf8") == "ok"
