from __future__ import absolute_import

import pytest
import structlog

from tasktiger import TaskTiger, Worker
from tasktiger.logging import tasktiger_processor

from .test_base import BaseTestCase
from .utils import get_redis, get_tiger

tiger = get_tiger()
logger = structlog.getLogger("tasktiger")


@pytest.fixture(autouse=True)
def restore_structlog_config():
    previous_config = structlog.get_config()

    try:
        yield
    finally:
        structlog.configure(**previous_config)


def logging_task():
    log = logger.info("simple task")

    # Confirm tasktiger_processor injected task id and queue name
    assert log[1]["task_id"] == tiger.current_task.id
    assert log[1]["task_func"] == "tests.test_logging:logging_task"
    assert log[1]["queue"] == "foo_qux"


class TestLogging(BaseTestCase):
    """Test logging."""

    def test_structlog_processor(self):
        # Use ReturnLogger for testing
        structlog.configure(
            processors=[tasktiger_processor],
            context_class=dict,
            logger_factory=structlog.ReturnLoggerFactory(),
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )

        # Run a simple task. Logging output is verified in
        # the task.
        self.tiger.delay(logging_task, queue="foo_qux")
        queues = self._ensure_queues(queued={"foo_qux": 1})

        task = queues["queued"]["foo_qux"][0]
        assert task["func"] == "tests.test_logging:logging_task"

        Worker(self.tiger).run(once=True)
        self._ensure_queues(queued={"foo_qux": 0})
        assert not self.conn.exists("t:task:%s" % task["id"])


class TestSetupStructlog(BaseTestCase):
    def test_setup_structlog_basic(self):
        conn = get_redis()
        tiger = TaskTiger(connection=conn, setup_structlog=True)
        assert tiger
        conn.close()
        # no errors on init, cool
