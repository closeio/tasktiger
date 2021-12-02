from __future__ import absolute_import

import structlog

from tasktiger import TaskTiger, Worker
from tasktiger.logging import tasktiger_processor

from .test_base import BaseTestCase
from .utils import get_tiger, get_redis


tiger = get_tiger()

logger = structlog.getLogger("tasktiger")


def logging_task():
    log = logger.info("simple task")
    # Confirm tasktiger_processor injected task id and queue name
    assert log[1]["task_id"] == tiger.current_task.id
    assert log[1]["queue"] == tiger.current_task.queue


class TestLogging(BaseTestCase):
    """Test logging."""

    def test_structlog_processor(self):
        try:
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
            self.tiger.delay(logging_task)
            queues = self._ensure_queues(queued={"default": 1})
            task = queues["queued"]["default"][0]
            assert task["func"] == "tests.test_logging:logging_task"
            Worker(self.tiger).run(once=True)
            self._ensure_queues(queued={"default": 0})
            assert not self.conn.exists("t:task:%s" % task["id"])
        finally:
            structlog.configure(
                processors=[
                    structlog.stdlib.add_log_level,
                    structlog.stdlib.filter_by_level,
                    structlog.processors.TimeStamper(fmt="iso", utc=True),
                    structlog.processors.StackInfoRenderer(),
                    structlog.processors.format_exc_info,
                    structlog.processors.JSONRenderer(),
                ],
                context_class=dict,
                logger_factory=structlog.ReturnLoggerFactory(),
                wrapper_class=structlog.stdlib.BoundLogger,
                cache_logger_on_first_use=True,
            )


class TestSetupStructlog(BaseTestCase):
    def test_setup_structlog_basic(self):
        conn = get_redis()
        tiger = TaskTiger(connection=conn, setup_structlog=True)
        assert tiger
        conn.close()
        # no errors on init, cool
