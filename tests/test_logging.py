import structlog

from .test_base import BaseTestCase

from tasktiger import Worker
from tasktiger.logging import tasktiger_processor

from .utils import get_tiger

tiger = get_tiger()

logger = structlog.getLogger("tasktiger")


def logging_task():
    log = logger.info("simple task")
    # Confirm tasktiger_processor injected task id
    assert log[1]["task_id"] == tiger.current_task.id


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
        self.tiger.delay(logging_task)
        queues = self._ensure_queues(queued={"default": 1})
        task = queues["queued"]["default"][0]
        assert task["func"] == "tests.test_logging:logging_task"
        Worker(self.tiger).run(once=True)
        self._ensure_queues(queued={"default": 0})
        assert not self.conn.exists("t:task:%s" % task["id"])
