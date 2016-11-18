import logging
import redis
import structlog
from tasktiger import TaskTiger, Worker, fixed

from .config import *

def get_tiger(**kwargs):
    """
    Sets up logging and returns a new tasktiger instance.
    """
    structlog.configure(
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
    )
    logging.basicConfig(format='%(message)s')
    conn = redis.Redis(db=TEST_DB, decode_responses=True)
    config = {
        # We need this 0 here so we don't pick up scheduled tasks when
        # doing a single worker run.
        'SELECT_TIMEOUT': 0,

        'LOCK_RETRY': DELAY*2.,

        'DEFAULT_RETRY_METHOD': fixed(DELAY, 2),

        'BATCH_QUEUES': {
            'batch': 3,
        }
    }
    config.update(kwargs)
    tiger = TaskTiger(connection=conn, config=config)
    tiger.log.setLevel(logging.CRITICAL)
    return tiger

def external_worker(n=None):
    """
    Runs a worker. To be used with multiprocessing.Pool.map.
    """
    tiger = get_tiger()
    worker = Worker(tiger)
    worker.run(once=True)
