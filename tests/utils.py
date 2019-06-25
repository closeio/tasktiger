import logging
import redis
import structlog
from tasktiger import TaskTiger, Worker, fixed

from .config import DELAY, TEST_DB


class Patch(object):
    """
    Simple context manager to patch a function, e.g.:

    with Patch(module, 'func_name', mocked_func):
        module.func_name() # will use mocked_func
    module.func_name() # will use the original function

    """
    def __init__(self, orig_obj, func_name, new_func):
        self.orig_obj = orig_obj
        self.func_name = func_name
        self.new_func = new_func

    def __enter__(self):
        self.orig_func = getattr(self.orig_obj, self.func_name)
        setattr(self.orig_obj, self.func_name, self.new_func)

    def __exit__(self, *args):
        setattr(self.orig_obj, self.func_name, self.orig_func)


def get_tiger():
    """
    Sets up logging and returns a new tasktiger instance.
    """
    structlog.configure(
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
    )
    logging.basicConfig(format='%(message)s')
    conn = redis.Redis(db=TEST_DB, decode_responses=True)
    tiger = TaskTiger(connection=conn, config={
        # We need this 0 here so we don't pick up scheduled tasks when
        # doing a single worker run.
        'SELECT_TIMEOUT': 0,

        'ACTIVE_TASK_UPDATE_TIMEOUT': 2 * DELAY,

        'REQUEUE_EXPIRED_TASKS_INTERVAL': DELAY,

        'LOCK_RETRY': DELAY * 2.,

        'DEFAULT_RETRY_METHOD': fixed(DELAY, 2),

        'BATCH_QUEUES': {
            'batch': 3,
        },

        'SINGLE_WORKER_QUEUES': ['swq'],

        'EXCLUDE_QUEUES': ['periodic_ignore'],
    })
    tiger.log.setLevel(logging.CRITICAL)
    return tiger


def external_worker(n=None, patch_config=None, max_workers_per_queue=None):
    """
    Runs a worker. To be used with multiprocessing.Pool.map.
    """
    tiger = get_tiger()

    if patch_config:
        tiger.config.update(patch_config)

    worker = Worker(tiger)

    if max_workers_per_queue is not None:
        worker.max_workers_per_queue = max_workers_per_queue

    worker.run(once=True, force_once=True)
