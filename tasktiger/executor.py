import errno
import fcntl
import os
import random
import select
import signal
import socket
import sys
import threading
import time
import traceback
from contextlib import ExitStack
from typing import (
    TYPE_CHECKING,
    Any,
    Collection,
    ContextManager,
    Dict,
    List,
    Optional,
)

from redis.exceptions import LockError
from redis.lock import Lock
from structlog.stdlib import BoundLogger

from ._internal import (
    g,
    g_fork_lock,
    serialize_func_name,
    serialize_retry_method,
)
from .exceptions import RetryException
from .redis_semaphore import Semaphore
from .runner import get_runner_class
from .task import Task
from .timeouts import JobTimeoutException

if TYPE_CHECKING:
    from .worker import Worker


def sigchld_handler(*args: Any) -> None:
    # Nothing to do here. This is just a dummy handler that we set up to catch
    # the child process exiting.
    pass


class WorkerContextManagerStack(ExitStack):
    def __init__(self, context_managers: List[ContextManager]) -> None:
        super(WorkerContextManagerStack, self).__init__()

        for mgr in context_managers:
            self.enter_context(mgr)


class Executor:
    exit_worker_on_job_timeout = False

    def __init__(self, worker: "Worker"):
        self.tiger = worker.tiger
        self.worker = worker
        self.connection = worker.connection
        self.config = worker.config

    def heartbeat(
        self,
        queue: str,
        task_ids: Collection[str],
        log: BoundLogger,
        locks: Collection[Lock],
        queue_lock: Optional[Semaphore],
    ) -> None:
        self.worker.heartbeat(queue, task_ids)
        for lock in locks:
            try:
                lock.reacquire()
            except LockError:
                log.warning("could not reacquire lock", lock=lock.name)
        if queue_lock:
            acquired, current_locks = queue_lock.renew()
            if not acquired:
                log.debug("queue lock renew failure")

    def execute(
        self,
        queue: str,
        tasks: List[Task],
        log: BoundLogger,
        locks: Collection[Lock],
        queue_lock: Optional[Semaphore],
    ) -> bool:
        """
        Executes the given tasks. Returns a boolean indicating whether
        the tasks were executed successfully.

        Args:
            queue: Name of the task queue.
            tasks: List of tasks to execute,
            log: Logger.
            locks: List of task locks to renew periodically.
            queue_lock: Optional queue lock to renew periodically for max
                workers per queue.

        Returns:
            Whether task execution was successful.
        """
        raise NotImplementedError

    def execute_tasks(self, tasks: List[Task], log: BoundLogger) -> bool:
        """
        Executes the tasks in the current process. Multiple tasks can be passed
        for batch processing. However, they must all use the same function and
        will share the execution entry.
        """
        success = False

        execution: Dict[str, Any] = {}

        assert len(tasks)
        task_func = tasks[0].serialized_func
        assert all([task_func == task.serialized_func for task in tasks[1:]])

        execution["time_started"] = time.time()

        try:
            func = tasks[0].func

            runner_class = get_runner_class(log, tasks)
            runner = runner_class(self.tiger)

            is_batch_func = getattr(func, "_task_batch", False)
            g["tiger"] = self.tiger
            g["current_task_is_batch"] = is_batch_func

            hard_timeouts = self.worker.get_hard_timeouts(func, tasks)

            with WorkerContextManagerStack(
                self.config["CHILD_CONTEXT_MANAGERS"]
            ):
                if is_batch_func:
                    # Batch process if the task supports it.
                    g["current_tasks"] = tasks
                    runner.run_batch_tasks(tasks, hard_timeouts[0])
                else:
                    # Process sequentially.
                    for task, hard_timeout in zip(tasks, hard_timeouts):
                        g["current_tasks"] = [task]
                        runner.run_single_task(task, hard_timeout)

        except RetryException as exc:
            execution["retry"] = True
            if exc.method:
                execution["retry_method"] = serialize_retry_method(exc.method)
            execution["log_error"] = exc.log_error
            execution["exception_name"] = serialize_func_name(exc.__class__)
            exc_info = exc.exc_info or sys.exc_info()
        except (JobTimeoutException, Exception) as exc:
            execution["exception_name"] = serialize_func_name(exc.__class__)
            exc_info = sys.exc_info()
        else:
            success = True

        if not success:
            execution["time_failed"] = time.time()
            if self.worker.store_tracebacks:
                # Currently we only log failed task executions to Redis.
                execution["traceback"] = "".join(
                    traceback.format_exception(*exc_info)
                )
            execution["success"] = success
            execution["host"] = socket.gethostname()

            self.worker.store_task_execution(tasks, execution)

        g["current_task_is_batch"] = None
        g["current_tasks"] = None
        g["tiger"] = None

        return success


class ForkExecutor(Executor):
    """
    Executor that runs tasks in a forked process.

    Child process is killed after a hard timeout + margin.
    """

    def execute(
        self,
        queue: str,
        tasks: List[Task],
        log: BoundLogger,
        locks: Collection[Lock],
        queue_lock: Optional[Semaphore],
    ) -> bool:
        task_func = tasks[0].func
        serialized_task_func = tasks[0].serialized_func

        all_task_ids = {task.id for task in tasks}
        with g_fork_lock:
            child_pid = os.fork()

        if child_pid == 0:
            # Child process
            log = log.bind(child_pid=os.getpid())
            assert isinstance(log, BoundLogger)

            # Disconnect the Redis connection inherited from the main process.
            # Note that this doesn't disconnect the socket in the main process.
            self.connection.connection_pool.disconnect()

            random.seed()

            # Ignore Ctrl+C in the child so we don't abort the job -- the main
            # process already takes care of a graceful shutdown.
            signal.signal(signal.SIGINT, signal.SIG_IGN)

            # Run the tasks.
            success = self.execute_tasks(tasks, log)

            # Wait for any threads that might be running in the child, just
            # like sys.exit() would. Note we don't call sys.exit() directly
            # because it would perform additional cleanup (e.g. calling atexit
            # handlers twice). See also: https://bugs.python.org/issue18966
            threading._shutdown()  # type: ignore[attr-defined]

            os._exit(int(not success))
        else:
            # Main process
            log = log.bind(child_pid=child_pid)
            assert isinstance(log, BoundLogger)
            for task in tasks:
                log.info(
                    "processing",
                    func=serialized_task_func,
                    task_id=task.id,
                    params={"args": task.args, "kwargs": task.kwargs},
                )

            # Attach a signal handler to SIGCHLD (sent when the child process
            # exits) so we can capture it.
            signal.signal(signal.SIGCHLD, sigchld_handler)

            # Since newer Python versions retry interrupted system calls we can't
            # rely on the fact that select() is interrupted with EINTR. Instead,
            # we'll set up a wake-up file descriptor below.

            # Create a new pipe and apply the non-blocking flag (required for
            # set_wakeup_fd).
            pipe_r, pipe_w = os.pipe()

            opened_fd = os.fdopen(pipe_r)
            flags = fcntl.fcntl(pipe_r, fcntl.F_GETFL, 0)
            flags = flags | os.O_NONBLOCK
            fcntl.fcntl(pipe_r, fcntl.F_SETFL, flags)

            flags = fcntl.fcntl(pipe_w, fcntl.F_GETFL, 0)
            flags = flags | os.O_NONBLOCK
            fcntl.fcntl(pipe_w, fcntl.F_SETFL, flags)

            # A byte will be written to pipe_w if a signal occurs (and can be
            # read from pipe_r).
            old_wakeup_fd = signal.set_wakeup_fd(pipe_w)

            def check_child_exit() -> Optional[int]:
                """
                Do a non-blocking check to see if the child process exited.
                Returns None if the process is still running, or the exit code
                value of the child process.
                """
                try:
                    pid, return_code = os.waitpid(child_pid, os.WNOHANG)
                    if pid != 0:  # The child process is done.
                        return return_code
                except OSError as e:
                    # Of course EINTR can happen if the child process exits
                    # while we're checking whether it exited. In this case it
                    # should be safe to retry.
                    if e.errno == errno.EINTR:
                        return check_child_exit()
                    else:
                        raise
                return None

            hard_timeouts = self.worker.get_hard_timeouts(task_func, tasks)
            time_started = time.time()

            # Upper bound for when we expect the child processes to finish.
            # Since the hard timeout doesn't cover any processing overhead,
            # we're adding an extra buffer of ACTIVE_TASK_UPDATE_TIMEOUT
            # (which is the same time we use to determine if a task has
            # expired).
            timeout_at = (
                time_started
                + sum(hard_timeouts)
                + self.config["ACTIVE_TASK_UPDATE_TIMEOUT"]
            )

            # Wait for the child to exit and perform a periodic heartbeat.
            # We check for the child twice in this loop so that we avoid
            # unnecessary waiting if the child exited just before entering
            # the while loop or while renewing heartbeat/locks.
            while True:
                return_code = check_child_exit()
                if return_code is not None:
                    break

                # Wait until the timeout or a signal / child exit occurs.
                try:
                    # If observed the following behavior will be seen
                    # in the pipe when the parent process receives a
                    # SIGTERM while a task is running in a child process:
                    # Linux:
                    #   - 0 when parent receives SIGTERM
                    #   - select() exits with EINTR when child exit
                    #     triggers signal, so the signal in the
                    #     pipe is never seen since check_child_exit()
                    #     will see the child is gone
                    #
                    # macOS:
                    #   - 15 (SIGTERM) when parent receives SIGTERM
                    #   - 20 (SIGCHLD) when child exits
                    results = select.select(
                        [pipe_r],
                        [],
                        [],
                        self.config["ACTIVE_TASK_UPDATE_TIMER"],
                    )

                    if results[0]:
                        # Purge pipe so select will pause on next call
                        try:
                            # Behavior of a would be blocking read()
                            # Linux:
                            #   Python 2.7 Raises IOError
                            #   Python 3.x returns empty string
                            #
                            # macOS:
                            #   Returns empty string
                            opened_fd.read(1)
                        except IOError:
                            pass

                except select.error as e:
                    if e.args[0] != errno.EINTR:
                        raise

                return_code = check_child_exit()
                if return_code is not None:
                    break

                now = time.time()
                if now > timeout_at:
                    log.error("hard timeout elapsed in parent process")
                    os.kill(child_pid, signal.SIGKILL)
                    pid, return_code = os.waitpid(child_pid, 0)
                    log.error("child killed", return_code=return_code)
                    execution = {
                        "time_started": time_started,
                        "time_failed": now,
                        "exception_name": serialize_func_name(
                            JobTimeoutException
                        ),
                        "success": False,
                        "host": socket.gethostname(),
                    }
                    self.worker.store_task_execution(tasks, execution)
                    break

                try:
                    self.heartbeat(queue, all_task_ids, log, locks, queue_lock)
                except OSError as e:
                    # EINTR happens if the task completed. Since we're just
                    # renewing locks/heartbeat it's okay if we get interrupted.
                    if e.errno != errno.EINTR:
                        raise

            # Restore signals / clean up
            signal.signal(signal.SIGCHLD, signal.SIG_DFL)
            signal.set_wakeup_fd(old_wakeup_fd)
            opened_fd.close()
            os.close(pipe_w)

            success = return_code == 0
            return success


class SyncExecutor(Executor):
    """
    Executor that runs tasks in the current thread/process.
    """

    exit_worker_on_job_timeout = True

    def _periodic_heartbeat(
        self,
        queue: str,
        task_ids: Collection[str],
        log: BoundLogger,
        locks: Collection[Lock],
        queue_lock: Optional[Semaphore],
        stop_event: threading.Event,
    ) -> None:
        while not stop_event.wait(self.config["ACTIVE_TASK_UPDATE_TIMER"]):
            try:
                self.heartbeat(queue, task_ids, log, locks, queue_lock)
            except Exception:
                log.exception("task heartbeat failed")

    def execute(
        self,
        queue: str,
        tasks: List[Task],
        log: BoundLogger,
        locks: Collection[Lock],
        queue_lock: Optional[Semaphore],
    ) -> bool:
        assert tasks

        # Run heartbeat thread.
        all_task_ids = {task.id for task in tasks}
        stop_event = threading.Event()
        heartbeat_thread = threading.Thread(
            target=self._periodic_heartbeat,
            kwargs={
                "queue": queue,
                "task_ids": all_task_ids,
                "log": log,
                "locks": locks,
                "queue_lock": queue_lock,
                "stop_event": stop_event,
            },
        )
        heartbeat_thread.start()

        serialized_task_func = tasks[0].serialized_func
        for task in tasks:
            log.info(
                "processing",
                func=serialized_task_func,
                task_id=task.id,
                params={"args": task.args, "kwargs": task.kwargs},
            )

        # Run the tasks.
        try:
            result = self.execute_tasks(tasks, log)
        # Always stop the heartbeat thread -- even in case of an unhandled
        # exception after running the task code, or when an unhandled
        # BaseException is raised from within the task.
        finally:
            stop_event.set()
            heartbeat_thread.join()

        return result
