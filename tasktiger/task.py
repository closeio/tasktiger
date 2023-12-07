import copy
import datetime
import json
import time
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Collection,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    Union,
)

import redis
from structlog.stdlib import BoundLogger

from ._internal import (
    ERROR,
    QUEUED,
    SCHEDULED,
    g,
    gen_id,
    gen_unique_id,
    get_timestamp,
    import_attribute,
    serialize_func_name,
    serialize_retry_method,
)
from .exceptions import QueueFullException, TaskImportError, TaskNotFound
from .runner import BaseRunner, get_runner_class
from .types import RetryStrategy

if TYPE_CHECKING:
    from . import TaskTiger

__all__ = ["Task"]


class Task:
    def __init__(
        self,
        tiger: "TaskTiger",
        func: Optional[Callable] = None,
        args: Optional[Any] = None,
        kwargs: Optional[Any] = None,
        queue: Optional[str] = None,
        hard_timeout: Optional[float] = None,
        unique: Optional[bool] = None,
        unique_key: Optional[Collection[str]] = None,
        lock: Optional[bool] = None,
        lock_key: Optional[Collection[str]] = None,
        retry: Optional[bool] = None,
        retry_on: Optional[Collection[Type[BaseException]]] = None,
        retry_method: Optional[
            Union[Callable[[int], float], Tuple[Callable[..., float], Tuple]]
        ] = None,
        max_queue_size: Optional[int] = None,
        max_stored_executions: Optional[int] = None,
        runner_class: Optional[Type["BaseRunner"]] = None,
        # internal variables
        _data: Any = None,
        _state: Any = None,
        _ts: Any = None,
        _executions: Optional[List[Dict[str, Any]]] = None,
    ):
        """
        Queues a task. See README.rst for an explanation of the options.
        """

        if func and queue is None:
            queue = Task.queue_from_function(func, tiger)

        self.tiger = tiger
        self._func = func
        self._queue = queue
        self._state = _state
        self._ts = _ts
        self._executions = _executions or []

        # Internal initialization based on raw data.
        if _data is not None:
            self._data = _data
            return

        assert func

        serialized_name = serialize_func_name(func)

        if unique is None:
            unique = getattr(func, "_task_unique", False)

        if unique_key is None:
            unique_key = getattr(func, "_task_unique_key", None)

        if lock is None:
            lock = getattr(func, "_task_lock", False)

        if lock_key is None:
            lock_key = getattr(func, "_task_lock_key", None)

        if retry is None:
            retry = getattr(func, "_task_retry", False)

        if retry_on is None:
            retry_on = getattr(func, "_task_retry_on", None)

        if retry_method is None:
            retry_method = getattr(func, "_task_retry_method", None)

        if max_queue_size is None:
            max_queue_size = getattr(func, "_task_max_queue_size", None)

        if max_stored_executions is None:
            max_stored_executions = getattr(
                func, "_task_max_stored_executions", None
            )

        if runner_class is None:
            runner_class = getattr(func, "_task_runner_class", None)

        # normalize falsy args/kwargs to empty structures
        args = args or []
        kwargs = kwargs or {}
        if unique or unique_key:
            if unique_key:
                task_id = gen_unique_id(
                    serialized_name,
                    None,
                    {key: kwargs.get(key) for key in unique_key},
                )
            else:
                task_id = gen_unique_id(serialized_name, args, kwargs)
        else:
            task_id = gen_id()

        task: Dict[str, Any] = {"id": task_id, "func": serialized_name}
        if unique or unique_key:
            task["unique"] = True
            if unique_key:
                task["unique_key"] = unique_key
        if lock or lock_key:
            task["lock"] = True
            if lock_key:
                task["lock_key"] = lock_key
        if args:
            task["args"] = args
        if kwargs:
            task["kwargs"] = kwargs
        if hard_timeout:
            task["hard_timeout"] = hard_timeout
        if retry or retry_on or retry_method:
            if not retry_method:
                retry_method = tiger.config["DEFAULT_RETRY_METHOD"]

            task["retry_method"] = serialize_retry_method(retry_method)
            if retry_on:
                task["retry_on"] = [
                    serialize_func_name(cls) for cls in retry_on
                ]
        if max_queue_size:
            task["max_queue_size"] = max_queue_size
        if max_stored_executions is not None:
            task["max_stored_executions"] = max_stored_executions
        if runner_class:
            serialized_runner_class = serialize_func_name(runner_class)
            task["runner_class"] = serialized_runner_class

        self._data = task

    @property
    def id(self) -> str:
        return self._data["id"]

    @property
    def data(self) -> Dict[str, Any]:
        return self._data

    @property
    def time_last_queued(self) -> Optional[datetime.datetime]:
        timestamp = self._data.get("time_last_queued")
        if timestamp is None:
            return None
        else:
            return datetime.datetime.utcfromtimestamp(timestamp)

    @property
    def state(self) -> str:
        return self._state

    @property
    def queue(self) -> str:
        assert self._queue
        return self._queue

    @property
    def serialized_func(self) -> str:
        return self._data["func"]

    @property
    def lock(self) -> bool:
        return self._data.get("lock", False)

    @property
    def lock_key(self) -> Optional[str]:
        return self._data.get("lock_key")

    @property
    def args(self) -> List[Any]:
        return self._data.get("args", [])

    @property
    def kwargs(self) -> Dict[str, Any]:
        return self._data.get("kwargs", {})

    @property
    def hard_timeout(self) -> Optional[float]:
        return self._data.get("hard_timeout", None)

    @property
    def unique(self) -> bool:
        return self._data.get("unique", False)

    @property
    def unique_key(self) -> Optional[str]:
        return self._data.get("unique_key")

    @property
    def retry_method(self) -> Optional[RetryStrategy]:
        if "retry_method" in self._data:
            retry_func, retry_args = self._data["retry_method"]
            return retry_func, retry_args
        else:
            return None

    @property
    def retry_on(self) -> List[str]:
        return self._data.get("retry_on")

    def should_retry_on(
        self,
        exception_class: Type[BaseException],
        logger: Optional[BoundLogger] = None,
    ) -> bool:
        """
        Whether this task should be retried when the given exception occurs.
        """
        for n in self.retry_on or []:
            try:
                if issubclass(exception_class, import_attribute(n)):
                    return True
            except TaskImportError:
                if logger:
                    logger.error(
                        "should_retry_on could not import class",
                        exception_name=n,
                    )
        return False

    @property
    def func(self) -> Callable:
        if not self._func:
            self._func = import_attribute(self.serialized_func)
        return self._func

    @property
    def max_stored_executions(self) -> Optional[int]:
        return self._data.get("max_stored_executions")

    @property
    def serialized_runner_class(self) -> str:
        return self._data.get("runner_class")

    @property
    def ts(self) -> Optional[datetime.datetime]:
        """
        The timestamp (datetime) of the task in the queue, or None, if the task
        hasn't been queued.
        """
        return self._ts

    @property
    def executions(self) -> List[Dict[str, Any]]:
        return self._executions

    def _move(
        self,
        from_state: Optional[str] = None,
        to_state: Optional[str] = None,
        when: Optional[float] = None,
        mode: Optional[str] = None,
    ) -> None:
        """
        Internal helper to move a task from one state to another (e.g. from
        QUEUED to DELAYED). The "when" argument indicates the timestamp of the
        task in the new state. If no to_state is specified, the task will be
        simply removed from the original state.

        The "mode" param can be specified to define how the timestamp in the
        new state should be updated and is passed to the ZADD Redis script (see
        its documentation for details).

        Raises TaskNotFound if the task is not in the expected state or not in
        the expected queue.
        """

        scripts = self.tiger.scripts

        from_state = from_state or self.state
        queue = self.queue

        assert from_state
        assert queue

        try:
            scripts.move_task(
                id=self.id,
                queue=self.queue,
                from_state=from_state,
                to_state=to_state,
                unique=self.unique,
                when=when or time.time(),
                mode=mode,
                key_func=self.tiger._key,
                publish_queued_tasks=self.tiger.config["PUBLISH_QUEUED_TASKS"],
            )
        except redis.ResponseError as e:
            if "<FAIL_IF_NOT_IN_ZSET>" in e.args[0]:
                raise TaskNotFound(
                    'Task {} not found in queue "{}" in state "{}".'.format(
                        self.id, queue, from_state
                    )
                )
            raise
        else:
            self._state = to_state

    def execute(self) -> None:
        func = self.func
        is_batch_func = getattr(func, "_task_batch", False)

        g["current_task_is_batch"] = is_batch_func
        g["current_tasks"] = [self]
        g["tiger"] = self.tiger

        try:
            runner_class = get_runner_class(self.tiger.log, [self])
            runner = runner_class(self.tiger)
            return runner.run_eager_task(self)
        finally:
            g["current_task_is_batch"] = None
            g["current_tasks"] = None
            g["tiger"] = None

    def delay(
        self,
        when: Optional[Union[datetime.timedelta, datetime.datetime]] = None,
        max_queue_size: Optional[int] = None,
    ) -> None:
        tiger = self.tiger

        ts = get_timestamp(when)

        now = time.time()
        self._data["time_last_queued"] = now

        if max_queue_size is None:
            max_queue_size = self._data.get("max_queue_size")

        if not ts or ts <= now:
            # Immediately queue if the timestamp is in the past.
            ts = now
            state = QUEUED
        else:
            state = SCHEDULED

        # When using ALWAYS_EAGER, make sure we have serialized the task to
        # ensure there are no serialization errors.
        serialized_task = json.dumps(self._data)

        if max_queue_size:
            # This will fail adding a unique task that already is queued but
            # the queue size is at the max
            queue_size = tiger.get_total_queue_size(self.queue)
            if queue_size >= max_queue_size:
                raise QueueFullException("Queue size: {}".format(queue_size))

        if tiger.config["ALWAYS_EAGER"] and state == QUEUED:
            return self.execute()

        pipeline = tiger.connection.pipeline()
        pipeline.sadd(tiger._key(state), self.queue)
        pipeline.set(tiger._key("task", self.id), serialized_task)
        # In case of unique tasks, don't update the score.
        tiger.scripts.zadd(
            tiger._key(state, self.queue),
            ts,
            self.id,
            mode="nx",
            client=pipeline,
        )
        if state == QUEUED and tiger.config["PUBLISH_QUEUED_TASKS"]:
            pipeline.publish(tiger._key("activity"), self.queue)
        pipeline.execute()

        self._state = state
        self._ts = ts

    def update_scheduled_time(
        self, when: Optional[Union[datetime.timedelta, datetime.datetime]]
    ) -> None:
        """
        Updates a scheduled task's date to the given date. If the task is not
        scheduled, a TaskNotFound exception is raised.
        """
        tiger = self.tiger

        ts = get_timestamp(when)
        assert ts

        pipeline = tiger.connection.pipeline()
        key = tiger._key(SCHEDULED, self.queue)
        tiger.scripts.zadd(key, ts, self.id, mode="xx", client=pipeline)
        pipeline.zscore(key, self.id)
        _, score = pipeline.execute()
        if not score:
            raise TaskNotFound(
                'Task {} not found in queue "{}" in state "{}".'.format(
                    self.id, self.queue, SCHEDULED
                )
            )

        self._ts = ts

    def __repr__(self) -> str:
        return "<Task %s>" % self.func

    @classmethod
    def from_id(
        cls,
        tiger: "TaskTiger",
        queue: str,
        state: str,
        task_id: str,
        load_executions: int = 0,
    ) -> "Task":
        """
        Loads a task with the given ID from the given queue in the given
        state. An integer may be passed in the load_executions parameter
        to indicate how many executions should be loaded (starting from the
        latest). If the task doesn't exist, None is returned.
        """
        pipeline = tiger.connection.pipeline()
        pipeline.get(tiger._key("task", task_id))
        pipeline.zscore(tiger._key(state, queue), task_id)
        if load_executions:
            pipeline.lrange(
                tiger._key("task", task_id, "executions"), -load_executions, -1
            )
            (
                serialized_data,
                score,
                serialized_executions,
            ) = pipeline.execute()
        else:
            serialized_data, score = pipeline.execute()
            serialized_executions = []

        if serialized_data and score:
            data = json.loads(serialized_data)
            executions = [json.loads(e) for e in serialized_executions if e]
            return Task(
                tiger,
                queue=queue,
                _data=data,
                _state=state,
                _executions=executions,
                _ts=datetime.datetime.utcfromtimestamp(score),
            )
        else:
            raise TaskNotFound("Task {} not found.".format(task_id))

    @classmethod
    def tasks_from_queue(
        cls,
        tiger: "TaskTiger",
        queue: str,
        state: str,
        skip: int = 0,
        limit: int = 1000,
        load_executions: int = 0,
        include_not_found: bool = False,
    ) -> Tuple[int, List["Task"]]:
        """
        Return tasks from a queue.

        Args:
            tiger: TaskTiger instance.
            queue: Name of the queue.
            state: State of the task (QUEUED, ACTIVE, SCHEDULED, ERROR).
            limit: Maximum number of tasks to return.
            load_executions: Maximum number of executions to load for each task
                (starting from the latest).
            include_not_found: Whether to include tasks that cannot be loaded.

        Returns:
            Tuple with the following information:
            * total items in the queue
            * tasks from the given queue in the given state, latest first.
        """

        key = tiger._key(state, queue)
        pipeline = tiger.connection.pipeline()
        pipeline.zcard(key)
        pipeline.zrange(key, -limit - skip, -1 - skip, withscores=True)
        n, items = pipeline.execute()

        tasks = []

        if items:
            tss = [
                datetime.datetime.utcfromtimestamp(item[1]) for item in items
            ]
            if load_executions:
                pipeline = tiger.connection.pipeline()
                pipeline.mget([tiger._key("task", item[0]) for item in items])
                for item in items:
                    pipeline.lrange(
                        tiger._key("task", item[0], "executions"),
                        -load_executions,
                        -1,
                    )
                results = pipeline.execute()

                for idx, serialized_data, serialized_executions, ts in zip(
                    range(len(items)), results[0], results[1:], tss
                ):
                    if serialized_data is None and include_not_found:
                        data = {"id": items[idx][0]}
                    else:
                        data = json.loads(serialized_data)

                    executions = [
                        json.loads(e) for e in serialized_executions if e
                    ]

                    task = Task(
                        tiger,
                        queue=queue,
                        _data=data,
                        _state=state,
                        _ts=ts,
                        _executions=executions,
                    )

                    tasks.append(task)
            else:
                result = tiger.connection.mget(
                    [tiger._key("task", item[0]) for item in items]
                )
                for idx, serialized_data, ts in zip(
                    range(len(items)), result, tss
                ):
                    if serialized_data is None and include_not_found:
                        data = {"id": items[idx][0]}
                    else:
                        data = json.loads(serialized_data)

                    task = Task(
                        tiger, queue=queue, _data=data, _state=state, _ts=ts
                    )
                    tasks.append(task)

        return n, tasks

    @classmethod
    def queue_from_function(cls, func: Any, tiger: "TaskTiger") -> str:
        """Get queue from function."""
        return getattr(func, "_task_queue", tiger.config["DEFAULT_QUEUE"])

    def n_executions(self) -> int:
        """
        Queries and returns the number of past task executions.
        """
        pipeline = self.tiger.connection.pipeline()
        pipeline.exists(self.tiger._key("task", self.id))
        pipeline.get(self.tiger._key("task", self.id, "executions_count"))

        exists, executions_count = pipeline.execute()
        if not exists:
            raise TaskNotFound("Task {} not found.".format(self.id))

        return int(executions_count or 0)

    def retry(self) -> None:
        """
        Retries a task that's in the error queue.

        Raises TaskNotFound if the task could not be found in the ERROR
        queue.
        """
        self._move(from_state=ERROR, to_state=QUEUED)

    def cancel(self) -> None:
        """
        Cancels a task that is queued in the SCHEDULED queue.

        Raises TaskNotFound if the task could not be found in the SCHEDULED
        queue.
        """
        self._move(from_state=SCHEDULED)

    def delete(self) -> None:
        """
        Removes a task that's in the error queue.

        Raises TaskNotFound if the task could not be found in the ERROR
        queue.
        """
        self._move(from_state=ERROR)

    def clone(self) -> "Task":
        """Returns a clone of the this task"""
        return type(self)(
            tiger=self.tiger,
            func=self.func,
            queue=self.queue,
            _state=self._state,
            _ts=self._ts,
            _executions=copy.copy(self._executions),
            _data=copy.copy(self._data),
        )

    def _queue_for_next_period(self) -> float:
        now = datetime.datetime.utcnow()
        schedule = self.func._task_schedule  # type: ignore[attr-defined]
        if callable(schedule):
            schedule_func = schedule
            schedule_args = ()
        else:
            schedule_func, schedule_args = schedule
        when = schedule_func(now, *schedule_args)
        if when:
            # recalculate the unique id so that malformed ids don't persist
            # between executions
            task = self.clone()
            task._data["id"] = gen_unique_id(
                task.serialized_func, task.args, task.kwargs
            )
            task.delay(when=when)
        return when
