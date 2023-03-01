from ._internal import g


def tasktiger_processor(logger, method_name, event_dict):
    """
    TaskTiger structlog processor.

    Inject the current task ID and queue for non-batch tasks.
    """

    if g["current_tasks"] is not None and not g["current_task_is_batch"]:
        current_task = g["current_tasks"][0]
        event_dict["task_id"] = current_task.id
        event_dict["task_func"] = current_task.serialized_func
        event_dict["queue"] = current_task.queue

    return event_dict
