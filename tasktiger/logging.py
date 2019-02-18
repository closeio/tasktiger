from ._internal import g


def tasktiger_processor(logger, method_name, event_dict):
    """
    TaskTiger structlog processor.

    Inject the current task id for non-batch tasks.
    """

    if g['current_tasks'] is not None and not g['current_task_is_batch']:
        event_dict['task_id'] = g['current_tasks'][0].id

    return event_dict
