from ._internal import g


def tasktiger_processor(logger, method_name, event_dict):
    """
    TaskTiger structlog processor.

    Inject the current task id for non-batch tasks.
    """

    if not g['current_task_is_batch'] and g['current_tasks'] is not None:
        event_dict['task_id'] = g['current_tasks'][0].id
    elif g['current_task_is_batch'] and g['current_batch_task'] is not None:
        event_dict['task_id'] = g['current_batch_task'].id

    return event_dict
