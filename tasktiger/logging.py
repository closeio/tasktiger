from ._internal import g


def tasktiger_processor(logger, method_name, event_dict):
    """
    TaskTiger structlog processor.

    Inject the current task id for non-batch tasks.
    """

    if not g['current_task_is_batch'] and g['current_tasks'] is not None and :
        event_dict['task_id'] = g['current_tasks'][0].id
    elif g['current_task_is_batch'] and g['current_batch_task'] is not None:
        event_dict['task_id'] = g['current_batch_task'].id

    return event_dict


def batch_param_iterator(params):
    """
    Helper to set current batch task.

    This helper should be used in conjunction with tasktiger_processor
    to facilitate logging of task ids.
    """
    for i, p in enumerate(params):
        g['current_batch_task'] = g['current_tasks'][i]
        yield p
    g['current_batch_task'] = None
