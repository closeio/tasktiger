from ._internal import g


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
