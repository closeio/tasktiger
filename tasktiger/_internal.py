import importlib
import hashlib
import json
import os

# Task states (represented by different queues)
# Note some client code may rely on the string values (e.g. get_queue_stats).
QUEUED = 'queued'
ACTIVE = 'active'
SCHEDULED = 'scheduled'
ERROR = 'error'

# from rq
def import_attribute(name):
    """Return an attribute from a dotted path name (e.g. "path.to.func")."""
    module_name, attribute = name.rsplit('.', 1)
    module = importlib.import_module(module_name)
    return getattr(module, attribute)

def gen_id():
    """
    Generates and returns a random hex-encoded 256-bit unique ID.
    """
    return os.urandom(32).encode('hex')

def gen_unique_id(serialized_name, args, kwargs):
    """
    Generates and returns a hex-encoded 256-bit ID for the given task name and
    args. Used to generate IDs for unique tasks or for task locks.
    """
    return hashlib.sha256(json.dumps({
        'func': serialized_name,
        'args': args,
        'kwargs': kwargs,
    }, sort_keys=True)).hexdigest()

def serialize_func_name(func):
    """
    Returns the dotted serialized path to the passed function.
    """
    if func.__module__ == '__main__':
        raise ValueError('Functions from the __main__ module cannot be '
                         'processed by workers.')
    return '.'.join([func.__module__, func.__name__])
