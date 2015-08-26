import importlib
import hashlib
import json
import os

# Queue types
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
    return os.urandom(32).encode('hex')

def gen_unique_id(serialized_name, args, kwargs):
    return hashlib.sha256(json.dumps({
        'func': serialized_name,
        'args': args,
        'kwargs': kwargs,
    }, sort_keys=True)).hexdigest()

def serialize_func_name(func):
    if func.__module__ == '__main__':
        raise ValueError('Functions from the __main__ module cannot be '
                         'processed by workers.')
    return '.'.join([func.__module__, func.__name__])
