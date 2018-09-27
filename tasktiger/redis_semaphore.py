import os
import time

class Semaphore(object):
    def __init__(self, redis, name, id, timeout, max=1):
        self.redis = redis
        self.name = name
        self.id = id
        self.max = max
        self.timeout = timeout
        with open(os.path.join(os.path.dirname(os.path.realpath(__file__)),
                               'lua/semaphore.lua')) as f:
            self._semaphore = self.redis.register_script(f.read())

    def release(self):
        self.redis.zrem(self.name, self.id)

    def acquire(self):
        """Obtain a semaphore lock."""
        return self._semaphore(keys=[self.name], args=[self.id, self.max,
                                                       self.timeout, time.time()])

    def renew(self):
        return self.acquire()
