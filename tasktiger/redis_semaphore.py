"""Redis Semaphore lock."""

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
        """Release semaphore."""

        self.redis.zrem(self.name, self.id)

    def acquire(self):
        """Obtain a semaphore lock."""

        acquired, locks = self._semaphore(keys=[self.name],
                                          args=[self.id, self.max,
                                                self.timeout, time.time()])

        # Convert Lua boolean returns to Python booleans
        acquired = True if acquired == 1 else False

        return acquired, locks

    def renew(self):
        """
        Attempt to renew semaphore.

        Technically this doesn't know the difference between losing the lock
        but then successfully getting a new lock versus renewing your lock
        before the timeout. Both will return True.
        """

        return self.acquire()
