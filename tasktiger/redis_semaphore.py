"""Redis Semaphore lock."""

import os
import time

SYSTEM_LOCK_ID = "SYSTEM_LOCK"


class Semaphore:
    """Semaphore lock using Redis ZSET."""

    def __init__(self, redis, name, lock_id, timeout, max_locks=1):
        """
        Semaphore lock.

        Semaphore logic is implemented in the lua/semaphore.lua script.
        Individual locks within the semaphore are managed inside a ZSET
        using scores to track when they expire.

        Arguments:
            redis: Redis client
            name: Name of lock. Used as ZSET key.
            lock_id: Lock ID
            timeout: Timeout in seconds
            max_locks: Maximum number of locks allowed for this semaphore
        """

        self.redis = redis
        self.name = name
        self.lock_id = lock_id
        self.max_locks = max_locks
        self.timeout = timeout
        with open(
            os.path.join(
                os.path.dirname(os.path.realpath(__file__)),
                "lua/semaphore.lua",
            )
        ) as f:
            self._semaphore = self.redis.register_script(f.read())

    @classmethod
    def get_system_lock(cls, redis, name):
        """
        Get system lock timeout for the semaphore.

        Arguments:
            redis: Redis client
            name: Name of lock. Used as ZSET key.

        Returns: Time system lock expires or None if lock does not exist
        """

        return redis.zscore(name, SYSTEM_LOCK_ID)

    @classmethod
    def set_system_lock(cls, redis, name, timeout):
        """
        Set system lock for the semaphore.

        Sets a system lock that will expire in timeout seconds. This
        overrides all other locks. Existing locks cannot be renewed
        and no new locks will be permitted until the system lock
        expires.

        Arguments:
            redis: Redis client
            name: Name of lock. Used as ZSET key.
            timeout: Timeout in seconds for system lock
        """

        pipeline = redis.pipeline()
        pipeline.zadd(name, {SYSTEM_LOCK_ID: time.time() + timeout})
        pipeline.expire(
            name, timeout + 10
        )  # timeout plus buffer for troubleshooting
        pipeline.execute()

    def release(self):
        """Release semaphore."""

        self.redis.zrem(self.name, self.lock_id)

    def acquire(self):
        """
        Obtain a semaphore lock.

        Returns: Tuple that contains True/False if the lock was acquired and number of
                 locks in semaphore.
        """

        acquired, locks = self._semaphore(
            keys=[self.name],
            args=[self.lock_id, self.max_locks, self.timeout, time.time()],
        )

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
