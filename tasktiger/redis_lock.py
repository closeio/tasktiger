from redis.lock import LockError
import time

# TODO: Switch to Redlock (http://redis.io/topics/distlock) once the following
# bugs are fixed:
# * https://github.com/andymccurdy/redis-py/issues/554
# * https://github.com/andymccurdy/redis-py/issues/629
# * https://github.com/andymccurdy/redis-py/issues/601

# For now, we're using the old-style lock pattern (based on py-redis 2.8.0)
# The class below additionally catches ValueError for better compatibility with
# new-style locks (for when we upgrade), and adds a renew() method.

class Lock(object):
    """
    A shared, distributed Lock. Using Redis for locking allows the Lock
    to be shared across processes and/or machines.

    It's left to the user to resolve deadlock issues and make sure
    multiple clients play nicely together.
    """

    LOCK_FOREVER = float(2 ** 31 + 1)  # 1 past max unix time

    def __init__(self, redis, name, timeout=None, sleep=0.1):
        """
        Create a new Lock instnace named ``name`` using the Redis client
        supplied by ``redis``.

        ``timeout`` indicates a maximum life for the lock.
        By default, it will remain locked until release() is called.

        ``sleep`` indicates the amount of time to sleep per loop iteration
        when the lock is in blocking mode and another client is currently
        holding the lock.

        Note: If using ``timeout``, you should make sure all the hosts
        that are running clients have their time synchronized with a network
        time service like ntp.
        """
        self.redis = redis
        self.name = name
        self.acquired_until = None
        self.timeout = timeout
        self.sleep = sleep
        if self.timeout and self.sleep > self.timeout:
            raise LockError("'sleep' must be less than 'timeout'")

    def __enter__(self):
        return self.acquire()

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()

    def acquire(self, blocking=True):
        """
        Use Redis to hold a shared, distributed lock named ``name``.
        Returns True once the lock is acquired.

        If ``blocking`` is False, always return immediately. If the lock
        was acquired, return True, otherwise return False.
        """
        sleep = self.sleep
        timeout = self.timeout
        while 1:
            unixtime = time.time()
            if timeout:
                timeout_at = unixtime + timeout
            else:
                timeout_at = Lock.LOCK_FOREVER
            timeout_at = float(timeout_at)
            if self.redis.setnx(self.name, timeout_at):
                self.acquired_until = timeout_at
                return True
            # We want blocking, but didn't acquire the lock
            # check to see if the current lock is expired
            try:
                existing = float(self.redis.get(self.name) or 1)
            except ValueError:
                existing = Lock.LOCK_FOREVER

            if existing < unixtime:
                # the previous lock is expired, attempt to overwrite it
                try:
                    existing = float(self.redis.getset(self.name, timeout_at) or 1)
                except ValueError:
                    existing = Lock.LOCK_FOREVER
                if existing < unixtime:
                    # we successfully acquired the lock
                    self.acquired_until = timeout_at
                    return True
            if not blocking:
                return False
            time.sleep(sleep)

    def release(self):
        "Releases the already acquired lock"
        if self.acquired_until is None:
            raise ValueError("Cannot release an unlocked lock")
        existing = float(self.redis.get(self.name) or 1)
        # if the lock time is in the future, delete the lock
        if existing >= self.acquired_until:
            self.redis.delete(self.name)
        self.acquired_until = None

    def renew(self, timeout=None):
        if timeout is None:
            timeout = self.timeout

        if timeout:
            unixtime = int(time.time())
            timeout_at = unixtime + timeout
            self.redis.getset(self.name, timeout_at)
            self.acquired_until = timeout_at

# For now unused:
# New-style Lock with renew() method (andymccurdy/redis-py#629)
# XXX: when upgrading to the new-style class, take old-style locks into account
from redis import WatchError
from redis.lock import Lock as RedisLock
class NewStyleLock(RedisLock):
    def renew(self, new_timeout):
        """
        Sets a new timeout for an already acquired lock.

        ``new_timeout`` can be specified as an integer or a float, both
        representing the number of seconds.
        """
        if self.local.token is None:
            raise LockError("Cannot extend an unlocked lock")
        if self.timeout is None:
            raise LockError("Cannot extend a lock with no timeout")
        return self.do_renew(new_timeout)

    def do_renew(self, new_timeout):
        pipe = self.redis.pipeline()
        pipe.watch(self.name)
        lock_value = pipe.get(self.name)
        if lock_value != self.local.token:
            raise LockError("Cannot extend a lock that's no longer owned")
        pipe.multi()
        pipe.pexpire(self.name, int(new_timeout * 1000))

        try:
            response = pipe.execute()
        except WatchError:
            # someone else acquired the lock
            raise LockError("Cannot extend a lock that's no longer owned")
        if not response[0]:
            # pexpire returns False if the key doesn't exist
            raise LockError("Cannot extend a lock that's no longer owned")
        return True
