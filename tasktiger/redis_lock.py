from redis.lock import Lock as RedisLock

# https://github.com/andymccurdy/redis-py/issues/629
class Lock(RedisLock):
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
