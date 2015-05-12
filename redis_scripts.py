import time
from redis.client import Lock, LockError

# ARGV = { score member }
ZADD_NOUPDATE = """
    if not redis.call('zscore', KEYS[1], ARGV[2]) then
        return redis.call('zadd', KEYS[1], ARGV[1], ARGV[2])
    end
"""

# ARGV = { score, count, new_score }
ZPOPPUSH = """
    local members = redis.call('zrangebyscore', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
    local new_scoremembers = {}
    for i, member in ipairs(members) do
        new_scoremembers[2*i] = member
        new_scoremembers[2*i-1] = ARGV[3]
    end
    if #members > 0 then
        redis.call('zremrangebyrank', KEYS[1], 0, #members-1)
        redis.call('zadd', KEYS[2], unpack(new_scoremembers))
    end
    return members
"""

# ARGV = { score, count, new_score }
ZPOPPUSH_WITHSCORES = """
    local members_scores = redis.call('zrangebyscore', KEYS[1], '-inf', ARGV[1], 'WITHSCORES', 'LIMIT', 0, ARGV[2])
    local new_scoremembers = {}
    for i, member in ipairs(members_scores) do
        if i % 2 == 1 then  -- Just the members (1, 3, 5, ...)
            -- Insert scores at 1, 3, 5, ...
            new_scoremembers[i] = ARGV[3]

            -- Insert members at 2, 4, 6, ...
            new_scoremembers[i+1] = member
        end
    end
    if #members_scores > 0 then
        redis.call('zremrangebyrank', KEYS[1], 0, #members_scores/2-1)
        redis.call('zadd', KEYS[2], unpack(new_scoremembers))
    end
    return members_scores
"""

# ARGV = { unixtime, timeout_at }
MULTILOCK_ACQUIRE = """
    local keys = {} -- successfully acquired locks
    local val
    for i=1,#KEYS do
        if redis.call('setnx', KEYS[i], ARGV[2]) == 0 then
            -- key exists, check if expired
            val = redis.call('get', KEYS[i])
            if val and val < ARGV[1] then
                val = redis.call('getset', KEYS[i], ARGV[2])
                if val and val < ARGV[1] then
                    keys[#keys+1] = KEYS[i] -- acquired
                end
            end
        else -- setnx == 1
            keys[#keys+1] = KEYS[i] -- acquired
        end
    end
    return keys
"""

# ARGV = { timeout_at }
MULTILOCK_RELEASE = """
    local keys = {} -- successfully released locks
    local val
    for i=1,#KEYS do
        val = redis.call('get', KEYS[i])
        if val and val >= ARGV[1] then
            redis.call('del', KEYS[i])
            keys[#keys+1] = KEYS[i]
        end
    end
    return keys
"""

# ARGV = { original_timeout_at, timeout_at }
MULTILOCK_RENEW = """
    local keys = {} -- successfully renewed locks
    local val
    for i=1,#KEYS do
        val = redis.call('get', KEYS[i])
        if val and val == ARGV[1] then
            redis.call('set', KEYS[i], ARGV[2])
            keys[#keys+1] = KEYS[i]
        end
    end
    return keys
"""

class RedisScripts(object):
    def __init__(self, redis):
        self._zadd_noupdate = redis.register_script(ZADD_NOUPDATE)
        self._zpoppush = redis.register_script(ZPOPPUSH)
        self._zpoppush_withscores = redis.register_script(ZPOPPUSH_WITHSCORES)
        self._multilock_acquire = redis.register_script(MULTILOCK_ACQUIRE)
        self._multilock_release = redis.register_script(MULTILOCK_RELEASE)
        self._multilock_renew = redis.register_script(MULTILOCK_RENEW)

    def zadd_noupdate(self, key, score, member, client=None):
        """
        Like ZADD, but doesn't update the score of a member if the member
        already exists in the set.
        """
        return self._zadd_noupdate(keys=[key], args=[score, member], client=client)

    def zpoppush(self, source, destination, count, score, new_score, client=None, withscores=False):
        """
        Pops the first ``count`` members from the ZSET ``source`` and adds them
        to the ZSET ``destination`` with a score of ``new_score``. If ``score``
        is not None, only members up to a score of ``score`` are used. Returns
        the members that were moved and, if ``withscores`` is True, their
        original scores.
        """
        if score is None:
            score = '+inf' # Include all elements.
        if withscores:
            return self._zpoppush_withscores(keys=[source, destination], args=[score, count, new_score], client=client)
        else:
            return self._zpoppush(keys=[source, destination], args=[score, count, new_score], client=client)

    def multilock_acquire(self, now, timeout_at, keys, client=None):
        """
        Acquires the lock for the given keys and returns the keys which were
        successfully locked.
        """
        return self._multilock_acquire(keys=keys, args=[now, timeout_at], client=client)

    def multilock_release(self, timeout_at, keys, client=None):
        """
        Releases the lock for the given keys. Returns the keys that were
        deleted.
        """
        return self._multilock_release(keys=keys, args=[timeout_at], client=client)

    def multilock_renew(self, original_timeout_at, timeout_at, keys, client=None):
        """
        Renews the lock for the given keys. Returns the keys that were renewed.
        """
        return self._multilock_renew(keys=keys, args=[original_timeout_at, timeout_at], client=client)


class MultiLock(object):
    """
    Non-blocking Redis lock that efficiently creates locks for multiple keys
    at a time.
    """

    # TODO: shouldn't depend on app

    def __init__(self, redis, keys, timeout):
        """
        Create a new MultiLock instance for the given ``keys`` using the Redis
        client supplied by ``redis``.

        ``timeout`` indicates a maximum life for the lock.
        By default, it will remain locked until release() is called.

        Note: If using ``timeout``, you should make sure all the hosts
        that are running clients are within the same timezone and are using
        a network time service like ntp.
        """

        self.redis = redis
        self.keys = keys
        self.timeout = timeout

        self.acquired_keys = []
        self.acquired_until = None

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()

    def acquire(self):
        """
        Returns the list of keys for which a lock was acquired.
        """
        from closeio.main import app
        now = int(time.time())
        if self.timeout:
            timeout_at = now + self.timeout
        else:
            timeout_at = Lock.LOCK_FOREVER
        self.acquired_keys = app.redis_scripts.multilock_acquire(now, timeout_at, self.keys, client=self.redis)
        self.acquired_until = timeout_at
        return self.acquired_keys

    def release(self):
        "Releases the already acquired lock"
        from closeio.main import app
        if self.acquired_until and self.acquired_keys:
            app.redis_scripts.multilock_release(self.acquired_until, self.acquired_keys, client=self.redis)
            self.acquired_until = None
            self.acquired_keys = []

    def renew(self):
        "Renews the already acquired lock"
        from closeio.main import app
        now = int(time.time())
        if self.timeout:
            timeout_at = now + self.timeout
        else:
            return # no need to renew
        if self.acquired_until and self.acquired_keys:
            acquired_keys = app.redis_scripts.multilock_renew(self.acquired_until, timeout_at, self.acquired_keys, client=self.redis)

            if acquired_keys != self.acquired_keys:
                exc = LockError("Could not fully renew MultiLock")
            else:
                exc = None

            self.acquired_until = timeout_at
            self.acquired_keys = acquired_keys

            if exc:
                raise exc
