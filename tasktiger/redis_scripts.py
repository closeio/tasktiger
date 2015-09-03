# ARGV = { score member }
ZADD_NOUPDATE = """
    if not redis.call('zscore', KEYS[1], ARGV[2]) then
        return redis.call('zadd', KEYS[1], ARGV[1], ARGV[2])
    end
"""

_ZPOPPUSH_TEMPLATE = """
    local members = redis.call('zrangebyscore', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
    local new_scoremembers = {{}}
    for i, member in ipairs(members) do
        new_scoremembers[2*i] = member
        new_scoremembers[2*i-1] = ARGV[3]
    end
    if #members > 0 then
        redis.call('zremrangebyrank', KEYS[1], 0, #members-1)
        redis.call('zadd', KEYS[2], unpack(new_scoremembers))
        {on_success}
    end
    return members
"""

# ARGV = { score, count, new_score }
ZPOPPUSH = _ZPOPPUSH_TEMPLATE.format(on_success='')

# ARGV = { score, count, new_score, set_value }
ZPOPPUSH_UPDATE_SETS = _ZPOPPUSH_TEMPLATE.format(on_success="""
    local src_exists = redis.call('exists', KEYS[1])
    if src_exists == 0 then
        redis.call('srem', KEYS[3], ARGV[4])
    end
    redis.call('sadd', KEYS[4], ARGV[4])
""")

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

# KEYS = { key, other_key }
# ARGV = { member }
SREM_IF_NOT_EXISTS = """
    local exists = redis.call('exists', KEYS[2])
    local result
    if exists == 0 then
        result = redis.call('srem', KEYS[1], ARGV[1])
    else
        result = 0
    end
    return result
"""

# KEYS = { key, zset1 [, ..., zsetN] }
# ARGV = { member }
DELETE_IF_NOT_IN_ZSETS = """
    local found = 0
    for i=2,#KEYS do
        if redis.call('zscore', KEYS[i], ARGV[1]) then
            found = 1
            break
        end
    end
    if found == 0 then
        redis.call('del', KEYS[1])
    end
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
        self._zpoppush_update_sets = redis.register_script(ZPOPPUSH_UPDATE_SETS)
        self._zpoppush_withscores = redis.register_script(ZPOPPUSH_WITHSCORES)
        self._srem_if_not_exists = redis.register_script(SREM_IF_NOT_EXISTS)
        self._delete_if_not_in_zsets = redis.register_script(DELETE_IF_NOT_IN_ZSETS)
        self._multilock_acquire = redis.register_script(MULTILOCK_ACQUIRE)
        self._multilock_release = redis.register_script(MULTILOCK_RELEASE)
        self._multilock_renew = redis.register_script(MULTILOCK_RENEW)

    def zadd_noupdate(self, key, score, member, client=None):
        """
        Like ZADD, but doesn't update the score of a member if the member
        already exists in the set.
        """
        return self._zadd_noupdate(keys=[key], args=[score, member], client=client)

    def zpoppush(self, source, destination, count, score, new_score,
                 client=None, withscores=False, on_success=None):
        """
        Pops the first ``count`` members from the ZSET ``source`` and adds them
        to the ZSET ``destination`` with a score of ``new_score``. If ``score``
        is not None, only members up to a score of ``score`` are used. Returns
        the members that were moved and, if ``withscores`` is True, their
        original scores. If items were moved, the action defined in
        ``on_success`` is executed.
        """
        if score is None:
            score = '+inf' # Include all elements.
        if withscores:
            if on_success:
                raise NotImplementedError()
            return self._zpoppush_withscores(
                keys=[source, destination],
                args=[score, count, new_score],
                client=client)
        else:
            if on_success:
                if on_success[0] != 'update_sets':
                    raise NotImplementedError()
                else:
                    return self._zpoppush_update_sets(
                        keys=[source, destination, on_success[1],
                              on_success[2]],
                        args=[score, count, new_score, on_success[3]],
                        client=client)
            else:
                return self._zpoppush(
                    keys=[source, destination],
                    args=[score, count, new_score],
                    client=client)

    def srem_if_not_exists(self, key, member, other_key, client=None):
        """
        Removes ``member`` from the set ``key`` if ``other_key`` does not
        exist (i.e. is empty). Returns the number of removed elements (0 or 1).
        """
        return self._srem_if_not_exists(
            keys=[key, other_key],
            args=[member],
            client=client)

    def delete_if_not_in_zsets(self, key, member, set_list, client=None):
        """
        Removes ``key`` only if ``member`` is not member of any sets in the
        ``set_list``. Returns the number of removed elements (0 or 1).
        """
        return self._delete_if_not_in_zsets(
            keys=[key]+set_list,
            args=[member],
            client=client)

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
