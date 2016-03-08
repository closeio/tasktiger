# ARGV = { score, member }
ZADD_NOUPDATE_TEMPLATE = """
    if {condition} redis.call('zscore', {key}, {member}) then
        redis.call('zadd', {key}, {score}, {member})
    end
"""
ZADD_NOUPDATE =  ZADD_NOUPDATE_TEMPLATE.format(
    key='KEYS[1]', score='ARGV[1]', member='ARGV[2]', condition='not'
)
ZADD_UPDATE_EXISTING =  ZADD_NOUPDATE_TEMPLATE.format(
    key='KEYS[1]', score='ARGV[1]', member='ARGV[2]', condition=''
)
ZADD_UPDATE_TEMPLATE = """
    local score = redis.call('zscore', {key}, {member})
    local new_score
    if score then
        new_score = math.{f}(score, {score})
    else
        new_score = {score}
    end
    {ret} redis.call('zadd', {key}, new_score, {member})
"""
ZADD_UPDATE_MIN = ZADD_UPDATE_TEMPLATE.format(
    f='min', key='KEYS[1]', score='ARGV[1]', member='ARGV[2]', ret='return')
ZADD_UPDATE_MAX = ZADD_UPDATE_TEMPLATE.format(
    f='max', key='KEYS[1]', score='ARGV[1]', member='ARGV[2]', ret='return')

_ZPOPPUSH_EXISTS_TEMPLATE = """
    -- Load keys and arguments
    local source = KEYS[1]
    local destination = KEYS[2]
    local remove_from_set = KEYS[3]
    local add_to_set = KEYS[4]
    local add_to_set_if_exists = KEYS[5]
    local if_exists_key = KEYS[6]

    local score = ARGV[1]
    local count = ARGV[2]
    local new_score = ARGV[3]
    local set_value = ARGV[4]
    local if_exists_score = ARGV[5]

    -- Fetch affected members from the source set.
    local members = redis.call('zrangebyscore', source, '-inf', score, 'LIMIT', 0, count)

    -- Tables to keep track of the members that we're moving to the destination
    -- (moved_members), along with their new scores (new_scoremembers), and
    -- members that already exist at the destination (existing_members).
    local new_scoremembers = {{}}
    local moved_members = {{}}
    local existing_members = {{}}

    -- Counters so we can quickly append to the tables
    local existing_idx = 0
    local moved_idx = 0

    -- Populate the tables defined above.
    for i, member in ipairs(members) do
        if redis.call('zscore', destination, member) then
            existing_idx = existing_idx + 1
            existing_members[existing_idx] = member
        else
            moved_idx = moved_idx + 1
            new_scoremembers[2*moved_idx] = member
            new_scoremembers[2*moved_idx-1] = new_score
            moved_members[moved_idx] = member
        end
    end

    if #members > 0 then
        -- If we matched any members, remove them from the source.
        redis.call('zremrangebyrank', source, 0, #members-1)

        -- Add members to the destination.
        if #new_scoremembers > 0 then
            redis.call('zadd', destination, unpack(new_scoremembers))
        end

        -- Perform the "if exists" action for members that exist at the
        -- destination.
        for i, member in ipairs(existing_members) do
            {if_exists_template}
        end

        -- Perform any "on success" action.
        {on_success}

        -- If we moved any members to the if_exists_key, add the set_value
        -- to the add_to_set_if_exists (see zpoppush docstring).
        local if_exists_key_exists = redis.call('exists', if_exists_key)
        if if_exists_key_exists == 1 then
            redis.call('sadd', add_to_set_if_exists, set_value)
        end

    end

    -- Return just the moved members.
    return moved_members
"""

_ON_SUCCESS_UPDATE_SETS_TEMPLATE = """
    local src_exists = redis.call('exists', source)
    if src_exists == 0 then
        redis.call('srem', {remove_from_set}, {set_value})
    end
    redis.call('sadd', {add_to_set}, {set_value})
"""

# KEYS = { source, destination, remove_from_set, add_to_set,
#          add_to_set_if_exists, if_exists_key }
# ARGV = { score, count, new_score, set_value, if_exists_score }
ZPOPPUSH_EXISTS_MIN_UPDATE_SETS = _ZPOPPUSH_EXISTS_TEMPLATE.format(
    if_exists_template=ZADD_UPDATE_TEMPLATE.format(
        f='min', key='if_exists_key', score='if_exists_score',
        member='member', ret=''),
    on_success=_ON_SUCCESS_UPDATE_SETS_TEMPLATE.format(
        set_value='set_value', add_to_set='add_to_set',
        remove_from_set='remove_from_set')
)

# KEYS = { source, destination, remove_from_set, add_to_set }
# ARGV = { score, count, new_score, set_value }
ZPOPPUSH_EXISTS_IGNORE_UPDATE_SETS = _ZPOPPUSH_EXISTS_TEMPLATE.format(
    if_exists_template='',
    on_success=_ON_SUCCESS_UPDATE_SETS_TEMPLATE.format(
        set_value='set_value', add_to_set='add_to_set',
        remove_from_set='remove_from_set')
)

# KEYS = { source, destination, ... }
# ARGV = { score, count, new_score, ... }
_ZPOPPUSH_TEMPLATE = """
    -- Load keys and arguments
    local source = KEYS[1]
    local destination = KEYS[2]

    local score = ARGV[1]
    local count = ARGV[2]
    local new_score = ARGV[3]

    -- Fetch affected members from the source set.
    local members = redis.call('zrangebyscore', source, '-inf', score, 'LIMIT', 0, count)

    -- Table to keep track of the members along with their new scores, which is
    -- passed to ZADD.
    local new_scoremembers = {{}}
    for i, member in ipairs(members) do
        new_scoremembers[2*i] = member
        new_scoremembers[2*i-1] = new_score
    end

    if #members > 0 then
        -- Remove affected members and add them to the destination.
        redis.call('zremrangebyrank', source, 0, #members-1)
        redis.call('zadd', destination, unpack(new_scoremembers))

        -- Perform any "on success" action.
        {on_success}
    end

    -- Return moved members
    return members
"""

ZPOPPUSH = _ZPOPPUSH_TEMPLATE.format(on_success='')

# KEYS = { source, destination, remove_from_set, add_to_set }
# ARGV = { score, count, new_score, set_value }
ZPOPPUSH_UPDATE_SETS = _ZPOPPUSH_TEMPLATE.format(on_success=
    _ON_SUCCESS_UPDATE_SETS_TEMPLATE.format(
        set_value='ARGV[4]',
        add_to_set='KEYS[4]',
        remove_from_set='KEYS[3]',
))

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
        return redis.call('del', KEYS[1])
    end
    return 0
"""

# KEYS = { key }
# ARGV = { member }
FAIL_IF_NOT_IN_ZSET = """
    assert(redis.call('zscore', KEYS[1], ARGV[1]), '<FAIL_IF_NOT_IN_ZSET>')
"""

class RedisScripts(object):
    def __init__(self, redis):
        self._zadd_noupdate = redis.register_script(ZADD_NOUPDATE)
        self._zadd_update_existing = redis.register_script(ZADD_UPDATE_EXISTING)
        self._zadd_update_min = redis.register_script(ZADD_UPDATE_MIN)
        self._zadd_update_max = redis.register_script(ZADD_UPDATE_MAX)

        self._zpoppush = redis.register_script(ZPOPPUSH)
        self._zpoppush_update_sets = redis.register_script(ZPOPPUSH_UPDATE_SETS)
        self._zpoppush_withscores = redis.register_script(ZPOPPUSH_WITHSCORES)
        self._zpoppush_exists_min_update_sets = redis.register_script(
            ZPOPPUSH_EXISTS_MIN_UPDATE_SETS)
        self._zpoppush_exists_ignore_update_sets = redis.register_script(
            ZPOPPUSH_EXISTS_IGNORE_UPDATE_SETS)

        self._srem_if_not_exists = redis.register_script(SREM_IF_NOT_EXISTS)

        self._delete_if_not_in_zsets = redis.register_script(
            DELETE_IF_NOT_IN_ZSETS)

        self._fail_if_not_in_zset = redis.register_script(
            FAIL_IF_NOT_IN_ZSET)

    def zadd(self, key, score, member, mode, client=None):
        """
        Like ZADD, but supports different score update modes, in case the
        member already exists in the ZSET:
        - "nx": Don't update the score
        - "xx": Only update elements that already exist. Never add elements.
        - "min": Use the smaller of the given and existing score
        - "max": Use the larger of the given and existing score
        """
        if mode == 'nx':
            f = self._zadd_noupdate
        elif mode == 'xx':
            f = self._zadd_update_existing
        elif mode == 'min':
            f = self._zadd_update_min
        elif mode == 'max':
            f = self._zadd_update_max
        else:
            raise NotImplementedError('mode "%s" unsupported' % mode)
        return f(keys=[key], args=[score, member], client=client)

    def zpoppush(self, source, destination, count, score, new_score,
                 client=None, withscores=False, on_success=None,
                 if_exists=None):
        """
        Pops the first ``count`` members from the ZSET ``source`` and adds them
        to the ZSET ``destination`` with a score of ``new_score``. If ``score``
        is not None, only members up to a score of ``score`` are used. Returns
        the members that were moved and, if ``withscores`` is True, their
        original scores.

        If items were moved, the action defined in ``on_success`` is executed.
        The only implemented option is a tuple in the form ('update_sets',
        ``set_value``, ``remove_from_set``, ``add_to_set``
        [, ``add_to_set_if_exists``]).
        If no items are left in the ``source`` ZSET, the ``set_value`` is
        removed from ``remove_from_set``. If any items were moved to the
        ``destination`` ZSET, the ``set_value`` is added to ``add_to_set``. If
        any items were moved to the ``if_exists_key`` ZSET (see below), the
        ``set_value`` is added to the ``add_to_set_if_exists`` set.

        If ``if_exists`` is specified as a tuple ('add', if_exists_key,
        if_exists_score, if_exists_mode), then members that are already in the
        ``destination`` set will not be returned or updated, but they will be
        added to a ZSET ``if_exists_key`` with a score of ``if_exists_score``
        and the given behavior specified in ``if_exists_mode`` for members that
        already exist in the ``if_exists_key`` ZSET. ``if_exists_mode`` can be
        one of the following:
        - "nx": Don't update the score
        - "min": Use the smaller of the given and existing score
        - "max": Use the larger of the given and existing score

        If ``if_exists`` is specified as a tuple ('noupdate',), then no action
        will be taken for members that are already in the ``destination`` ZSET
        (their score will not be updated).
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
            if if_exists and if_exists[0] == 'add':
                _, if_exists_key, if_exists_score, if_exists_mode = if_exists
                if if_exists_mode != 'min':
                    raise NotImplementedError()

                if not on_success or on_success[0] != 'update_sets':
                    raise NotImplementedError()
                set_value, remove_from_set, add_to_set, add_to_set_if_exists \
                    = on_success[1:]

                return self._zpoppush_exists_min_update_sets(
                        keys=[source, destination, remove_from_set, add_to_set,
                              add_to_set_if_exists, if_exists_key],
                        args=[score, count, new_score, set_value, if_exists_score],
                )
            elif if_exists and if_exists[0] == 'noupdate':
                if not on_success or on_success[0] != 'update_sets':
                    raise NotImplementedError()
                set_value, remove_from_set, add_to_set \
                    = on_success[1:]

                return self._zpoppush_exists_ignore_update_sets(
                        keys=[source, destination, remove_from_set, add_to_set],
                        args=[score, count, new_score, set_value],
                )

            if on_success:
                if on_success[0] != 'update_sets':
                    raise NotImplementedError()
                else:
                    set_value, remove_from_set, add_to_set = on_success[1:]
                    return self._zpoppush_update_sets(
                        keys=[source, destination, remove_from_set, add_to_set],
                        args=[score, count, new_score, set_value],
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

    def fail_if_not_in_zset(self, key, member, client=None):
        """
        Fails with an error containing the string '<FAIL_IF_NOT_IN_ZSET>' if
        the given ``member`` is not in the ZSET ``key``. This can be used in
        a pipeline to assert that the member is in the ZSET and cancel the
        execution otherwise.
        """
        self._fail_if_not_in_zset(keys=[key], args=[member], client=client)
