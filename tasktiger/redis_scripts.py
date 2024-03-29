import os
from typing import Any, Callable, List, Literal, Optional, Tuple, Union

from redis import Redis

from ._internal import ACTIVE, ERROR, QUEUED, SCHEDULED

try:
    from redis.commands.core import Script
except ImportError:
    # redis-py < 4.0 : https://github.com/redis/redis-py/pull/1534
    from redis.client import Script  # type: ignore


LOCAL_FUNC_TEMPLATE = """
local function {func_name}(KEYS, ARGV)
    {func_body}
end

"""

# ARGV = { score, member }
ZADD_NOUPDATE_TEMPLATE = """
    if {condition} redis.call('zscore', {key}, {member}) then
        redis.call('zadd', {key}, {score}, {member})
    end
"""
ZADD_NOUPDATE = ZADD_NOUPDATE_TEMPLATE.format(
    key="KEYS[1]", score="ARGV[1]", member="ARGV[2]", condition="not"
)
ZADD_UPDATE_EXISTING = ZADD_NOUPDATE_TEMPLATE.format(
    key="KEYS[1]", score="ARGV[1]", member="ARGV[2]", condition=""
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
    f="min", key="KEYS[1]", score="ARGV[1]", member="ARGV[2]", ret="return"
)
ZADD_UPDATE_MAX = ZADD_UPDATE_TEMPLATE.format(
    f="max", key="KEYS[1]", score="ARGV[1]", member="ARGV[2]", ret="return"
)

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
        if if_exists_key then
            local if_exists_key_exists = redis.call('exists', if_exists_key)
            if if_exists_key_exists == 1 then
                redis.call('sadd', add_to_set_if_exists, set_value)
            end
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
        f="min",
        key="if_exists_key",
        score="if_exists_score",
        member="member",
        ret="",
    ),
    on_success=_ON_SUCCESS_UPDATE_SETS_TEMPLATE.format(
        set_value="set_value",
        add_to_set="add_to_set",
        remove_from_set="remove_from_set",
    ),
)

# KEYS = { source, destination, remove_from_set, add_to_set }
# ARGV = { score, count, new_score, set_value }
ZPOPPUSH_EXISTS_IGNORE_UPDATE_SETS = _ZPOPPUSH_EXISTS_TEMPLATE.format(
    if_exists_template="",
    on_success=_ON_SUCCESS_UPDATE_SETS_TEMPLATE.format(
        set_value="set_value",
        add_to_set="add_to_set",
        remove_from_set="remove_from_set",
    ),
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

ZPOPPUSH = _ZPOPPUSH_TEMPLATE.format(on_success="")

# KEYS = { source, destination, remove_from_set, add_to_set }
# ARGV = { score, count, new_score, set_value }
ZPOPPUSH_UPDATE_SETS = _ZPOPPUSH_TEMPLATE.format(
    on_success=_ON_SUCCESS_UPDATE_SETS_TEMPLATE.format(
        set_value="ARGV[4]", add_to_set="KEYS[4]", remove_from_set="KEYS[3]"
    )
)

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

# KEYS = { del1, [, ..., delN], zset1 [, ..., zsetN] }
# ARGV = { to_delete_count, value }
DELETE_IF_NOT_IN_ZSETS = """
    local found = 0
    for i=ARGV[1] + 1,#KEYS do
        if redis.call('zscore', KEYS[i], ARGV[2]) then
            found = 1
            break
        end
    end
    if found == 0 then
        return redis.call('del', unpack(KEYS, 1, ARGV[1]))
    end
    return 0
"""

# KEYS = { key }
# ARGV = { member }
FAIL_IF_NOT_IN_ZSET = """
    assert(redis.call('zscore', KEYS[1], ARGV[1]), '<FAIL_IF_NOT_IN_ZSET>')
"""

# KEYS = { }
# ARGV = { key_prefix, time, batch_size }
GET_EXPIRED_TASKS = """
    local key_prefix = ARGV[1]
    local time = ARGV[2]
    local batch_size = ARGV[3]
    local active_queues = redis.call('smembers', key_prefix .. ':' .. 'active')
    local result = {}
    local result_n = 1

    for i=1, #active_queues do
        local queue_name = active_queues[i]
        local queue_key = key_prefix .. ':' .. 'active' ..
                                        ':' .. queue_name

        local members = redis.call('zrangebyscore',
                                   queue_key, 0, time, 'LIMIT', 0, batch_size)

        for j=1, #members do
            result[result_n] = queue_name
            result[result_n + 1] = members[j]
            result_n = result_n + 2
        end

        batch_size = batch_size - #members
        if batch_size <= 0 then
            break
        end
    end

    return result
"""


class RedisScripts:
    def __init__(self, redis: Redis) -> None:
        self.redis = redis

        self._zadd_noupdate = redis.register_script(ZADD_NOUPDATE)
        self._zadd_update_existing = redis.register_script(
            ZADD_UPDATE_EXISTING
        )
        self._zadd_update_min = redis.register_script(ZADD_UPDATE_MIN)
        self._zadd_update_max = redis.register_script(ZADD_UPDATE_MAX)

        self._zpoppush = redis.register_script(ZPOPPUSH)
        self._zpoppush_update_sets = redis.register_script(
            ZPOPPUSH_UPDATE_SETS
        )
        self._zpoppush_withscores = redis.register_script(ZPOPPUSH_WITHSCORES)
        self._zpoppush_exists_min_update_sets = redis.register_script(
            ZPOPPUSH_EXISTS_MIN_UPDATE_SETS
        )
        self._zpoppush_exists_ignore_update_sets = redis.register_script(
            ZPOPPUSH_EXISTS_IGNORE_UPDATE_SETS
        )

        self._srem_if_not_exists = redis.register_script(SREM_IF_NOT_EXISTS)

        self._delete_if_not_in_zsets = redis.register_script(
            DELETE_IF_NOT_IN_ZSETS
        )

        self._fail_if_not_in_zset = redis.register_script(FAIL_IF_NOT_IN_ZSET)

        self._get_expired_tasks = redis.register_script(GET_EXPIRED_TASKS)

        self._move_task = self.register_script_from_file(
            "lua/move_task.lua",
            include_functions={
                "zadd_noupdate": ZADD_NOUPDATE,
                "zadd_update_min": ZADD_UPDATE_MIN,
                "srem_if_not_exists": SREM_IF_NOT_EXISTS,
                "delete_if_not_in_zsets": DELETE_IF_NOT_IN_ZSETS,
            },
        )

    @property
    def can_replicate_commands(self) -> bool:
        """
        Whether Redis supports single command replication.
        """
        if not hasattr(self, "_can_replicate_commands"):
            info = self.redis.info("server")
            version_info = info["redis_version"].split(".")
            major, minor = int(version_info[0]), int(version_info[1])
            result = major > 3 or major == 3 and minor >= 2
            self._can_replicate_commands = result
        return self._can_replicate_commands

    def register_script_from_file(
        self, filename: str, include_functions: Optional[dict] = None
    ) -> Script:
        with open(
            os.path.join(os.path.dirname(os.path.realpath(__file__)), filename)
        ) as f:
            script = f.read()
            if include_functions:
                function_definitions = []
                for func_name in sorted(include_functions.keys()):
                    function_definitions.append(
                        LOCAL_FUNC_TEMPLATE.format(
                            func_name=func_name,
                            func_body=include_functions[func_name],
                        )
                    )
                script = "\n".join(function_definitions + [script])

            return self.redis.register_script(script)

    def zadd(
        self,
        key: str,
        score: float,
        member: str,
        mode: str,
        client: Optional[Redis] = None,
    ) -> int:
        """
        Like ZADD, but supports different score update modes, in case the
        member already exists in the ZSET:
        - "nx": Don't update the score
        - "xx": Only update elements that already exist. Never add elements.
        - "min": Use the smaller of the given and existing score
        - "max": Use the larger of the given and existing score
        """
        if mode == "nx":
            f = self._zadd_noupdate
        elif mode == "xx":
            f = self._zadd_update_existing
        elif mode == "min":
            f = self._zadd_update_min
        elif mode == "max":
            f = self._zadd_update_max
        else:
            raise NotImplementedError('mode "%s" unsupported' % mode)
        return f(keys=[key], args=[score, member], client=client)

    def zpoppush(
        self,
        source: str,
        destination: str,
        count: int,
        score: Optional[Union[float, Literal["+inf"]]],
        new_score: float,
        client: Optional[Redis] = None,
        withscores: bool = False,
        on_success: Any = None,
        if_exists: Any = None,
    ) -> Any:
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
            score = "+inf"  # Include all elements.
        if withscores:
            if on_success:
                raise NotImplementedError()
            return self._zpoppush_withscores(
                keys=[source, destination],
                args=[score, count, new_score],
                client=client,
            )
        else:
            if if_exists and if_exists[0] == "add":
                _, if_exists_key, if_exists_score, if_exists_mode = if_exists
                if if_exists_mode != "min":
                    raise NotImplementedError()

                if not on_success or on_success[0] != "update_sets":
                    raise NotImplementedError()
                (
                    set_value,
                    remove_from_set,
                    add_to_set,
                    add_to_set_if_exists,
                ) = on_success[1:]

                return self._zpoppush_exists_min_update_sets(
                    keys=[
                        source,
                        destination,
                        remove_from_set,
                        add_to_set,
                        add_to_set_if_exists,
                        if_exists_key,
                    ],
                    args=[score, count, new_score, set_value, if_exists_score],
                )
            elif if_exists and if_exists[0] == "noupdate":
                if not on_success or on_success[0] != "update_sets":
                    raise NotImplementedError()
                set_value, remove_from_set, add_to_set = on_success[1:]

                return self._zpoppush_exists_ignore_update_sets(
                    keys=[source, destination, remove_from_set, add_to_set],
                    args=[score, count, new_score, set_value],
                )

            if on_success:
                if on_success[0] != "update_sets":
                    raise NotImplementedError()
                else:
                    set_value, remove_from_set, add_to_set = on_success[1:]
                    return self._zpoppush_update_sets(
                        keys=[
                            source,
                            destination,
                            remove_from_set,
                            add_to_set,
                        ],
                        args=[score, count, new_score, set_value],
                        client=client,
                    )
            else:
                return self._zpoppush(
                    keys=[source, destination],
                    args=[score, count, new_score],
                    client=client,
                )

    def srem_if_not_exists(
        self,
        key: str,
        member: str,
        other_key: str,
        client: Optional[Redis] = None,
    ) -> int:
        """
        Removes ``member`` from the set ``key`` if ``other_key`` does not
        exist (i.e. is empty). Returns the number of removed elements (0 or 1).
        """
        return self._srem_if_not_exists(
            keys=[key, other_key], args=[member], client=client
        )

    def delete_if_not_in_zsets(
        self,
        to_delete: List[str],
        value: str,
        zsets: List[str],
        client: Optional[Redis] = None,
    ) -> int:
        """
        Removes keys in ``to_delete`` only if ``value`` is not a member of any
        sorted sets in ``zsets``. Returns the number of removed elements.
        """
        return self._delete_if_not_in_zsets(
            keys=to_delete + zsets,
            args=[len(to_delete), value],
            client=client,
        )

    def fail_if_not_in_zset(
        self, key: str, member: str, client: Optional[Redis] = None
    ) -> None:
        """
        Fails with an error containing the string '<FAIL_IF_NOT_IN_ZSET>' if
        the given ``member`` is not in the ZSET ``key``. This can be used in
        a pipeline to assert that the member is in the ZSET and cancel the
        execution otherwise.
        """
        self._fail_if_not_in_zset(keys=[key], args=[member], client=client)

    def get_expired_tasks(
        self,
        key_prefix: str,
        time: float,
        batch_size: int,
        client: Optional[Redis] = None,
    ) -> List[Tuple[str, str]]:
        """
        Returns a list of expired tasks (older than ``time``) by looking at all
        active queues. The list is capped at ``batch_size``. The list contains
        tuples (queue, task_id).
        """
        result = self._get_expired_tasks(
            args=[key_prefix, time, batch_size], client=client
        )

        # [queue1, task1, queue2, task2] -> [(queue1, task1), (queue2, task2)]
        return list(zip(result[::2], result[1::2]))

    def move_task(
        self,
        id: str,
        queue: str,
        from_state: str,
        to_state: Optional[str],
        unique: bool,
        when: float,
        mode: Optional[str],
        key_func: Callable[..., str],
        publish_queued_tasks: bool,
        client: Optional[Redis] = None,
    ) -> Any:
        """
        Refer to task._move internal helper documentation.
        """

        def _bool_to_str(v: bool) -> str:
            return "true" if v else "false"

        def _none_to_empty_str(v: Optional[str]) -> str:
            return v or ""

        key_task_id = key_func("task", id)
        key_task_id_executions = key_func("task", id, "executions")
        key_task_id_executions_count = key_func("task", id, "executions_count")
        key_from_state = key_func(from_state)
        key_to_state = key_func(to_state) if to_state else ""
        key_active_queue = key_func(ACTIVE, queue)
        key_queued_queue = key_func(QUEUED, queue)
        key_error_queue = key_func(ERROR, queue)
        key_scheduled_queue = key_func(SCHEDULED, queue)
        key_activity = key_func("activity")

        return self._move_task(
            keys=[
                key_task_id,
                key_task_id_executions,
                key_task_id_executions_count,
                key_from_state,
                key_to_state,
                key_active_queue,
                key_queued_queue,
                key_error_queue,
                key_scheduled_queue,
                key_activity,
            ],
            args=[
                id,
                queue,
                from_state,
                _none_to_empty_str(to_state),
                _bool_to_str(unique),
                when,
                _none_to_empty_str(mode),
                _bool_to_str(publish_queued_tasks),
            ],
            client=client,
        )
