from .utils import redis_glob_escape


def migrate_executions_count(tiger):
    """
    Backfills ``t:task:<uuid>:executions_count`` by counting
    elements in ``t:task:<uuid>:executions``.
    """

    migrate_task = tiger.connection.register_script(
        """
        local count = redis.call('llen', KEYS[1])
        if tonumber(redis.call('get', KEYS[2]) or 0) < count then
            redis.call('set', KEYS[2], count)
        end
        """
    )

    match = (
        redis_glob_escape(tiger.config["REDIS_PREFIX"]) + ":task:*:executions"
    )

    for key in tiger.connection.scan_iter(count=100, match=match):
        migrate_task(keys=[key, key + "_count"])
