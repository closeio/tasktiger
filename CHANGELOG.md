# Changelog

## Version 0.13

### Breaking changes

#### Changing the locking mechanism

This new version of TaskTiger uses a new locking mechanism: the `Lock` provided by redis-py. It is incompatible with the old locking mechanism we were using, and several core functions in TaskTiger depends on locking to work correctly, so this warrants a careful migration process.

You can perform this migration in two ways: via a live migration, or via a downtime migration. After the migration, there's an optional cleanup step.

##### The live migration

1. Update your environment to TaskTiger 0.12 as usual.
1. Deploy TaskTiger as it is in the commit SHA `cf600449d594ac22e6d8393dc1009a84b52be0c1`. In `pip` parlance, it would be:

       -e git+ssh://git@github.com/closeio/tasktiger.git@cf600449d594ac22e6d8393dc1009a84b52be0c1#egg=tasktiger

1. Wait at least 2-3 minutes with it running in production in all your TaskTiger workers. This is to give time for the old locks to expire, and after that the new locks will be fully in effect.
1. Deploy TaskTiger 0.13. Your system is migrated.

##### The downtime migration

1. Update your environment to TaskTiger 0.12 as usual.
1. Scale your TaskTiger workers down to zero.
1. Deploy TaskTiger 0.13. Your system is migrated.

##### The cleanup step

Run the script in `scripts/redis_scan.py` to delete the old lock keys from your Redis instance:

    ./scripts/redis_scan.py --host HOST --port PORT --db DB --print --match "t:lock:*" --ttl 300

The flags:

- `--host`: The Redis host. Required.
- `--port`: The port the Redis instance is listening on. Defaults to `6379`.
- `--db`: The Redis database. Defaults to `0`.
- `--print`: If you want the script to print which keys it is modifying, use this.
- `--match`: What pattern to look for. If you didn't change the default prefix TaskTiger uses for keys, this will be `t:lock:*`, otherwise it will be `PREFIX:lock:*`. By default, scans all keys.
- `--ttl`: A TTL to set. A TTL of 300 will give you time to undo if you want to halt the migration for whatever reason. (Just call this command again with `--ttl -1`.) By default, does not change keys' TTLs.

Plus, there is:

- `--file`: A log file that will receive the changes made. Defaults to `redis-stats.log` in the current working directory.
- `--delay`: How long, in seconds, to wait between `SCAN` iterations. Defaults to `0.1`.

## Version 0.12

### Breaking changes

* Drop support for redis-py 2 ([#183](https://github.com/closeio/tasktiger/pull/183))

### Other changes

* Make the `TaskTiger` instance available for the task via global state ([#170](https://github.com/closeio/tasktiger/pull/170))
* Support for custom task runners ([#175](https://github.com/closeio/tasktiger/pull/175))
* Add ability to configure a poll- vs push-method for task runners to discover new tasks ([#176](https://github.com/closeio/tasktiger/pull/176))
* `unique_key` specifies the list of kwargs to use to construct the unique key ([#180](https://github.com/closeio/tasktiger/pull/180))

### Bugfixes

* Ensure task exists in the given queue when retrieving it ([#184](https://github.com/closeio/tasktiger/pull/184))
* Clear retried executions from successful periodic tasks ([#188](https://github.com/closeio/tasktiger/pull/188))

## Version 0.11

### Breaking changes

* Drop support for Python 3.4 and add testing for Python 3.7 ([#163](https://github.com/closeio/tasktiger/pull/163))

### Other changes

* Add support for redis-py 3 ([#163](https://github.com/closeio/tasktiger/pull/163))
* Fix test timings ([#164](https://github.com/closeio/tasktiger/pull/164))
* Allow custom context managers to see task errors ([#165](https://github.com/closeio/tasktiger/pull/165)). Thanks @igor47
