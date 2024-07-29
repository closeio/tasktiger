# Changelog

## Version 0.21.0

* When raising `RetryException` with no `method`, use task decorator retry method if set ([356](https://github.com/closeio/tasktiger/pull/356))

## Version 0.20.0

* Add `tiger.get_sizes_for_queues_and_states` ([352](https://github.com/closeio/tasktiger/pull/352))

## Version 0.19.5

* First version using the automated-release process

## Version 0.19.4

* Log task processing in sync executor ([347](https://github.com/closeio/tasktiger/pull/347))
* Test and Docstring improvements ([338](https://github.com/closeio/tasktiger/pull/338), [343](https://github.com/closeio/tasktiger/pull/343))

## Version 0.19.3

* Stop heartbeat thread in case of unhandled exceptions ([335](https://github.com/closeio/tasktiger/pull/335))

## Version 0.19.2

* Heartbeat threading-related fixes with synchronous worker ([333](https://github.com/closeio/tasktiger/pull/333))

## Version 0.19.1

* Implement heartbeat with synchronous TaskTiger worker ([331](https://github.com/closeio/tasktiger/pull/331))

## Version 0.19

* Adding synchronous (non-forking) executor ([319](https://github.com/closeio/tasktiger/pull/319), [320](https://github.com/closeio/tasktiger/pull/320))
* If possible, retry tasks that fail with "execution not found" ([323](https://github.com/closeio/tasktiger/pull/323))
* Option to exit TaskTiger after a certain amount of time ([324](https://github.com/closeio/tasktiger/pull/324))

## Version 0.18.2

* Purge errored tasks even if task object is not found ([310](https://github.com/closeio/tasktiger/pull/310))

## Version 0.18.1

* Added `current_serialized_func` property to `TaskTiger` object ([296](https://github.com/closeio/tasktiger/pull/296))

## Version 0.18.0

* Added support for Redis >= 6.2.7 ([268](https://github.com/closeio/tasktiger/issues/268))

### ⚠️ Breaking changes

* Removed `execute_pipeline` script ([284](https://github.com/closeio/tasktiger/pull/284))

### Other changes

* Added typing to more parts of the codebase
* Dropped Python 3.7 support, added Python 3.11 support
* Added CI checks to ensure compatibility on redis-py versions (currently >=3.3.0,<5)

## Version 0.17.1

### Other changes

* Add 'task_func' to logging processor ([265](https://github.com/closeio/tasktiger/pull/265))
* Deprecate Flask-Script integration ([260](https://github.com/closeio/tasktiger/pull/260))

## Version 0.17.0

### ⚠️ Breaking changes

#### Allow truncating task executions ([251](https://github.com/closeio/tasktiger/pull/251))

##### Overview

This version of TaskTiger switches to using the `t:task:<id>:executions_count` Redis key to determine the total number of task executions. In previous versions this was accomplished by obtaining the length of `t:task:<id>:executions`. This change was required for the introduction of a parameter to enable the truncation of task execution entries. This is useful for tasks with many retries, where execution entries consume a lot of memory.

This behavior is incompatible with the previous mechanism and requires a migration to populate the task execution counters.
Without the migration, the execution counters will behave as though they were reset, which may result in existing tasks retrying more times than they should.

##### Migration

The migration can be executed fully live without concern for data integrity.

1. Upgrade TaskTiger to `0.16.2` if running a version lower than that.
2. Call `tasktiger.migrations.migrate_executions_count` with your `TaskTiger` instance, e.g.:
```py
from tasktiger import TaskTiger
from tasktiger.migrations import migrate_executions_count

# Instantiate directly or import from your application module
tiger = TaskTiger(...)

# This could take a while depending on the
# number of failed/retrying tasks you have
migrate_executions_count(tiger)
```
3. Upgrade TaskTiger to `0.17.0`. Done!

#### Import cleanup ([258](https://github.com/closeio/tasktiger/pull/258))

Due to a cleanup of imports, some internal TaskTiger objects can no longer be imported from the public modules. This shouldn't cause problems for most users, but it's a good idea to double check that all imports from the TaskTiger package continue to function correctly in your application.

## Version 0.16.2

### Other changes

* Prefilter polled queues ([242](https://github.com/closeio/tasktiger/pull/242))
* Use SSCAN to prefilter queues in scheduled state ([248](https://github.com/closeio/tasktiger/pull/248))
* Add task execution counter ([252](https://github.com/closeio/tasktiger/pull/252))

## Version 0.16.1

### Other changes

* Add function name to tasktiger done log messages ([203](https://github.com/closeio/tasktiger/pull/203))
* Add task args / kwargs to the task_error log statement ([215](https://github.com/closeio/tasktiger/pull/215))
* Fix `hard_timeout` in parent process when stored on task function ([235](https://github.com/closeio/tasktiger/pull/235))

## Version 0.16

### Other changes

* Handle hard timeout in parent process ([f3b3e24](https://github.com/closeio/tasktiger/commit/f3b3e24485497a2b87281a1b809966bcb525c5fc))
* Add queue name to logs ([a090d00](https://github.com/closeio/tasktiger/commit/a090d00bca496082f149f2187b026ff96a0d4fac))

## Version 0.15

### Other changes

* Populate `Task.ts` field in `Task.from_id` function ([019bf18](https://github.com/closeio/tasktiger/commit/019bf189c9622b299691dbe3b71cefa0bf2ee8dc))
* Add `TaskTiger.would_process_configured_queue()` function  ([217152d](https://github.com/closeio/tasktiger/commit/217152d16ff21a87b70643a0a2571efc91c0aeb9))

## Version 0.14

### Other changes

* Add `Task.time_last_queued` property getter ([6d2285d](https://github.com/closeio/tasktiger/commit/6d2285da5bd5f82455765e6b132594d4ceab2d82))

## Version 0.13

### ⚠️ Breaking changes

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

### ⚠️ Breaking changes

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
