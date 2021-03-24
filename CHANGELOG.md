# Changelog

## Version 0.12

### Breaking changes

* Drop support for redis-py 2 ([#183](https://github.com/closeio/tasktiger/pull/183))

### Other changes

* Make the `TaskTiger` instance available for the task via global state ([#170](https://github.com/closeio/tasktiger/pull/170))
* Support for custom task runners ([#175](https://github.com/closeio/tasktiger/pull/175))
* Add ability to configure a poll- vs push-method for task runners to discover new tasks ([#176](https://github.com/closeio/tasktiger/pull/176))

## Version 0.11

### Breaking changes

* Drop support for Python 3.4 and add testing for Python 3.7 ([#163](https://github.com/closeio/tasktiger/pull/163))

### Other changes

* Add support for redis-py 3 ([#163](https://github.com/closeio/tasktiger/pull/163))
* Fix test timings ([#164](https://github.com/closeio/tasktiger/pull/164))
* Allow custom context managers to see task errors ([#165](https://github.com/closeio/tasktiger/pull/165)). Thanks @igor47
