-- Using the time command requires this script to be replicated via commands
--redis.replicate_commands()

-- Use redis server time so it is consistent across callers
-- The following line gets replaced before loading this script during tests
-- so that we can freeze the time values.  See throttle.py.

local lock_key = KEYS[1]

local lock_id = ARGV[1]
local max_locks = tonumber(ARGV[2])
local timeout = tonumber(ARGV[3])
--local time = redis.call("time")
--local now = tonumber(time[1])
local now = tonumber(ARGV[4])

--Remove expired locks
redis.call("ZREMRANGEBYSCORE", lock_key, 0, now - timeout)
redis.call("EXPIRE", lock_key, math.ceil(timeout * 2))

local lock_count = redis.call("ZCARD", lock_key)

local updating_lock = redis.call("ZRANK", lock_key, lock_id)
if updating_lock == false then
  lock_count = lock_count + 1
end

if lock_count > max_locks then
  return -1
else
  redis.call("ZADD", lock_key, now, lock_id)
  return lock_count
end
