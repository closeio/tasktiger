-- Semaphore lock
--
-- Using redis server time ensures consistent time across callers but it requires
-- Redis v3.2+ because the replicate_commands command is needed to replicate
-- the actions in this script to a slave instance.
--redis.replicate_commands()

local lock_key = KEYS[1]

local lock_id = ARGV[1]
local max_locks = tonumber(ARGV[2])
local timeout = tonumber(ARGV[3])
-- TODO: Dynamically enable Redis time usage in Redis 3.2+
--local time = redis.call("time")
--local now = tonumber(time[1])
local now = tonumber(ARGV[4])

--Remove expired locks and update TTL for lock key
redis.call("ZREMRANGEBYSCORE", lock_key, 0, now - timeout)
redis.call("EXPIRE", lock_key, math.ceil(timeout * 2))

-- Get current count of active locks
local lock_count = redis.call("ZCARD", lock_key)

-- Check if this lock_id already has an active lock
local updating_lock = redis.call("ZRANK", lock_key, lock_id)

-- The lock count will increase by 1 if we are getting a new lock
local current_lock_count = lock_count
if updating_lock == false then
  lock_count = lock_count + 1
end

-- Check if we should allow to give this lock
if lock_count > max_locks then
  return {false, current_lock_count}
else
  -- This also handles renewing an existing lock
  redis.call("ZADD", lock_key, now, lock_id)

  return {true, lock_count}
end
