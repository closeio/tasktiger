-- Semaphore lock
--
-- KEYS = { semaphore key }
-- ARGV = { lock id (must be unique across callers),
--          semaphore size,
--          lock timeout in seconds,
--          current time in seconds }
--
-- Returns: { lock acquired (True or False),
--            number of locks in semaphore }

-- Using redis server time ensures consistent time across callers but it requires
-- Redis v3.2+ because the replicate_commands command is needed to replicate
-- the actions in this script to a slave instance.
--redis.replicate_commands()

local semaphore_key = KEYS[1]

local lock_id = ARGV[1]
local semaphore_size = tonumber(ARGV[2])
local timeout = tonumber(ARGV[3])
-- TODO: Dynamically enable Redis time usage in Redis 3.2+
--local time = redis.call("time")
--local now = tonumber(time[1])
local now = tonumber(ARGV[4])

--Remove expired locks
redis.call("ZREMRANGEBYSCORE", semaphore_key, 0, now)

--Check if there is a system lock which will override all other locks
if redis.call("ZSCORE", semaphore_key, "SYSTEM_LOCK") ~= false then
  return {false, -1}
end

--Update TTL for semaphore key. This is done after checking
--for a system lock so we don't accidently make the TTL
--less than the system lock timeout.
redis.call("EXPIRE", semaphore_key, math.ceil(timeout * 2))

-- Get current count of active locks
local lock_count = redis.call("ZCARD", semaphore_key)

-- Check if this lock_id already has an active lock
local updating_lock = redis.call("ZSCORE", semaphore_key, lock_id)

-- The lock count will increase by 1 if we are getting a new lock
local current_lock_count = lock_count
if updating_lock == false then
  lock_count = lock_count + 1
end

-- Check if we should allow this lock
if lock_count > semaphore_size then
  return {false, current_lock_count}
else
  -- This also handles renewing an existing lock
  -- Score is set to the time this lock expires
  redis.call("ZADD", semaphore_key, now + timeout, lock_id)
  return {true, lock_count}
end
