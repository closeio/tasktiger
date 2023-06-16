local function zadd_w_mode(key, score, member, mode)
    if mode == "" then
        redis.call('zadd', key, score, member)
    elseif mode == "nx" then
        zadd_noupdate({ key }, { score, member })
    elseif mode == "min" then
        zadd_update_min({ key }, { score, member })
    else
        error("mode " .. mode .. " unsupported")
    end
end


local key_task_id = KEYS[1]
local key_task_id_executions = KEYS[2]
local key_task_id_executions_count = KEYS[3]
local key_from_state = KEYS[4]
local key_to_state = KEYS[5]
local key_active_queue = KEYS[6]
local key_queued_queue = KEYS[7]
local key_error_queue = KEYS[8]
local key_scheduled_queue = KEYS[9]
local key_activity = KEYS[10]

local id = ARGV[1]
local queue = ARGV[2]
local from_state = ARGV[3]
local to_state = ARGV[4]
local unique = ARGV[5]
local when = ARGV[6]
local mode = ARGV[7]
local publish_queued_tasks = ARGV[8]

local state_queues_keys_by_state = {
    active = key_active_queue,
    queued = key_queued_queue,
    error = key_error_queue,
    scheduled = key_scheduled_queue,
}
local key_from_state_queue = state_queues_keys_by_state[from_state]
local key_to_state_queue = state_queues_keys_by_state[to_state]

assert(redis.call('zscore', key_from_state_queue, id), '<FAIL_IF_NOT_IN_ZSET>')

if to_state ~= "" then
    zadd_w_mode(key_to_state_queue, when, id, mode)
    redis.call('sadd', key_to_state, queue)
end
redis.call('zrem', key_from_state_queue, id)

if to_state == "" then -- Remove the task if necessary
    if unique == 'true' then
        -- Delete executions if there were no errors
        local to_delete = {
            key_task_id_executions,
            key_task_id_executions_count,
        }
        local keys = { unpack(to_delete) }
        if from_state ~= 'error' then
            table.insert(keys, key_error_queue)
        end
        -- keys=[to_delete + zsets], args=[len(to_delete), value]
        delete_if_not_in_zsets(keys, { #to_delete, id })

        -- Only delete task if it's not in any other queue
        local to_delete = { key_task_id }
        local zsets = {}
        for i, v in pairs({ 'active', 'queued', 'error', 'scheduled' }) do
            if v ~= from_state then
                table.insert(zsets, state_queues_keys_by_state[v])
            end
        end
        -- keys=[to_delete + zsets], args=[len(to_delete), value]
        delete_if_not_in_zsets({ unpack(to_delete), unpack(zsets) }, { #to_delete, id })
    else
        -- Safe to remove
        redis.call(
            'del',
            key_task_id,
            key_task_id_executions,
            key_task_id_executions_count
        )
    end
end

-- keys=[key, other_key], args=[member]
srem_if_not_exists({ key_from_state, key_from_state_queue }, { queue })

if to_state == 'queued' and publish_queued_tasks == 'true' then
    redis.call('publish', key_activity, queue)
end
