local function zadd_w_mode(key, score, member, mode)
    if mode == "" then
        redis.call('zadd', key, score, member)
    elseif mode == "nx" then
        zadd_noupdate({ key }, { score, member })
    elseif mode == "xx" then
        zadd_update_existing({ key }, { score, member })
    elseif mode == "min" then
        zadd_update_min({ key }, { score, member })
    elseif mode == "max" then
        zadd_update_max({ key }, { score, member })
    else
        error("mode " .. mode .. " unsupported")
    end
end


local function get_key_func(key_prefix)
    return function(...)
        table.insert(arg, 1, key_prefix)
        return table.concat(arg, ':')
    end
end

local key_prefix = ARGV[1]
local id = ARGV[2]
local queue = ARGV[3]
local from_state = ARGV[4]
local to_state = ARGV[5]
local unique = ARGV[6]
local when = ARGV[7]
local mode = ARGV[8]
local publish_queued_tasks = ARGV[9]

local key = get_key_func(key_prefix)

assert(redis.call('zscore', key(from_state, queue), id), '<FAIL_IF_NOT_IN_ZSET>')

if to_state ~= "" then
    zadd_w_mode(key(to_state, queue), when, id, mode)
    redis.call('sadd', key(to_state), queue)
end
redis.call('zrem', key(from_state, queue), id)

if to_state == "" then -- Remove the task if necessary
    if unique == 'true' then
        -- Delete executions if there were no errors
        local to_delete = {
            key('task', id, 'executions'),
            key('task', id, 'executions_count'),
        }
        local keys = { unpack(to_delete) }
        if from_state ~= 'error' then
            table.insert(keys, key('error', queue))
        end
        -- keys=[to_delete + zsets], args=[len(to_delete), value]
        delete_if_not_in_zsets(keys, { #to_delete, id })

        -- Only delete task if it's not in any other queue
        local to_delete = { key('task', id) }
        local zsets = {}
        for i, v in pairs({ 'active', 'queued', 'error', 'scheduled' }) do
            if v ~= from_state then
                table.insert(zsets, key(v, queue))
            end
        end
        -- keys=[to_delete + zsets], args=[len(to_delete), value]
        delete_if_not_in_zsets({ unpack(to_delete), unpack(zsets) }, { #to_delete, id })
    else
        -- Safe to remove
        redis.call(
            'del',
            key('task', id),
            key('task', id, 'executions'),
            key('task', id, 'executions_count')
        )
    end
end

-- keys=[key, other_key], args=[member]
srem_if_not_exists({ key(from_state), key(from_state, queue) }, { queue })

if to_state == 'queued' and publish_queued_tasks == 'true' then
    redis.call('publish', key('activity'), queue)
end
