-- execute_pipeline
--
-- Executes the given Redis commands as a Lua script. When an error occurs, the
-- underlying exception is raised and execution stops. Supports executing
-- registered Redis Lua scripts using EVALSHA.
--
-- KEYS = { }
-- ARGV = { can_replicate_commands, n_commands
--          [, cmd_1_n_args, cmd_1_name, [cmd_1_arg_1 , ..., cmd_1_arg_n]
--            [, ...
--              [, cmd_n_n_args, cmd_n_name, [cmd_n_arg_1 ..., cmd_1_arg_n]]]]}
--
-- Example: ARGV = { 2, 1, "GET", "key", 0, "INFO" }

local argv = ARGV
local can_replicate_commands = argv[1]
local n_cmds = argv[2]
local cmd_ptr = 3
local n_args
local results = {}

if can_replicate_commands == '1' then
    redis.replicate_commands()
end


-- Returns a subrange of the given table, from (and including) the first
-- index, to (and including) the last index.
local function subrange(t, first, last)
    local sub = {}
    for i=first,last do
        sub[#sub + 1] = t[i]
    end
    return sub
end

for cmd_n=1, n_cmds do
    -- Execute command cmd_n
    n_args = argv[cmd_ptr]

    local cmd_name = argv[cmd_ptr+1]

    if cmd_name == 'EVALSHA' then
        -- Script execution needs special treatment: Scripts are registered
        -- under the global _G variable (as 'f_'+sha) and need the KEYS and
        -- ARGV prepopulated.
        local sha = argv[cmd_ptr+2]
        local numkeys = argv[cmd_ptr+3]
        local result
        KEYS = subrange(argv, cmd_ptr+4, cmd_ptr+4+numkeys-1)
        ARGV = subrange(argv, cmd_ptr+4+numkeys, cmd_ptr+n_args+1)
        result = _G['f_' .. sha]()
        if result == nil then
            -- The table is truncated if we have nil values
            results[cmd_n] = false
        else
            results[cmd_n] = result
        end
    else
        results[cmd_n] = redis.call(unpack(subrange(argv, cmd_ptr+1,
                                                    cmd_ptr+n_args+1)))
    end

    cmd_ptr = cmd_ptr + 1 + n_args + 1
end

return results
