-- compare_and_swap.lua
-- Atomically update a run state only if the current version matches.
-- KEYS[1] = run state key
-- ARGV[1] = expected version (number)
-- ARGV[2] = new JSON value
-- ARGV[3] = TTL in milliseconds (0 = no expiry)
local key = KEYS[1]
local expectedVersion = tonumber(ARGV[1])
local newValue = ARGV[2]
local ttlMs = tonumber(ARGV[3])

local current = redis.call('GET', key)
if current == false then
    return 0
end

local run = cjson.decode(current)
if run.version ~= expectedVersion then
    return 0
end

if ttlMs > 0 then
    redis.call('PSETEX', key, ttlMs, newValue)
else
    redis.call('SET', key, newValue)
end
return 1
