
local raw = redis.call('GET', KEYS[1])
local entry

if raw == false then
  entry = {
    epsilon_total = tonumber(ARGV[2]),
    epsilon_spent = 0,
    epsilon_remaining = tonumber(ARGV[2]),
    query_count = 0,
    last_query = ARGV[3]
  }
else
  entry = cjson.decode(raw)
end

local eps = tonumber(ARGV[1])
if entry.epsilon_remaining < eps then
  return redis.error_reply('BUDGET_EXCEEDED')
end


entry.epsilon_spent = entry.epsilon_spent + eps
entry.epsilon_remaining = entry.epsilon_total - entry.epsilon_spent
entry.query_count = entry.query_count + 1
entry.last_query = ARGV[3]
redis.call('SETEX', KEYS[1], tonumber(ARGV[4]), cjson.encode(entry))
return cjson.encode(entry)