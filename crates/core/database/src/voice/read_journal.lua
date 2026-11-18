local stream = KEYS[1]
local cursor, limit = ARGV[1], tonumber(ARGV[2])
-- XRANGE is inclusive on Redis 5 / KeyDB. Drop the cursor if still retained.
local entries = redis.call('XRANGE', stream, cursor, '+', 'COUNT', limit + 1)
if entries[1] and entries[1][1] == cursor then
    table.remove(entries, 1)
end
if not entries[1] then
    if journal_tail(stream) == cursor then
        return {}
    end
    return false
end
if #entries > limit then
    table.remove(entries)
end
return entries
