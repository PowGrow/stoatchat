local alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_'

-- Match the unpadded URL-safe encoding used by the canonical slot keys.
local function encode_id(id)
    local result = {}
    local function emit(value)
        local index = value + 1
        result[#result + 1] = string.sub(alphabet, index, index)
    end
    for i = 1, #id, 3 do
        local a, b, c = string.byte(id, i, i + 2)
        emit(bit.rshift(a, 2))
        emit(bit.bor(bit.lshift(bit.band(a, 3), 4), bit.rshift(b or 0, 4)))
        if b then
            emit(bit.bor(bit.lshift(bit.band(b, 15), 2), bit.rshift(c or 0, 6)))
        end
        if c then
            emit(bit.band(c, 63))
        end
    end
    return table.concat(result)
end

local snapshot = {}
for i = 2, #KEYS do
    local index = redis.call('HGETALL', KEYS[i])
    local participants = {}
    for j = 1, #index, 2 do
        local slot = ARGV[1] .. ':slot:' .. encode_id(index[j]) .. ':' .. index[j + 1]
        local active = redis.call('HGET', slot, 'active')
        if active and active ~= '' then
            participants[#participants + 1] = active
        end
    end
    snapshot[#snapshot + 1] = participants
end
return {journal_tail(KEYS[1]), snapshot}
