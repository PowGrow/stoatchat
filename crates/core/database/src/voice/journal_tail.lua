local function journal_tail(key)
    if redis.call('EXISTS', key) == 0 then
        return '0-0'
    end

    -- A fully trimmed stream still remembers its last generated ID.
    local info = redis.call('XINFO', 'STREAM', key)
    for i = 1, #info, 2 do
        if info[i] == 'last-generated-id' then
            return info[i + 1]
        end
    end
    error('voice event stream has no last generated ID')
end
