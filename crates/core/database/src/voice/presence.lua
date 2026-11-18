local slot, user_index, old_channel, old_room, new_channel, new_room,
      ended_participant, ended_room, events = unpack(KEYS)
local revision, current, next_revision, operation, user_id, scope, channel_id,
      room_member, next_active, payload, from_channel = unpack(ARGV)

-- Compare decimal strings so revisions beyond Lua's exact integer range stay distinct.
local saved = redis.call('HMGET', slot, 'rev', 'active')
if (saved[1] or '0') ~= revision or (saved[2] or '') ~= current then
    return 'conflict'
end

if redis.call('GET', ended_participant) then
    return 'unchanged'
end
if (operation ~= 'leave' and redis.call('GET', ended_room))
        or (operation == 'join' and payload == '') then
    return 'unchanged'
end
if next_revision == '' then
    return redis.error_reply('ERR voice presence revision exhausted')
end

if payload ~= '' then
    -- Redis does not roll back script writes on error. Check the index types
    -- before XADD, then append before touching indexes in case the stream is exhausted.
    local changes_membership = operation == 'join' or operation == 'leave'
    if changes_membership then
        for _, key in ipairs({user_index, old_channel, old_room, new_channel, new_room}) do
            if key ~= '' then
                local kind = redis.call('TYPE', key).ok
                if kind ~= 'hash' and kind ~= 'none' then
                    return redis.error_reply('WRONGTYPE voice presence index is not a hash')
                end
            end
        end
    end
    -- Record the predecessor even when retention evicts it. The reader can
    -- distinguish a missing anchor from a missing event it has yet to see.
    local previous = journal_tail(events)
    local retained = 10000
    if from_channel == '' then
        redis.call('XADD', events, 'MAXLEN', '~', retained, '*',
            'previous', previous, 'channel_id', channel_id, 'payload', payload)
    else
        redis.call('XADD', events, 'MAXLEN', '~', retained, '*',
            'previous', previous, 'channel_id', channel_id, 'payload', payload,
            'from_channel_id', from_channel)
    end

    if changes_membership and old_channel ~= '' then
        redis.call('HDEL', old_channel, user_id)
        redis.call('HDEL', old_room, user_id)
    end
    if operation == 'join' then
        redis.call('HSET', user_index, scope, channel_id)
        redis.call('HSET', new_channel, user_id, scope)
        redis.call('HSET', new_room, user_id, room_member)
    elseif operation == 'leave' then
        redis.call('HDEL', user_index, scope)
    end
end

if payload ~= '' or operation == 'tracks' then
    current = next_active
end

-- A first Leave fences earlier snapshots even if it did not remove an active slot.
if operation == 'leave' then
    redis.call('SET', ended_participant, '1')
end
redis.call('HSET', slot, 'rev', next_revision, 'active', current)
return 'applied'
