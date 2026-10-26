/**
 * Lua scripts for the Redis store. Each runs atomically, which is what makes claims and
 * cross-processor limits safe without locks.
 *
 * Every script gets the key prefix as ARGV[1]. Keys are built inside the scripts because the
 * per-key concurrency keys aren't known up front; KEYS[1] is only declared for cluster routing.
 * The default prefix carries a hash tag so all of txob's keys live in one cluster slot.
 *
 * Layout (P = prefix):
 *   P e:<event_id>                  hash: type, data, correlation_id, created_at,
 *                                   fanout_attempts, fanout_lease_token, fanned_out_at
 *   P fanout:<type>                 zset of unfanned events, by created_at
 *   P fanoutleased:<type>           zset of events leased for fan-out, by lease expiry
 *   P events                        zset of all events, by created_at
 *   P d:<event_id>:<handler>        hash: delivery fields, plus ready_member while queued
 *   P deliveries                    zset of all deliveries, by created_at
 *   P ready:<handler slot>          zset of claimable deliveries without a concurrency key:
 *                                   score -priority, member zero-padded run_after .. delivery
 *                                   id (FIFO per priority)
 *   P kready:<handler slot>:<key>   same, for one concurrency key
 *   P kheads:<handler slot>         zset holding the first kready entry of each key, so a
 *                                   claim skips a saturated key in one step however many
 *                                   deliveries it has queued
 *   P khead:<handler slot>          hash: key -> its entry in kheads
 *   P delayed:<handler slot>        zset of deliveries waiting for run_after
 *   P leased:<handler slot>         zset of leased deliveries, by lease expiry
 *   P kleased:<handler slot>:<key>  same, per concurrency key
 *   P rl:<handler slot>             zset of claim times, for rate limits
 *
 * A handler slot is `<len(event_type)>:<event_type>:<handler>`; handler names can't contain
 * ':', so slots can't collide.
 */

const common = `
local P = ARGV[1]
local function now_ms()
  local t = redis.call('TIME')
  return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end
local function slot_key(kind, t, h)
  return P .. kind .. ':' .. string.len(t) .. ':' .. t .. ':' .. h
end
local function key_key(t, h, k)
  return slot_key('kleased', t, h) .. ':' .. k
end
local function kready_key(t, h, k)
  return slot_key('kready', t, h) .. ':' .. k
end
-- Point kheads at the key's first claimable delivery
local function refresh_head(t, h, k)
  local heads, index = slot_key('kheads', t, h), slot_key('khead', t, h)
  local old = redis.call('HGET', index, k)
  if old then redis.call('ZREM', heads, old) end
  local top = redis.call('ZRANGE', kready_key(t, h, k), 0, 0, 'WITHSCORES')
  if #top > 0 then
    redis.call('ZADD', heads, top[2], top[1])
    redis.call('HSET', index, k, top[1])
  else
    redis.call('HDEL', index, k)
  end
end
local function wakeup()
  redis.call('PUBLISH', P .. 'wakeup', '1')
end
-- Queue a delivery as claimable from run_after on
local function make_ready(dk, id, run_after)
  local f = redis.call('HMGET', dk, 'event_type', 'handler', 'priority', 'concurrency_key')
  local member = string.format('%016.0f', run_after) .. id
  if f[4] then
    redis.call('ZADD', kready_key(f[1], f[2], f[4]), -tonumber(f[3]), member)
    refresh_head(f[1], f[2], f[4])
  else
    redis.call('ZADD', slot_key('ready', f[1], f[2]), -tonumber(f[3]), member)
  end
  redis.call('HSET', dk, 'ready_member', member, 'run_after', run_after)
end
-- Remove a delivery from every queue and lease index
local function detach(dk, id)
  local f = redis.call('HMGET', dk, 'event_type', 'handler', 'concurrency_key', 'ready_member')
  local t, h = f[1], f[2]
  redis.call('ZREM', slot_key('leased', t, h), id)
  redis.call('ZREM', slot_key('delayed', t, h), id)
  if f[3] then redis.call('ZREM', key_key(t, h, f[3]), id) end
  if f[4] then
    if f[3] then
      redis.call('ZREM', kready_key(t, h, f[3]), f[4])
      refresh_head(t, h, f[3])
    else
      redis.call('ZREM', slot_key('ready', t, h), f[4])
    end
    redis.call('HDEL', dk, 'ready_member')
  end
end
-- Reads a zset in order, 100 entries at a time
local function stream(key)
  local s = { pos = 0, buf = {}, i = 1, done = false }
  function s.peek()
    if s.i > #s.buf then
      if s.done then return nil end
      s.buf = redis.call('ZRANGE', key, s.pos, s.pos + 99, 'WITHSCORES')
      s.pos = s.pos + 100
      s.i = 1
      if #s.buf < 200 then s.done = true end
      if #s.buf == 0 then return nil end
    end
    return s.buf[s.i], tonumber(s.buf[s.i + 1])
  end
  function s.advance() s.i = s.i + 2 end
  return s
end
-- Claim order: score (-priority), then member (run_after, id)
local function before(score_a, member_a, score_b, member_b)
  if score_a ~= score_b then return score_a < score_b end
  return member_a < member_b
end
`;

/** ARGV: P, id, type, data, created_at, correlation_id ('' for none, else '=' .. id) */
export const insertEventScript = `${common}
local id, t, data, created, corr = ARGV[2], ARGV[3], ARGV[4], ARGV[5], ARGV[6]
local ek = P .. 'e:' .. id
-- MULTI can't abort, so a duplicate id is ignored rather than rejected
if redis.call('EXISTS', ek) == 1 then return 0 end
redis.call('HSET', ek, 'type', t, 'data', data, 'created_at', created, 'fanout_attempts', 0)
if corr ~= '' then redis.call('HSET', ek, 'correlation_id', string.sub(corr, 2)) end
redis.call('ZADD', P .. 'fanout:' .. t, created, id)
redis.call('ZADD', P .. 'events', created, id)
wakeup()
return 1
`;

/** ARGV: P, leaseMs, limit, token, ...types. Returns [id, event hash][] */
export const claimFanoutScript = `${common}
local now = now_ms()
local leaseMs, limit, token = tonumber(ARGV[2]), tonumber(ARGV[3]), ARGV[4]
local candidates = {}
for i = 5, #ARGV do
  local t = ARGV[i]
  local fk, lk = P .. 'fanout:' .. t, P .. 'fanoutleased:' .. t
  for _, id in ipairs(redis.call('ZRANGEBYSCORE', lk, '-inf', now, 'LIMIT', 0, 1000)) do
    redis.call('ZREM', lk, id)
    local created = redis.call('HGET', P .. 'e:' .. id, 'created_at')
    if created then redis.call('ZADD', fk, created, id) end
  end
  local ids = redis.call('ZRANGE', fk, 0, limit - 1, 'WITHSCORES')
  for j = 1, #ids, 2 do
    table.insert(candidates, { ids[j], tonumber(ids[j + 1]), t })
  end
end
table.sort(candidates, function(a, b)
  if a[2] ~= b[2] then return a[2] < b[2] end
  return a[1] < b[1]
end)
local out = {}
for i = 1, math.min(limit, #candidates) do
  local id, t = candidates[i][1], candidates[i][3]
  local ek = P .. 'e:' .. id
  redis.call('ZREM', P .. 'fanout:' .. t, id)
  redis.call('ZADD', P .. 'fanoutleased:' .. t, now + leaseMs, id)
  redis.call('HINCRBY', ek, 'fanout_attempts', 1)
  redis.call('HSET', ek, 'fanout_lease_token', token)
  table.insert(out, { id, redis.call('HGETALL', ek) })
end
return out
`;

/**
 * ARGV: P, n, ...event ids (n), then per delivery: event_id, type, handler, priority,
 * concurrency key ('' for none, else '=' .. key)
 */
export const completeFanoutScript = `${common}
local now = now_ms()
local n = tonumber(ARGV[2])
local created = 0
local i = 3 + n
while i <= #ARGV do
  local eid, t, h, pr, ck = ARGV[i], ARGV[i + 1], ARGV[i + 2], ARGV[i + 3], ARGV[i + 4]
  local id = eid .. ':' .. h
  local dk = P .. 'd:' .. id
  if redis.call('EXISTS', dk) == 0 then
    redis.call('HSET', dk, 'event_id', eid, 'event_type', t, 'handler', h, 'status', 'pending',
      'attempts', 0, 'priority', pr, 'created_at', now)
    if ck ~= '' then redis.call('HSET', dk, 'concurrency_key', string.sub(ck, 2)) end
    redis.call('ZADD', P .. 'deliveries', now, id)
    make_ready(dk, id, now)
    created = created + 1
  end
  i = i + 5
end
for j = 3, 2 + n do
  local id = ARGV[j]
  local ek = P .. 'e:' .. id
  local t = redis.call('HGET', ek, 'type')
  if t then
    redis.call('HSET', ek, 'fanned_out_at', now)
    redis.call('HDEL', ek, 'fanout_lease_token')
    redis.call('ZREM', P .. 'fanout:' .. t, id)
    redis.call('ZREM', P .. 'fanoutleased:' .. t, id)
  end
end
if created > 0 then wakeup() end
return created
`;

/**
 * ARGV: P, leaseMs, limit, token, slot count, then per slot: type, handler, limit,
 * concurrency, per-key limit, rate max, rate window ms ('' for no limit).
 * Returns [delivery id, delivery hash, event hash][]
 */
export const claimDeliveriesScript = `${common}
local now = now_ms()
local leaseMs, limit, token = tonumber(ARGV[2]), tonumber(ARGV[3]), ARGV[4]
local picks = {}
local idx = 6
for s = 1, tonumber(ARGV[5]) do
  local t, h = ARGV[idx], ARGV[idx + 1]
  local slot_limit = tonumber(ARGV[idx + 2])
  local conc, key_limit = tonumber(ARGV[idx + 3]), tonumber(ARGV[idx + 4])
  local rate_max, rate_ms = tonumber(ARGV[idx + 5]), tonumber(ARGV[idx + 6])
  idx = idx + 7
  local ready, leased = slot_key('ready', t, h), slot_key('leased', t, h)

  -- Retries whose backoff has passed become claimable
  local delayed = slot_key('delayed', t, h)
  local due = redis.call('ZRANGEBYSCORE', delayed, '-inf', now, 'WITHSCORES', 'LIMIT', 0, 1000)
  for j = 1, #due, 2 do
    redis.call('ZREM', delayed, due[j])
    make_ready(P .. 'd:' .. due[j], due[j], tonumber(due[j + 1]))
  end
  -- So do deliveries whose lease expired (the holder crashed or stalled). They keep their
  -- lease token until reclaimed, like the other stores.
  local expired = redis.call('ZRANGEBYSCORE', leased, '-inf', now, 'WITHSCORES', 'LIMIT', 0, 1000)
  for j = 1, #expired, 2 do
    local id = expired[j]
    local dk = P .. 'd:' .. id
    redis.call('ZREM', leased, id)
    local ck = redis.call('HGET', dk, 'concurrency_key')
    if ck then redis.call('ZREM', key_key(t, h, ck), id) end
    make_ready(dk, id, tonumber(expired[j + 1]))
  end

  local budget = slot_limit
  if conc then
    budget = math.min(budget, conc - redis.call('ZCOUNT', leased, '(' .. now, '+inf'))
  end
  if rate_max then
    local rl = slot_key('rl', t, h)
    redis.call('ZREMRANGEBYSCORE', rl, '-inf', now - rate_ms)
    budget = math.min(budget, rate_max - redis.call('ZCARD', rl))
  end

  if budget > 0 then
    -- Merge deliveries without a key with each key's queue, in claim order. Keys are visited
    -- through their first delivery, so a key at its limit costs one step to skip.
    local keyless = stream(ready)
    local heads = stream(slot_key('kheads', t, h))
    local more = {} -- next delivery of keys already taken from
    local running = {}
    local taken = 0
    while taken < budget do
      local member, score, from = keyless.peek()
      if member then from = 'keyless' end
      local head_member, head_score = heads.peek()
      if head_member and (not from or before(head_score, head_member, score, member)) then
        member, score, from = head_member, head_score, 'head'
      end
      local more_index
      for i, entry in ipairs(more) do
        if not from or before(entry.score, entry.member, score, member) then
          member, score, from, more_index = entry.member, entry.score, 'more', i
        end
      end
      if not from then break end

      local rated = rate_max ~= nil
      if from == 'keyless' then
        keyless.advance()
        taken = taken + 1
        table.insert(picks, { id = string.sub(member, 17), member = member, score = score,
          zkey = ready, t = t, h = h, rated = rated })
      else
        local key, rank
        if from == 'head' then
          heads.advance()
          key = redis.call('HGET', P .. 'd:' .. string.sub(member, 17), 'concurrency_key')
          rank = 0
        else
          key, rank = more[more_index].key, more[more_index].rank
          table.remove(more, more_index)
        end
        local count = running[key]
        if count == nil then
          count = redis.call('ZCOUNT', key_key(t, h, key), '(' .. now, '+inf')
        end
        if not key_limit or count < key_limit then
          count = count + 1
          taken = taken + 1
          table.insert(picks, { id = string.sub(member, 17), member = member, score = score,
            zkey = kready_key(t, h, key), key = key, t = t, h = h, rated = rated })
          if not key_limit or count < key_limit then
            local next_entry = redis.call('ZRANGE', kready_key(t, h, key), rank + 1, rank + 1,
              'WITHSCORES')
            if #next_entry > 0 then
              table.insert(more, { member = next_entry[1], score = tonumber(next_entry[2]),
                key = key, rank = rank + 1 })
            end
          end
        end
        running[key] = count
      end
    end
  end
end

table.sort(picks, function(a, b)
  if a.score ~= b.score then return a.score < b.score end
  return a.member < b.member
end)

local out = {}
local expires = now + leaseMs
local taken_keys = {}
for i = 1, math.min(limit, #picks) do
  local p = picks[i]
  local dk = P .. 'd:' .. p.id
  redis.call('ZREM', p.zkey, p.member)
  if p.key then table.insert(taken_keys, p) end
  redis.call('HDEL', dk, 'ready_member')
  redis.call('HINCRBY', dk, 'attempts', 1)
  redis.call('HSET', dk, 'lease_token', token, 'run_after', expires)
  redis.call('ZADD', slot_key('leased', p.t, p.h), expires, p.id)
  local ck = redis.call('HGET', dk, 'concurrency_key')
  if ck then redis.call('ZADD', key_key(p.t, p.h, ck), expires, p.id) end
  if p.rated then redis.call('ZADD', slot_key('rl', p.t, p.h), now, token .. ':' .. i) end
  local eid = redis.call('HGET', dk, 'event_id')
  table.insert(out, { p.id, redis.call('HGETALL', dk), redis.call('HGETALL', P .. 'e:' .. eid) })
end
for _, p in ipairs(taken_keys) do refresh_head(p.t, p.h, p.key) end
return out
`;

/** ARGV: P, delivery id, token */
export const completeDeliveryScript = `${common}
local id, token = ARGV[2], ARGV[3]
local dk = P .. 'd:' .. id
if redis.call('HGET', dk, 'lease_token') ~= token then return 0 end
detach(dk, id)
redis.call('HSET', dk, 'status', 'completed', 'completed_at', now_ms())
redis.call('HDEL', dk, 'lease_token')
return 1
`;

/** ARGV: P, delivery id, token, run_after ms ('' marks it dead), error JSON */
export const failDeliveryScript = `${common}
local id, token, run_after, err = ARGV[2], ARGV[3], ARGV[4], ARGV[5]
local dk = P .. 'd:' .. id
if redis.call('HGET', dk, 'lease_token') ~= token then return 0 end
detach(dk, id)
redis.call('HDEL', dk, 'lease_token')
redis.call('HSET', dk, 'last_error', err)
if run_after == '' then
  redis.call('HSET', dk, 'status', 'dead', 'completed_at', now_ms())
else
  local f = redis.call('HMGET', dk, 'event_type', 'handler')
  redis.call('HSET', dk, 'run_after', run_after)
  redis.call('ZADD', slot_key('delayed', f[1], f[2]), run_after, id)
end
return 1
`;

/**
 * ARGV: P, leaseMs, then pairs of delivery id, token. Returns the 0-based positions of the
 * pairs whose lease was lost.
 */
export const extendLeasesScript = `${common}
local expires = now_ms() + tonumber(ARGV[2])
local lost = {}
for i = 3, #ARGV, 2 do
  local id, token = ARGV[i], ARGV[i + 1]
  local dk = P .. 'd:' .. id
  if redis.call('HGET', dk, 'lease_token') ~= token then
    table.insert(lost, (i - 3) / 2)
  else
    -- An expired lease that nobody reclaimed yet is still ours; take it off the ready queue
    detach(dk, id)
    local f = redis.call('HMGET', dk, 'event_type', 'handler', 'concurrency_key')
    redis.call('ZADD', slot_key('leased', f[1], f[2]), expires, id)
    if f[3] then redis.call('ZADD', key_key(f[1], f[2], f[3]), expires, id) end
    redis.call('HSET', dk, 'run_after', expires)
  end
end
return lost
`;

/** ARGV: P, then pairs of delivery id, token */
export const releaseDeliveriesScript = `${common}
local now = now_ms()
for i = 2, #ARGV, 2 do
  local id, token = ARGV[i], ARGV[i + 1]
  local dk = P .. 'd:' .. id
  if redis.call('HGET', dk, 'lease_token') == token then
    detach(dk, id)
    redis.call('HDEL', dk, 'lease_token')
    redis.call('HINCRBY', dk, 'attempts', -1)
    make_ready(dk, id, now)
  end
end
if #ARGV > 1 then wakeup() end
return 1
`;
