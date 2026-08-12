-- select_key.lua — keypool 选 key 脚本（SPEC §3.1）
-- KEYS[1] = keypool:cursor:{cid}      (String, INCR 轮询游标)
-- KEYS[2] = keypool:usage:{cid}       (Hash {idx:float} 用量计数)
-- KEYS[3] = keypool:usage_meta:{cid}  (Hash {last_decay:ts})
-- ARGV[1] = mode        (polling|peek|random|usage)
-- ARGV[2] = candidates  (JSON int 数组, 已按轮换过滤, 元素为真实 key 索引)
-- ARGV[3] = est         (float, usage 模式预扣量)
-- ARGV[4] = decay_interval_sec
-- ARGV[5] = decay_factor
-- ARGV[6] = now_ts
-- ARGV[7] = jitter_pct  (如 0.05)
-- RETURN: 选中的真实 key 索引（candidates 元素值）；候选为空返回 -1。

local mode       = ARGV[1]
local candidates = cjson.decode(ARGV[2])
local est        = tonumber(ARGV[3]) or 0
local interval   = tonumber(ARGV[4]) or 0
local factor     = tonumber(ARGV[5]) or 1
local now        = tonumber(ARGV[6]) or 0
local jitter     = tonumber(ARGV[7]) or 0

-- ① usage 模式惰性衰减：now - last_decay > interval → 全部计数 *= factor，写 last_decay
if mode == "usage" and interval > 0 then
	local last = tonumber(redis.call("HGET", KEYS[3], "last_decay")) or 0
	if now - last > interval then
		local all = redis.call("HGETALL", KEYS[2])
		for i = 1, #all, 2 do
			local k = all[i]
			local v = tonumber(all[i + 1]) or 0
			redis.call("HSET", KEYS[2], k, v * factor)
		end
		redis.call("HSET", KEYS[3], "last_decay", now)
	end
end

-- ② 候选为空
local n = #candidates
if n == 0 then
	return -1
end

-- ③ 按模式选取
local idx
if mode == "polling" then
	-- INCR 游标后对候选数取模（Lua 表 1 起始，故 +1）
	local cur = redis.call("INCR", KEYS[1])
	idx = candidates[(cur % n) + 1]
elseif mode == "peek" then
	-- 只读游标不 INCR（测活不推游标）；GET 缺省按 0
	local cur = tonumber(redis.call("GET", KEYS[1])) or 0
	idx = candidates[(cur % n) + 1]
elseif mode == "random" then
	idx = candidates[math.random(n)]
elseif mode == "usage" then
	-- 候选中取有效计数 = 计数*(1+rand*jitter) 最小者，并列取首个
	local best, bestEff
	for i = 1, n do
		local k = candidates[i]
		local cnt = tonumber(redis.call("HGET", KEYS[2], tostring(k))) or 0
		local eff = cnt * (1 + math.random() * jitter)
		if best == nil or eff < bestEff then
			best, bestEff = k, eff
		end
	end
	idx = best
	-- ④ usage 模式预扣 est
	redis.call("HINCRBYFLOAT", KEYS[2], tostring(idx), est)
else
	-- 未知模式回退 polling 语义，保证有界行为
	local cur = redis.call("INCR", KEYS[1])
	idx = candidates[(cur % n) + 1]
end

-- ⑤ 返回真实 key 索引（candidates 元素值，非下标）
return idx
