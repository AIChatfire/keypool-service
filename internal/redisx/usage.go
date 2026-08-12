package redisx

import (
	"context"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// UsageIncr 对 keypool:usage:{cid} 的 idx 字段做 HINCRBYFLOAT delta。
// 用于 select 预扣 est 之外的实际校正（actual-est，由 state/api 传入）。
func (c *Client) UsageIncr(ctx context.Context, cid, idx int, delta float64) error {
	if c.degraded() {
		return ErrDegraded
	}
	return c.rdb.HIncrByFloat(ctx, UsageKey(cid), strconv.Itoa(idx), delta).Err()
}

// UsageAll 读取某渠道全部用量计数与 last_decay 时间戳（SPEC §5 GET usage）。
func (c *Client) UsageAll(ctx context.Context, cid int) (counters map[int]float64, lastDecay int64, err error) {
	if c.degraded() {
		return nil, 0, ErrDegraded
	}
	counters = make(map[int]float64)
	raw, err := c.rdb.HGetAll(ctx, UsageKey(cid)).Result()
	if err != nil {
		return nil, 0, err
	}
	for k, v := range raw {
		idx, err := strconv.Atoi(k)
		if err != nil {
			continue // 忽略非数字字段
		}
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			continue
		}
		counters[idx] = f
	}
	last, err := c.rdb.HGet(ctx, UsageMetaKey(cid), "last_decay").Result()
	if err == redis.Nil {
		err = nil // 从未衰减过
	} else if err != nil {
		return nil, 0, err
	} else {
		lastDecay, _ = strconv.ParseInt(last, 10, 64)
	}
	return counters, lastDecay, nil
}

// UsageReset 将某 key 索引的计数强制设为 val（HSET）。
func (c *Client) UsageReset(ctx context.Context, cid, idx int, val float64) error {
	if c.degraded() {
		return ErrDegraded
	}
	return c.rdb.HSet(ctx, UsageKey(cid), strconv.Itoa(idx), val).Err()
}
