package redisx

import (
	"context"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// usage 预扣租约（修复双重计数，SPEC §4 方案 b）：
// select 在 usage 模式且 est>0 时由 Lua 预扣 est；同时 selector 生成
// lease_id 并 LeaseSet 记录 est。report 携带 lease_id 时 LeaseTake
// 原子取回 est 并删除租约，校正 delta = actual - est（避免 est 与
// actual 双重计数）。租约过期（PX 10min）未取回时按 actual 全额累加。

// LeaseSet 写入租约：SET keypool:lease:{lease_id} est PX 600000。
func (c *Client) LeaseSet(ctx context.Context, leaseID string, est float64) error {
	if c.degraded() {
		return ErrDegraded
	}
	return c.rdb.Set(ctx, LeaseKey(leaseID), strconv.FormatFloat(est, 'f', -1, 64), leaseTTL).Err()
}

// leaseTakeScript 原子 GET + DEL，避免并发 report 重复取租约。
var leaseTakeScript = redis.NewScript(`
local v = redis.call("GET", KEYS[1])
if v then
	redis.call("DEL", KEYS[1])
end
return v
`)

// LeaseTake 原子取回并删除租约。返回 (est, ok, err)：ok=false 且 err=nil
// 表示租约不存在（过期或从未写入），调用方按 actual 全额累加。
func (c *Client) LeaseTake(ctx context.Context, leaseID string) (est float64, ok bool, err error) {
	if c.degraded() {
		return 0, false, ErrDegraded
	}
	res, err := leaseTakeScript.Run(ctx, c.rdb, []string{LeaseKey(leaseID)}).Result()
	if err != nil {
		return 0, false, err
	}
	s, isStr := res.(string)
	if res == nil || !isStr || s == "" {
		return 0, false, nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		// 租约值损坏：已删除，按未取到处理（report 侧全额累加）
		return 0, false, nil
	}
	return f, true, nil
}
