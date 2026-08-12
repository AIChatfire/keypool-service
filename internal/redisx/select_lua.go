package redisx

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// selectKeyLua 内嵌 SPEC §3.1 的 select_key.lua 源码。
//
//go:embed select_key.lua
var selectKeyLua string

// selectKeyScript 预载脚本；Script.Run 先 EvalSha，NOSCRIPT 时自动回退 Eval。
var selectKeyScript = redis.NewScript(selectKeyLua)

// SelectKeyArgs 是 SelectKey 的参数集（SPEC §3.1 ARGV 契约）。
type SelectKeyArgs struct {
	Mode          string  // polling|peek|random|usage（peek=只读游标不 INCR）
	Candidates    []int   // 真实 key 索引，已按轮换过滤
	Est           float64 // usage 模式预扣量
	DecayInterval float64 // 衰减间隔（秒）
	DecayFactor   float64 // 衰减系数
	Now           int64   // 当前秒级时间戳
	JitterPct     float64 // usage 抖动比例，如 0.05
}

// selectKeyArgv 序列化 KEYS/ARGV，供 SelectKey 与单测断言使用。
// ARGV: mode, candidates(JSON), est, decay_interval, decay_factor, now, jitter。
func selectKeyArgv(cid int, a SelectKeyArgs) (keys []string, args []any, err error) {
	cand, err := json.Marshal(a.Candidates)
	if err != nil {
		return nil, nil, fmt.Errorf("redisx: marshal candidates: %w", err)
	}
	keys = []string{CursorKey(cid), UsageKey(cid), UsageMetaKey(cid)}
	args = []any{
		a.Mode,
		string(cand),
		strconv.FormatFloat(a.Est, 'f', -1, 64),
		strconv.FormatFloat(a.DecayInterval, 'f', -1, 64),
		strconv.FormatFloat(a.DecayFactor, 'f', -1, 64),
		strconv.FormatInt(a.Now, 10),
		strconv.FormatFloat(a.JitterPct, 'f', -1, 64),
	}
	return keys, args, nil
}

// SelectKey 单 RTT 调 select_key.lua 选取 key（SPEC §3.1）。
// 返回 candidates 中的真实 key 索引；返回 -1 表示候选为空。
func (c *Client) SelectKey(ctx context.Context, mode string, cid int, candidates []int, est, decayInterval, decayFactor float64, now int64, jitterPct float64) (int, error) {
	if c.degraded() {
		return -1, ErrDegraded
	}
	keys, args, err := selectKeyArgv(cid, SelectKeyArgs{
		Mode:          mode,
		Candidates:    candidates,
		Est:           est,
		DecayInterval: decayInterval,
		DecayFactor:   decayFactor,
		Now:           now,
		JitterPct:     jitterPct,
	})
	if err != nil {
		return -1, err
	}
	res, err := selectKeyScript.Run(ctx, c.rdb, keys, args...).Int()
	if err != nil {
		return -1, fmt.Errorf("redisx: select_key: %w", err)
	}
	return res, nil
}
