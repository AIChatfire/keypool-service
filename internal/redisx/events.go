package redisx

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// Publish 向事件 Stream 追加一条事件：
// XADD keypool:events MAXLEN ~ 10000 * <event 字段>（SPEC §3）。
// event 约定字段：type,cid,idx,from,to,reason,ts；map 原样作为 XADD values。
func (c *Client) Publish(ctx context.Context, event map[string]any) (string, error) {
	if c.degraded() {
		return "", ErrDegraded
	}
	values := make([]any, 0, len(event)*2)
	for k, v := range event {
		values = append(values, k, v)
	}
	return c.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: EventsKey,
		MaxLen: 10000,
		Approx: true, // MAXLEN ~ 近似裁剪
		Values: values,
	}).Result()
}
