package redisx

import "context"

// CursorReset 删除轮询游标（key 集合变化后调用，避免取模错位）。
func (c *Client) CursorReset(ctx context.Context, cid int) error {
	if c.degraded() {
		return ErrDegraded
	}
	return c.rdb.Del(ctx, CursorKey(cid)).Err()
}
