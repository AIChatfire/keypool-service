package redisx

import "context"

// IdemSet report 幂等：SET keypool:idem:{key} 1 NX PX 600000。
// 返回 true 表示首次写入（可继续处理），false 表示重复请求（SPEC §3.2）。
func (c *Client) IdemSet(ctx context.Context, key string) (bool, error) {
	if c.degraded() {
		return false, ErrDegraded
	}
	return c.rdb.SetNX(ctx, IdemKey(key), 1, idemTTL).Result()
}

// IdemDel 删除幂等键（幂等已 SET 但后续处理失败时回滚，见 state.Report）。
// 删除不存在的键视为成功。
func (c *Client) IdemDel(ctx context.Context, key string) error {
	if c.degraded() {
		return ErrDegraded
	}
	return c.rdb.Del(ctx, IdemKey(key)).Err()
}
