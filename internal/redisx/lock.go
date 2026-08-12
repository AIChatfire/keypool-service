package redisx

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"

	"github.com/redis/go-redis/v9"
)

// unlockScript 校验 value 后 DEL，防止误删他人锁（SPEC §3.2）。
var unlockScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
else
	return 0
end
`)

// randomToken 生成锁持有者 token（uuid 语义，标准库实现避免新增依赖）。
func randomToken() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// Lock 尝试获取渠道串行锁：SET keypool:lock:{cid} <token> NX PX 5000。
// 返回 (token, ok, err)：ok=false 且 err=nil 表示锁被占用。
//
// 看门狗（锁续期）未实现：当前锁租约 5s，足以覆盖禁用/启用写穿事务
// 的时长；若后续引入更长临界区，应在此增加自动续期 goroutine。
func (c *Client) Lock(ctx context.Context, cid int) (token string, ok bool, err error) {
	if c.degraded() {
		return "", false, ErrDegraded
	}
	token, err = randomToken()
	if err != nil {
		return "", false, err
	}
	ok, err = c.rdb.SetNX(ctx, LockKey(cid), token, lockTTL).Result()
	if err != nil {
		return "", false, err
	}
	return token, ok, nil
}

// Unlock 释放锁：Lua 校验 value==token 后 DEL。token 不匹配视为已释放
// （锁可能已过期被他人获得），返回 nil。
func (c *Client) Unlock(ctx context.Context, cid int, token string) error {
	if c.degraded() {
		return ErrDegraded
	}
	n, err := unlockScript.Run(ctx, c.rdb, []string{LockKey(cid)}, token).Int()
	if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	_ = n // 0=未持锁/已过期, 1=已释放；两者对调用方均视为成功
	return nil
}
