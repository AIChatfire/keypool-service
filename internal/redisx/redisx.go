// Package redisx 封装 keypool 对 Redis 的全部访问：
// 客户端、分布式锁、幂等、用量计数、事件流、游标与 select_key.lua。
// 键契约见 SPEC §3，统一前缀 keypool:。
package redisx

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// ErrDegraded 是包级 sentinel：Redis 不可用（main 的 degraded 模式把
// *Client 置为 nil）时，所有导出方法立即返回它而非 nil-receiver panic
// （SPEC §6 降级语义）。调用方（selector/state/api）据此映射 503/50001。
var ErrDegraded = errors.New("redisx: redis unavailable (degraded mode)")

// 键前缀与常量（SPEC §3）
const (
	KeyPrefix = "keypool:"

	// EventsKey 事件 Stream，XADD MAXLEN ~ 10000
	EventsKey = KeyPrefix + "events"

	lockTTL  = 5 * time.Second  // keypool:lock:{cid} PX
	idemTTL  = 10 * time.Minute // keypool:idem:{key} PX 600000
	leaseTTL = 10 * time.Minute // keypool:lease:{lease_id} PX 600000
)

// CursorKey 轮询游标（String, INCR）
func CursorKey(cid int) string { return fmt.Sprintf("%scursor:%d", KeyPrefix, cid) }

// UsageKey 用量计数 Hash {idx:float}
func UsageKey(cid int) string { return fmt.Sprintf("%susage:%d", KeyPrefix, cid) }

// UsageMetaKey 衰减元数据 Hash {last_decay:ts}
func UsageMetaKey(cid int) string { return fmt.Sprintf("%susage_meta:%d", KeyPrefix, cid) }

// LockKey 每渠道串行锁（String, SET NX PX 5000, value=token）
func LockKey(cid int) string { return fmt.Sprintf("%slock:%d", KeyPrefix, cid) }

// IdemKey report 幂等键（String, SET NX PX 600000）
func IdemKey(key string) string { return KeyPrefix + "idem:" + key }

// LeaseKey usage 预扣租约（String, SET PX 600000，值为 est）
func LeaseKey(leaseID string) string { return KeyPrefix + "lease:" + leaseID }

// degraded 报告 client 是否不可用（nil receiver 或未初始化底层连接）。
func (c *Client) degraded() bool { return c == nil || c.rdb == nil }

// Client 是 go-redis 的薄封装，方法即 SPEC §3 的 Redis 操作集。
type Client struct {
	rdb *redis.Client
}

// NewClient 建立连接并 PING（3s 超时）。PING 失败返回错误并关闭底层
// 连接，由调用方（main）降级为 degraded 模式继续运行（SPEC §6）。
func NewClient(addr, pass string, db int) (*Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: pass,
		DB:       db,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("redisx: ping %s: %w", addr, err)
	}
	return &Client{rdb: rdb}, nil
}

// Raw 暴露底层 go-redis client（仅用于需要原生命令的少量场景）。
// 降级（nil client）时返回 nil，调用方需判空。
func (c *Client) Raw() *redis.Client {
	if c.degraded() {
		return nil
	}
	return c.rdb
}

// Close 关闭底层连接；降级时为 no-op 返回 ErrDegraded。
func (c *Client) Close() error {
	if c.degraded() {
		return ErrDegraded
	}
	return c.rdb.Close()
}
