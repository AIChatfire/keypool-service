// Package state 是 keypool 的禁用/启用执行器（SPEC §4）：
// 幂等 → epoch 校验 → 用量校正 → 渠道锁 → classifier 判定 →
// ApplyKeyStatus 事务写穿 → 事件发布。单写者纪律见 SPEC §2.3。
package state

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
	"unicode/utf8"

	"gorm.io/gorm"

	"keypool/internal/classifier"
	"keypool/internal/redisx"
	"keypool/internal/store"
)

// Sentinel errors（供 api 映射错误包络：40002/40010/50001）。
var (
	ErrChannelNotFound = errors.New("state: channel not found")      // → 40002
	ErrInvalidRequest  = errors.New("state: invalid request")        // → 40010
	ErrLockFailed      = errors.New("state: failed to acquire lock") // → 50001
	// ErrDependency 是底层依赖（DB 等）故障 sentinel：GetChannel 的非
	// ErrRecordNotFound 错误包装为它（api 映射 503/50001），避免把
	// DB 故障误映射为 404/40002。
	ErrDependency = errors.New("state: dependency failure")
)

// maxReasonLen 是 disabled_reason 的截断长度（SPEC §4：errMsg 截断 256）。
const maxReasonLen = 256

// ReportReq 是用量/错误上报（SPEC §4）。
// 评审修复说明（P1-3）：KeyIndex 由 int 改为 *int 以区分“未传”（nil）
// 与显式 0——int 零值会遮蔽“未传”语义导致 key_index=0 永远优先于 key
// 字符串定位。这是 SPEC 类型的有意的字段类型变更。
type ReportReq struct {
	LeaseID                 string
	ChannelID               int
	KeyIndex                *int // nil=未传；非 nil 且 >=0 时按索引定位
	Key, Epoch              string
	Success                 bool
	StatusCode              int
	ErrorCode, ErrorMessage string
	Usage                   *Usage
	IdempotencyKey          string
}

// Usage 是单次调用的用量（SPEC §4）。
type Usage struct {
	PromptTokens, CompletionTokens float64
	Cost                           float64
}

// ReportResp 是上报/手动启停的结果（SPEC §4）。
// Action ∈ none|cooldown|key_disabled|channel_disabled|enabled|stale_epoch_ignored|duplicate
type ReportResp struct {
	Action        string `json:"action"`
	ChannelStatus int    `json:"channel_status"`
}

// channelStore 是 Manager 对 store.Store 的窄接口依赖（便于单测 fake）。
type channelStore interface {
	GetChannel(id int) (*store.Channel, error)
	ApplyKeyStatus(cid, idx, status int, reason string) (channelStatus int, allDead bool, err error)
}

// redisOps 是 Manager 对 redisx.Client 的窄接口依赖。
type redisOps interface {
	IdemSet(ctx context.Context, key string) (bool, error)
	IdemDel(ctx context.Context, key string) error
	UsageIncr(ctx context.Context, cid, idx int, delta float64) error
	LeaseTake(ctx context.Context, leaseID string) (est float64, ok bool, err error)
	Lock(ctx context.Context, cid int) (token string, ok bool, err error)
	Unlock(ctx context.Context, cid int, token string) error
	Publish(ctx context.Context, event map[string]any) (string, error)
}

// Manager 组合 store / redisx / classifier / settingsProvider（SPEC §4）。
type Manager struct {
	channels channelStore
	redis    redisOps
	sp       store.SettingsProvider

	mu      sync.Mutex
	metrics map[string]int64
}

// NewManager 装配生产 Manager（SPEC §4）。*store.Store / *redisx.Client
// 天然满足包内窄接口。
func NewManager(s *store.Store, rdb *redisx.Client, sp store.SettingsProvider) *Manager {
	return newManager(s, rdb, sp)
}

// newManager 供单测注入 fake。
func newManager(ch channelStore, ro redisOps, sp store.SettingsProvider) *Manager {
	return &Manager{channels: ch, redis: ro, sp: sp, metrics: map[string]int64{}}
}

// Report 处理一次调用结果上报（SPEC §4 流程）。
func (m *Manager) Report(ctx context.Context, r ReportReq) (resp *ReportResp, retErr error) {
	// ① 幂等：IdempotencyKey 非空 → 重复直接返回 duplicate
	idemHeld := false
	if r.IdempotencyKey != "" {
		ok, err := m.redis.IdemSet(ctx, r.IdempotencyKey)
		if err != nil {
			return nil, fmt.Errorf("state: idem: %w", err)
		}
		if !ok {
			resp := &ReportResp{Action: "duplicate"}
			m.countAction(resp.Action)
			return resp, nil
		}
		idemHeld = true
	}
	// P2-3（轻量版）：幂等键已 SET 但后续处理失败（非 duplicate 的错误
	// 返回）时 DEL 幂等键，允许调用方修正后重试。语义说明：此处幂等键
	// 只承诺“成功受理”的唯一性；处理失败即释放，避免一次失败永久
	// 阻塞相同 IdempotencyKey 的重试。DEL 失败仅尽力而为。
	defer func() {
		if idemHeld && retErr != nil {
			_ = m.redis.IdemDel(ctx, r.IdempotencyKey)
		}
	}()

	// ② 渠道加载 + epoch 校验
	ch, err := m.getChannel(r.ChannelID)
	if err != nil {
		return nil, err
	}
	if r.Epoch != "" && r.Epoch != ch.Epoch() {
		// key 集合已变化，上报基于旧快照：忽略，不做任何写
		resp := &ReportResp{Action: "stale_epoch_ignored", ChannelStatus: ch.Status}
		m.countAction(resp.Action)
		return resp, nil
	}

	// ③ 定位 idx：KeyIndex 非 nil 且 >=0 用索引（与 key 冲突 → 40010）；
	// 否则按 key 字符串精确匹配首个
	idx, err := locateKeyIndex(ch, r.KeyIndex, r.Key)
	if err != nil {
		return nil, err
	}

	// ④ 用量校正（SPEC §4）
	if r.Usage != nil {
		delta := usageValue(m.metric(r.ChannelID), r.Usage)
		if r.LeaseID != "" {
			// P1-4 租约校正：select 已预扣 est，此处取回租约并按
			// actual−est 校正（可为负，HINCRBYFLOAT 支持）。
			est, ok, err := m.redis.LeaseTake(ctx, r.LeaseID)
			if err != nil {
				return nil, fmt.Errorf("state: lease take: %w", err)
			}
			if ok {
				delta -= est
			}
			// 取不到租约（PX 10min 过期或从未写入）→ 直接按 actual
			// 全额累加：此时 select 的预扣也已随租约过期无法核对，
			// 接受少量偏差。
		}
		if err := m.redis.UsageIncr(ctx, r.ChannelID, idx, delta); err != nil {
			return nil, fmt.Errorf("state: usage incr: %w", err)
		}
	}

	// ⑤ 渠道串行锁
	token, ok, err := m.redis.Lock(ctx, r.ChannelID)
	if err != nil {
		// 双 %w：同时保留 ErrLockFailed（→50001）与底层原因
		//（如 redisx.ErrDegraded 降级）。
		return nil, fmt.Errorf("%w: %w", ErrLockFailed, err)
	}
	if !ok {
		return nil, fmt.Errorf("%w: channel %d busy", ErrLockFailed, r.ChannelID)
	}
	defer func() { _ = m.redis.Unlock(ctx, r.ChannelID, token) }()

	// P2-2（轻量版）：prevStatus 判定用的渠道在锁内重读——锁外加载的
	// 快照在等待锁期间可能已被其他写者修改，锁内重读保证禁用/启用
	// 判定基于最新状态。
	ch, err = m.getChannel(r.ChannelID)
	if err != nil {
		return nil, err
	}

	st := m.settings()
	prevStatus := keyStatus(ch, idx)
	if !ch.ChannelInfo.IsMultiKey {
		// P1-2：非多 key 渠道没有 per-key 状态表，prevStatus 必须取
		// 渠道级 status，否则自动禁用(3)的渠道成功上报后永远不会
		// 命中 ShouldEnable。
		prevStatus = ch.Status
	}

	// ⑥ 成功路径：自动启用判定
	if r.Success {
		resp := &ReportResp{Action: "none", ChannelStatus: ch.Status}
		if classifier.ShouldEnable(st, prevStatus) {
			cs, _, err := m.channels.ApplyKeyStatus(r.ChannelID, idx, store.ChannelStatusEnabled, "")
			if err != nil {
				return nil, fmt.Errorf("state: apply enable: %w", err)
			}
			resp.Action = "enabled"
			resp.ChannelStatus = cs
			m.publish(ctx, r.ChannelID, idx, prevStatus, store.ChannelStatusEnabled, "auto enable on success")
		}
		m.countAction(resp.Action)
		return resp, nil
	}

	// ⑦ 失败路径：自动禁用判定
	resp = &ReportResp{Action: "none", ChannelStatus: ch.Status}
	if classifier.ShouldDisable(st, autoBan(ch), r.StatusCode, r.ErrorMessage) {
		reason := truncate(r.ErrorMessage, maxReasonLen)
		cs, allDead, err := m.channels.ApplyKeyStatus(r.ChannelID, idx, store.ChannelStatusAutoDisabled, reason)
		if err != nil {
			return nil, fmt.Errorf("state: apply disable: %w", err)
		}
		if allDead {
			resp.Action = "channel_disabled"
		} else {
			resp.Action = "key_disabled"
		}
		resp.ChannelStatus = cs
		m.publish(ctx, r.ChannelID, idx, prevStatus, store.ChannelStatusAutoDisabled, reason)
	}
	m.countAction(resp.Action)
	return resp, nil
}

// getChannel 加载渠道并映射错误：仅 gorm.ErrRecordNotFound →
// ErrChannelNotFound（404/40002）；其余 DB 故障包装 ErrDependency
// （api 映射 503/50001），避免误报渠道不存在（P1-6）。
func (m *Manager) getChannel(cid int) (*store.Channel, error) {
	ch, err := m.channels.GetChannel(cid)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("%w: %d", ErrChannelNotFound, cid)
		}
		return nil, fmt.Errorf("%w: get channel %d: %v", ErrDependency, cid, err)
	}
	return ch, nil
}

// SetKeyStatus 管理端手动启停 key（SPEC §4：status ∈ {1,2}）。
func (m *Manager) SetKeyStatus(ctx context.Context, cid, idx, status int, reason string) (*ReportResp, error) {
	if status != store.ChannelStatusEnabled && status != store.ChannelStatusManuallyDisabled {
		return nil, fmt.Errorf("%w: status must be 1|2, got %d", ErrInvalidRequest, status)
	}
	if idx < 0 {
		return nil, fmt.Errorf("%w: key index %d", ErrInvalidRequest, idx)
	}

	token, ok, err := m.redis.Lock(ctx, cid)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrLockFailed, err)
	}
	if !ok {
		return nil, fmt.Errorf("%w: channel %d busy", ErrLockFailed, cid)
	}
	defer func() { _ = m.redis.Unlock(ctx, cid, token) }()

	ch, err := m.getChannel(cid)
	if err != nil {
		return nil, err
	}
	if _, err := locateKeyIndex(ch, &idx, ""); err != nil {
		return nil, err
	}
	prevStatus := keyStatus(ch, idx)

	cs, allDead, err := m.channels.ApplyKeyStatus(cid, idx, status, truncate(reason, maxReasonLen))
	if err != nil {
		return nil, fmt.Errorf("state: apply set status: %w", err)
	}

	// P2-4：手动禁用后所有 key 全灭 → 渠道已被联动禁用，action 如实
	// 报告 channel_disabled。
	action := "key_disabled"
	if status != store.ChannelStatusEnabled && allDead {
		action = "channel_disabled"
	}
	if status == store.ChannelStatusEnabled {
		action = "enabled"
	}
	m.publish(ctx, cid, idx, prevStatus, status, truncate(reason, maxReasonLen))
	resp := &ReportResp{Action: action, ChannelStatus: cs}
	m.countAction(action)
	return resp, nil
}

// settings 读取最新快照；nil 视为全关（classifier 对 nil 返回 false）。
func (m *Manager) settings() *store.Settings {
	if m.sp == nil {
		return nil
	}
	return m.sp.Get()
}

// metric 取渠道的 balance metric（tokens|cost），缺省 tokens。
func (m *Manager) metric(cid int) string {
	st := m.settings()
	if st != nil && st.Balance != nil {
		if cfg, ok := st.Balance[cid]; ok && cfg.Metric != "" {
			return cfg.Metric
		}
	}
	return "tokens"
}

// usageValue 按 metric 取用量值：cost 且 Cost>0 用 Cost，否则 tokens 合计。
func usageValue(metric string, u *Usage) float64 {
	if metric == "cost" && u.Cost > 0 {
		return u.Cost
	}
	return u.PromptTokens + u.CompletionTokens
}

// locateKeyIndex 实现 §2.3 的 key 定位（P1-3 修复零值遮蔽）：
// keyIndex 非 nil 且 >=0 时按索引定位（越界 → 40010）；此时若同时传了
// key 字符串且索引处的 key 与传入 key 不一致 → 40010（参数冲突）。
// 否则（nil 或负值）按 key 字符串在 GetKeys() 中精确匹配取首个。
// 两者都未给（索引 nil/负 且 key 为空）→ 40010。
func locateKeyIndex(ch *store.Channel, keyIndex *int, key string) (int, error) {
	keys := ch.GetKeys()
	if keyIndex != nil && *keyIndex >= 0 {
		if *keyIndex >= len(keys) {
			return -1, fmt.Errorf("%w: key index %d out of range (0..%d)", ErrInvalidRequest, *keyIndex, len(keys)-1)
		}
		if key != "" && keys[*keyIndex] != key {
			return -1, fmt.Errorf("%w: key_index %d points to a different key than %q", ErrInvalidRequest, *keyIndex, key)
		}
		return *keyIndex, nil
	}
	if key != "" {
		for i, k := range keys {
			if k == key {
				return i, nil
			}
		}
	}
	return -1, fmt.Errorf("%w: key not found in channel %d", ErrInvalidRequest, ch.Id)
}

// keyStatus 返回 key 当前状态：status_list 缺失视为启用(1)（SPEC §2.2）。
func keyStatus(ch *store.Channel, idx int) int {
	if st, ok := ch.ChannelInfo.MultiKeyStatusList[idx]; ok {
		return st
	}
	return store.ChannelStatusEnabled
}

// autoBan 实现 §2.4 的 auto_ban 语义：AutoBan *int，nil 或 1 → true。
func autoBan(ch *store.Channel) bool {
	return ch.AutoBan == nil || *ch.AutoBan == 1
}

// truncate 按 rune 截断到 n 个字符（保证合法 UTF-8）。
func truncate(s string, n int) string {
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	r := []rune(s)
	return string(r[:n])
}

// publish 发送 key_status 事件（SPEC §3：type,cid,idx,from,to,reason,ts）。
// 发布失败不阻断主流程（写穿已成功），仅视为可恢复的事件丢失。
func (m *Manager) publish(ctx context.Context, cid, idx, from, to int, reason string) {
	_, _ = m.redis.Publish(ctx, map[string]any{
		"type":   "key_status",
		"cid":    cid,
		"idx":    idx,
		"from":   from,
		"to":     to,
		"reason": reason,
		"ts":     time.Now().Unix(),
	})
}

// countAction 递增 reportTotal 计数器（按 action 分桶）。
func (m *Manager) countAction(action string) {
	m.mu.Lock()
	m.metrics[fmt.Sprintf(`report_total{action="%s"}`, action)]++
	m.mu.Unlock()
}

// SnapshotMetrics 返回计数器快照（供 api /metrics 读取）。
// 键：report_total{action="..."}。
func (m *Manager) SnapshotMetrics() map[string]int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]int64, len(m.metrics))
	for k, v := range m.metrics {
		out[k] = v
	}
	return out
}
