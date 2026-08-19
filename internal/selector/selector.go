// Package selector 实现 keypool 的渠道选择与 key 选取（SPEC §4）。
//
// 流程：渠道确定（直达或 abilities 档位加权）→ 候选集（EnabledKeyIndexes ∩
// 轮换批次，含 look-ahead）→ Exclude 过滤 → mode 解析 → redisx.SelectKey
// 单 RTT 取 idx → 组装 SelectResp。
//
// 精确直达：SelectReq.KeyIndex 非 nil 时走 selectDirect 短路——在渠道加载后
// 立即校验索引范围与 key 启用状态并返回，跳过候选集/Exclude/Redis 选 key
// （零 Redis RTT、不推游标、不做 usage 打分与预扣）。
//
// 缓存说明：渠道对象每次 Select 都从 Store 实时读取（DB 级），不引入本地
// 缓存，保证禁用/启用写穿后立即生效。后续若成为热点，可在 Store 之上加一层
// 带 TTL/失效通知的渠道快照缓存（由 keypool:events 或 POST /v1/settings/reload
// 驱动失效），Selector 无需改动。
package selector

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"gorm.io/gorm"

	"keypool/internal/redisx"
	"keypool/internal/store"
)

// Sentinel errors（SPEC §5：ErrNoKey→40001，ErrNoChannel→40002）。
var (
	ErrNoKey     = errors.New("selector: no available key")
	ErrNoChannel = errors.New("selector: channel not found")
	// ErrDependency 是底层依赖（DB/Redis）故障 sentinel：GetChannel 的
	// 非 ErrRecordNotFound 错误、Abilities 错误均包装为它（api 映射
	// 503/50001），避免把 DB 故障误映射为 404/40002。
	ErrDependency = errors.New("selector: dependency failure")
	// ErrInvalidRequest 是参数非法 sentinel（api 映射 400/40010）：
	// key_index 精确直达的前置校验（缺 channel_id、索引越界）用它。
	ErrInvalidRequest = errors.New("selector: invalid request")
)

// SelectReq 是 key 选取请求（SPEC §4，签名一字不改）。
type SelectReq struct {
	ChannelID     int
	Group, Model  string
	Retry         int
	Exclude       []KeyRef
	Mode          string // ""|polling|random|usage 覆盖渠道配置
	EstTokens     float64
	AdvanceCursor bool // 默认 true；false=测活不推游标
	// IncludeChannel 为 true 时 SelectResp 附带 new-api 渠道元数据
	//（渠道已在选取流程中加载，零额外 DB 开销）。
	IncludeChannel bool
	// KeyIndex 非 nil 时启用「channel_id + key_index 精确直达」：
	// 跳过候选集构建（轮换批次/exclude）与 Redis 选 key（游标/usage 打分），
	// 只做索引范围与 key 启用状态校验后直接返回该 key。
	// 约束：必须与 ChannelID>0 同时给出（否则 ErrInvalidRequest）；
	// 与 Mode/EstTokens/AdvanceCursor/Exclude 组合时后者一律被忽略。
	KeyIndex *int
}

// KeyRef 定位某渠道的一个 key（SPEC §4）。
type KeyRef struct{ ChannelID, KeyIndex int }

// SelectResp 是选取结果（SPEC §4）。
type SelectResp struct {
	ChannelID, KeyIndex       int
	Key, BaseURL, Mode, Epoch string
	Band                      *BandInfo `json:"band,omitempty"`
	// LeaseID 是 usage 预扣租约 id（usage 模式且 est>0 时生成），report
	// 回传以校正 actual−est 双重计数（SPEC §4 方案 b）。
	LeaseID string `json:"lease_id,omitempty"`
	// Channel 是 new-api 渠道元数据（仅 SelectReq.IncludeChannel 时填充）。
	Channel *store.ChannelMeta `json:"channel,omitempty"`
}

// BandInfo 描述当前轮换时间带（SPEC §4）。
type BandInfo struct {
	Index  int
	EndsAt int64
}

// 缺省值（SPEC §4）：usage 衰减与抖动。
const (
	defaultDecayInterval = 3600.0
	defaultDecayFactor   = 0.5
	jitterPct            = 0.05
)

// channelStore 是 Selector 对 store.Store 的窄接口依赖（便于单测 fake）。
type channelStore interface {
	GetChannel(id int) (*store.Channel, error)
	Abilities(group, model string) ([]store.Ability, error)
}

// keySelector 是 Selector 对 redisx.Client 的窄接口依赖（单 RTT Lua）。
type keySelector interface {
	SelectKey(ctx context.Context, mode string, cid int, candidates []int, est, decayInterval, decayFactor float64, now int64, jitterPct float64) (int, error)
}

// leaser 是租约写入能力（*redisx.Client 满足）。fake 可通过实现该接口
// 验证租约行为；不实现时 Selector 跳过租约（保持旧行为，仅单测）。
type leaser interface {
	LeaseSet(ctx context.Context, leaseID string, est float64) error
}

// Selector 组合 store / redisx / settingsProvider（SPEC §4）。
type Selector struct {
	channels channelStore
	keys     keySelector
	leases   leaser // nil = 无租约能力（单测 fake）
	sp       store.SettingsProvider

	mu      sync.Mutex
	rnd     *rand.Rand
	metrics map[string]int64
}

// NewSelector 装配生产 Selector（SPEC §4）。具体类型 *store.Store /
// *redisx.Client 天然满足包内窄接口。
func NewSelector(s *store.Store, rdb *redisx.Client, sp store.SettingsProvider) *Selector {
	return newSelector(s, rdb, sp)
}

// newSelector 供单测注入 fake。ks 若同时实现 leaser（如 *redisx.Client）
// 则启用 usage 预扣租约写入。
func newSelector(ch channelStore, ks keySelector, sp store.SettingsProvider) *Selector {
	sl := &Selector{
		channels: ch,
		keys:     ks,
		sp:       sp,
		rnd:      rand.New(rand.NewSource(time.Now().UnixNano())),
		metrics:  map[string]int64{},
	}
	if l, ok := ks.(leaser); ok {
		sl.leases = l
	}
	return sl
}

// modeDirect 是 key_index 精确直达返回的 mode 标识（非调度模式，
// 表示"调用方指定索引、未走任何选取算法"）。
const modeDirect = "direct"

// Select 选取一个可用 key（SPEC §4）。
func (sl *Selector) Select(ctx context.Context, req SelectReq) (*SelectResp, error) {
	now := time.Now().Unix()

	// ⓿ key_index 精确直达前置校验：必须搭配 channel_id，索引非负。
	if req.KeyIndex != nil {
		if req.ChannelID <= 0 {
			return nil, fmt.Errorf("%w: key_index requires channel_id", ErrInvalidRequest)
		}
		if *req.KeyIndex < 0 {
			return nil, fmt.Errorf("%w: key_index must be >= 0", ErrInvalidRequest)
		}
	}

	// ① 渠道确定
	ch, err := sl.resolveChannel(req)
	if err != nil {
		return nil, err
	}
	cid := ch.Id

	// ①.5 精确直达：跳过候选集/Exclude/Redis 选 key，只校验索引与启用状态。
	if req.KeyIndex != nil {
		return sl.selectDirect(ch, *req.KeyIndex, req)
	}

	// ② 候选集：EnabledKeyIndexes ∩ 轮换批次（含 look-ahead）
	st := sl.settings()
	candidates, band, lookaheadSteps := sl.candidates(ch, st, now)
	if len(candidates) == 0 {
		return nil, ErrNoKey
	}
	if lookaheadSteps > 0 {
		sl.incr("band_lookahead_total", 1)
	}

	// ③ Exclude 过滤（匹配 ChannelID+KeyIndex）
	if len(req.Exclude) > 0 {
		excluded := make(map[int]bool, len(req.Exclude))
		for _, ex := range req.Exclude {
			if ex.ChannelID == cid {
				excluded[ex.KeyIndex] = true
			}
		}
		filtered := candidates[:0]
		for _, idx := range candidates {
			if !excluded[idx] {
				filtered = append(filtered, idx)
			}
		}
		candidates = filtered
		if len(candidates) == 0 {
			return nil, ErrNoKey
		}
	}
	sort.Ints(candidates)

	// ④ mode 解析
	mode := resolveMode(req, st, ch, cid)
	luaMode := mode
	if !req.AdvanceCursor && mode == "polling" {
		luaMode = "peek" // 测活：只读游标不 INCR（SPEC §3.1/§4）
	}

	// ⑤ redisx.SelectKey 单 RTT 取 idx
	est := req.EstTokens // 缺省 0
	decayInterval, decayFactor := decayParams(st, cid)
	idx, err := sl.keys.SelectKey(ctx, luaMode, cid, candidates, est, decayInterval, decayFactor, now, jitterPct)
	if err != nil {
		return nil, fmt.Errorf("selector: select key: %w", err)
	}
	if idx < 0 {
		return nil, ErrNoKey
	}
	keys := ch.GetKeys()
	if idx >= len(keys) {
		return nil, ErrNoKey
	}

	// ⑥ 组装 SelectResp + 计数器
	resp := &SelectResp{
		ChannelID: cid,
		KeyIndex:  idx,
		Key:       keys[idx],
		BaseURL:   ch.BaseURL, // 空即渠道默认占位 ""
		Mode:      mode,
		Epoch:     ch.Epoch(),
	}
	if band != nil {
		resp.Band = band
	}
	if req.IncludeChannel {
		resp.Channel = ch.Meta() // 渠道已在步骤①加载，零额外 DB 开销
	}

	// usage 预扣租约（SPEC §4 方案 b）：Lua 已预扣 est，生成 lease_id
	// 并记录 est，report 回传后按 actual−est 校正，避免双重计数。
	if mode == "usage" && est > 0 && sl.leases != nil {
		leaseID, err := newLeaseID()
		if err != nil {
			return nil, fmt.Errorf("selector: lease id: %w", err)
		}
		if err := sl.leases.LeaseSet(ctx, leaseID, est); err != nil {
			return nil, fmt.Errorf("selector: lease set: %w", err)
		}
		resp.LeaseID = leaseID
	}

	sl.incr(fmt.Sprintf(`select_total{cid="%d",idx="%d"}`, cid, idx), 1)
	return resp, nil
}

// selectDirect 实现 channel_id + key_index 精确直达（零 Redis RTT）：
// 只做索引范围与 key 启用状态校验，直接返回该 key。
//
// 语义约定：
//   - 索引越界（>= len(keys)）→ ErrInvalidRequest（400/40010，永久性错误，
//     重试无意义；调用方应改走常规选取）。
//   - key 被禁用（multi_key_status_list[idx] != enabled）→ ErrNoKey
//     （503/40001，可能被重新启用，语义上可重试）。
//   - 不推轮询游标、不做 usage 打分/预扣、不签发 lease_id、不返回 band；
//     mode 固定返回 "direct"，req 的 Mode/EstTokens/AdvanceCursor/Exclude 被忽略。
func (sl *Selector) selectDirect(ch *store.Channel, idx int, req SelectReq) (*SelectResp, error) {
	cid := ch.Id
	keys := ch.GetKeys()
	if idx >= len(keys) {
		return nil, fmt.Errorf("%w: key_index %d out of range (channel %d has %d keys)",
			ErrInvalidRequest, idx, cid, len(keys))
	}
	if st, ok := ch.ChannelInfo.MultiKeyStatusList[idx]; ok && st != store.ChannelStatusEnabled {
		return nil, ErrNoKey // key 被禁用：按无可用 key 处理
	}
	resp := &SelectResp{
		ChannelID: cid,
		KeyIndex:  idx,
		Key:       keys[idx],
		BaseURL:   ch.BaseURL,
		Mode:      modeDirect,
		Epoch:     ch.Epoch(),
	}
	if req.IncludeChannel {
		resp.Channel = ch.Meta()
	}
	sl.incr(fmt.Sprintf(`select_total{cid="%d",idx="%d"}`, cid, idx), 1)
	sl.incr("select_direct_total", 1)
	return resp, nil
}

// newLeaseID 生成 crypto/rand 16 字节 hex 租约 id。
func newLeaseID() (string, error) {
	var b [16]byte
	if _, err := crand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// resolveChannel 实现步骤①：cid 直达或 abilities 档位加权选择。
func (sl *Selector) resolveChannel(req SelectReq) (*store.Channel, error) {
	if req.ChannelID > 0 {
		ch, err := sl.channels.GetChannel(req.ChannelID)
		if err != nil {
			return nil, mapGetChannelErr(req.ChannelID, err)
		}
		if ch.Status != store.ChannelStatusEnabled {
			return nil, ErrNoKey // 渠道未启用：当无候选处理
		}
		return ch, nil
	}

	abs, err := sl.channels.Abilities(req.Group, req.Model)
	if err != nil {
		return nil, fmt.Errorf("%w: abilities: %v", ErrDependency, err)
	}
	if len(abs) == 0 {
		return nil, ErrNoChannel
	}
	tiers := priorityTiers(abs)
	tier := tierForRetry(tiers, req.Retry)
	sl.mu.Lock()
	i := pickWeightedIndex(tierWeights(tier), sl.rnd)
	sl.mu.Unlock()
	ch, err := sl.channels.GetChannel(tier[i].ChannelId)
	if err != nil {
		return nil, mapGetChannelErr(tier[i].ChannelId, err)
	}
	if ch.Status != store.ChannelStatusEnabled {
		return nil, ErrNoKey // 渠道未启用：当无候选处理
	}
	return ch, nil
}

// mapGetChannelErr 区分 GetChannel 错误：仅 ErrRecordNotFound 映射
// ErrNoChannel（404/40002）；其余（连接失败等 DB 故障）包装 ErrDependency
// （api 映射 503/50001），避免 DB 故障误报为渠道不存在（P1-6）。
func mapGetChannelErr(id int, err error) error {
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return fmt.Errorf("%w: %d", ErrNoChannel, id)
	}
	return fmt.Errorf("%w: get channel %d: %v", ErrDependency, id, err)
}

// settings 读取最新快照；nil provider / nil snapshot 均视为无配置。
func (sl *Selector) settings() *store.Settings {
	if sl.sp == nil {
		return nil
	}
	return sl.sp.Get()
}

// candidates 计算候选集：EnabledKeyIndexes ∩ 轮换批次。
// 无轮换配置时返回全部启用索引。返回 band 信息（仅轮换启用时非 nil）与
// look-ahead 步数。
func (sl *Selector) candidates(ch *store.Channel, st *store.Settings, now int64) (cands []int, band *BandInfo, lookaheadSteps int) {
	enabled := ch.EnabledKeyIndexes()
	cfg, ok := rotationCfgOf(st, ch.Id)
	if !ok {
		return enabled, nil, 0
	}

	keyCount := len(ch.GetKeys())
	batches := buildBatches(cfg, ch.Id, keyCount)
	if len(batches) == 0 {
		return nil, &BandInfo{}, 0
	}
	bandNum := now / int64(cfg.BandSeconds)

	enabledSet := make(map[int]bool, len(enabled))
	for _, i := range enabled {
		enabledSet[i] = true
	}

	cands = bandCandidates(batches, enabledSet, bandNum, cfg.OverlapBands)
	steps := 0
	if len(cands) == 0 {
		cands, steps = lookahead(batches, enabledSet, bandNum)
	}
	if len(cands) == 0 {
		return nil, &BandInfo{Index: int(bandNum % int64(len(batches))), EndsAt: (bandNum + 1) * int64(cfg.BandSeconds)}, steps
	}
	// look-ahead 命中（steps>0）时 BandInfo 按实际返回的 band 计算
	// Index 与 EndsAt，而非当前 band（P2-11）。
	return cands, &BandInfo{
		Index:  int((bandNum + int64(steps)) % int64(len(batches))),
		EndsAt: (bandNum + int64(steps) + 1) * int64(cfg.BandSeconds),
	}, steps
}

// rotationCfgOf 取渠道的轮换配置；缺配置或配置非法（BandSeconds/ActiveCount<=0）
// 视为未启用轮换。
func rotationCfgOf(st *store.Settings, cid int) (store.RotationCfg, bool) {
	if st == nil || st.Rotation == nil {
		return store.RotationCfg{}, false
	}
	cfg, ok := st.Rotation[cid]
	if !ok || cfg.BandSeconds <= 0 || cfg.ActiveCount <= 0 {
		return store.RotationCfg{}, false
	}
	return cfg, true
}

// resolveMode 实现步骤④的 mode 解析：
// req.Mode 非空覆盖；否则 Balance.Mode：usage→usage；request→
// channel_info.multi_key_mode；auto→（简化为 request）。最终空值落到
// channel_info.multi_key_mode，仍空 → polling。
func resolveMode(req SelectReq, st *store.Settings, ch *store.Channel, cid int) string {
	if req.Mode != "" {
		return req.Mode
	}
	mode := ""
	if st != nil && st.Balance != nil {
		mode = st.Balance[cid].Mode
	}
	if mode == "usage" {
		return "usage"
	}
	// request / auto / ""：auto 按渠道是否有用量上报历史简化为 request，
	// request 映射 channel_info.multi_key_mode（polling|random）。
	if ch.ChannelInfo.MultiKeyMode == "random" {
		return "random"
	}
	return "polling"
}

// decayParams 取 usage 衰减参数，缺省 interval=3600 factor=0.5。
func decayParams(st *store.Settings, cid int) (interval, factor float64) {
	interval, factor = defaultDecayInterval, defaultDecayFactor
	if st == nil || st.Balance == nil {
		return interval, factor
	}
	cfg, ok := st.Balance[cid]
	if !ok {
		return interval, factor
	}
	if cfg.DecayInterval > 0 {
		interval = cfg.DecayInterval
	}
	if cfg.DecayFactor > 0 {
		factor = cfg.DecayFactor
	}
	return interval, factor
}

// incr 递增计数器（供 SnapshotMetrics 读取）。
func (sl *Selector) incr(key string, delta int64) {
	sl.mu.Lock()
	sl.metrics[key] += delta
	sl.mu.Unlock()
}

// SnapshotMetrics 返回计数器快照（供 api /metrics 读取）。
// 键：select_total{cid="X",idx="Y"}、band_lookahead_total、
// select_direct_total（key_index 精确直达命中次数）。
func (sl *Selector) SnapshotMetrics() map[string]int64 {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	out := make(map[string]int64, len(sl.metrics))
	for k, v := range sl.metrics {
		out[k] = v
	}
	return out
}
