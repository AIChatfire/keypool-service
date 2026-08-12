package state

import (
	"context"
	"errors"
	"strings"
	"testing"

	"gorm.io/gorm"

	"keypool/internal/redisx"
	"keypool/internal/store"
)

// intPtr 是 ReportReq.KeyIndex（*int，P1-3）的测试辅助。
func intPtr(i int) *int { return &i }

// ---- fakes（窄接口注入，不依赖真实 DB/Redis）----

type fakeChannels struct {
	ch         *store.Channel
	getErr     error
	applied    []appliedCall
	applyRetCS int
	applyDead  bool
	applyErr   error
}

type appliedCall struct {
	cid, idx, status int
	reason           string
}

func (f *fakeChannels) GetChannel(id int) (*store.Channel, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	return f.ch, nil
}

func (f *fakeChannels) ApplyKeyStatus(cid, idx, status int, reason string) (int, bool, error) {
	f.applied = append(f.applied, appliedCall{cid, idx, status, reason})
	return f.applyRetCS, f.applyDead, f.applyErr
}

type fakeRedis struct {
	idemOK     bool
	idemErr    error
	idemDelN   int // IdemDel 调用次数（P2-3 回滚验证）
	usageDelta float64
	leaseEst   float64 // 租约中的 est（P1-4）
	leaseOK    bool    // 租约是否存在
	leaseErr   error
	lockOK     bool
	lockErr    error
	events     []map[string]any
}

func (f *fakeRedis) IdemSet(ctx context.Context, key string) (bool, error) {
	return f.idemOK, f.idemErr
}
func (f *fakeRedis) IdemDel(ctx context.Context, key string) error {
	f.idemDelN++
	return nil
}
func (f *fakeRedis) UsageIncr(ctx context.Context, cid, idx int, delta float64) error {
	f.usageDelta = delta
	return nil
}
func (f *fakeRedis) LeaseTake(ctx context.Context, leaseID string) (float64, bool, error) {
	return f.leaseEst, f.leaseOK, f.leaseErr
}
func (f *fakeRedis) Lock(ctx context.Context, cid int) (string, bool, error) {
	return "tok", f.lockOK, f.lockErr
}
func (f *fakeRedis) Unlock(ctx context.Context, cid int, token string) error { return nil }
func (f *fakeRedis) Publish(ctx context.Context, ev map[string]any) (string, error) {
	f.events = append(f.events, ev)
	return "1-1", nil
}

type fakeSP struct{ st *store.Settings }

func (f fakeSP) Get() *store.Settings { return f.st }

func testChannel() *store.Channel {
	ab := 1
	return &store.Channel{
		Id:      11,
		Key:     "alpha\nbeta\ngamma",
		Status:  store.ChannelStatusEnabled,
		AutoBan: &ab,
		ChannelInfo: store.ChannelInfo{
			IsMultiKey:         true,
			MultiKeySize:       3,
			MultiKeyStatusList: map[int]int{},
			MultiKeyMode:       "polling",
		},
	}
}

func settingsOn() *store.Settings {
	return &store.Settings{
		AutoDisableOn:     true,
		AutoEnableOn:      true,
		DisableCodeRanges: []store.CodeRange{{Start: 401, End: 401}},
		DisableKeywords:   []string{"permission denied"},
	}
}

// ---- 验收要求的两条核心用例 ----

func TestReportStaleEpoch(t *testing.T) {
	fc := &fakeChannels{ch: testChannel()}
	fr := &fakeRedis{idemOK: true, lockOK: true}
	m := newManager(fc, fr, fakeSP{settingsOn()})

	resp, err := m.Report(context.Background(), ReportReq{
		ChannelID: 11, KeyIndex: intPtr(0), Success: false,
		StatusCode: 401, Epoch: "deadbeef", // 与 ch.Epoch() 不一致
	})
	if err != nil {
		t.Fatalf("Report err: %v", err)
	}
	if resp.Action != "stale_epoch_ignored" {
		t.Fatalf("Action = %q, want stale_epoch_ignored", resp.Action)
	}
	if len(fc.applied) != 0 {
		t.Fatalf("stale epoch must not write, applied=%v", fc.applied)
	}
}

func TestReportDuplicateIdempotencyKey(t *testing.T) {
	fc := &fakeChannels{ch: testChannel()}
	fr := &fakeRedis{idemOK: false} // 已存在 → 重复
	m := newManager(fc, fr, fakeSP{settingsOn()})

	resp, err := m.Report(context.Background(), ReportReq{
		ChannelID: 11, KeyIndex: intPtr(0), Success: true, IdempotencyKey: "k-1",
	})
	if err != nil {
		t.Fatalf("Report err: %v", err)
	}
	if resp.Action != "duplicate" {
		t.Fatalf("Action = %q, want duplicate", resp.Action)
	}
	if len(fc.applied) != 0 {
		t.Fatal("duplicate must not reach store")
	}
}

// ---- 其余路径 ----

func TestReportChannelNotFound(t *testing.T) {
	m := newManager(&fakeChannels{getErr: gorm.ErrRecordNotFound}, &fakeRedis{}, fakeSP{})
	_, err := m.Report(context.Background(), ReportReq{ChannelID: 99, KeyIndex: intPtr(0), Success: true})
	if !errors.Is(err, ErrChannelNotFound) {
		t.Fatalf("err = %v, want ErrChannelNotFound", err)
	}
}

// P1-6 回归：GetChannel 的非 ErrRecordNotFound 错误（DB 故障）必须映射
// ErrDependency（→503/50001），不得误报为渠道不存在（40002）。
func TestReportChannelDBErrorMapsDependency(t *testing.T) {
	m := newManager(&fakeChannels{getErr: errors.New("connection refused")}, &fakeRedis{}, fakeSP{})
	_, err := m.Report(context.Background(), ReportReq{ChannelID: 99, KeyIndex: intPtr(0), Success: true})
	if !errors.Is(err, ErrDependency) {
		t.Fatalf("err = %v, want ErrDependency", err)
	}
	if errors.Is(err, ErrChannelNotFound) {
		t.Fatalf("err = %v, must not be ErrChannelNotFound", err)
	}
	// SetKeyStatus 同样映射（锁成功，失败在锁内 GetChannel）
	m2 := newManager(&fakeChannels{getErr: errors.New("connection refused")}, &fakeRedis{lockOK: true}, fakeSP{})
	_, err = m2.SetKeyStatus(context.Background(), 99, 0, 1, "")
	if !errors.Is(err, ErrDependency) {
		t.Fatalf("SetKeyStatus err = %v, want ErrDependency", err)
	}
}

func TestReportLocateKeyByString(t *testing.T) {
	fc := &fakeChannels{ch: testChannel(), applyRetCS: 1}
	fr := &fakeRedis{lockOK: true}
	// AutoEnableOn 开，但 key 之前状态为启用(1) → 不触发 enabled
	m := newManager(fc, fr, fakeSP{settingsOn()})
	resp, err := m.Report(context.Background(), ReportReq{
		ChannelID: 11, Key: "beta", Success: true, // KeyIndex 未传（nil）→ 按 key 定位
	})
	if err != nil {
		t.Fatalf("Report err: %v", err)
	}
	if resp.Action != "none" {
		t.Fatalf("Action = %q, want none", resp.Action)
	}
	// key 字符串找不到 → 参数错误
	_, err = m.Report(context.Background(), ReportReq{ChannelID: 11, Key: "nope", Success: true})
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("err = %v, want ErrInvalidRequest", err)
	}
}

func TestReportSuccessAutoEnable(t *testing.T) {
	ch := testChannel()
	ch.ChannelInfo.MultiKeyStatusList = map[int]int{1: store.ChannelStatusAutoDisabled}
	fc := &fakeChannels{ch: ch, applyRetCS: 1}
	fr := &fakeRedis{lockOK: true}
	m := newManager(fc, fr, fakeSP{settingsOn()})

	resp, err := m.Report(context.Background(), ReportReq{ChannelID: 11, KeyIndex: intPtr(1), Success: true})
	if err != nil {
		t.Fatalf("Report err: %v", err)
	}
	if resp.Action != "enabled" {
		t.Fatalf("Action = %q, want enabled", resp.Action)
	}
	if len(fc.applied) != 1 || fc.applied[0].status != store.ChannelStatusEnabled {
		t.Fatalf("applied = %+v", fc.applied)
	}
	if len(fr.events) != 1 || fr.events[0]["type"] != "key_status" || fr.events[0]["to"] != 1 {
		t.Fatalf("event = %+v", fr.events)
	}
}

func TestReportFailureAutoDisable(t *testing.T) {
	fc := &fakeChannels{ch: testChannel(), applyRetCS: 1, applyDead: false}
	fr := &fakeRedis{lockOK: true}
	m := newManager(fc, fr, fakeSP{settingsOn()})

	long := strings.Repeat("x", 300)
	resp, err := m.Report(context.Background(), ReportReq{
		ChannelID: 11, KeyIndex: intPtr(0), Success: false,
		StatusCode: 401, ErrorMessage: long,
	})
	if err != nil {
		t.Fatalf("Report err: %v", err)
	}
	if resp.Action != "key_disabled" {
		t.Fatalf("Action = %q, want key_disabled", resp.Action)
	}
	if len(fc.applied) != 1 || fc.applied[0].status != 3 {
		t.Fatalf("applied = %+v", fc.applied)
	}
	if got := len([]rune(fc.applied[0].reason)); got != maxReasonLen {
		t.Fatalf("reason len = %d, want %d (truncated)", got, maxReasonLen)
	}

	// 全灭 → channel_disabled
	fc2 := &fakeChannels{ch: testChannel(), applyRetCS: 3, applyDead: true}
	m2 := newManager(fc2, &fakeRedis{lockOK: true}, fakeSP{settingsOn()})
	resp2, err := m2.Report(context.Background(), ReportReq{ChannelID: 11, KeyIndex: intPtr(0), Success: false, StatusCode: 401})
	if err != nil {
		t.Fatalf("Report err: %v", err)
	}
	if resp2.Action != "channel_disabled" || resp2.ChannelStatus != 3 {
		t.Fatalf("resp2 = %+v", resp2)
	}

	// 不命中判定 → none
	m3 := newManager(&fakeChannels{ch: testChannel()}, &fakeRedis{lockOK: true}, fakeSP{settingsOn()})
	resp3, _ := m3.Report(context.Background(), ReportReq{ChannelID: 11, KeyIndex: intPtr(0), Success: false, StatusCode: 429, ErrorMessage: "slow down"})
	if resp3.Action != "none" {
		t.Fatalf("resp3.Action = %q, want none", resp3.Action)
	}
}

func TestReportUsageCorrection(t *testing.T) {
	fc := &fakeChannels{ch: testChannel()}
	fr := &fakeRedis{lockOK: true}
	st := settingsOn()
	st.Balance = map[int]store.BalanceCfg{11: {Metric: "cost"}}
	m := newManager(fc, fr, fakeSP{st})

	_, err := m.Report(context.Background(), ReportReq{
		ChannelID: 11, KeyIndex: intPtr(0), Success: true,
		Usage: &Usage{PromptTokens: 100, CompletionTokens: 50, Cost: 0.02},
	})
	if err != nil {
		t.Fatalf("Report err: %v", err)
	}
	if fr.usageDelta != 0.02 {
		t.Fatalf("usage delta = %v, want 0.02 (cost metric)", fr.usageDelta)
	}

	// tokens metric：prompt+completion
	fr2 := &fakeRedis{lockOK: true}
	m2 := newManager(&fakeChannels{ch: testChannel()}, fr2, fakeSP{settingsOn()})
	_, _ = m2.Report(context.Background(), ReportReq{
		ChannelID: 11, KeyIndex: intPtr(0), Success: true,
		Usage: &Usage{PromptTokens: 100, CompletionTokens: 50},
	})
	if fr2.usageDelta != 150 {
		t.Fatalf("usage delta = %v, want 150", fr2.usageDelta)
	}
}

func TestReportLockFailure(t *testing.T) {
	m := newManager(&fakeChannels{ch: testChannel()}, &fakeRedis{lockOK: false}, fakeSP{settingsOn()})
	_, err := m.Report(context.Background(), ReportReq{ChannelID: 11, KeyIndex: intPtr(0), Success: true})
	if !errors.Is(err, ErrLockFailed) {
		t.Fatalf("err = %v, want ErrLockFailed", err)
	}
}

func TestSetKeyStatus(t *testing.T) {
	fc := &fakeChannels{ch: testChannel(), applyRetCS: 1}
	fr := &fakeRedis{lockOK: true}
	m := newManager(fc, fr, fakeSP{settingsOn()})

	// 手动禁用
	resp, err := m.SetKeyStatus(context.Background(), 11, 2, 2, "manual")
	if err != nil {
		t.Fatalf("SetKeyStatus err: %v", err)
	}
	if resp.Action != "key_disabled" {
		t.Fatalf("Action = %q, want key_disabled", resp.Action)
	}
	if fc.applied[0].status != 2 || fc.applied[0].reason != "manual" {
		t.Fatalf("applied = %+v", fc.applied[0])
	}
	if len(fr.events) != 1 {
		t.Fatal("expected 1 event")
	}

	// 手动启用
	resp, err = m.SetKeyStatus(context.Background(), 11, 2, 1, "")
	if err != nil || resp.Action != "enabled" {
		t.Fatalf("enable: resp=%+v err=%v", resp, err)
	}

	// 非法 status
	_, err = m.SetKeyStatus(context.Background(), 11, 0, 3, "")
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("err = %v, want ErrInvalidRequest", err)
	}

	// metrics
	if got := m.SnapshotMetrics()[`report_total{action="enabled"}`]; got != 1 {
		t.Fatalf("report_total enabled = %d", got)
	}
}

func TestAutoBanSemantics(t *testing.T) {
	// nil → true
	if !autoBan(&store.Channel{AutoBan: nil}) {
		t.Fatal("nil AutoBan should be true")
	}
	one, zero := 1, 0
	if !autoBan(&store.Channel{AutoBan: &one}) {
		t.Fatal("AutoBan=1 should be true")
	}
	if autoBan(&store.Channel{AutoBan: &zero}) {
		t.Fatal("AutoBan=0 should be false")
	}
	// auto_ban=0 时即使 401 也不禁用
	ch := testChannel()
	ch.AutoBan = &zero
	m := newManager(&fakeChannels{ch: ch}, &fakeRedis{lockOK: true}, fakeSP{settingsOn()})
	resp, err := m.Report(context.Background(), ReportReq{ChannelID: 11, KeyIndex: intPtr(0), Success: false, StatusCode: 401})
	if err != nil {
		t.Fatalf("Report err: %v", err)
	}
	if resp.Action != "none" {
		t.Fatalf("Action = %q, want none (auto_ban=0)", resp.Action)
	}
}

// ---- 评审修复回归测试（P1/P2）----

// P1-2 回归：非多 key 渠道（status=3 自动禁用）在 AutoEnableOn 下成功上报
// 后必须触发自动启用——prevStatus 取渠道级 status 而非 per-key 状态表。
func TestReportNonMultiKeyAutoEnable(t *testing.T) {
	ch := &store.Channel{
		Id:     12,
		Key:    "solo",
		Status: store.ChannelStatusAutoDisabled, // 渠道被自动禁用
		// ChannelInfo.IsMultiKey 缺省 false
	}
	fc := &fakeChannels{ch: ch, applyRetCS: store.ChannelStatusEnabled}
	fr := &fakeRedis{lockOK: true}
	m := newManager(fc, fr, fakeSP{settingsOn()})

	resp, err := m.Report(context.Background(), ReportReq{ChannelID: 12, KeyIndex: intPtr(0), Success: true})
	if err != nil {
		t.Fatalf("Report err: %v", err)
	}
	if resp.Action != "enabled" {
		t.Fatalf("Action = %q, want enabled (non-multi-key channel was auto-disabled)", resp.Action)
	}
	if resp.ChannelStatus != store.ChannelStatusEnabled {
		t.Fatalf("ChannelStatus = %d, want 1", resp.ChannelStatus)
	}
	if len(fc.applied) != 1 || fc.applied[0].status != store.ChannelStatusEnabled {
		t.Fatalf("applied = %+v", fc.applied)
	}
}

// P1-3 回归：只传 key="gamma"（不传 key_index）定位到 idx=2。
func TestReportLocateByKeyStringOnly(t *testing.T) {
	fc := &fakeChannels{ch: testChannel()}
	fr := &fakeRedis{lockOK: true}
	m := newManager(fc, fr, fakeSP{settingsOn()})

	// 让用量路径暴露最终 idx（fakeRedis.UsageIncr 记录不到 idx，
	// 改用失败上报 + 事件断言 idx）。
	resp, err := m.Report(context.Background(), ReportReq{
		ChannelID: 11, Key: "gamma", Success: false, StatusCode: 401,
	})
	if err != nil {
		t.Fatalf("Report err: %v", err)
	}
	if resp.Action != "key_disabled" {
		t.Fatalf("Action = %q, want key_disabled", resp.Action)
	}
	if len(fc.applied) != 1 || fc.applied[0].idx != 2 {
		t.Fatalf("applied = %+v, want idx=2 (gamma)", fc.applied)
	}
	if len(fr.events) != 1 || fr.events[0]["idx"] != 2 {
		t.Fatalf("event idx = %+v, want 2", fr.events)
	}
}

// P1-3 回归：key_index 与 key 同时给且不一致 → 40010；都未给 → 40010。
func TestReportLocateConflict(t *testing.T) {
	m := newManager(&fakeChannels{ch: testChannel()}, &fakeRedis{lockOK: true}, fakeSP{settingsOn()})

	// idx=0 对应 "alpha"，与传入 "gamma" 冲突 → 40010
	_, err := m.Report(context.Background(), ReportReq{
		ChannelID: 11, KeyIndex: intPtr(0), Key: "gamma", Success: true,
	})
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("conflict err = %v, want ErrInvalidRequest", err)
	}

	// 一致 → 正常定位 idx=0
	fr := &fakeRedis{lockOK: true}
	m2 := newManager(&fakeChannels{ch: testChannel()}, fr, fakeSP{settingsOn()})
	if _, err := m2.Report(context.Background(), ReportReq{
		ChannelID: 11, KeyIndex: intPtr(0), Key: "alpha", Success: true,
	}); err != nil {
		t.Fatalf("consistent locate err: %v", err)
	}

	// 都未给 → 40010
	_, err = m.Report(context.Background(), ReportReq{ChannelID: 11, Success: true})
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("empty locate err = %v, want ErrInvalidRequest", err)
	}
}

// P1-4 回归：携带 lease_id 的 usage 上报按 actual−est 校正；
// 租约缺失（过期）时按 actual 全额累加。
func TestReportUsageLeaseCorrection(t *testing.T) {
	fc := &fakeChannels{ch: testChannel()}
	fr := &fakeRedis{lockOK: true, leaseEst: 100, leaseOK: true}
	m := newManager(fc, fr, fakeSP{settingsOn()})

	_, err := m.Report(context.Background(), ReportReq{
		ChannelID: 11, KeyIndex: intPtr(0), Success: true, LeaseID: "lease-1",
		Usage: &Usage{PromptTokens: 120, CompletionTokens: 30}, // actual=150
	})
	if err != nil {
		t.Fatalf("Report err: %v", err)
	}
	if fr.usageDelta != 50 { // 150 - 100
		t.Fatalf("usage delta = %v, want 50 (actual 150 - est 100)", fr.usageDelta)
	}

	// 租约不存在 → 全额 actual
	fr2 := &fakeRedis{lockOK: true, leaseOK: false}
	m2 := newManager(&fakeChannels{ch: testChannel()}, fr2, fakeSP{settingsOn()})
	_, err = m2.Report(context.Background(), ReportReq{
		ChannelID: 11, KeyIndex: intPtr(0), Success: true, LeaseID: "lease-gone",
		Usage: &Usage{PromptTokens: 120, CompletionTokens: 30},
	})
	if err != nil {
		t.Fatalf("Report err: %v", err)
	}
	if fr2.usageDelta != 150 {
		t.Fatalf("usage delta = %v, want 150 (lease missing, full actual)", fr2.usageDelta)
	}

	// actual < est → 校正量为负（HINCRBYFLOAT 支持）
	fr3 := &fakeRedis{lockOK: true, leaseEst: 200, leaseOK: true}
	m3 := newManager(&fakeChannels{ch: testChannel()}, fr3, fakeSP{settingsOn()})
	_, err = m3.Report(context.Background(), ReportReq{
		ChannelID: 11, KeyIndex: intPtr(0), Success: true, LeaseID: "lease-neg",
		Usage: &Usage{PromptTokens: 120, CompletionTokens: 30},
	})
	if err != nil {
		t.Fatalf("Report err: %v", err)
	}
	if fr3.usageDelta != -50 {
		t.Fatalf("usage delta = %v, want -50 (actual 150 - est 200)", fr3.usageDelta)
	}
}

// P1-1 回归：nil redis（degraded 模式）调用 Report/SetKeyStatus 不再
// panic，错误携带 redisx.ErrDegraded（或经 ErrLockFailed 包装）→ api
// 映射 503/50001。
func TestReportDegradedRedisNoPanic(t *testing.T) {
	var nilRdb *redisx.Client // nil receiver：方法返回 ErrDegraded 而非 panic
	m := newManager(&fakeChannels{ch: testChannel()}, nilRdb, fakeSP{settingsOn()})

	// 幂等路径：IdemSet → ErrDegraded
	_, err := m.Report(context.Background(), ReportReq{
		ChannelID: 11, KeyIndex: intPtr(0), Success: true, IdempotencyKey: "k",
	})
	if !errors.Is(err, redisx.ErrDegraded) {
		t.Fatalf("idem err = %v, want redisx.ErrDegraded", err)
	}

	// 锁路径：Lock → ErrDegraded 包装为 ErrLockFailed（→50001）
	_, err = m.Report(context.Background(), ReportReq{ChannelID: 11, KeyIndex: intPtr(0), Success: true})
	if !errors.Is(err, ErrLockFailed) || !errors.Is(err, redisx.ErrDegraded) {
		t.Fatalf("lock err = %v, want ErrLockFailed wrapping ErrDegraded", err)
	}

	// 用量路径：UsageIncr → ErrDegraded
	_, err = m.Report(context.Background(), ReportReq{
		ChannelID: 11, KeyIndex: intPtr(0), Success: true,
		Usage: &Usage{PromptTokens: 1},
	})
	if !errors.Is(err, redisx.ErrDegraded) {
		t.Fatalf("usage err = %v, want redisx.ErrDegraded", err)
	}

	// SetKeyStatus：同样不 panic
	_, err = m.SetKeyStatus(context.Background(), 11, 0, 1, "")
	if !errors.Is(err, ErrLockFailed) || !errors.Is(err, redisx.ErrDegraded) {
		t.Fatalf("SetKeyStatus err = %v, want ErrLockFailed wrapping ErrDegraded", err)
	}
}

// P2-3 回归：幂等键已 SET 但后续处理失败（非 duplicate）时 DEL 幂等键，
// 允许修正后重试；成功路径不 DEL。
func TestReportIdemRollbackOnFailure(t *testing.T) {
	// 失败场景：锁获取失败 → IdemDel 被调用
	fr := &fakeRedis{idemOK: true, lockErr: errors.New("boom")}
	m := newManager(&fakeChannels{ch: testChannel()}, fr, fakeSP{settingsOn()})
	_, err := m.Report(context.Background(), ReportReq{
		ChannelID: 11, KeyIndex: intPtr(0), Success: true, IdempotencyKey: "rb-1",
	})
	if err == nil {
		t.Fatal("want error")
	}
	if fr.idemDelN != 1 {
		t.Fatalf("IdemDel calls = %d, want 1 (rollback on failure)", fr.idemDelN)
	}

	// 成功场景：不 DEL
	fr2 := &fakeRedis{idemOK: true, lockOK: true}
	m2 := newManager(&fakeChannels{ch: testChannel()}, fr2, fakeSP{settingsOn()})
	if _, err := m2.Report(context.Background(), ReportReq{
		ChannelID: 11, KeyIndex: intPtr(0), Success: true, IdempotencyKey: "ok-1",
	}); err != nil {
		t.Fatalf("Report err: %v", err)
	}
	if fr2.idemDelN != 0 {
		t.Fatalf("IdemDel calls = %d, want 0 on success", fr2.idemDelN)
	}

	// duplicate 场景：返回 duplicate 不视为失败，不 DEL
	fr3 := &fakeRedis{idemOK: false}
	m3 := newManager(&fakeChannels{ch: testChannel()}, fr3, fakeSP{settingsOn()})
	resp, err := m3.Report(context.Background(), ReportReq{
		ChannelID: 11, KeyIndex: intPtr(0), Success: true, IdempotencyKey: "dup-1",
	})
	if err != nil || resp.Action != "duplicate" {
		t.Fatalf("resp=%+v err=%v, want duplicate", resp, err)
	}
	if fr3.idemDelN != 0 {
		t.Fatalf("IdemDel calls = %d, want 0 on duplicate", fr3.idemDelN)
	}
}

// P2-4 回归：SetKeyStatus 手动禁用导致所有 key 全灭（allDead）时
// action 必须是 channel_disabled。
func TestSetKeyStatusAllDeadAction(t *testing.T) {
	fc := &fakeChannels{ch: testChannel(), applyRetCS: store.ChannelStatusAutoDisabled, applyDead: true}
	m := newManager(fc, &fakeRedis{lockOK: true}, fakeSP{settingsOn()})

	resp, err := m.SetKeyStatus(context.Background(), 11, 0, store.ChannelStatusManuallyDisabled, "off")
	if err != nil {
		t.Fatalf("SetKeyStatus err: %v", err)
	}
	if resp.Action != "channel_disabled" {
		t.Fatalf("Action = %q, want channel_disabled (allDead)", resp.Action)
	}
	if resp.ChannelStatus != store.ChannelStatusAutoDisabled {
		t.Fatalf("ChannelStatus = %d, want 3", resp.ChannelStatus)
	}
}
