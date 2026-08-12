package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"keypool/internal/config"
	"keypool/internal/redisx"
	"keypool/internal/selector"
	"keypool/internal/state"
	"keypool/internal/store"
)

// ---- fakes（窄接口实现，不依赖真实 DB/Redis）----

type fakeSelector struct {
	resp    *selector.SelectResp
	err     error
	metrics map[string]int64
	lastReq selector.SelectReq
}

func (f *fakeSelector) Select(_ context.Context, req selector.SelectReq) (*selector.SelectResp, error) {
	f.lastReq = req
	if f.err != nil {
		return nil, f.err
	}
	return f.resp, nil
}

func (f *fakeSelector) SnapshotMetrics() map[string]int64 { return f.metrics }

type fakeManager struct {
	resp    *state.ReportResp
	err     error
	metrics map[string]int64
	lastRep state.ReportReq
	setArgs struct {
		cid, idx, status int
		reason           string
	}
}

func (f *fakeManager) Report(_ context.Context, r state.ReportReq) (*state.ReportResp, error) {
	f.lastRep = r
	if f.err != nil {
		return nil, f.err
	}
	return f.resp, nil
}

func (f *fakeManager) SetKeyStatus(_ context.Context, cid, idx, status int, reason string) (*state.ReportResp, error) {
	f.setArgs.cid, f.setArgs.idx, f.setArgs.status, f.setArgs.reason = cid, idx, status, reason
	if f.err != nil {
		return nil, f.err
	}
	return f.resp, nil
}

func (f *fakeManager) SnapshotMetrics() map[string]int64 { return f.metrics }

type fakeStore struct {
	ch      *store.Channel
	err     error
	upserts map[string]string
}

func (f *fakeStore) GetChannel(id int) (*store.Channel, error) {
	if f.err != nil {
		return nil, f.err
	}
	if f.ch == nil || f.ch.Id != id {
		return nil, errors.New("record not found")
	}
	return f.ch, nil
}

func (f *fakeStore) UpsertOption(key, value string) error {
	if f.upserts == nil {
		f.upserts = map[string]string{}
	}
	f.upserts[key] = value
	return nil
}

type fakeUsage struct {
	counters  map[int]float64
	lastDecay int64
	err       error
}

func (f *fakeUsage) UsageAll(_ context.Context, cid int) (map[int]float64, int64, error) {
	if f.err != nil {
		return nil, 0, f.err
	}
	return f.counters, f.lastDecay, nil
}

// fakeProvider 实现 store.SettingsProvider + reloader。
type fakeProvider struct {
	st          atomic.Value // *store.Settings
	reloadCalls int
	reloadErr   error
}

func (p *fakeProvider) Get() *store.Settings {
	if v := p.st.Load(); v != nil {
		return v.(*store.Settings)
	}
	return nil
}

func (p *fakeProvider) set(st *store.Settings) { p.st.Store(st) }

func (p *fakeProvider) Reload() error {
	p.reloadCalls++
	return p.reloadErr
}

// ---- 测试辅助 ----

const testToken = "test-token"

func newTestRouter(sl keySelector, m stateManager, s storeAPI, sp store.SettingsProvider, rdb usageReader) http.Handler {
	return newRouter(config.Config{Port: 8080, AuthToken: testToken}, sl, m, s, sp, rdb)
}

func authed(req *http.Request) *http.Request {
	req.Header.Set("Authorization", "Bearer "+testToken)
	return req
}

type envelopeDTO struct {
	Code      int             `json:"code"`
	Message   string          `json:"message"`
	Data      json.RawMessage `json:"data"`
	RequestID string          `json:"request_id"`
}

func do(t *testing.T, h http.Handler, req *http.Request) (int, envelopeDTO) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var env envelopeDTO
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("response is not envelope JSON: %v; body=%s", err, rec.Body.String())
	}
	return rec.Code, env
}

func okSelector() *fakeSelector {
	return &fakeSelector{
		resp: &selector.SelectResp{ChannelID: 7, KeyIndex: 1, Key: "sk-x", BaseURL: "https://u", Mode: "polling", Epoch: "abc12345"},
		metrics: map[string]int64{
			`select_total{cid="7",idx="1"}`: 3,
			"band_lookahead_total":          1,
		},
	}
}

func okManager() *fakeManager {
	return &fakeManager{
		resp: &state.ReportResp{Action: "none", ChannelStatus: 1},
		metrics: map[string]int64{
			`report_total{action="none"}`: 5,
		},
	}
}

// ---- 用例 ----

// 包络格式：成功响应含 code=0/message=ok/data/request_id（16 hex）。
func TestEnvelopeFormat(t *testing.T) {
	h := newTestRouter(okSelector(), okManager(), &fakeStore{}, &fakeProvider{}, nil)
	req := authed(httptest.NewRequest("POST", "/v1/keys/select", strings.NewReader(`{"channel_id":7}`)))
	status, env := do(t, h, req)
	if status != http.StatusOK {
		t.Fatalf("status=%d", status)
	}
	if env.Code != 0 || env.Message != "ok" {
		t.Fatalf("envelope=%+v", env)
	}
	if len(env.RequestID) != 16 {
		t.Fatalf("request_id=%q want 16 hex chars", env.RequestID)
	}
	var data selectRespDTO
	if err := json.Unmarshal(env.Data, &data); err != nil {
		t.Fatalf("data decode: %v", err)
	}
	if data.ChannelID != 7 || data.KeyIndex != 1 || data.Epoch != "abc12345" {
		t.Fatalf("data=%+v", data)
	}
}

// 鉴权：无/错 token → 401/40100；/healthz 豁免。
func TestAuth(t *testing.T) {
	h := newTestRouter(okSelector(), okManager(), &fakeStore{}, &fakeProvider{}, nil)

	status, env := do(t, h, httptest.NewRequest("POST", "/v1/keys/select", strings.NewReader(`{}`)))
	if status != http.StatusUnauthorized || env.Code != CodeUnauthorized {
		t.Fatalf("no token: status=%d env=%+v", status, env)
	}

	bad := httptest.NewRequest("GET", "/metrics", nil)
	bad.Header.Set("Authorization", "Bearer wrong")
	status, env = do(t, h, bad)
	if status != http.StatusUnauthorized || env.Code != CodeUnauthorized {
		t.Fatalf("bad token: status=%d env=%+v", status, env)
	}

	// /healthz 无需鉴权（裸 JSON 断言见 TestHealthzBareJSON）
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("healthz: status=%d", rec.Code)
	}
}

// 40001：ErrNoKey → 503，data.retry_after_ms=1000。
func TestErrNoKeyMapping(t *testing.T) {
	sl := okSelector()
	sl.err = selector.ErrNoKey
	h := newTestRouter(sl, okManager(), &fakeStore{}, &fakeProvider{}, nil)
	req := authed(httptest.NewRequest("POST", "/v1/keys/select", strings.NewReader(`{"channel_id":7}`)))
	status, env := do(t, h, req)
	if status != http.StatusServiceUnavailable || env.Code != CodeNoKey {
		t.Fatalf("status=%d env=%+v", status, env)
	}
	if !strings.Contains(string(env.Data), `"retry_after_ms":1000`) {
		t.Fatalf("data=%s want retry_after_ms=1000", env.Data)
	}
}

// 40002：ErrNoChannel → 404；state.ErrChannelNotFound 同样映射。
func TestErrNoChannelMapping(t *testing.T) {
	sl := okSelector()
	sl.err = selector.ErrNoChannel
	h := newTestRouter(sl, okManager(), &fakeStore{}, &fakeProvider{}, nil)
	req := authed(httptest.NewRequest("POST", "/v1/keys/select", strings.NewReader(`{"channel_id":404}`)))
	status, env := do(t, h, req)
	if status != http.StatusNotFound || env.Code != CodeChannelMissing {
		t.Fatalf("select: status=%d env=%+v", status, env)
	}

	m := okManager()
	m.err = fmt.Errorf("%w: 404", state.ErrChannelNotFound)
	h = newTestRouter(okSelector(), m, &fakeStore{}, &fakeProvider{}, nil)
	req = authed(httptest.NewRequest("POST", "/v1/keys/report", strings.NewReader(`{"channel_id":404,"success":true}`)))
	status, env = do(t, h, req)
	if status != http.StatusNotFound || env.Code != CodeChannelMissing {
		t.Fatalf("report: status=%d env=%+v", status, env)
	}
}

// 40003：report 幂等命中（Action=duplicate）→ 409。
func TestDuplicateMapping(t *testing.T) {
	m := okManager()
	m.resp = &state.ReportResp{Action: "duplicate"}
	h := newTestRouter(okSelector(), m, &fakeStore{}, &fakeProvider{}, nil)
	req := authed(httptest.NewRequest("POST", "/v1/keys/report", strings.NewReader(`{"channel_id":7,"success":true}`)))
	req.Header.Set("Idempotency-Key", "idem-1")
	status, env := do(t, h, req)
	if status != http.StatusConflict || env.Code != CodeDuplicate {
		t.Fatalf("status=%d env=%+v", status, env)
	}
	// 头部 Idempotency-Key 优先于 body
	if m.lastRep.IdempotencyKey != "idem-1" {
		t.Fatalf("idempotency key=%q want header value", m.lastRep.IdempotencyKey)
	}
}

// body 的 idempotency_key 在头部缺失时生效。
func TestIdempotencyKeyFromBody(t *testing.T) {
	m := okManager()
	h := newTestRouter(okSelector(), m, &fakeStore{}, &fakeProvider{}, nil)
	req := authed(httptest.NewRequest("POST", "/v1/keys/report",
		strings.NewReader(`{"channel_id":7,"success":true,"idempotency_key":"body-k"}`)))
	status, _ := do(t, h, req)
	if status != http.StatusOK {
		t.Fatalf("status=%d", status)
	}
	if m.lastRep.IdempotencyKey != "body-k" {
		t.Fatalf("idempotency key=%q want body-k", m.lastRep.IdempotencyKey)
	}
}

// 40010：路径 id 解析失败、JSON 体非法、state.ErrInvalidRequest。
func TestBadRequestMapping(t *testing.T) {
	h := newTestRouter(okSelector(), okManager(), &fakeStore{}, &fakeProvider{}, nil)

	status, env := do(t, h, authed(httptest.NewRequest("GET", "/v1/channels/abc/keys", nil)))
	if status != http.StatusBadRequest || env.Code != CodeBadRequest {
		t.Fatalf("bad id: status=%d env=%+v", status, env)
	}

	status, env = do(t, h, authed(httptest.NewRequest("GET", "/v1/channels/0/keys", nil)))
	if status != http.StatusBadRequest || env.Code != CodeBadRequest {
		t.Fatalf("zero id: status=%d env=%+v", status, env)
	}

	status, env = do(t, h, authed(httptest.NewRequest("POST", "/v1/keys/select", strings.NewReader(`{bad json`))))
	if status != http.StatusBadRequest || env.Code != CodeBadRequest {
		t.Fatalf("bad json: status=%d env=%+v", status, env)
	}

	m := okManager()
	m.err = fmt.Errorf("%w: key index 9 out of range", state.ErrInvalidRequest)
	h = newTestRouter(okSelector(), m, &fakeStore{}, &fakeProvider{}, nil)
	status, env = do(t, h, authed(httptest.NewRequest("POST", "/v1/keys/report",
		strings.NewReader(`{"channel_id":7,"key_index":9,"success":true}`))))
	if status != http.StatusBadRequest || env.Code != CodeBadRequest {
		t.Fatalf("invalid req: status=%d env=%+v", status, env)
	}
}

// 50001：state.ErrLockFailed → 503；未知错误 → 500。
func TestInternalMapping(t *testing.T) {
	m := okManager()
	m.err = fmt.Errorf("%w: channel 7 busy", state.ErrLockFailed)
	h := newTestRouter(okSelector(), m, &fakeStore{}, &fakeProvider{}, nil)
	status, env := do(t, h, authed(httptest.NewRequest("POST", "/v1/keys/report",
		strings.NewReader(`{"channel_id":7,"success":true}`))))
	if status != http.StatusServiceUnavailable || env.Code != CodeInternal {
		t.Fatalf("lock failed: status=%d env=%+v", status, env)
	}

	sl := okSelector()
	sl.err = errors.New("redis down")
	h = newTestRouter(sl, okManager(), &fakeStore{}, &fakeProvider{}, nil)
	status, env = do(t, h, authed(httptest.NewRequest("POST", "/v1/keys/select", strings.NewReader(`{"channel_id":7}`))))
	if status != http.StatusInternalServerError || env.Code != CodeInternal {
		t.Fatalf("unknown: status=%d env=%+v", status, env)
	}
}

// GET /v1/channels/{id}/keys：epoch/mode/keys 结构、脱敏、usage、status_list 语义。
func TestListKeys(t *testing.T) {
	ch := &store.Channel{
		Id:     7,
		Status: 1,
		Key:    `["sk-aaaa1111bbbb","short","sk-cccc3333dddd"]`,
		ChannelInfo: store.ChannelInfo{
			IsMultiKey:             true,
			MultiKeySize:           3,
			MultiKeyMode:           "polling",
			MultiKeyStatusList:     map[int]int{1: 3},
			MultiKeyDisabledReason: map[int]string{1: "quota"},
			MultiKeyDisabledTime:   map[int]int64{1: 1735689600},
		},
	}
	st := &fakeStore{ch: ch}
	sp := &fakeProvider{}
	sp.set(&store.Settings{
		Rotation: map[int]store.RotationCfg{7: {BandSeconds: 3600, ActiveCount: 1, Order: "index"}},
	})
	rdb := &fakeUsage{counters: map[int]float64{0: 12.5, 2: 3}, lastDecay: 100}
	h := newTestRouter(okSelector(), okManager(), st, sp, rdb)

	status, env := do(t, h, authed(httptest.NewRequest("GET", "/v1/channels/7/keys", nil)))
	if status != http.StatusOK {
		t.Fatalf("status=%d env=%+v", status, env)
	}
	var data listKeysDTO
	if err := json.Unmarshal(env.Data, &data); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if data.Epoch != ch.Epoch() || data.Mode != "polling" || len(data.Keys) != 3 {
		t.Fatalf("data=%+v", data)
	}
	k0 := data.Keys[0]
	if k0.Status != 1 || k0.Usage == nil || *k0.Usage != 12.5 {
		t.Fatalf("k0=%+v", k0)
	}
	if k0.KeyMask != "sk-a****bbbb" {
		t.Fatalf("mask=%q", k0.KeyMask)
	}
	if k0.RotationState != "active" && k0.RotationState != "standby" {
		t.Fatalf("rotation_state=%q", k0.RotationState)
	}
	k1 := data.Keys[1]
	if k1.Status != 3 || k1.Reason != "quota" || k1.DisabledTime != 1735689600 {
		t.Fatalf("k1=%+v", k1)
	}
	if k1.KeyMask != "*****" { // len("short")=5 < 8 → 全*
		t.Fatalf("mask short=%q", k1.KeyMask)
	}
	if k1.RotationState != "standby" { // 禁用 key 不参与轮换
		t.Fatalf("k1 rotation=%q", k1.RotationState)
	}
}

// usage 获取失败（或 rdb 降级为 nil）时省略 usage 字段。
func TestListKeysUsageOmitted(t *testing.T) {
	ch := &store.Channel{Id: 7, Status: 1, Key: "sk-aaaa1111bbbb"}
	h := newTestRouter(okSelector(), okManager(), &fakeStore{ch: ch}, &fakeProvider{}, nil)
	status, env := do(t, h, authed(httptest.NewRequest("GET", "/v1/channels/7/keys", nil)))
	if status != http.StatusOK {
		t.Fatalf("status=%d", status)
	}
	if strings.Contains(string(env.Data), `"usage"`) {
		t.Fatalf("usage should be omitted: %s", env.Data)
	}
}

// 渠道不存在 → 40002/404。
func TestListKeysNotFound(t *testing.T) {
	h := newTestRouter(okSelector(), okManager(), &fakeStore{}, &fakeProvider{}, nil)
	status, env := do(t, h, authed(httptest.NewRequest("GET", "/v1/channels/999/keys", nil)))
	if status != http.StatusNotFound || env.Code != CodeChannelMissing {
		t.Fatalf("status=%d env=%+v", status, env)
	}
}

// PATCH /v1/channels/{id}/keys/{idx}：status 字符串映射（disabled→2/enabled→1）、
// idx 解析与校验。
func TestPatchKeyStatus(t *testing.T) {
	m := okManager()
	h := newTestRouter(okSelector(), m, &fakeStore{}, &fakeProvider{}, nil)

	status, _ := do(t, h, authed(httptest.NewRequest("PATCH", "/v1/channels/7/keys/2",
		strings.NewReader(`{"status":"disabled","reason":"manual off"}`))))
	if status != http.StatusOK {
		t.Fatalf("disable status=%d", status)
	}
	if m.setArgs.cid != 7 || m.setArgs.idx != 2 || m.setArgs.status != 2 || m.setArgs.reason != "manual off" {
		t.Fatalf("setArgs=%+v", m.setArgs)
	}

	status, _ = do(t, h, authed(httptest.NewRequest("PATCH", "/v1/channels/7/keys/2",
		strings.NewReader(`{"status":"enabled"}`))))
	if status != http.StatusOK || m.setArgs.status != 1 {
		t.Fatalf("enable status=%d args=%+v", status, m.setArgs)
	}

	// 非法 status → 40010
	status, env := do(t, h, authed(httptest.NewRequest("PATCH", "/v1/channels/7/keys/2",
		strings.NewReader(`{"status":"boom"}`))))
	if status != http.StatusBadRequest || env.Code != CodeBadRequest {
		t.Fatalf("bad status: status=%d env=%+v", status, env)
	}

	// 非法 idx → 40010
	status, env = do(t, h, authed(httptest.NewRequest("PATCH", "/v1/channels/7/keys/-1",
		strings.NewReader(`{"status":"enabled"}`))))
	if status != http.StatusBadRequest || env.Code != CodeBadRequest {
		t.Fatalf("bad idx: status=%d env=%+v", status, env)
	}
}

// GET /v1/channels/{id}：渠道元数据投影；不存在 → 404/40002。
func TestGetChannelMeta(t *testing.T) {
	prio := int64(5)
	weight := uint(3)
	ch := &store.Channel{
		Id: 7, Type: 1, Status: 1, Name: "upstream-a", Key: "k0\nk1",
		BaseURL: "https://api.x", Models: "gpt-4o, gpt-4o-mini", Group: "default",
		Priority: &prio, Weight: &weight,
		ChannelInfo: store.ChannelInfo{IsMultiKey: true, MultiKeySize: 2, MultiKeyMode: "random"},
	}
	h := newTestRouter(okSelector(), okManager(), &fakeStore{ch: ch}, &fakeProvider{}, nil)

	status, env := do(t, h, authed(httptest.NewRequest("GET", "/v1/channels/7", nil)))
	if status != http.StatusOK {
		t.Fatalf("status=%d env=%+v", status, env)
	}
	var meta store.ChannelMeta
	if err := json.Unmarshal(env.Data, &meta); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if meta.ID != 7 || meta.Name != "upstream-a" || meta.Priority != 5 || meta.Weight != 3 ||
		!meta.MultiKey || meta.MultiKeyMode != "random" || meta.KeyCount != 2 ||
		len(meta.Models) != 2 || meta.Models[1] != "gpt-4o-mini" || !meta.AutoBan || meta.Epoch == "" {
		t.Fatalf("meta=%+v", meta)
	}

	status, env = do(t, h, authed(httptest.NewRequest("GET", "/v1/channels/999", nil)))
	if status != http.StatusNotFound || env.Code != CodeChannelMissing {
		t.Fatalf("not found: status=%d env=%+v", status, env)
	}
}

// select include_channel：请求透传 IncludeChannel，响应透传 channel 元数据。
func TestSelectIncludeChannel(t *testing.T) {
	sl := okSelector()
	h := newTestRouter(sl, okManager(), &fakeStore{}, &fakeProvider{}, nil)

	status, env := do(t, h, authed(httptest.NewRequest("POST", "/v1/keys/select",
		strings.NewReader(`{"channel_id":7,"include_channel":true}`))))
	if status != http.StatusOK {
		t.Fatalf("status=%d", status)
	}
	if !sl.lastReq.IncludeChannel {
		t.Fatalf("IncludeChannel not propagated: %+v", sl.lastReq)
	}
	// fake 未带元数据 → 响应省略 channel 字段
	if strings.Contains(string(env.Data), `"channel"`) {
		t.Fatalf("channel should be omitted when nil: %s", env.Data)
	}

	// selector 返回元数据 → 响应透传
	sl2 := okSelector()
	sl2.resp.Channel = &store.ChannelMeta{ID: 7, Name: "upstream-a", MultiKey: true, KeyCount: 2}
	h2 := newTestRouter(sl2, okManager(), &fakeStore{}, &fakeProvider{}, nil)
	status, env = do(t, h2, authed(httptest.NewRequest("POST", "/v1/keys/select",
		strings.NewReader(`{"channel_id":7,"include_channel":true}`))))
	if status != http.StatusOK {
		t.Fatalf("status=%d", status)
	}
	if !strings.Contains(string(env.Data), `"name":"upstream-a"`) {
		t.Fatalf("channel meta missing: %s", env.Data)
	}
}

// PUT balance：校验 + 写 keypool.balance.{cid} + 触发 reload；GET 返回。
func TestBalancePutGet(t *testing.T) {
	st := &fakeStore{}
	sp := &fakeProvider{}
	h := newTestRouter(okSelector(), okManager(), st, sp, nil)

	// 非法 mode → 40010
	status, env := do(t, h, authed(httptest.NewRequest("PUT", "/v1/channels/7/balance",
		strings.NewReader(`{"mode":"wat"}`))))
	if status != http.StatusBadRequest || env.Code != CodeBadRequest {
		t.Fatalf("bad mode: status=%d env=%+v", status, env)
	}
	// 非法 metric → 40010
	status, env = do(t, h, authed(httptest.NewRequest("PUT", "/v1/channels/7/balance",
		strings.NewReader(`{"metric":"money"}`))))
	if status != http.StatusBadRequest || env.Code != CodeBadRequest {
		t.Fatalf("bad metric: status=%d env=%+v", status, env)
	}

	status, env = do(t, h, authed(httptest.NewRequest("PUT", "/v1/channels/7/balance",
		strings.NewReader(`{"mode":"usage","metric":"tokens","decay_interval":1800,"decay_factor":0.5}`))))
	if status != http.StatusOK || env.Code != 0 {
		t.Fatalf("put: status=%d env=%+v", status, env)
	}
	raw, ok := st.upserts["keypool.balance.7"]
	if !ok || !strings.Contains(raw, `"mode":"usage"`) {
		t.Fatalf("upserts=%v", st.upserts)
	}
	if sp.reloadCalls != 1 {
		t.Fatalf("reloadCalls=%d", sp.reloadCalls)
	}

	// GET 无配置 → 默认值
	status, env = do(t, h, authed(httptest.NewRequest("GET", "/v1/channels/7/balance", nil)))
	if status != http.StatusOK {
		t.Fatalf("get: status=%d", status)
	}
	var cfg store.BalanceCfg
	if err := json.Unmarshal(env.Data, &cfg); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if cfg != defaultBalanceCfg {
		t.Fatalf("default cfg=%+v", cfg)
	}

	// GET 有配置 → 生效值
	sp.set(&store.Settings{Balance: map[int]store.BalanceCfg{7: {Mode: "usage", Metric: "cost"}}})
	status, env = do(t, h, authed(httptest.NewRequest("GET", "/v1/channels/7/balance", nil)))
	if status != http.StatusOK {
		t.Fatalf("get2: status=%d", status)
	}
	if err := json.Unmarshal(env.Data, &cfg); err != nil {
		t.Fatalf("decode2: %v", err)
	}
	if cfg.Mode != "usage" || cfg.Metric != "cost" {
		t.Fatalf("cfg=%+v", cfg)
	}
}

// PUT rotation：band_seconds>=30、active_count>=1 校验 + 写 keypool.rotation.{cid}。
func TestRotationPutGet(t *testing.T) {
	st := &fakeStore{}
	sp := &fakeProvider{}
	h := newTestRouter(okSelector(), okManager(), st, sp, nil)

	for _, body := range []string{
		`{"band_seconds":10,"active_count":2}`,
		`{"band_seconds":60,"active_count":0}`,
	} {
		status, env := do(t, h, authed(httptest.NewRequest("PUT", "/v1/channels/7/rotation", strings.NewReader(body))))
		if status != http.StatusBadRequest || env.Code != CodeBadRequest {
			t.Fatalf("body=%s status=%d env=%+v", body, status, env)
		}
	}

	status, env := do(t, h, authed(httptest.NewRequest("PUT", "/v1/channels/7/rotation",
		strings.NewReader(`{"band_seconds":300,"active_count":2,"overlap_bands":1,"order":"shuffle"}`))))
	if status != http.StatusOK {
		t.Fatalf("put: status=%d env=%+v", status, env)
	}
	if _, ok := st.upserts["keypool.rotation.7"]; !ok {
		t.Fatalf("upserts=%v", st.upserts)
	}
	if sp.reloadCalls != 1 {
		t.Fatalf("reloadCalls=%d", sp.reloadCalls)
	}

	// GET 默认
	status, env = do(t, h, authed(httptest.NewRequest("GET", "/v1/channels/7/rotation", nil)))
	if status != http.StatusOK {
		t.Fatalf("get: status=%d", status)
	}
	var cfg store.RotationCfg
	if err := json.Unmarshal(env.Data, &cfg); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if cfg != defaultRotationCfg {
		t.Fatalf("default cfg=%+v", cfg)
	}
}

// GET usage：counters/last_decay/metric；rdb 降级 → 50001/503。
func TestUsage(t *testing.T) {
	sp := &fakeProvider{}
	sp.set(&store.Settings{Balance: map[int]store.BalanceCfg{7: {Metric: "cost"}}})
	rdb := &fakeUsage{counters: map[int]float64{0: 1.5}, lastDecay: 42}
	h := newTestRouter(okSelector(), okManager(), &fakeStore{}, sp, rdb)

	status, env := do(t, h, authed(httptest.NewRequest("GET", "/v1/channels/7/usage", nil)))
	if status != http.StatusOK {
		t.Fatalf("status=%d", status)
	}
	var data struct {
		Cid       int                `json:"cid"`
		Metric    string             `json:"metric"`
		Counters  map[string]float64 `json:"counters"`
		LastDecay int64              `json:"last_decay"`
	}
	if err := json.Unmarshal(env.Data, &data); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if data.Cid != 7 || data.Metric != "cost" || data.Counters["0"] != 1.5 || data.LastDecay != 42 {
		t.Fatalf("data=%+v", data)
	}

	h = newTestRouter(okSelector(), okManager(), &fakeStore{}, sp, nil)
	status, env = do(t, h, authed(httptest.NewRequest("GET", "/v1/channels/7/usage", nil)))
	if status != http.StatusServiceUnavailable || env.Code != CodeInternal {
		t.Fatalf("degraded: status=%d env=%+v", status, env)
	}
}

// settings/reload → Reload + {reloaded:true}。
func TestCacheInvalidate(t *testing.T) {
	sp := &fakeProvider{}
	h := newTestRouter(okSelector(), okManager(), &fakeStore{}, sp, nil)
	status, env := do(t, h, authed(httptest.NewRequest("POST", "/v1/settings/reload", nil)))
	if status != http.StatusOK || sp.reloadCalls != 1 {
		t.Fatalf("status=%d reloads=%d", status, sp.reloadCalls)
	}
	if !strings.Contains(string(env.Data), `"reloaded":true`) {
		t.Fatalf("data=%s", env.Data)
	}

	sp.reloadErr = errors.New("db down")
	status, env = do(t, h, authed(httptest.NewRequest("POST", "/v1/settings/reload", nil)))
	if status != http.StatusServiceUnavailable || env.Code != CodeInternal {
		t.Fatalf("reload fail: status=%d env=%+v", status, env)
	}
}

// /metrics：Prometheus 文本包含汇总指标名 + uptime。
func TestMetrics(t *testing.T) {
	h := newTestRouter(okSelector(), okManager(), &fakeStore{}, &fakeProvider{}, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, authed(httptest.NewRequest("GET", "/metrics", nil)))
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{
		`keypool_select_total{cid="7",idx="1"} 3`,
		"keypool_band_lookahead_total 1",
		`keypool_report_total{action="none"} 5`,
		"keypool_process_uptime_seconds",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics missing %q:\n%s", want, body)
		}
	}
}

// recover 中间件：panic → 500/50001。
func TestPanicRecover(t *testing.T) {
	sl := okSelector()
	sl.resp = nil // Select 返回 (nil, nil) 触发 handler 解引用 panic
	h := newTestRouter(sl, okManager(), &fakeStore{}, &fakeProvider{}, nil)
	status, env := do(t, h, authed(httptest.NewRequest("POST", "/v1/keys/select", strings.NewReader(`{"channel_id":7}`))))
	if status != http.StatusInternalServerError || env.Code != CodeInternal {
		t.Fatalf("status=%d env=%+v", status, env)
	}
}

// maskKey 边界。
func TestMaskKey(t *testing.T) {
	cases := map[string]string{
		"":                "",
		"abcdefg":         "*******",
		"abcdefgh":        "abcd****efgh",
		"sk-aaaa1111bbbb": "sk-a****bbbb",
	}
	for in, want := range cases {
		if got := maskKey(in); got != want {
			t.Fatalf("maskKey(%q)=%q want %q", in, got, want)
		}
	}
}

// rotationStates：无配置→""；启用轮换时 active/standby 语义与 selector 同规则。
func TestRotationStates(t *testing.T) {
	ch := &store.Channel{Id: 7, Status: 1, Key: "k0\nk1\nk2\nk3"}

	// 无配置
	if st := rotationStates(ch, nil, 0); st[0] != "" || st[3] != "" {
		t.Fatalf("no cfg states=%v", st)
	}

	// BandSeconds=100 ActiveCount=2 → 批 [0,1] [2,3]；now=250 → band=2 → 批0 → active {0,1}
	sp := &store.Settings{Rotation: map[int]store.RotationCfg{
		7: {BandSeconds: 100, ActiveCount: 2, Order: "index"},
	}}
	st := rotationStates(ch, sp, 250)
	want := []string{"active", "active", "standby", "standby"}
	for i := range want {
		if st[i] != want[i] {
			t.Fatalf("states=%v want %v", st, want)
		}
	}
}

// ---- 评审修复回归测试 ----

// P2-8 回归：/healthz 返回裸 JSON {"status":"ok"}，不套统一包络。
func TestHealthzBareJSON(t *testing.T) {
	h := newTestRouter(okSelector(), okManager(), &fakeStore{}, &fakeProvider{}, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d", rec.Code)
	}
	body := strings.TrimSpace(rec.Body.String())
	if body != `{"status":"ok"}` {
		t.Fatalf("healthz body = %s, want bare {\"status\":\"ok\"}", body)
	}
	// 不得包含包络字段
	for _, field := range []string{`"code"`, `"message"`, `"request_id"`} {
		if strings.Contains(body, field) {
			t.Fatalf("healthz must not use envelope, found %s in %s", field, body)
		}
	}
}

// P2-6 回归：keys/select 的 mode 校验与参数全空校验 → 40010。
func TestKeyGetValidation(t *testing.T) {
	h := newTestRouter(okSelector(), okManager(), &fakeStore{}, &fakeProvider{}, nil)

	// 非法 mode → 40010
	status, env := do(t, h, authed(httptest.NewRequest("POST", "/v1/keys/select",
		strings.NewReader(`{"channel_id":7,"mode":"banana"}`))))
	if status != http.StatusBadRequest || env.Code != CodeBadRequest {
		t.Fatalf("bad mode: status=%d env=%+v", status, env)
	}

	// 合法 mode 集合
	for _, m := range []string{"polling", "random", "usage", ""} {
		body := `{"channel_id":7,"mode":"` + m + `"}`
		status, _ := do(t, h, authed(httptest.NewRequest("POST", "/v1/keys/select", strings.NewReader(body))))
		if status != http.StatusOK {
			t.Fatalf("mode %q should be valid: status=%d", m, status)
		}
	}

	// 参数全空（无 channel_id 且无 group+model）→ 40010
	for _, body := range []string{
		`{}`,
		`{"group":"default"}`,          // 缺 model
		`{"model":"gpt-x"}`,            // 缺 group
		`{"channel_id":0,"group":"g"}`, // channel_id=0 视同未传
	} {
		status, env := do(t, h, authed(httptest.NewRequest("POST", "/v1/keys/select", strings.NewReader(body))))
		if status != http.StatusBadRequest || env.Code != CodeBadRequest {
			t.Fatalf("body=%s: status=%d env=%+v, want 40010", body, status, env)
		}
	}

	// group+model 齐全（无 channel_id）→ 通过校验
	status, _ = do(t, h, authed(httptest.NewRequest("POST", "/v1/keys/select",
		strings.NewReader(`{"group":"default","model":"gpt-x"}`))))
	if status != http.StatusOK {
		t.Fatalf("group+model: status=%d", status)
	}
}

// P1-1 回归：Redis 降级错误经 api 映射为 503/50001（不 panic）。
func TestDegradedErrorMapping(t *testing.T) {
	// selector 路径：SelectKey 返回 ErrDegraded（包装后）
	sl := okSelector()
	sl.err = fmt.Errorf("selector: select key: %w", redisx.ErrDegraded)
	h := newTestRouter(sl, okManager(), &fakeStore{}, &fakeProvider{}, nil)
	status, env := do(t, h, authed(httptest.NewRequest("POST", "/v1/keys/select", strings.NewReader(`{"channel_id":7}`))))
	if status != http.StatusServiceUnavailable || env.Code != CodeInternal {
		t.Fatalf("select degraded: status=%d env=%+v", status, env)
	}

	// state 路径：Report 的 Redis 操作返回 ErrDegraded（包装后）
	m := okManager()
	m.err = fmt.Errorf("state: usage incr: %w", redisx.ErrDegraded)
	h = newTestRouter(okSelector(), m, &fakeStore{}, &fakeProvider{}, nil)
	status, env = do(t, h, authed(httptest.NewRequest("POST", "/v1/keys/report",
		strings.NewReader(`{"channel_id":7,"success":true}`))))
	if status != http.StatusServiceUnavailable || env.Code != CodeInternal {
		t.Fatalf("report degraded: status=%d env=%+v", status, env)
	}
}

// P1-6 回归：selector/state 的 ErrDependency（DB 非 NotFound 故障）映射
// 503/50001 而非 404/40002。
func TestDependencyErrorMapping(t *testing.T) {
	sl := okSelector()
	sl.err = fmt.Errorf("%w: get channel 7: connection refused", selector.ErrDependency)
	h := newTestRouter(sl, okManager(), &fakeStore{}, &fakeProvider{}, nil)
	status, env := do(t, h, authed(httptest.NewRequest("POST", "/v1/keys/select", strings.NewReader(`{"channel_id":7}`))))
	if status != http.StatusServiceUnavailable || env.Code != CodeInternal {
		t.Fatalf("select dependency: status=%d env=%+v", status, env)
	}

	m := okManager()
	m.err = fmt.Errorf("%w: get channel 7: connection refused", state.ErrDependency)
	h = newTestRouter(okSelector(), m, &fakeStore{}, &fakeProvider{}, nil)
	status, env = do(t, h, authed(httptest.NewRequest("POST", "/v1/keys/report",
		strings.NewReader(`{"channel_id":7,"success":true}`))))
	if status != http.StatusServiceUnavailable || env.Code != CodeInternal {
		t.Fatalf("report dependency: status=%d env=%+v", status, env)
	}
}

// P1-4 回归（api 侧）：select 响应透传 lease_id 字段。
func TestKeyGetLeaseIDPassthrough(t *testing.T) {
	sl := okSelector()
	sl.resp.LeaseID = "0123456789abcdef0123456789abcdef"
	h := newTestRouter(sl, okManager(), &fakeStore{}, &fakeProvider{}, nil)
	status, env := do(t, h, authed(httptest.NewRequest("POST", "/v1/keys/select", strings.NewReader(`{"channel_id":7}`))))
	if status != http.StatusOK {
		t.Fatalf("status=%d", status)
	}
	var data selectRespDTO
	if err := json.Unmarshal(env.Data, &data); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if data.LeaseID != sl.resp.LeaseID {
		t.Fatalf("lease_id = %q, want %q", data.LeaseID, sl.resp.LeaseID)
	}

	// 无租约（非 usage / est=0）→ 字段省略
	sl2 := okSelector()
	status, env = do(t, newTestRouter(sl2, okManager(), &fakeStore{}, &fakeProvider{}, nil),
		authed(httptest.NewRequest("POST", "/v1/keys/select", strings.NewReader(`{"channel_id":7}`))))
	if status != http.StatusOK {
		t.Fatalf("status=%d", status)
	}
	if strings.Contains(string(env.Data), "lease_id") {
		t.Fatalf("lease_id should be omitted: %s", env.Data)
	}
}

// P1-3 回归（api 侧）：keys/report 的 key_index 为 *int——未传 → nil
// （不被零值遮蔽为显式 0），显式 0 → 指针指向 0。
func TestKeyReportKeyIndexPointer(t *testing.T) {
	m := okManager()
	h := newTestRouter(okSelector(), m, &fakeStore{}, &fakeProvider{}, nil)

	status, _ := do(t, h, authed(httptest.NewRequest("POST", "/v1/keys/report",
		strings.NewReader(`{"channel_id":7,"key":"gamma","success":true}`))))
	if status != http.StatusOK {
		t.Fatalf("status=%d", status)
	}
	if m.lastRep.KeyIndex != nil {
		t.Fatalf("KeyIndex = %v, want nil (not provided)", *m.lastRep.KeyIndex)
	}

	status, _ = do(t, h, authed(httptest.NewRequest("POST", "/v1/keys/report",
		strings.NewReader(`{"channel_id":7,"key_index":0,"success":true}`))))
	if status != http.StatusOK {
		t.Fatalf("status=%d", status)
	}
	if m.lastRep.KeyIndex == nil || *m.lastRep.KeyIndex != 0 {
		t.Fatalf("KeyIndex = %v, want pointer to 0", m.lastRep.KeyIndex)
	}
}
