// Package api 是 keypool 的 HTTP 装配层（SPEC §5）：
// Go1.22 ServeMux 路由、Bearer 鉴权、统一包络 {code,message,data,request_id}、
// 集中错误映射（writeErr）、panic recover 与 /metrics Prometheus 输出。
//
// 包间依赖：api → {selector, state, store, redisx, config}（SPEC §1）。
package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"keypool/internal/config"
	"keypool/internal/redisx"
	"keypool/internal/selector"
	"keypool/internal/state"
	"keypool/internal/store"
)

// 错误码（SPEC §5）。
const (
	CodeOK             = 0
	CodeNoKey          = 40001 // 无可用 key
	CodeChannelMissing = 40002 // 渠道不存在
	CodeDuplicate      = 40003 // 幂等冲突
	CodeBadRequest     = 40010 // 参数错误
	CodeInternal       = 50001 // 依赖故障/内部错误
	CodeUnauthorized   = 40100 // 鉴权失败
)

// retryAfterMs 是 40001 响应携带的重试建议（SPEC §5 任务约束）。
const retryAfterMs = 1000

// ---- 窄接口（便于 httptest 注入 fake；*selector.Selector / *state.Manager /
// *store.Store / *redisx.Client 天然满足）----

// keySelector 是 api 对 selector 的窄接口依赖。
type keySelector interface {
	Select(ctx context.Context, req selector.SelectReq) (*selector.SelectResp, error)
	SnapshotMetrics() map[string]int64
}

// stateManager 是 api 对 state 的窄接口依赖。
type stateManager interface {
	Report(ctx context.Context, r state.ReportReq) (*state.ReportResp, error)
	SetKeyStatus(ctx context.Context, cid, idx, status int, reason string) (*state.ReportResp, error)
	SnapshotMetrics() map[string]int64
}

// channelReader 是 api 对 store 的窄接口依赖。
type channelReader interface {
	GetChannel(id int) (*store.Channel, error)
}

// optionWriter 是 api 对 store 的写扩展（UpsertOption 为 api 层新增，
// 见 internal/store/options_write.go 头注）。
type optionWriter interface {
	UpsertOption(key, value string) error
}

// storeAPI 组合 api 对 store 的读写需求。
type storeAPI interface {
	channelReader
	optionWriter
}

// usageReader 是 api 对 redisx 的窄接口依赖（用量查询）。
type usageReader interface {
	UsageAll(ctx context.Context, cid int) (counters map[int]float64, lastDecay int64, err error)
}

// reloader 是可选的快照重建能力（*store.OptionsPoller 通过新增的
// Reload 方法满足；NewRouter 对 sp 做类型断言获得）。
type reloader interface {
	Reload() error
}

// router 持有全部依赖与运行时状态。
type router struct {
	cfg     config.Config
	sl      keySelector
	m       stateManager
	s       storeAPI
	sp      store.SettingsProvider
	rdb     usageReader // nil = Redis 降级
	started time.Time
	mux     *http.ServeMux
}

// NewRouter 装配生产 HTTP Handler（SPEC §4，签名一字不改）。
func NewRouter(cfg config.Config, sl *selector.Selector, m *state.Manager, s *store.Store, sp store.SettingsProvider, rdb *redisx.Client) http.Handler {
	// rdb 可能为 nil（main 的 degraded 模式）：显式转为 nil 接口，
	// 避免“非 nil 接口包裹 nil 指针”。
	var ur usageReader
	if rdb != nil {
		ur = rdb
	}
	return newRouter(cfg, sl, m, s, sp, ur)
}

// newRouter 供单测注入 fake。
func newRouter(cfg config.Config, sl keySelector, m stateManager, s storeAPI, sp store.SettingsProvider, rdb usageReader) http.Handler {
	rt := &router{cfg: cfg, sl: sl, m: m, s: s, sp: sp, rdb: rdb, started: time.Now()}
	mux := http.NewServeMux()

	// RESTful 路由（v1 全量；不再保留 key:get / {idx}:enable 等旧式动作路径）。
	mux.HandleFunc("POST /v1/keys/select", rt.handleKeySelect)
	mux.HandleFunc("POST /v1/keys/report", rt.handleKeyReport)
	mux.HandleFunc("GET /v1/channels/{id}", rt.handleGetChannel)
	mux.HandleFunc("GET /v1/channels/{id}/keys", rt.handleListKeys)
	mux.HandleFunc("PATCH /v1/channels/{id}/keys/{idx}", rt.handlePatchKeyStatus)
	mux.HandleFunc("PUT /v1/channels/{id}/balance", rt.handlePutBalance)
	mux.HandleFunc("GET /v1/channels/{id}/balance", rt.handleGetBalance)
	mux.HandleFunc("PUT /v1/channels/{id}/rotation", rt.handlePutRotation)
	mux.HandleFunc("GET /v1/channels/{id}/rotation", rt.handleGetRotation)
	mux.HandleFunc("GET /v1/channels/{id}/usage", rt.handleUsage)
	mux.HandleFunc("POST /v1/settings/reload", rt.handleSettingsReload)
	mux.HandleFunc("GET /healthz", rt.handleHealthz)
	mux.HandleFunc("GET /metrics", rt.handleMetrics)

	rt.mux = mux
	return rt.wrap(mux)
}

// wrap 中间件链：recover → request_id → auth（/healthz 除外）。
func (rt *router) wrap(next http.Handler) http.Handler {
	return rt.recoverMW(rt.requestIDMW(rt.authMW(next)))
}

// ---- 中间件 ----

type ctxKey string

const ctxRequestID ctxKey = "request_id"

// requestIDMW 为每个请求生成 crypto/rand 8 字节 hex request_id。
func (rt *router) requestIDMW(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b [8]byte
		if _, err := rand.Read(b[:]); err != nil {
			// crypto/rand 失败极端罕见；退化为时间戳保证字段非空。
			id := fmt.Sprintf("t%d", time.Now().UnixNano())
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxRequestID, id)))
			return
		}
		id := hex.EncodeToString(b[:])
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxRequestID, id)))
	})
}

// authMW 校验 Authorization: Bearer <AuthToken>；/healthz 豁免（SPEC §5）。
func (rt *router) authMW(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			next.ServeHTTP(w, r)
			return
		}
		want := "Bearer " + rt.cfg.AuthToken
		if rt.cfg.AuthToken == "" || r.Header.Get("Authorization") != want {
			writeErr(w, r, http.StatusUnauthorized, CodeUnauthorized, "unauthorized", nil)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// recoverMW 兜底 panic → 500/50001（SPEC §5 任务约束）。
func (rt *router) recoverMW(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				log.Printf("keypool: panic serving %s %s: %v", r.Method, r.URL.Path, rec)
				writeErr(w, r, http.StatusInternalServerError, CodeInternal, "internal error", nil)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// ---- 统一包络与集中错误映射 ----

// envelope 是统一响应包络（SPEC §5）。
type envelope struct {
	Code      int    `json:"code"`
	Message   string `json:"message"`
	Data      any    `json:"data"`
	RequestID string `json:"request_id"`
}

func requestIDOf(r *http.Request) string {
	if v, ok := r.Context().Value(ctxRequestID).(string); ok {
		return v
	}
	return ""
}

// writeJSON 输出包络。
func writeJSON(w http.ResponseWriter, r *http.Request, status int, env envelope) {
	env.RequestID = requestIDOf(r)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(env)
}

// writeOK 输出成功包络。
func writeOK(w http.ResponseWriter, r *http.Request, data any) {
	writeJSON(w, r, http.StatusOK, envelope{Code: CodeOK, Message: "ok", Data: data})
}

// writeErr 集中错误映射（SPEC §5 错误码表）：
//
//	selector.ErrNoKey            → 503/40001（data.retry_after_ms=1000）
//	selector.ErrNoChannel        → 404/40002
//	state.ErrChannelNotFound     → 404/40002
//	selector.ErrInvalidRequest   → 400/40010（key_index 越界等）
//	state.ErrInvalidRequest      → 400/40010
//	state.ErrLockFailed      → 503/50001
//	redisx.ErrDegraded       → 503/50001（Redis 降级）
//	selector.ErrDependency   → 503/50001（DB 故障，非 NotFound）
//	state.ErrDependency      → 503/50001（同上）
//	其他                      → 500/50001
func writeErr(w http.ResponseWriter, r *http.Request, status int, code int, message string, data any) {
	writeJSON(w, r, status, envelope{Code: code, Message: message, Data: data})
}

// writeDomainErr 把 selector/state 的 sentinel error 映射为包络。
func writeDomainErr(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, selector.ErrNoKey):
		writeErr(w, r, http.StatusServiceUnavailable, CodeNoKey,
			"no available key", map[string]any{"retry_after_ms": retryAfterMs})
	case errors.Is(err, selector.ErrNoChannel), errors.Is(err, state.ErrChannelNotFound):
		writeErr(w, r, http.StatusNotFound, CodeChannelMissing, "channel not found", nil)
	case errors.Is(err, selector.ErrInvalidRequest), errors.Is(err, state.ErrInvalidRequest):
		writeErr(w, r, http.StatusBadRequest, CodeBadRequest, err.Error(), nil)
	case errors.Is(err, state.ErrLockFailed):
		writeErr(w, r, http.StatusServiceUnavailable, CodeInternal, err.Error(), nil)
	case errors.Is(err, redisx.ErrDegraded),
		errors.Is(err, selector.ErrDependency), errors.Is(err, state.ErrDependency):
		// 依赖故障（Redis 降级 / DB 非 NotFound 错误）→ 503/50001
		writeErr(w, r, http.StatusServiceUnavailable, CodeInternal, err.Error(), nil)
	default:
		writeErr(w, r, http.StatusInternalServerError, CodeInternal, err.Error(), nil)
	}
}

// writeBadRequest 参数错误 → 400/40010。
func writeBadRequest(w http.ResponseWriter, r *http.Request, format string, args ...any) {
	writeErr(w, r, http.StatusBadRequest, CodeBadRequest, fmt.Sprintf(format, args...), nil)
}

// ---- 通用解析辅助 ----

// pathID 解析 {id} 路径参数为 int；失败返回 error（调用方映射 40010）。
func pathID(r *http.Request) (int, error) {
	raw := r.PathValue("id")
	id, err := strconv.Atoi(raw)
	if err != nil || id <= 0 {
		return 0, fmt.Errorf("invalid channel id %q", raw)
	}
	return id, nil
}

// decodeJSON 解析请求体 JSON；空体视为 {}。
func decodeJSON(r *http.Request, v any) error {
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(v); err != nil {
		if err.Error() == "EOF" {
			return nil
		}
		return err
	}
	return nil
}

// settings 读取最新快照；nil provider 视为 nil。
func (rt *router) settings() *store.Settings {
	if rt.sp == nil {
		return nil
	}
	return rt.sp.Get()
}

// reload 触发快照重建（若 sp 具备 Reload 能力）。
func (rt *router) reload() error {
	if rl, ok := rt.sp.(reloader); ok {
		return rl.Reload()
	}
	return nil // 无 Reload 能力（如纯静态 provider）：视为已生效
}

// ---- handlers ----

// handleHealthz 返回裸 JSON {"status":"ok"}（P2-8：探活端点不套统一包络，
// 便于负载均衡/探针直接断言）。
func (rt *router) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

// handleSettingsReload POST /v1/settings/reload：重建 options 快照（SPEC §5）。
func (rt *router) handleSettingsReload(w http.ResponseWriter, r *http.Request) {
	if err := rt.reload(); err != nil {
		writeErr(w, r, http.StatusServiceUnavailable, CodeInternal,
			fmt.Sprintf("reload settings: %v", err), map[string]any{"reloaded": false})
		return
	}
	writeOK(w, r, map[string]any{"reloaded": true})
}

// handleMetrics GET /metrics：Prometheus 文本，汇总 selector/state 的
// SnapshotMetrics 并附加进程 uptime（SPEC §5）。
func (rt *router) handleMetrics(w http.ResponseWriter, r *http.Request) {
	merged := map[string]int64{}
	for k, v := range rt.sl.SnapshotMetrics() {
		merged[k] += v
	}
	for k, v := range rt.m.SnapshotMetrics() {
		merged[k] += v
	}
	keys := make([]string, 0, len(merged))
	for k := range merged {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	seenType := map[string]bool{}
	for _, k := range keys {
		name := "keypool_" + k
		base := name
		if i := strings.IndexByte(base, '{'); i >= 0 {
			base = base[:i]
		}
		if !seenType[base] {
			seenType[base] = true
			fmt.Fprintf(&b, "# TYPE %s counter\n", base)
		}
		fmt.Fprintf(&b, "%s %d\n", name, merged[k])
	}
	fmt.Fprintf(&b, "# TYPE keypool_process_uptime_seconds gauge\n")
	fmt.Fprintf(&b, "keypool_process_uptime_seconds %.3f\n", time.Since(rt.started).Seconds())

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(b.String()))
}
