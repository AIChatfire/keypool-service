package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"keypool/internal/selector"
	"keypool/internal/state"
	"keypool/internal/store"
)

// 本文件实现 SPEC §5 的业务 handlers。HTTP 层使用 snake_case wire DTO，
// 在边界转换为 SPEC §4 的领域类型（SelectReq/ReportReq 无 json tag）。

// ---- POST /v1/keys/select ----

type keyRefDTO struct {
	ChannelID int `json:"channel_id"`
	KeyIndex  int `json:"key_index"`
}

// selectReqDTO 是 SelectReq 的 wire 形态（snake_case）。
// AdvanceCursor 用指针以区分“未传”（默认 true）与显式 false。
type selectReqDTO struct {
	ChannelID      int         `json:"channel_id"`
	Group          string      `json:"group"`
	Model          string      `json:"model"`
	Retry          int         `json:"retry"`
	Exclude        []keyRefDTO `json:"exclude"`
	Mode           string      `json:"mode"`
	EstTokens      float64     `json:"est_tokens"`
	AdvanceCursor  *bool       `json:"advance_cursor"`
	IncludeChannel bool        `json:"include_channel"` // 响应附带 new-api 渠道元数据
}

type bandDTO struct {
	Index  int   `json:"index"`
	EndsAt int64 `json:"ends_at"`
}

// selectRespDTO 是 SelectResp 的 wire 形态（snake_case）。
type selectRespDTO struct {
	ChannelID int                `json:"channel_id"`
	KeyIndex  int                `json:"key_index"`
	Key       string             `json:"key"`
	BaseURL   string             `json:"base_url"`
	Mode      string             `json:"mode"`
	Epoch     string             `json:"epoch"`
	Band      *bandDTO           `json:"band,omitempty"`
	LeaseID   string             `json:"lease_id,omitempty"` // usage 预扣租约（SPEC §4 方案 b）
	Channel   *store.ChannelMeta `json:"channel,omitempty"`  // include_channel=true 时返回
}

func (rt *router) handleKeySelect(w http.ResponseWriter, r *http.Request) {
	var body selectReqDTO
	if err := decodeJSON(r, &body); err != nil {
		writeBadRequest(w, r, "invalid JSON body: %v", err)
		return
	}
	// P2-6：mode 仅允许 polling|random|usage|""；参数全空（无 channel_id
	// 且无完整 group+model）→ 40010。
	if body.Mode != "" && body.Mode != "polling" && body.Mode != "random" && body.Mode != "usage" {
		writeBadRequest(w, r, "invalid mode %q: want polling|random|usage", body.Mode)
		return
	}
	if body.ChannelID == 0 && (body.Group == "" || body.Model == "") {
		writeBadRequest(w, r, "channel_id or group+model is required")
		return
	}
	req := selector.SelectReq{
		ChannelID: body.ChannelID,
		Group:     body.Group,
		Model:     body.Model,
		Retry:     body.Retry,
		Mode:      body.Mode,
		EstTokens: body.EstTokens,
		// SPEC §4：AdvanceCursor 默认 true
		AdvanceCursor:  body.AdvanceCursor == nil || *body.AdvanceCursor,
		IncludeChannel: body.IncludeChannel,
	}
	for _, ex := range body.Exclude {
		req.Exclude = append(req.Exclude, selector.KeyRef{ChannelID: ex.ChannelID, KeyIndex: ex.KeyIndex})
	}

	resp, err := rt.sl.Select(r.Context(), req)
	if err != nil {
		writeDomainErr(w, r, err) // ErrNoKey→503/40001(+retry_after_ms)；ErrNoChannel→404/40002
		return
	}
	out := selectRespDTO{
		ChannelID: resp.ChannelID,
		KeyIndex:  resp.KeyIndex,
		Key:       resp.Key,
		BaseURL:   resp.BaseURL,
		Mode:      resp.Mode,
		Epoch:     resp.Epoch,
		LeaseID:   resp.LeaseID,
		Channel:   resp.Channel,
	}
	if resp.Band != nil {
		out.Band = &bandDTO{Index: resp.Band.Index, EndsAt: resp.Band.EndsAt}
	}
	writeOK(w, r, out)
}

// ---- POST /v1/keys/report ----

type usageDTO struct {
	PromptTokens     float64 `json:"prompt_tokens"`
	CompletionTokens float64 `json:"completion_tokens"`
	Cost             float64 `json:"cost"`
}

// reportReqDTO 是 ReportReq 的 wire 形态（snake_case）。
// KeyIndex 为 *int：nil=未传（与 state.ReportReq.KeyIndex 同步，P1-3），
// 避免 JSON 零值把“未传”遮蔽成“显式 0”。
type reportReqDTO struct {
	LeaseID        string    `json:"lease_id"`
	ChannelID      int       `json:"channel_id"`
	KeyIndex       *int      `json:"key_index"`
	Key            string    `json:"key"`
	Epoch          string    `json:"epoch"`
	Success        bool      `json:"success"`
	StatusCode     int       `json:"status_code"`
	ErrorCode      string    `json:"error_code"`
	ErrorMessage   string    `json:"error_message"`
	Usage          *usageDTO `json:"usage"`
	IdempotencyKey string    `json:"idempotency_key"`
}

func (rt *router) handleKeyReport(w http.ResponseWriter, r *http.Request) {
	var body reportReqDTO
	if err := decodeJSON(r, &body); err != nil {
		writeBadRequest(w, r, "invalid JSON body: %v", err)
		return
	}
	req := state.ReportReq{
		LeaseID:      body.LeaseID,
		ChannelID:    body.ChannelID,
		KeyIndex:     body.KeyIndex,
		Key:          body.Key,
		Epoch:        body.Epoch,
		Success:      body.Success,
		StatusCode:   body.StatusCode,
		ErrorCode:    body.ErrorCode,
		ErrorMessage: body.ErrorMessage,
		// 头部 Idempotency-Key 优先于 body 同名字段（SPEC §5）
		IdempotencyKey: body.IdempotencyKey,
	}
	if h := strings.TrimSpace(r.Header.Get("Idempotency-Key")); h != "" {
		req.IdempotencyKey = h
	}
	if body.Usage != nil {
		req.Usage = &state.Usage{
			PromptTokens:     body.Usage.PromptTokens,
			CompletionTokens: body.Usage.CompletionTokens,
			Cost:             body.Usage.Cost,
		}
	}

	resp, err := rt.m.Report(r.Context(), req)
	if err != nil {
		writeDomainErr(w, r, err) // 40002/40010/50001 见 writeDomainErr
		return
	}
	// 幂等冲突：state 侧幂等命中返回 Action=duplicate → 409/40003（SPEC §5）。
	if resp.Action == "duplicate" {
		writeErr(w, r, http.StatusConflict, CodeDuplicate, "duplicate report", resp)
		return
	}
	writeOK(w, r, resp)
}

// ---- PATCH /v1/channels/{id}/keys/{idx} ----

// keyStatusPatchDTO 是手动启停 key 的请求体：
// status ∈ enabled|disabled（映射 new-api status 1|2），reason 仅禁用时生效。
type keyStatusPatchDTO struct {
	Status string `json:"status"`
	Reason string `json:"reason"`
}

// handlePatchKeyStatus 手动启用/禁用渠道内某个 key（RESTful 资源状态变更，
// 取代旧式 POST .../keys/{idx}:enable|:disable 动作路径）。
func (rt *router) handlePatchKeyStatus(w http.ResponseWriter, r *http.Request) {
	cid, err := pathID(r)
	if err != nil {
		writeBadRequest(w, r, "%v", err)
		return
	}
	idxRaw := r.PathValue("idx")
	idx, err := strconv.Atoi(idxRaw)
	if err != nil || idx < 0 {
		writeBadRequest(w, r, "invalid key index %q", idxRaw)
		return
	}

	var body keyStatusPatchDTO
	if err := decodeJSON(r, &body); err != nil {
		writeBadRequest(w, r, "invalid JSON body: %v", err)
		return
	}
	var status int
	switch body.Status {
	case "enabled":
		status = store.ChannelStatusEnabled
	case "disabled":
		status = store.ChannelStatusManuallyDisabled // 手动禁用 → 2（SPEC §2.1）
	default:
		writeBadRequest(w, r, "invalid status %q: want enabled|disabled", body.Status)
		return
	}

	resp, err := rt.m.SetKeyStatus(r.Context(), cid, idx, status, body.Reason)
	if err != nil {
		writeDomainErr(w, r, err)
		return
	}
	writeOK(w, r, resp)
}

// ---- GET /v1/channels/{id} ----

// handleGetChannel 返回 new-api 渠道元数据（store.ChannelMeta 投影），
// 供对接方查询渠道上下文，无需直连 DB。
func (rt *router) handleGetChannel(w http.ResponseWriter, r *http.Request) {
	cid, err := pathID(r)
	if err != nil {
		writeBadRequest(w, r, "%v", err)
		return
	}
	ch, err := rt.s.GetChannel(cid)
	if err != nil {
		writeErr(w, r, http.StatusNotFound, CodeChannelMissing, "channel not found", nil)
		return
	}
	writeOK(w, r, ch.Meta())
}

// ---- GET /v1/channels/{id}/keys ----

// keyEntryDTO 是 key 列表项（SPEC §5）。Usage 为指针：Redis 不可用/查询
// 失败时整体省略该字段；值为 0 时仍输出 0。
type keyEntryDTO struct {
	Index         int      `json:"index"`
	Status        int      `json:"status"`
	Reason        string   `json:"reason,omitempty"`
	DisabledTime  int64    `json:"disabled_time,omitempty"`
	Usage         *float64 `json:"usage,omitempty"`
	RotationState string   `json:"rotation_state"`
	KeyMask       string   `json:"key_mask"`
}

type listKeysDTO struct {
	Epoch string        `json:"epoch"`
	Mode  string        `json:"mode"`
	Keys  []keyEntryDTO `json:"keys"`
}

func (rt *router) handleListKeys(w http.ResponseWriter, r *http.Request) {
	cid, err := pathID(r)
	if err != nil {
		writeBadRequest(w, r, "%v", err)
		return
	}
	ch, err := rt.s.GetChannel(cid)
	if err != nil {
		writeErr(w, r, http.StatusNotFound, CodeChannelMissing, "channel not found", nil)
		return
	}

	// 用量（可选）：rdb 降级或 UsageAll 失败时省略 usage 字段。
	var counters map[int]float64
	if rt.rdb != nil {
		if c, _, err := rt.rdb.UsageAll(r.Context(), cid); err == nil {
			counters = c
		}
	}

	// rotation_state 重算（见 rotation.go：selector 未导出批次函数，
	// 此处按 SPEC §4 同规则在 api 内重算）。
	rotStates := rotationStates(ch, rt.settings(), nowUnix())

	mode := ch.ChannelInfo.MultiKeyMode
	if mode == "" {
		mode = "polling"
	}
	keys := ch.GetKeys()
	out := listKeysDTO{Epoch: ch.Epoch(), Mode: mode, Keys: make([]keyEntryDTO, 0, len(keys))}
	for i, k := range keys {
		entry := keyEntryDTO{
			Index:         i,
			Status:        store.ChannelStatusEnabled, // status_list 缺失视为启用（SPEC §2.2）
			KeyMask:       maskKey(k),
			RotationState: rotStates[i],
		}
		if st, ok := ch.ChannelInfo.MultiKeyStatusList[i]; ok {
			entry.Status = st
			entry.Reason = ch.ChannelInfo.MultiKeyDisabledReason[i]
			entry.DisabledTime = ch.ChannelInfo.MultiKeyDisabledTime[i]
		}
		if counters != nil {
			v := counters[i]
			entry.Usage = &v
		}
		out.Keys = append(out.Keys, entry)
	}
	writeOK(w, r, out)
}

// maskKey 脱敏：前4后4中间*；长度<8 全*（SPEC §5 任务约束）。
func maskKey(k string) string {
	if len(k) < 8 {
		return strings.Repeat("*", len(k))
	}
	return k[:4] + "****" + k[len(k)-4:]
}

// ---- GET /v1/channels/{id}/usage ----

type usageRespDTO struct {
	Cid       int             `json:"cid"`
	Metric    string          `json:"metric"`
	Counters  map[int]float64 `json:"counters"`
	LastDecay int64           `json:"last_decay"`
}

func (rt *router) handleUsage(w http.ResponseWriter, r *http.Request) {
	cid, err := pathID(r)
	if err != nil {
		writeBadRequest(w, r, "%v", err)
		return
	}
	if rt.rdb == nil {
		writeErr(w, r, http.StatusServiceUnavailable, CodeInternal, "redis unavailable (degraded mode)", nil)
		return
	}
	counters, lastDecay, err := rt.rdb.UsageAll(r.Context(), cid)
	if err != nil {
		writeErr(w, r, http.StatusServiceUnavailable, CodeInternal, "read usage: "+err.Error(), nil)
		return
	}
	metric := "tokens"
	if st := rt.settings(); st != nil && st.Balance != nil {
		if cfg, ok := st.Balance[cid]; ok && cfg.Metric != "" {
			metric = cfg.Metric
		}
	}
	writeOK(w, r, usageRespDTO{Cid: cid, Metric: metric, Counters: counters, LastDecay: lastDecay})
}

// ---- PUT/GET /v1/channels/{id}/balance | rotation ----
//
// 写入 options 表 keypool.balance.{cid} / keypool.rotation.{cid}（SPEC §2.5），
// 写后触发快照重建使其立即生效；GET 返回当前生效配置（无→默认值）。

// 默认生效配置（GET 无配置时返回；与 selector 内部缺省保持一致）。
var (
	defaultBalanceCfg = store.BalanceCfg{
		Mode: "auto", Metric: "tokens", DecayInterval: 3600, DecayFactor: 0.5,
	}
	defaultRotationCfg = store.RotationCfg{
		BandSeconds: 3600, ActiveCount: 1, OverlapBands: 0, Order: "index",
	}
)

func validBalanceMode(m string) bool {
	return m == "" || m == "usage" || m == "request" || m == "auto"
}

func validBalanceMetric(m string) bool {
	return m == "" || m == "tokens" || m == "cost"
}

func (rt *router) handlePutBalance(w http.ResponseWriter, r *http.Request) {
	cid, err := pathID(r)
	if err != nil {
		writeBadRequest(w, r, "%v", err)
		return
	}
	var cfg store.BalanceCfg
	if err := decodeJSON(r, &cfg); err != nil {
		writeBadRequest(w, r, "invalid JSON body: %v", err)
		return
	}
	// 校验（SPEC §5 任务约束）：mode∈usage|request|auto、metric∈tokens|cost。
	if !validBalanceMode(cfg.Mode) {
		writeBadRequest(w, r, "invalid mode %q: want usage|request|auto", cfg.Mode)
		return
	}
	if !validBalanceMetric(cfg.Metric) {
		writeBadRequest(w, r, "invalid metric %q: want tokens|cost", cfg.Metric)
		return
	}
	if cfg.DecayInterval < 0 || cfg.DecayFactor < 0 || cfg.Catchup < 0 {
		writeBadRequest(w, r, "decay_interval/decay_factor/catchup must be >= 0")
		return
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		writeErr(w, r, http.StatusInternalServerError, CodeInternal, err.Error(), nil)
		return
	}
	if err := rt.s.UpsertOption(store.OptBalancePrefix+strconv.Itoa(cid), string(raw)); err != nil {
		writeErr(w, r, http.StatusServiceUnavailable, CodeInternal, "write option: "+err.Error(), nil)
		return
	}
	if err := rt.reload(); err != nil {
		writeErr(w, r, http.StatusServiceUnavailable, CodeInternal, "reload settings: "+err.Error(), nil)
		return
	}
	writeOK(w, r, cfg)
}

func (rt *router) handleGetBalance(w http.ResponseWriter, r *http.Request) {
	cid, err := pathID(r)
	if err != nil {
		writeBadRequest(w, r, "%v", err)
		return
	}
	cfg := defaultBalanceCfg
	if st := rt.settings(); st != nil && st.Balance != nil {
		if c, ok := st.Balance[cid]; ok {
			cfg = c
		}
	}
	writeOK(w, r, cfg)
}

func (rt *router) handlePutRotation(w http.ResponseWriter, r *http.Request) {
	cid, err := pathID(r)
	if err != nil {
		writeBadRequest(w, r, "%v", err)
		return
	}
	var cfg store.RotationCfg
	if err := decodeJSON(r, &cfg); err != nil {
		writeBadRequest(w, r, "invalid JSON body: %v", err)
		return
	}
	// 校验（SPEC §5 任务约束）：band_seconds>=30、active_count>=1。
	if cfg.BandSeconds < 30 {
		writeBadRequest(w, r, "band_seconds must be >= 30, got %d", cfg.BandSeconds)
		return
	}
	if cfg.ActiveCount < 1 {
		writeBadRequest(w, r, "active_count must be >= 1, got %d", cfg.ActiveCount)
		return
	}
	if cfg.OverlapBands < 0 {
		writeBadRequest(w, r, "overlap_bands must be >= 0, got %d", cfg.OverlapBands)
		return
	}
	if cfg.Order != "" && cfg.Order != "index" && cfg.Order != "shuffle" {
		writeBadRequest(w, r, "invalid order %q: want index|shuffle", cfg.Order)
		return
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		writeErr(w, r, http.StatusInternalServerError, CodeInternal, err.Error(), nil)
		return
	}
	if err := rt.s.UpsertOption(store.OptRotationPrefix+strconv.Itoa(cid), string(raw)); err != nil {
		writeErr(w, r, http.StatusServiceUnavailable, CodeInternal, "write option: "+err.Error(), nil)
		return
	}
	if err := rt.reload(); err != nil {
		writeErr(w, r, http.StatusServiceUnavailable, CodeInternal, "reload settings: "+err.Error(), nil)
		return
	}
	writeOK(w, r, cfg)
}

func (rt *router) handleGetRotation(w http.ResponseWriter, r *http.Request) {
	cid, err := pathID(r)
	if err != nil {
		writeBadRequest(w, r, "%v", err)
		return
	}
	cfg := defaultRotationCfg
	if st := rt.settings(); st != nil && st.Rotation != nil {
		if c, ok := st.Rotation[cid]; ok {
			cfg = c
		}
	}
	writeOK(w, r, cfg)
}
