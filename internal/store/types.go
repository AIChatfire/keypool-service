// Package store implements the data-access layer over new-api's
// channels / abilities / options tables (SPEC §2, §4).
package store

import (
	"crypto/sha1"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// Channel status values (SPEC §2.1; names mirror new-api).
const (
	ChannelStatusEnabled          = 1
	ChannelStatusManuallyDisabled = 2
	ChannelStatusAutoDisabled     = 3
)

// ChannelInfo mirrors new-api's channel_info JSON column (SPEC §2.2).
// Field names must match the stored JSON byte-for-byte.
type ChannelInfo struct {
	IsMultiKey             bool           `json:"is_multi_key"`
	MultiKeySize           int            `json:"multi_key_size"`
	MultiKeyStatusList     map[int]int    `json:"multi_key_status_list,omitempty"`
	MultiKeyDisabledReason map[int]string `json:"multi_key_disabled_reason,omitempty"`
	MultiKeyDisabledTime   map[int]int64  `json:"multi_key_disabled_time,omitempty"`
	MultiKeyPollingIndex   int            `json:"multi_key_polling_index"`
	MultiKeyMode           string         `json:"multi_key_mode"`
}

// Value implements driver.Valuer: serialize ChannelInfo to JSON for the DB.
func (ci ChannelInfo) Value() (driver.Value, error) {
	b, err := json.Marshal(ci)
	if err != nil {
		return nil, err
	}
	return string(b), nil
}

// Scan implements sql.Scanner: deserialize the channel_info JSON column.
// NULL / empty values yield the zero ChannelInfo.
func (ci *ChannelInfo) Scan(value interface{}) error {
	if value == nil {
		*ci = ChannelInfo{}
		return nil
	}
	var data []byte
	switch v := value.(type) {
	case []byte:
		data = v
	case string:
		data = []byte(v)
	default:
		return fmt.Errorf("channel_info: unsupported scan type %T", value)
	}
	if len(data) == 0 {
		*ci = ChannelInfo{}
		return nil
	}
	return json.Unmarshal(data, ci)
}

// Channel maps the new-api channels table columns used by keypool (SPEC §2.1).
// 列名/gorm tag 逐字节对齐 new-api model.Channel，便于元数据投影全量透出。
// 注意：keypool 仅读取这些列；写路径（ApplyKeyStatus）只 Updates
// channel_info/status/other_info 三列（single-writer discipline，SPEC §2.3）。
type Channel struct {
	Id                 int         `json:"id"`
	Type               int         `json:"type" gorm:"default:0"`
	Key                string      `json:"key" gorm:"not null"`
	OpenAIOrganization *string     `json:"openai_organization"`
	TestModel          *string     `json:"test_model"`
	Status             int         `json:"status" gorm:"default:1"`
	Name               string      `json:"name" gorm:"index"`
	Weight             *uint       `json:"weight" gorm:"default:0"`
	CreatedTime        int64       `json:"created_time" gorm:"bigint"`
	TestTime           int64       `json:"test_time" gorm:"bigint"`
	ResponseTime       int         `json:"response_time"` // in milliseconds
	BaseURL            string      `json:"base_url" gorm:"column:base_url;default:''"`
	Other              string      `json:"other"`
	Balance            float64     `json:"balance"` // in USD
	BalanceUpdatedTime int64       `json:"balance_updated_time" gorm:"bigint"`
	Models             string      `json:"models"`
	Group              string      `json:"group" gorm:"column:group;type:varchar(64);default:'default'"`
	UsedQuota          int64       `json:"used_quota" gorm:"bigint;default:0"`
	ModelMapping       *string     `json:"model_mapping" gorm:"type:text"`
	StatusCodeMapping  *string     `json:"status_code_mapping" gorm:"type:varchar(1024);default:''"`
	Priority           *int64      `json:"priority" gorm:"bigint;default:0"`
	AutoBan            *int        `json:"auto_ban" gorm:"default:1"`
	OtherInfo          string      `json:"other_info"`
	Tag                *string     `json:"tag" gorm:"index"`
	Setting            *string     `json:"setting" gorm:"type:text"` // 渠道额外设置
	ParamOverride      *string     `json:"param_override" gorm:"type:text"`
	HeaderOverride     *string     `json:"header_override" gorm:"type:text"` // 自定义请求标头
	Remark             *string     `json:"remark" gorm:"type:varchar(255)"`
	ChannelInfo        ChannelInfo `json:"channel_info" gorm:"type:json"`
	OtherSettings      string      `json:"settings" gorm:"column:settings"` // azure 版本等（dto.ChannelOtherSettings）
}

// TableName binds Channel to new-api's channels table.
func (Channel) TableName() string { return "channels" }

// GetKeys splits the channel key column (SPEC §2.1): a leading '[' means a
// JSON array; otherwise split on '\n' after trimming the trailing newline.
//
// 评审修复（P2-1）：'[' 开头但 JSON 解析失败时返回空切片（对齐 new-api
// 行为——JSON 形态的列无法解析即视为无 key），不再回退 '\n' 切分。
func (c *Channel) GetKeys() []string {
	if c.Key == "" {
		return []string{}
	}
	if strings.HasPrefix(c.Key, "[") {
		var res []string
		if err := json.Unmarshal([]byte(c.Key), &res); err != nil {
			return []string{}
		}
		return res
	}
	return strings.Split(strings.TrimSuffix(c.Key, "\n"), "\n")
}

// Epoch is sha1(strings.Join(GetKeys(), "\x00"))[:8] in hex (SPEC §4).
func (c *Channel) Epoch() string {
	sum := sha1.Sum([]byte(strings.Join(c.GetKeys(), "\x00")))
	return hex.EncodeToString(sum[:])[:8]
}

// EnabledKeyIndexes returns key indexes whose entry in
// multi_key_status_list is missing or equals ChannelStatusEnabled
// (missing index = enabled, SPEC §2.2).
func (c *Channel) EnabledKeyIndexes() []int {
	keys := c.GetKeys()
	res := make([]int, 0, len(keys))
	for i := range keys {
		if st, ok := c.ChannelInfo.MultiKeyStatusList[i]; ok && st != ChannelStatusEnabled {
			continue
		}
		res = append(res, i)
	}
	return res
}

// ChannelMeta 是面向调用方的渠道元数据只读投影（new-api channels 表）。
// 供 key 选取接口（include_channel）与 GET /v1/channels/{id} 返回，
// 便于对接方无需直连 DB 即可拿到渠道上下文。
//
// 只透出 new-api 后台 Web 端可配置的信息（渠道编辑表单项），
// 不含运行时/统计态字段（测活结果、余额、已用额度、禁用原因、key 数、epoch 等）。
// JSON 字符串列（model_mapping/status_code_mapping/header_override/
// param_override/setting/settings/other）解析为对象透出，解析失败或为空时省略。
// 注意 header_override/param_override 等可能包含敏感配置，接口本身由 AUTH_TOKEN 保护。
type ChannelMeta struct {
	ID           int      `json:"id"`
	Name         string   `json:"name"`
	Type         int      `json:"type"`
	Status       int      `json:"status"` // 1=enabled 2=manually_disabled 3=auto_disabled
	Group        string   `json:"group"`
	Tag          string   `json:"tag,omitempty"`
	Remark       string   `json:"remark,omitempty"`
	Models       []string `json:"models"`
	BaseURL      string   `json:"base_url"`
	Priority     int64    `json:"priority"`
	Weight       uint     `json:"weight"`
	AutoBan      bool     `json:"auto_ban"`
	MultiKey     bool     `json:"multi_key"`
	MultiKeyMode string   `json:"multi_key_mode"` // 多 key 渠道缺省按 polling 报告

	// ---- Web 端可配置的上游参数 ----
	OpenAIOrganization string `json:"openai_organization,omitempty"`
	TestModel          string `json:"test_model,omitempty"` // 测活模型

	// ---- 覆盖/映射配置（JSON 列解析后透出）----
	// 用 map[string]any 原样透出：线上这些列可能存嵌套对象（如
	// header_override 存 {"upstream":{...}} 适配配置），按 map[string]string
	// 解析会整体失败、字段被静默丢弃。
	ModelMapping      map[string]any `json:"model_mapping,omitempty"`       // 模型重定向
	StatusCodeMapping map[string]any `json:"status_code_mapping,omitempty"` // 状态码覆盖
	HeaderOverride    map[string]any `json:"header_override,omitempty"`     // 自定义请求标头（可为嵌套对象）
	ParamOverride     map[string]any `json:"param_override,omitempty"`      // 请求体参数覆盖
	Setting           map[string]any `json:"setting,omitempty"`             // 渠道额外设置（代理等）
	Settings          map[string]any `json:"settings,omitempty"`            // azure 版本等（settings 列）
	Other             map[string]any `json:"other,omitempty"`               // 其他设置（Vertex 部署地区等）
}

// parseObjectMap 把 JSON 字符串解析为 map[string]any
// （model_mapping/status_code_mapping/header_override/param_override/
// setting/settings/other/other_info）。空值或解析失败返回 nil。
func parseObjectMap(s string) map[string]any {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	m := map[string]any{}
	if err := json.Unmarshal([]byte(s), &m); err != nil || len(m) == 0 {
		return nil
	}
	return m
}

// pruneZeroEntries 剔除 map 中的零值项（nil/false/""/0/空数组/空对象），
// 全部剔除后返回 nil（该字段整体省略）。
//
// 背景：new-api 落库时把 setting/settings 两列按类型化 struct 全量序列化
// （无 omitempty），DB 里存的是带全部默认值的 JSON；原样透出会在响应里
// flood 一堆 false/""/0 噪声。投影只保留非默认（即用户实际配置）的项，
// 对齐 README 承诺的"只透出 Web 端可配置信息"语义。
// 注意：param_override 不走此函数——它是用户手填 JSON，显式 0/false
// 有语义（如 "temperature":0），必须原样透出。
func pruneZeroEntries(m map[string]any) map[string]any {
	for k, v := range m {
		if isZeroValue(v) {
			delete(m, k)
		}
	}
	if len(m) == 0 {
		return nil
	}
	return m
}

// isZeroValue 判定 JSON 反序列化后的值是否为零值。
func isZeroValue(v any) bool {
	switch t := v.(type) {
	case nil:
		return true
	case bool:
		return !t
	case string:
		return t == ""
	case float64: // encoding/json 数字统一为 float64
		return t == 0
	case int:
		return t == 0
	case int64:
		return t == 0
	case []any:
		return len(t) == 0
	case map[string]any:
		return len(t) == 0
	}
	return false
}

// deref 安全解引用 *string。
func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// Meta 构造 Channel 的元数据投影。
func (c *Channel) Meta() *ChannelMeta {
	var priority int64
	if c.Priority != nil {
		priority = *c.Priority
	}
	var weight uint
	if c.Weight != nil {
		weight = *c.Weight
	}
	models := []string{}
	for _, m := range strings.Split(c.Models, ",") {
		if t := strings.TrimSpace(m); t != "" {
			models = append(models, t)
		}
	}
	mode := c.ChannelInfo.MultiKeyMode
	if c.ChannelInfo.IsMultiKey && mode == "" {
		mode = "polling"
	}
	return &ChannelMeta{
		ID:                 c.Id,
		Name:               c.Name,
		Type:               c.Type,
		Status:             c.Status,
		Group:              c.Group,
		Tag:                deref(c.Tag),
		Remark:             deref(c.Remark),
		Models:             models,
		BaseURL:            c.BaseURL,
		Priority:           priority,
		Weight:             weight,
		AutoBan:            c.AutoBan == nil || *c.AutoBan == 1,
		MultiKey:           c.ChannelInfo.IsMultiKey,
		MultiKeyMode:       mode,
		OpenAIOrganization: deref(c.OpenAIOrganization),
		TestModel:          deref(c.TestModel),
		ModelMapping:       parseObjectMap(deref(c.ModelMapping)),
		StatusCodeMapping:  parseObjectMap(deref(c.StatusCodeMapping)),
		HeaderOverride:     parseObjectMap(deref(c.HeaderOverride)),
		ParamOverride:      parseObjectMap(deref(c.ParamOverride)),
		Setting:            pruneZeroEntries(parseObjectMap(deref(c.Setting))),
		Settings:           pruneZeroEntries(parseObjectMap(c.OtherSettings)),
		Other:              pruneZeroEntries(parseObjectMap(c.Other)),
	}
}

// Ability maps the new-api abilities table (SPEC §2.6).
type Ability struct {
	Group     string  `json:"group" gorm:"column:group;primaryKey;autoIncrement:false"`
	Model     string  `json:"model" gorm:"primaryKey;autoIncrement:false"`
	ChannelId int     `json:"channel_id" gorm:"primaryKey;autoIncrement:false"`
	Enabled   bool    `json:"enabled"`
	Priority  *int64  `json:"priority" gorm:"bigint;default:0"`
	Weight    uint    `json:"weight"`
	Tag       *string `json:"tag"`
}

// TableName binds Ability to new-api's abilities table.
func (Ability) TableName() string { return "abilities" }

// Option maps the new-api options table (key/value, PK=key; SPEC §2.5).
type Option struct {
	Key   string `json:"key" gorm:"primaryKey"`
	Value string `json:"value"`
}

// TableName binds Option to new-api's options table.
func (Option) TableName() string { return "options" }

// CodeRange is an inclusive status-code range (SPEC §4).
type CodeRange struct {
	Start, End int
}

// ParseCodeRanges parses "401,500-503" syntax into inclusive ranges.
// Empty input yields nil, nil. Malformed items return an error.
func ParseCodeRanges(s string) ([]CodeRange, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}
	parts := strings.Split(s, ",")
	ranges := make([]CodeRange, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if strings.Contains(p, "-") {
			ab := strings.SplitN(p, "-", 2)
			start, err1 := strconv.Atoi(strings.TrimSpace(ab[0]))
			end, err2 := strconv.Atoi(strings.TrimSpace(ab[1]))
			if err1 != nil || err2 != nil || start > end {
				return nil, fmt.Errorf("invalid code range %q", p)
			}
			ranges = append(ranges, CodeRange{Start: start, End: end})
			continue
		}
		n, err := strconv.Atoi(p)
		if err != nil {
			return nil, fmt.Errorf("invalid status code %q", p)
		}
		ranges = append(ranges, CodeRange{Start: n, End: n})
	}
	return ranges, nil
}

// Match reports whether code falls into any range.
func MatchCodeRange(ranges []CodeRange, code int) bool {
	for _, r := range ranges {
		if code >= r.Start && code <= r.End {
			return true
		}
	}
	return false
}

// BalanceCfg is the per-channel usage-balance config (SPEC §4).
// Mode: usage|request|auto; Metric: tokens|cost.
type BalanceCfg struct {
	Mode          string  `json:"mode"`
	Metric        string  `json:"metric"`
	DecayInterval float64 `json:"decay_interval"`
	DecayFactor   float64 `json:"decay_factor"`
	Catchup       float64 `json:"catchup"`
}

// RotationCfg is the per-channel time-band rotation config (SPEC §4).
// Order: index|shuffle.
type RotationCfg struct {
	BandSeconds  int    `json:"band_seconds"`
	ActiveCount  int    `json:"active_count"`
	OverlapBands int    `json:"overlap_bands"`
	Order        string `json:"order"`
}

// Settings is the atomic config snapshot rebuilt by the options poller.
// Balance/Rotation are keyed by channel id.
type Settings struct {
	AutoDisableOn, AutoEnableOn bool
	DisableCodeRanges           []CodeRange
	DisableKeywords             []string
	Balance                     map[int]BalanceCfg
	Rotation                    map[int]RotationCfg
}

// SettingsProvider supplies the latest Settings snapshot (SPEC §4).
type SettingsProvider interface {
	Get() *Settings
}
