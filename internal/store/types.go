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
type Channel struct {
	Id          int         `json:"id"`
	Type        int         `json:"type" gorm:"default:0"`
	Key         string      `json:"key" gorm:"not null"`
	Status      int         `json:"status" gorm:"default:1"`
	Name        string      `json:"name" gorm:"index"`
	Weight      *uint       `json:"weight" gorm:"default:0"`
	BaseURL     string      `json:"base_url" gorm:"column:base_url;default:''"`
	Models      string      `json:"models"`
	Group       string      `json:"group" gorm:"column:group;type:varchar(64);default:'default'"`
	Priority    *int64      `json:"priority" gorm:"bigint;default:0"`
	AutoBan     *int        `json:"auto_ban" gorm:"default:1"`
	OtherInfo   string      `json:"other_info"`
	ChannelInfo ChannelInfo `json:"channel_info" gorm:"type:json"`
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
