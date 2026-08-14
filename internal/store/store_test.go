package store

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/glebarez/sqlite"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// newTestStore builds an in-memory sqlite store with the minimal
// channels/abilities/options schema for ApplyKeyStatus tests.
func newTestStore(t *testing.T) *Store {
	t.Helper()
	// unique in-memory database per test to avoid cross-test state
	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", strings.ReplaceAll(t.Name(), "/", "_"))
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{SkipDefaultTransaction: true})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := db.AutoMigrate(&Channel{}, &Ability{}, &Option{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return &Store{db: db}
}

func seedMultiKeyChannel(t *testing.T, s *Store) {
	t.Helper()
	ch := Channel{
		Id:     1,
		Key:    "k0\nk1\nk2",
		Status: ChannelStatusEnabled,
		ChannelInfo: ChannelInfo{
			IsMultiKey:   true,
			MultiKeySize: 3,
			MultiKeyMode: "polling",
		},
	}
	if err := s.db.Create(&ch).Error; err != nil {
		t.Fatalf("seed channel: %v", err)
	}
	ab := Ability{Group: "default", Model: "gpt-x", ChannelId: 1, Enabled: true}
	if err := s.db.Create(&ab).Error; err != nil {
		t.Fatalf("seed ability: %v", err)
	}
}

func reloadChannel(t *testing.T, s *Store, id int) *Channel {
	t.Helper()
	ch, err := s.GetChannel(id)
	if err != nil {
		t.Fatalf("reload channel: %v", err)
	}
	return ch
}

func abilityEnabled(t *testing.T, s *Store, cid int) bool {
	t.Helper()
	var ab Ability
	if err := s.db.Where("channel_id = ?", cid).First(&ab).Error; err != nil {
		t.Fatalf("load ability: %v", err)
	}
	return ab.Enabled
}

func TestApplyKeyStatusDisableOneKeyKeepsChannel(t *testing.T) {
	s := newTestStore(t)
	seedMultiKeyChannel(t, s)

	cs, allDead, err := s.ApplyKeyStatus(1, 1, ChannelStatusAutoDisabled, "boom")
	if err != nil {
		t.Fatalf("ApplyKeyStatus: %v", err)
	}
	if allDead || cs != ChannelStatusEnabled {
		t.Fatalf("partial disable: cs=%d allDead=%v", cs, allDead)
	}
	ch := reloadChannel(t, s, 1)
	if ch.Status != ChannelStatusEnabled {
		t.Fatalf("channel status: %d", ch.Status)
	}
	if ch.ChannelInfo.MultiKeyStatusList[1] != ChannelStatusAutoDisabled {
		t.Fatalf("status_list: %v", ch.ChannelInfo.MultiKeyStatusList)
	}
	if ch.ChannelInfo.MultiKeyDisabledReason[1] != "boom" {
		t.Fatalf("reason: %v", ch.ChannelInfo.MultiKeyDisabledReason)
	}
	if ch.ChannelInfo.MultiKeyDisabledTime[1] == 0 {
		t.Fatal("disabled_time not written")
	}
	if !abilityEnabled(t, s, 1) {
		t.Fatal("abilities must stay enabled while a key is alive")
	}
}

func TestApplyKeyStatusAllDeadThenRecover(t *testing.T) {
	s := newTestStore(t)
	seedMultiKeyChannel(t, s)

	for i := 0; i < 3; i++ {
		cs, allDead, err := s.ApplyKeyStatus(1, i, ChannelStatusAutoDisabled, "dead")
		if err != nil {
			t.Fatalf("disable %d: %v", i, err)
		}
		if i < 2 && allDead {
			t.Fatalf("premature allDead at %d", i)
		}
		if i == 2 {
			if !allDead || cs != ChannelStatusAutoDisabled {
				t.Fatalf("all dead: cs=%d allDead=%v", cs, allDead)
			}
		}
	}
	ch := reloadChannel(t, s, 1)
	if ch.Status != ChannelStatusAutoDisabled {
		t.Fatalf("channel status: %d", ch.Status)
	}
	var oi map[string]interface{}
	if err := json.Unmarshal([]byte(ch.OtherInfo), &oi); err != nil {
		t.Fatalf("other_info not JSON: %v", err)
	}
	if oi["status_reason"] != allKeysDisabledReason || oi["status_time"] == nil {
		t.Fatalf("other_info merge: %v", oi)
	}
	if abilityEnabled(t, s, 1) {
		t.Fatal("abilities must be disabled when all keys are dead")
	}

	// recovery: enable one key -> status back to 1, abilities on,
	// status_list entry removed (not set to 1)
	cs, allDead, err := s.ApplyKeyStatus(1, 0, ChannelStatusEnabled, "")
	if err != nil {
		t.Fatalf("enable: %v", err)
	}
	if allDead || cs != ChannelStatusEnabled {
		t.Fatalf("recover: cs=%d allDead=%v", cs, allDead)
	}
	ch = reloadChannel(t, s, 1)
	if ch.Status != ChannelStatusEnabled {
		t.Fatalf("recovered status: %d", ch.Status)
	}
	if _, ok := ch.ChannelInfo.MultiKeyStatusList[0]; ok {
		t.Fatalf("enabled idx must be removed from status_list: %v", ch.ChannelInfo.MultiKeyStatusList)
	}
	if _, ok := ch.ChannelInfo.MultiKeyDisabledReason[0]; ok {
		t.Fatal("reason not cleared for enabled idx")
	}
	if !abilityEnabled(t, s, 1) {
		t.Fatal("abilities must be re-enabled on recovery")
	}
}

func TestApplyKeyStatusManualDisable(t *testing.T) {
	s := newTestStore(t)
	seedMultiKeyChannel(t, s)

	if _, _, err := s.ApplyKeyStatus(1, 2, ChannelStatusManuallyDisabled, "operator"); err != nil {
		t.Fatalf("manual disable: %v", err)
	}
	ch := reloadChannel(t, s, 1)
	if ch.ChannelInfo.MultiKeyStatusList[2] != ChannelStatusManuallyDisabled {
		t.Fatalf("manual status: %v", ch.ChannelInfo.MultiKeyStatusList)
	}
	if got := ch.EnabledKeyIndexes(); len(got) != 2 {
		t.Fatalf("enabled indexes: %v", got)
	}
}

func TestApplyKeyStatusNonMultiKey(t *testing.T) {
	s := newTestStore(t)
	ch := Channel{Id: 9, Key: "solo", Status: ChannelStatusEnabled}
	if err := s.db.Create(&ch).Error; err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := s.db.Create(&Ability{Group: "g", Model: "m", ChannelId: 9, Enabled: true}).Error; err != nil {
		t.Fatalf("seed ability: %v", err)
	}

	cs, allDead, err := s.ApplyKeyStatus(9, 0, ChannelStatusAutoDisabled, "quota")
	if err != nil {
		t.Fatalf("disable: %v", err)
	}
	if cs != ChannelStatusAutoDisabled || !allDead {
		t.Fatalf("non-multi disable: cs=%d allDead=%v", cs, allDead)
	}
	ch2 := reloadChannel(t, s, 9)
	if ch2.Status != ChannelStatusAutoDisabled {
		t.Fatalf("status: %d", ch2.Status)
	}
	if !strings.Contains(ch2.OtherInfo, "quota") {
		t.Fatalf("other_info missing reason: %s", ch2.OtherInfo)
	}
	if abilityEnabled(t, s, 9) {
		t.Fatal("abilities must be disabled")
	}

	cs, _, err = s.ApplyKeyStatus(9, 0, ChannelStatusEnabled, "")
	if err != nil {
		t.Fatalf("enable: %v", err)
	}
	if cs != ChannelStatusEnabled || reloadChannel(t, s, 9).Status != ChannelStatusEnabled {
		t.Fatalf("non-multi enable failed: cs=%d", cs)
	}
	if !abilityEnabled(t, s, 9) {
		t.Fatal("abilities must be re-enabled")
	}
}

func TestApplyKeyStatusIndexOutOfRange(t *testing.T) {
	s := newTestStore(t)
	seedMultiKeyChannel(t, s)
	if _, _, err := s.ApplyKeyStatus(1, 5, ChannelStatusAutoDisabled, "x"); err == nil {
		t.Fatal("expected out-of-range error")
	}
	// non-multi-key channel rejects idx != 0
	ch := Channel{Id: 10, Key: "solo", Status: ChannelStatusEnabled}
	if err := s.db.Create(&ch).Error; err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, _, err := s.ApplyKeyStatus(10, 1, ChannelStatusAutoDisabled, "x"); err == nil {
		t.Fatal("expected non-multi idx error")
	}
}

// ---- 评审修复回归测试（P1-5：PostgreSQL 反引号）----

// TestAbilitiesBehavior sqlite 下行为不变：按 (group, model, enabled) 过滤，
// priority desc, weight desc 排序。
func TestAbilitiesBehavior(t *testing.T) {
	s := newTestStore(t)
	p100, p50 := int64(100), int64(50)
	rows := []Ability{
		{Group: "default", Model: "gpt-x", ChannelId: 1, Enabled: true, Priority: &p50, Weight: 10},
		{Group: "default", Model: "gpt-x", ChannelId: 2, Enabled: true, Priority: &p100, Weight: 1},
		{Group: "default", Model: "gpt-x", ChannelId: 3, Enabled: true, Priority: &p100, Weight: 9},
		{Group: "default", Model: "gpt-x", ChannelId: 4, Enabled: false, Priority: &p100, Weight: 99}, // 禁用，过滤
		{Group: "default", Model: "gpt-y", ChannelId: 5, Enabled: true, Priority: &p100},              // 模型不符，过滤
		{Group: "vip", Model: "gpt-x", ChannelId: 6, Enabled: true, Priority: &p100},                  // 分组不符，过滤
	}
	for _, ab := range rows {
		if err := s.db.Create(&ab).Error; err != nil {
			t.Fatalf("seed ability: %v", err)
		}
	}

	got, err := s.Abilities("default", "gpt-x")
	if err != nil {
		t.Fatalf("Abilities: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("len = %d, want 3: %+v", len(got), got)
	}
	// priority desc, weight desc：2(100,1) → 3(100,9)？weight desc 在同档内：
	// (100,9) 应先于 (100,1)；然后 (50,10)
	wantOrder := []int{3, 2, 1}
	for i, cid := range wantOrder {
		if got[i].ChannelId != cid {
			t.Fatalf("order[%d] = %d, want %d: %+v", i, got[i].ChannelId, cid, got)
		}
	}
}

// TestAbilitiesSQLNoBackticks 用 DryRun 分别生成 sqlite / postgres 方言的
// SQL：保留字 group 必须由各 dialect 正确加引号；pg 方言下绝不允许出现
// 反引号（那是 MySQL 引号风格，pg 下是语法错误，P1-5）。
func TestAbilitiesSQLNoBackticks(t *testing.T) {
	dialectors := map[string]gorm.Dialector{
		"sqlite":   sqlite.Open("file::memory:"),
		"postgres": postgres.New(postgres.Config{DSN: "host=stub", PreferSimpleProtocol: true}),
	}
	for name, dia := range dialectors {
		db, err := gorm.Open(dia, &gorm.Config{DryRun: true, DisableAutomaticPing: true})
		if err != nil {
			t.Fatalf("%s open: %v", name, err)
		}
		var list []Ability
		stmt := db.
			Where(map[string]any{"group": "default", "model": "gpt-x", "enabled": true}).
			Order("priority desc, weight desc").
			Find(&list).Statement
		sql := stmt.SQL.String()
		quoted := strings.Contains(sql, `"group"`) || strings.Contains(sql, "`group`")
		if !quoted {
			t.Fatalf("%s SQL should quote reserved word group via dialect: %s", name, sql)
		}
		if name == "postgres" {
			if strings.Contains(sql, "`") {
				t.Fatalf("postgres SQL contains MySQL-style backticks: %s", sql)
			}
			if !strings.Contains(sql, `"group"`) {
				t.Fatalf("postgres SQL should double-quote group: %s", sql)
			}
		}
		if !strings.Contains(sql, "priority desc") || !strings.Contains(sql, "weight desc") {
			t.Fatalf("%s SQL lost ordering: %s", name, sql)
		}
	}
}

// UpsertOption 命名空间保护：非 keypool. 前缀的键必须被拒绝（防误写
// new-api 原生 options）；合法键正常 upsert。
func TestUpsertOptionPrefixGuard(t *testing.T) {
	s := newTestStore(t)

	if err := s.UpsertOption("AutomaticDisableChannelEnabled", "false"); err == nil {
		t.Fatalf("expected error for non-keypool key")
	}
	// 被拒的键不得落库
	var n int64
	s.db.Model(&Option{}).Where("key = ?", "AutomaticDisableChannelEnabled").Count(&n)
	if n != 0 {
		t.Fatalf("rejected key was persisted")
	}

	if err := s.UpsertOption(OptBalancePrefix+"7", `{"mode":"usage"}`); err != nil {
		t.Fatalf("upsert keypool key: %v", err)
	}
	if err := s.UpsertOption(OptBalancePrefix+"7", `{"mode":"auto"}`); err != nil {
		t.Fatalf("re-upsert: %v", err)
	}
	var opt Option
	if err := s.db.Where("key = ?", OptBalancePrefix+"7").First(&opt).Error; err != nil {
		t.Fatalf("read back: %v", err)
	}
	if !strings.Contains(opt.Value, `"mode":"auto"`) {
		t.Fatalf("value = %s, want updated", opt.Value)
	}
}

// Meta 投影：nil 指针字段安全、models 切分、多 key 缺省 mode。
// 投影只含 Web 可配置字段，key_count/epoch 等运行态字段不在其中。
func TestChannelMeta(t *testing.T) {
	ch := &Channel{
		Id: 9, Type: 1, Status: 1, Name: "n", Key: "a\nb\nc",
		Models: "gpt-4o, ,gpt-4o-mini ", Group: "g",
		ChannelInfo: ChannelInfo{IsMultiKey: true, MultiKeySize: 3},
	}
	m := ch.Meta()
	if m.ID != 9 || m.Priority != 0 || m.Weight != 0 || !m.AutoBan {
		t.Fatalf("meta = %+v", m)
	}
	if m.MultiKeyMode != "polling" { // 多 key 且未设 mode → polling
		t.Fatalf("mode = %q", m.MultiKeyMode)
	}
	if len(m.Models) != 2 || m.Models[1] != "gpt-4o-mini" {
		t.Fatalf("meta = %+v", m)
	}

	single := &Channel{Id: 1, Key: "sk-x", ChannelInfo: ChannelInfo{IsMultiKey: false}}
	if sm := single.Meta(); sm.MultiKey || sm.MultiKeyMode != "" {
		t.Fatalf("single meta = %+v", sm)
	}
}

// Meta 投影的零值剔除：new-api 把 setting/settings 按类型化 struct 全量落库，
// 投影必须剔除默认零值项（false/""/0/空数组），只透出实际配置；全零则字段省略。
// param_override 是用户手填 JSON，显式零值有语义，原样保留。
func TestChannelMetaPruneZeroEntries(t *testing.T) {
	setting := `{"force_format":false,"proxy":"http://127.0.0.1:7890","system_prompt":"","thinking_to_content":false}`
	settings := `{"allow_service_tier":false,"disable_store":false,"upstream_model_update_ignored_models":[],"upstream_model_update_last_check_time":0,"azure_api_version":"2024-08-01-preview"}`
	other := `{"region":"us","empty_list":[],"note":""}`
	paramOverride := `{"temperature":0,"stream":false,"max_tokens":512}`

	ch := &Channel{
		Id: 4, Key: "sk-x",
		Setting:       &setting,
		OtherSettings: settings,
		Other:         other,
		ParamOverride: &paramOverride,
	}
	m := ch.Meta()

	if len(m.Setting) != 1 || m.Setting["proxy"] != "http://127.0.0.1:7890" {
		t.Fatalf("setting = %v, want only proxy kept", m.Setting)
	}
	if len(m.Settings) != 1 || m.Settings["azure_api_version"] != "2024-08-01-preview" {
		t.Fatalf("settings = %v, want only azure_api_version kept", m.Settings)
	}
	if len(m.Other) != 1 || m.Other["region"] != "us" {
		t.Fatalf("other = %v, want only region kept", m.Other)
	}
	if len(m.ParamOverride) != 3 { // 显式 0/false 必须保留
		t.Fatalf("param_override = %v, want all 3 entries kept verbatim", m.ParamOverride)
	}

	// 全零值 → 字段整体省略（nil）
	zeroSetting := `{"force_format":false,"proxy":""}`
	zc := &Channel{Id: 5, Key: "sk-x", Setting: &zeroSetting}
	if zm := zc.Meta(); zm.Setting != nil {
		t.Fatalf("all-zero setting should be omitted, got %v", zm.Setting)
	}
}

// ApplyKeyStatus 在 sqlite（无行锁方言）下行为不变——rowLockSupported 跳过分支。
func TestRowLockDialectGate(t *testing.T) {
	s := newTestStore(t)
	if s.rowLockSupported() {
		t.Fatalf("sqlite must not claim row-lock support")
	}
}
