package store

import (
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestGetKeysJSONArray(t *testing.T) {
	ch := &Channel{Key: `["sk-a","sk-b","sk-c"]`}
	got := ch.GetKeys()
	want := []string{"sk-a", "sk-b", "sk-c"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("json array: got %v want %v", got, want)
	}
}

func TestGetKeysNewlineSplit(t *testing.T) {
	ch := &Channel{Key: "sk-a\nsk-b\nsk-c"}
	got := ch.GetKeys()
	want := []string{"sk-a", "sk-b", "sk-c"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("newline split: got %v want %v", got, want)
	}
}

func TestGetKeysTrailingNewlineAndSingle(t *testing.T) {
	// trailing newline must be trimmed, not produce an empty element
	ch := &Channel{Key: "sk-a\nsk-b\n"}
	if got := ch.GetKeys(); !reflect.DeepEqual(got, []string{"sk-a", "sk-b"}) {
		t.Fatalf("trailing newline: got %v", got)
	}
	// single key without newline
	ch = &Channel{Key: "sk-only"}
	if got := ch.GetKeys(); !reflect.DeepEqual(got, []string{"sk-only"}) {
		t.Fatalf("single key: got %v", got)
	}
	// empty key -> empty slice
	ch = &Channel{Key: ""}
	if got := ch.GetKeys(); len(got) != 0 {
		t.Fatalf("empty key: got %v", got)
	}
	// P2-1：'[' 开头但 JSON 解析失败 → 返回空切片（对齐 new-api），
	// 不再回退 '\n' 切分
	ch = &Channel{Key: "[bad\nsk-x"}
	if got := ch.GetKeys(); len(got) != 0 {
		t.Fatalf("invalid json: got %v, want empty", got)
	}
	ch = &Channel{Key: "[unterminated"}
	if got := ch.GetKeys(); len(got) != 0 {
		t.Fatalf("unterminated json: got %v, want empty", got)
	}
}

func TestParseCodeRanges(t *testing.T) {
	got, err := ParseCodeRanges("401,500-503")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []CodeRange{{401, 401}, {500, 503}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}

	// matching semantics
	for _, code := range []int{401, 500, 502, 503} {
		if !MatchCodeRange(got, code) {
			t.Fatalf("expected %d to match", code)
		}
	}
	for _, code := range []int{400, 402, 499, 504} {
		if MatchCodeRange(got, code) {
			t.Fatalf("expected %d not to match", code)
		}
	}

	// empty input -> nil
	if r, err := ParseCodeRanges("  "); err != nil || r != nil {
		t.Fatalf("empty: got %v err %v", r, err)
	}
	// malformed inputs
	for _, bad := range []string{"abc", "50x-503", "503-500", "-"} {
		if _, err := ParseCodeRanges(bad); err == nil {
			t.Fatalf("expected error for %q", bad)
		}
	}
}

func TestEnabledKeyIndexesDefaultEnabled(t *testing.T) {
	// missing indexes in status_list are enabled (SPEC §2.2)
	ch := &Channel{Key: "k0\nk1\nk2"}
	got := ch.EnabledKeyIndexes()
	if !reflect.DeepEqual(got, []int{0, 1, 2}) {
		t.Fatalf("all default enabled: got %v", got)
	}

	ch.ChannelInfo = ChannelInfo{IsMultiKey: true, MultiKeyStatusList: map[int]int{1: ChannelStatusAutoDisabled}}
	got = ch.EnabledKeyIndexes()
	if !reflect.DeepEqual(got, []int{0, 2}) {
		t.Fatalf("idx 1 disabled: got %v", got)
	}

	// explicit enabled entry stays enabled
	ch.ChannelInfo.MultiKeyStatusList[0] = ChannelStatusEnabled
	ch.ChannelInfo.MultiKeyStatusList[2] = ChannelStatusManuallyDisabled
	got = ch.EnabledKeyIndexes()
	if !reflect.DeepEqual(got, []int{0}) {
		t.Fatalf("only idx 0 enabled: got %v", got)
	}
}

func TestEpochStable(t *testing.T) {
	ch := &Channel{Key: "sk-a\nsk-b"}
	want := hex.EncodeToString(func() []byte { s := sha1.Sum([]byte("sk-a\x00sk-b")); return s[:] }())[:8]
	if got := ch.Epoch(); got != want {
		t.Fatalf("epoch: got %q want %q", got, want)
	}
	if len(ch.Epoch()) != 8 {
		t.Fatalf("epoch length: got %d", len(ch.Epoch()))
	}
	// stable across calls
	if ch.Epoch() != ch.Epoch() {
		t.Fatal("epoch not stable")
	}
	// same key set via JSON form -> same epoch
	ch2 := &Channel{Key: `["sk-a","sk-b"]`}
	if ch.Epoch() != ch2.Epoch() {
		t.Fatal("epoch differs for identical key sets")
	}
	// different key set -> different epoch
	ch3 := &Channel{Key: "sk-a\nsk-c"}
	if ch.Epoch() == ch3.Epoch() {
		t.Fatal("epoch collision for different key sets")
	}
}

func TestChannelInfoValueScanRoundtrip(t *testing.T) {
	in := ChannelInfo{
		IsMultiKey:             true,
		MultiKeySize:           2,
		MultiKeyStatusList:     map[int]int{0: 3},
		MultiKeyDisabledReason: map[int]string{0: "boom"},
		MultiKeyDisabledTime:   map[int]int64{0: 1735689600},
		MultiKeyPollingIndex:   1,
		MultiKeyMode:           "polling",
	}
	v, err := in.Value()
	if err != nil {
		t.Fatalf("Value: %v", err)
	}
	var out ChannelInfo
	if err := out.Scan(v); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("roundtrip mismatch: got %+v want %+v", out, in)
	}

	// nil / empty scan -> zero value
	out = ChannelInfo{IsMultiKey: true}
	if err := out.Scan(nil); err != nil || !reflect.DeepEqual(out, ChannelInfo{}) {
		t.Fatalf("Scan(nil): %v %+v", err, out)
	}
	if err := out.Scan(""); err != nil || !reflect.DeepEqual(out, ChannelInfo{}) {
		t.Fatalf("Scan(empty): %v %+v", err, out)
	}

	// JSON field names must match SPEC §2.2 byte-for-byte
	raw := v.(string)
	for _, name := range []string{
		`"is_multi_key":true`, `"multi_key_size":2`, `"multi_key_status_list":{"0":3}`,
		`"multi_key_polling_index":1`, `"multi_key_mode":"polling"`,
	} {
		if !strings.Contains(raw, name) {
			t.Fatalf("channel_info JSON missing %s: %s", name, raw)
		}
	}
}

func TestParseSettingsFromOptions(t *testing.T) {
	opts := map[string]string{
		OptAutoDisableEnabled: "true",
		OptAutoEnableEnabled:  "false",
		OptDisableStatusCodes: "401,500-503",
		OptDisableKeywords:    "Quota Exceeded\nBad Key\n",
		"keypool.balance.7":   `{"mode":"usage","metric":"tokens","decay_interval":60,"decay_factor":0.5,"catchup":1}`,
		"keypool.rotation.7":  `{"band_seconds":300,"active_count":2,"overlap_bands":1,"order":"shuffle"}`,
	}
	st := parseSettings(opts, nil)
	if !st.AutoDisableOn || st.AutoEnableOn {
		t.Fatalf("bools: %+v", st)
	}
	if !reflect.DeepEqual(st.DisableCodeRanges, []CodeRange{{401, 401}, {500, 503}}) {
		t.Fatalf("ranges: %v", st.DisableCodeRanges)
	}
	if !reflect.DeepEqual(st.DisableKeywords, []string{"quota exceeded", "bad key"}) {
		t.Fatalf("keywords not lowercased: %v", st.DisableKeywords)
	}
	if st.Balance[7].Mode != "usage" || st.Balance[7].DecayFactor != 0.5 {
		t.Fatalf("balance: %+v", st.Balance[7])
	}
	if st.Rotation[7].BandSeconds != 300 || st.Rotation[7].Order != "shuffle" {
		t.Fatalf("rotation: %+v", st.Rotation[7])
	}
}

func TestParseSettingsDefaultsAndFallback(t *testing.T) {
	// defaults: 401 range + 7 default keywords
	st := parseSettings(map[string]string{}, nil)
	if !reflect.DeepEqual(st.DisableCodeRanges, []CodeRange{{401, 401}}) {
		t.Fatalf("default ranges: %v", st.DisableCodeRanges)
	}
	if len(st.DisableKeywords) != len(defaultDisableKeywords) {
		t.Fatalf("default keywords: %d", len(st.DisableKeywords))
	}

	// parse failure keeps the old value
	old := &Settings{
		DisableCodeRanges: []CodeRange{{429, 429}},
		Balance:           map[int]BalanceCfg{3: {Mode: "auto"}},
		Rotation:          map[int]RotationCfg{4: {BandSeconds: 60}},
	}
	bad := map[string]string{
		OptDisableStatusCodes: "not-a-range",
		"keypool.balance.3":   `{invalid`,
		"keypool.rotation.4":  `{invalid`,
	}
	st = parseSettings(bad, old)
	if !reflect.DeepEqual(st.DisableCodeRanges, []CodeRange{{429, 429}}) {
		t.Fatalf("bad ranges should keep old: %v", st.DisableCodeRanges)
	}
	if st.Balance[3].Mode != "auto" {
		t.Fatalf("bad balance should keep old: %+v", st.Balance)
	}
	if st.Rotation[4].BandSeconds != 60 {
		t.Fatalf("bad rotation should keep old: %+v", st.Rotation)
	}
}

func TestSettingsSnapshotAtomic(t *testing.T) {
	s := &Store{}
	if s.GetSettings() != nil {
		t.Fatal("expected nil snapshot before update")
	}
	st := &Settings{AutoDisableOn: true}
	s.UpdateSettings(st)
	if got := s.GetSettings(); got != st {
		t.Fatalf("snapshot mismatch: %+v", got)
	}
}

func TestMergeOtherInfo(t *testing.T) {
	got := mergeOtherInfo(`{"foo":1,"status_reason":"old"}`, map[string]interface{}{
		"status_reason": allKeysDisabledReason,
		"status_time":   int64(100),
	})
	var obj map[string]interface{}
	if err := json.Unmarshal([]byte(got), &obj); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if obj["foo"].(float64) != 1 {
		t.Fatalf("existing key lost: %v", obj)
	}
	if obj["status_reason"] != allKeysDisabledReason {
		t.Fatalf("reason not merged: %v", obj)
	}
	// empty existing
	got = mergeOtherInfo("", map[string]interface{}{"a": "b"})
	if got != `{"a":"b"}` {
		t.Fatalf("empty merge: %s", got)
	}
}
