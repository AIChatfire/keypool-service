package classifier

import (
	"testing"

	"keypool/internal/store"
)

func settingsOn() *store.Settings {
	return &store.Settings{
		AutoDisableOn:     true,
		AutoEnableOn:      true,
		DisableCodeRanges: []store.CodeRange{{Start: 401, End: 401}, {Start: 500, End: 503}},
		DisableKeywords:   []string{"permission denied", "you exceeded your current quota"},
	}
}

func TestShouldDisable(t *testing.T) {
	cases := []struct {
		name       string
		st         *store.Settings
		autoBan    bool
		statusCode int
		errMsg     string
		want       bool
	}{
		{name: "nil settings = all off", st: nil, autoBan: true, statusCode: 401, want: false},
		{name: "global switch off", st: &store.Settings{AutoDisableOn: false, DisableCodeRanges: []store.CodeRange{{Start: 401, End: 401}}}, autoBan: true, statusCode: 401, want: false},
		{name: "autoBan off", st: settingsOn(), autoBan: false, statusCode: 401, want: false},
		{name: "401 hits code range", st: settingsOn(), autoBan: true, statusCode: 401, want: true},
		{name: "502 hits ranged interval", st: settingsOn(), autoBan: true, statusCode: 502, want: true},
		{name: "keyword hit case-insensitive", st: settingsOn(), autoBan: true, statusCode: 400, errMsg: "OpenAI: Permission Denied by org", want: true},
		{name: "no match", st: settingsOn(), autoBan: true, statusCode: 429, errMsg: "rate limit exceeded", want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ShouldDisable(tc.st, tc.autoBan, tc.statusCode, tc.errMsg); got != tc.want {
				t.Fatalf("ShouldDisable() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestShouldEnable(t *testing.T) {
	cases := []struct {
		name       string
		st         *store.Settings
		prevStatus int
		want       bool
	}{
		{name: "nil settings", st: nil, prevStatus: 3, want: false},
		{name: "switch off", st: &store.Settings{AutoEnableOn: false}, prevStatus: 3, want: false},
		{name: "prev auto disabled", st: &store.Settings{AutoEnableOn: true}, prevStatus: store.ChannelStatusAutoDisabled, want: true},
		{name: "prev manually disabled", st: &store.Settings{AutoEnableOn: true}, prevStatus: store.ChannelStatusManuallyDisabled, want: false},
		{name: "prev enabled", st: &store.Settings{AutoEnableOn: true}, prevStatus: store.ChannelStatusEnabled, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ShouldEnable(tc.st, tc.prevStatus); got != tc.want {
				t.Fatalf("ShouldEnable() = %v, want %v", got, tc.want)
			}
		})
	}
}
