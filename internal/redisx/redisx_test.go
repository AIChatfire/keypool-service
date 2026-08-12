package redisx

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestKeyConstructors(t *testing.T) {
	cases := []struct {
		got, want string
	}{
		{CursorKey(42), "keypool:cursor:42"},
		{UsageKey(42), "keypool:usage:42"},
		{UsageMetaKey(42), "keypool:usage_meta:42"},
		{LockKey(42), "keypool:lock:42"},
		{IdemKey("abc-123"), "keypool:idem:abc-123"},
		{EventsKey, "keypool:events"},
	}
	for i, c := range cases {
		if c.got != c.want {
			t.Errorf("case %d: got %q, want %q", i, c.got, c.want)
		}
	}
}

// TestSelectKeyArgv 对照 SPEC §3.1 断言 KEYS/ARGV 序列化契约。
func TestSelectKeyArgv(t *testing.T) {
	keys, args, err := selectKeyArgv(7, SelectKeyArgs{
		Mode:          "usage",
		Candidates:    []int{1, 3, 5},
		Est:           128.5,
		DecayInterval: 3600,
		DecayFactor:   0.5,
		Now:           1735689600,
		JitterPct:     0.05,
	})
	if err != nil {
		t.Fatalf("selectKeyArgv: %v", err)
	}
	wantKeys := []string{"keypool:cursor:7", "keypool:usage:7", "keypool:usage_meta:7"}
	if len(keys) != len(wantKeys) {
		t.Fatalf("keys len = %d, want %d", len(keys), len(wantKeys))
	}
	for i := range wantKeys {
		if keys[i] != wantKeys[i] {
			t.Errorf("KEYS[%d] = %q, want %q", i, keys[i], wantKeys[i])
		}
	}
	wantArgs := []string{
		"usage",      // ARGV[1] mode
		"[1,3,5]",    // ARGV[2] candidates JSON
		"128.5",      // ARGV[3] est
		"3600",       // ARGV[4] decay_interval_sec
		"0.5",        // ARGV[5] decay_factor
		"1735689600", // ARGV[6] now_ts
		"0.05",       // ARGV[7] jitter_pct
	}
	if len(args) != len(wantArgs) {
		t.Fatalf("args len = %d, want %d", len(args), len(wantArgs))
	}
	for i := range wantArgs {
		s, ok := args[i].(string)
		if !ok {
			t.Errorf("ARGV[%d] not a string: %T", i+1, args[i])
			continue
		}
		if s != wantArgs[i] {
			t.Errorf("ARGV[%d] = %q, want %q", i+1, s, wantArgs[i])
		}
	}
}

func TestSelectKeyArgvEmptyCandidates(t *testing.T) {
	_, args, err := selectKeyArgv(1, SelectKeyArgs{Mode: "polling", Candidates: []int{}})
	if err != nil {
		t.Fatalf("selectKeyArgv: %v", err)
	}
	// 空数组序列化为 "[]"，Lua cjson.decode 后 #candidates==0 → return -1
	if args[1] != "[]" {
		t.Errorf("empty candidates ARGV[2] = %q, want %q", args[1], "[]")
	}
}

// TestSelectKeyLuaEmbedded 校验 Lua 源码已内嵌且包含 SPEC §3.1 的关键逻辑。
func TestSelectKeyLuaEmbedded(t *testing.T) {
	if len(selectKeyLua) == 0 {
		t.Fatal("select_key.lua 未内嵌")
	}
	for _, frag := range []string{
		"cjson.decode(ARGV[2])",       // candidates JSON
		`redis.call("INCR", KEYS[1])`, // polling 推游标
		`redis.call("GET", KEYS[1])`,  // peek 只读
		"math.random",                 // random / jitter
		"HINCRBYFLOAT",                // usage 预扣 est
		"last_decay",                  // 衰减元数据
		"return -1",                   // 候选为空
	} {
		if !strings.Contains(selectKeyLua, frag) {
			t.Errorf("Lua 源码缺少片段 %q", frag)
		}
	}
}

// ---- 评审修复回归测试（P1-1：Redis 降级 panic）----

// TestNilClientNoPanic：nil *Client（main 的 degraded 模式）与零值
// &Client{}（rdb 未初始化）调用所有导出方法必须返回 ErrDegraded，
// 不得 nil-receiver panic。
func TestNilClientNoPanic(t *testing.T) {
	clients := map[string]*Client{"nil": nil, "zero": {}}
	ctx := context.Background()

	for name, c := range clients {
		if _, err := c.IdemSet(ctx, "k"); !errors.Is(err, ErrDegraded) {
			t.Errorf("%s IdemSet err = %v, want ErrDegraded", name, err)
		}
		if err := c.IdemDel(ctx, "k"); !errors.Is(err, ErrDegraded) {
			t.Errorf("%s IdemDel err = %v, want ErrDegraded", name, err)
		}
		if _, _, err := c.Lock(ctx, 1); !errors.Is(err, ErrDegraded) {
			t.Errorf("%s Lock err = %v, want ErrDegraded", name, err)
		}
		if err := c.Unlock(ctx, 1, "tok"); !errors.Is(err, ErrDegraded) {
			t.Errorf("%s Unlock err = %v, want ErrDegraded", name, err)
		}
		if err := c.CursorReset(ctx, 1); !errors.Is(err, ErrDegraded) {
			t.Errorf("%s CursorReset err = %v, want ErrDegraded", name, err)
		}
		if _, err := c.Publish(ctx, map[string]any{"type": "x"}); !errors.Is(err, ErrDegraded) {
			t.Errorf("%s Publish err = %v, want ErrDegraded", name, err)
		}
		if err := c.UsageIncr(ctx, 1, 0, 1.5); !errors.Is(err, ErrDegraded) {
			t.Errorf("%s UsageIncr err = %v, want ErrDegraded", name, err)
		}
		if _, _, err := c.UsageAll(ctx, 1); !errors.Is(err, ErrDegraded) {
			t.Errorf("%s UsageAll err = %v, want ErrDegraded", name, err)
		}
		if err := c.UsageReset(ctx, 1, 0, 0); !errors.Is(err, ErrDegraded) {
			t.Errorf("%s UsageReset err = %v, want ErrDegraded", name, err)
		}
		if _, err := c.SelectKey(ctx, "polling", 1, []int{0}, 0, 3600, 0.5, 0, 0.05); !errors.Is(err, ErrDegraded) {
			t.Errorf("%s SelectKey err = %v, want ErrDegraded", name, err)
		}
		if err := c.LeaseSet(ctx, "l1", 100); !errors.Is(err, ErrDegraded) {
			t.Errorf("%s LeaseSet err = %v, want ErrDegraded", name, err)
		}
		if _, _, err := c.LeaseTake(ctx, "l1"); !errors.Is(err, ErrDegraded) {
			t.Errorf("%s LeaseTake err = %v, want ErrDegraded", name, err)
		}
		if raw := c.Raw(); raw != nil {
			t.Errorf("%s Raw() = %v, want nil", name, raw)
		}
		if err := c.Close(); !errors.Is(err, ErrDegraded) {
			t.Errorf("%s Close err = %v, want ErrDegraded", name, err)
		}
	}
}

// TestLeaseKeyConstructor 校验租约键契约（P1-4）。
func TestLeaseKeyConstructor(t *testing.T) {
	if got := LeaseKey("abc"); got != "keypool:lease:abc" {
		t.Fatalf("LeaseKey = %q", got)
	}
}
