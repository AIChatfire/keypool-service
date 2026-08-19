package selector

import (
	"context"
	"errors"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"gorm.io/gorm"

	"keypool/internal/redisx"
	"keypool/internal/store"
)

func TestSplitBatches(t *testing.T) {
	got := splitBatches([]int{0, 1, 2, 3, 4, 5, 6}, 3)
	want := [][]int{{0, 1, 2}, {3, 4, 5}, {6}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("splitBatches = %v, want %v", got, want)
	}
	// size<=0 → 整体一批
	if got := splitBatches([]int{1, 2}, 0); !reflect.DeepEqual(got, [][]int{{1, 2}}) {
		t.Fatalf("splitBatches size=0 = %v", got)
	}
	if got := splitBatches(nil, 3); got != nil {
		t.Fatalf("splitBatches nil = %v", got)
	}
}

func TestDeterministicShuffle(t *testing.T) {
	in := []int{0, 1, 2, 3, 4, 5, 6, 7}
	a := deterministicShuffle(42, in)
	b := deterministicShuffle(42, in)
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("same cid shuffle not deterministic: %v vs %v", a, b)
	}
	// 不修改入参
	if !reflect.DeepEqual(in, []int{0, 1, 2, 3, 4, 5, 6, 7}) {
		t.Fatalf("input mutated: %v", in)
	}
	// 是原序列的排列
	sorted := append([]int(nil), a...)
	sortInts(sorted)
	if !reflect.DeepEqual(sorted, in) {
		t.Fatalf("shuffle is not a permutation: %v", a)
	}
	// 不同 cid 大概率不同：cid 1..10 中至少有一个与 cid 42 不同
	diff := false
	for cid := 1; cid <= 10; cid++ {
		if !reflect.DeepEqual(deterministicShuffle(cid, in), a) {
			diff = true
			break
		}
	}
	if !diff {
		t.Fatal("different cids produced identical shuffle for 1..10")
	}
}

func sortInts(s []int) {
	for i := 0; i < len(s); i++ {
		for j := i + 1; j < len(s); j++ {
			if s[j] < s[i] {
				s[i], s[j] = s[j], s[i]
			}
		}
	}
}

func TestBuildBatchesOrder(t *testing.T) {
	cfg := store.RotationCfg{BandSeconds: 60, ActiveCount: 2, Order: "index"}
	got := buildBatches(cfg, 1, 5)
	want := [][]int{{0, 1}, {2, 3}, {4}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("index order batches = %v, want %v", got, want)
	}
	cfg.Order = "shuffle"
	sh := buildBatches(cfg, 7, 5)
	if !reflect.DeepEqual(sh, buildBatches(cfg, 7, 5)) {
		t.Fatalf("shuffle batches not deterministic: %v", sh)
	}
}

func TestBandRotationAdvance(t *testing.T) {
	batches := [][]int{{0, 1}, {2, 3}}
	enabled := map[int]bool{0: true, 1: true, 2: true, 3: true}
	// band 偶数 → 批次0，奇数 → 批次1：随时间前进轮换
	got0 := bandCandidates(batches, enabled, 100, 0)
	got1 := bandCandidates(batches, enabled, 101, 0)
	if !reflect.DeepEqual(got0, []int{0, 1}) || !reflect.DeepEqual(got1, []int{2, 3}) {
		t.Fatalf("band rotation wrong: band100=%v band101=%v", got0, got1)
	}
}

func TestBandOverlap(t *testing.T) {
	batches := [][]int{{0}, {1}, {2}}
	enabled := map[int]bool{0: true, 1: true, 2: true}
	// OverlapBands=1：并入前一个 band 的批次
	got := bandCandidates(batches, enabled, 1, 1) // band1={1} + band0={0}
	if !reflect.DeepEqual(got, []int{0, 1}) {
		t.Fatalf("overlap candidates = %v, want [0 1]", got)
	}
	// 环绕：band=0，前一个 band=-1 → 批次2
	got = bandCandidates(batches, enabled, 0, 1)
	if !reflect.DeepEqual(got, []int{0, 2}) {
		t.Fatalf("overlap wrap candidates = %v, want [0 2]", got)
	}
	// 与启用集取交
	got = bandCandidates(batches, enabled, 1, 1) // same but enabled filtered
	delete(enabled, 0)
	got = bandCandidates(batches, enabled, 1, 1)
	if !reflect.DeepEqual(got, []int{1}) {
		t.Fatalf("overlap∩enabled = %v, want [1]", got)
	}
}

func TestLookahead(t *testing.T) {
	batches := [][]int{{0}, {1}, {2}}
	// 当前 band=0 的 key 0 已禁用 → look-ahead 到 band1
	enabled := map[int]bool{1: true, 2: true}
	cands, steps := lookahead(batches, enabled, 0)
	if steps != 1 || !reflect.DeepEqual(cands, []int{1}) {
		t.Fatalf("lookahead = %v steps=%d, want [1] steps=1", cands, steps)
	}
	// 全部禁用 → 空
	cands, steps = lookahead(batches, map[int]bool{}, 0)
	if cands != nil || steps != 0 {
		t.Fatalf("lookahead all dead = %v steps=%d", cands, steps)
	}
	// 跨过多个空批次
	enabled = map[int]bool{2: true}
	cands, steps = lookahead(batches, enabled, 0)
	if steps != 2 || !reflect.DeepEqual(cands, []int{2}) {
		t.Fatalf("lookahead skip = %v steps=%d, want [2] steps=2", cands, steps)
	}
}

func TestPriorityTiersAndRetry(t *testing.T) {
	p := func(v int64) *int64 { return &v }
	abs := []store.Ability{
		{ChannelId: 1, Priority: p(100), Weight: 5},
		{ChannelId: 2, Priority: p(100), Weight: 0},
		{ChannelId: 3, Priority: p(50), Weight: 0},
		{ChannelId: 4, Priority: nil, Weight: 0}, // nil → 0，最低档
	}
	tiers := priorityTiers(abs)
	if len(tiers) != 3 {
		t.Fatalf("tiers = %d, want 3", len(tiers))
	}
	if len(tiers[0]) != 2 || tiers[0][0].ChannelId != 1 {
		t.Fatalf("top tier wrong: %+v", tiers[0])
	}
	// retry 越界取最低档
	lowest := tierForRetry(tiers, 99)
	if len(lowest) != 1 || lowest[0].ChannelId != 4 {
		t.Fatalf("retry out of range should pick lowest tier, got %+v", lowest)
	}
	if tierForRetry(tiers, 0)[0].ChannelId != 1 {
		t.Fatal("retry=0 should pick top tier")
	}
	if tierForRetry(tiers, -1)[0].ChannelId != 1 {
		t.Fatal("retry<0 should pick top tier")
	}
}

func TestPickWeightedIndex(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	counts := map[int]int{}
	for i := 0; i < 10000; i++ {
		counts[pickWeightedIndex([]int{10, 30}, r)]++
	}
	// weight+10 语义已由 tierWeights 保证；此处 10:30=1:3
	if counts[1] < 6000 || counts[1] > 9000 {
		t.Fatalf("weighted distribution off: %v", counts)
	}
	if got := pickWeightedIndex([]int{0, 0, 0}, r); got < 0 || got > 2 {
		t.Fatalf("zero weights fallback wrong: %d", got)
	}
}

// ---- Select 集成路径（fake 窄接口）----

type fakeChannels struct {
	channels  map[int]*store.Channel
	getErr    error // 非 nil 时优先返回（P1-6：模拟 DB 故障）
	abilities []store.Ability
	abErr     error
}

func (f *fakeChannels) GetChannel(id int) (*store.Channel, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	if ch, ok := f.channels[id]; ok {
		return ch, nil
	}
	return nil, gorm.ErrRecordNotFound
}
func (f *fakeChannels) Abilities(group, model string) ([]store.Ability, error) {
	return f.abilities, f.abErr
}

type fakeKeys struct {
	mode       string
	candidates []int
	ret        int
	err        error
	// 租约能力（P1-4）：实现 leaser 接口后 Selector 启用租约写入
	leases    map[string]float64
	leaseErr  error
	leaseSets int
}

func (f *fakeKeys) SelectKey(ctx context.Context, mode string, cid int, candidates []int, est, di, df float64, now int64, j float64) (int, error) {
	f.mode = mode
	f.candidates = append([]int(nil), candidates...)
	return f.ret, f.err
}

// LeaseSet 实现 leaser。leases 为 nil 时只计数不记录（不关心租约值的
// 旧用例无需初始化 map）。
func (f *fakeKeys) LeaseSet(ctx context.Context, leaseID string, est float64) error {
	f.leaseSets++
	if f.leaseErr != nil {
		return f.leaseErr
	}
	if f.leases != nil {
		f.leases[leaseID] = est
	}
	return nil
}

type fakeSP struct{ st *store.Settings }

func (f fakeSP) Get() *store.Settings { return f.st }

func multiKeyChannel(id int, keys []string, statusList map[int]int) *store.Channel {
	ab := 1
	return &store.Channel{
		Id:      id,
		Key:     joinKeys(keys),
		Status:  store.ChannelStatusEnabled,
		AutoBan: &ab,
		ChannelInfo: store.ChannelInfo{
			IsMultiKey:         true,
			MultiKeySize:       len(keys),
			MultiKeyStatusList: statusList,
			MultiKeyMode:       "polling",
		},
	}
}

func joinKeys(keys []string) string {
	s := ""
	for i, k := range keys {
		if i > 0 {
			s += "\n"
		}
		s += k
	}
	return s
}

func TestSelectDirectChannel(t *testing.T) {
	ch := multiKeyChannel(7, []string{"k0", "k1", "k2"}, map[int]int{2: 3})
	fc := &fakeChannels{channels: map[int]*store.Channel{7: ch}}
	fk := &fakeKeys{ret: 1}
	sl := newSelector(fc, fk, fakeSP{})

	resp, err := sl.Select(context.Background(), SelectReq{ChannelID: 7, AdvanceCursor: true})
	if err != nil {
		t.Fatalf("Select err: %v", err)
	}
	if resp.ChannelID != 7 || resp.KeyIndex != 1 || resp.Key != "k1" || resp.Mode != "polling" {
		t.Fatalf("resp wrong: %+v", resp)
	}
	// 候选集=启用索引 {0,1}（无轮换配置）
	if !reflect.DeepEqual(fk.candidates, []int{0, 1}) {
		t.Fatalf("candidates = %v, want [0 1]", fk.candidates)
	}
	if resp.Epoch != ch.Epoch() {
		t.Fatalf("epoch mismatch")
	}
	if got := sl.SnapshotMetrics()[`select_total{cid="7",idx="1"}`]; got != 1 {
		t.Fatalf("select_total metric = %d", got)
	}
}

func TestSelectChannelNotFound(t *testing.T) {
	sl := newSelector(&fakeChannels{channels: map[int]*store.Channel{}}, &fakeKeys{}, fakeSP{})
	_, err := sl.Select(context.Background(), SelectReq{ChannelID: 9, AdvanceCursor: true})
	if !errors.Is(err, ErrNoChannel) {
		t.Fatalf("err = %v, want ErrNoChannel", err)
	}
}

func TestSelectNoKey(t *testing.T) {
	// 全部 key 禁用 → ErrNoKey
	ch := multiKeyChannel(3, []string{"k0", "k1"}, map[int]int{0: 3, 1: 3})
	sl := newSelector(&fakeChannels{channels: map[int]*store.Channel{3: ch}}, &fakeKeys{}, fakeSP{})
	_, err := sl.Select(context.Background(), SelectReq{ChannelID: 3, AdvanceCursor: true})
	if !errors.Is(err, ErrNoKey) {
		t.Fatalf("err = %v, want ErrNoKey", err)
	}
}

func TestSelectExcludeAndPeek(t *testing.T) {
	ch := multiKeyChannel(5, []string{"k0", "k1"}, nil)
	fk := &fakeKeys{ret: 1}
	sl := newSelector(&fakeChannels{channels: map[int]*store.Channel{5: ch}}, fk, fakeSP{})

	resp, err := sl.Select(context.Background(), SelectReq{
		ChannelID:     5,
		Exclude:       []KeyRef{{ChannelID: 5, KeyIndex: 0}},
		AdvanceCursor: false, // polling → peek
	})
	if err != nil {
		t.Fatalf("Select err: %v", err)
	}
	if !reflect.DeepEqual(fk.candidates, []int{1}) {
		t.Fatalf("exclude failed, candidates = %v", fk.candidates)
	}
	if fk.mode != "peek" {
		t.Fatalf("lua mode = %q, want peek", fk.mode)
	}
	if resp.Mode != "polling" {
		t.Fatalf("resp.Mode = %q, want polling", resp.Mode)
	}
}

func TestSelectRotationLookaheadMetric(t *testing.T) {
	// 轮换：ActiveCount=1, 4 keys, band 当前批次 key 全禁 → look-ahead
	band := time.Now().Unix() / 60
	cur := int(band % 4)
	disabled := map[int]int{cur: 3}
	ch := multiKeyChannel(8, []string{"k0", "k1", "k2", "k3"}, disabled)
	fk := &fakeKeys{ret: (cur + 1) % 4}
	st := &store.Settings{Rotation: map[int]store.RotationCfg{
		8: {BandSeconds: 60, ActiveCount: 1, Order: "index"},
	}}
	sl := newSelector(&fakeChannels{channels: map[int]*store.Channel{8: ch}}, fk, fakeSP{st})

	resp, err := sl.Select(context.Background(), SelectReq{ChannelID: 8, AdvanceCursor: true})
	if err != nil {
		t.Fatalf("Select err: %v", err)
	}
	if resp.Band == nil {
		t.Fatal("Band should be filled when rotation enabled")
	}
	if len(fk.candidates) != 1 || fk.candidates[0] == cur {
		t.Fatalf("look-ahead candidates wrong: %v (cur=%d)", fk.candidates, cur)
	}
	if got := sl.SnapshotMetrics()["band_lookahead_total"]; got != 1 {
		t.Fatalf("band_lookahead_total = %d, want 1", got)
	}
}

func TestSelectUsageModeParams(t *testing.T) {
	ch := multiKeyChannel(6, []string{"k0"}, nil)
	fk := &fakeKeys{ret: 0}
	st := &store.Settings{Balance: map[int]store.BalanceCfg{
		6: {Mode: "usage", Metric: "tokens", DecayInterval: 120, DecayFactor: 0.9},
	}}
	sl := newSelector(&fakeChannels{channels: map[int]*store.Channel{6: ch}}, fk, fakeSP{st})
	resp, err := sl.Select(context.Background(), SelectReq{ChannelID: 6, EstTokens: 512, AdvanceCursor: true})
	if err != nil {
		t.Fatalf("Select err: %v", err)
	}
	if resp.Mode != "usage" || fk.mode != "usage" {
		t.Fatalf("mode = %q/%q, want usage", resp.Mode, fk.mode)
	}
}

// ---- 评审修复回归测试（P1/P2）----

// P1-1 回归：nil redis（degraded 模式）调用 Select 不再 panic，错误携带
// redisx.ErrDegraded（api 映射 503/50001）。
func TestSelectDegradedRedisNoPanic(t *testing.T) {
	ch := multiKeyChannel(7, []string{"k0"}, nil)
	var nilRdb *redisx.Client // nil receiver：SelectKey 返回 ErrDegraded
	sl := newSelector(&fakeChannels{channels: map[int]*store.Channel{7: ch}}, nilRdb, fakeSP{})

	_, err := sl.Select(context.Background(), SelectReq{ChannelID: 7, AdvanceCursor: true})
	if err == nil {
		t.Fatal("want error")
	}
	if !errors.Is(err, redisx.ErrDegraded) {
		t.Fatalf("err = %v, want redisx.ErrDegraded", err)
	}
}

// P1-6 回归：GetChannel 的非 ErrRecordNotFound 错误（DB 故障）映射
// ErrDependency（→503/50001），不得误报 ErrNoChannel（→40002）。
func TestSelectChannelDBErrorMapsDependency(t *testing.T) {
	sl := newSelector(&fakeChannels{getErr: errors.New("connection refused")}, &fakeKeys{}, fakeSP{})
	_, err := sl.Select(context.Background(), SelectReq{ChannelID: 9, AdvanceCursor: true})
	if !errors.Is(err, ErrDependency) {
		t.Fatalf("err = %v, want ErrDependency", err)
	}
	if errors.Is(err, ErrNoChannel) {
		t.Fatalf("err = %v, must not be ErrNoChannel", err)
	}

	// Abilities 错误同样映射 ErrDependency
	sl2 := newSelector(&fakeChannels{abErr: errors.New("db down")}, &fakeKeys{}, fakeSP{})
	_, err = sl2.Select(context.Background(), SelectReq{Group: "default", Model: "gpt-x", AdvanceCursor: true})
	if !errors.Is(err, ErrDependency) {
		t.Fatalf("abilities err = %v, want ErrDependency", err)
	}

	// ErrRecordNotFound 仍映射 ErrNoChannel
	sl3 := newSelector(&fakeChannels{channels: map[int]*store.Channel{}}, &fakeKeys{}, fakeSP{})
	_, err = sl3.Select(context.Background(), SelectReq{ChannelID: 9, AdvanceCursor: true})
	if !errors.Is(err, ErrNoChannel) {
		t.Fatalf("not-found err = %v, want ErrNoChannel", err)
	}
}

// P1-4 回归：usage 模式且 est>0 时生成 lease_id 并把 est 写入租约；
// est=0 或非 usage 模式不生成。
func TestSelectUsageLease(t *testing.T) {
	ch := multiKeyChannel(6, []string{"k0"}, nil)
	fk := &fakeKeys{ret: 0, leases: map[string]float64{}}
	st := &store.Settings{Balance: map[int]store.BalanceCfg{
		6: {Mode: "usage", Metric: "tokens", DecayInterval: 120, DecayFactor: 0.9},
	}}
	sl := newSelector(&fakeChannels{channels: map[int]*store.Channel{6: ch}}, fk, fakeSP{st})

	resp, err := sl.Select(context.Background(), SelectReq{ChannelID: 6, EstTokens: 512, AdvanceCursor: true})
	if err != nil {
		t.Fatalf("Select err: %v", err)
	}
	if resp.LeaseID == "" {
		t.Fatal("usage+est>0 must generate lease_id")
	}
	if got, ok := fk.leases[resp.LeaseID]; !ok || got != 512 {
		t.Fatalf("lease est = %v ok=%v, want 512", got, ok)
	}

	// est=0 → 不生成租约
	fk2 := &fakeKeys{ret: 0, leases: map[string]float64{}}
	sl2 := newSelector(&fakeChannels{channels: map[int]*store.Channel{6: ch}}, fk2, fakeSP{st})
	resp2, err := sl2.Select(context.Background(), SelectReq{ChannelID: 6, AdvanceCursor: true})
	if err != nil {
		t.Fatalf("Select err: %v", err)
	}
	if resp2.LeaseID != "" || fk2.leaseSets != 0 {
		t.Fatalf("est=0 must not lease: lease_id=%q sets=%d", resp2.LeaseID, fk2.leaseSets)
	}

	// 非 usage 模式 → 不生成租约
	fk3 := &fakeKeys{ret: 0, leases: map[string]float64{}}
	ch3 := multiKeyChannel(7, []string{"k0"}, nil)
	sl3 := newSelector(&fakeChannels{channels: map[int]*store.Channel{7: ch3}}, fk3, fakeSP{})
	resp3, err := sl3.Select(context.Background(), SelectReq{ChannelID: 7, EstTokens: 512, AdvanceCursor: true})
	if err != nil {
		t.Fatalf("Select err: %v", err)
	}
	if resp3.LeaseID != "" || fk3.leaseSets != 0 {
		t.Fatalf("polling must not lease: lease_id=%q sets=%d", resp3.LeaseID, fk3.leaseSets)
	}

	// 租约写入失败 → Select 返回错误（不静默吞掉）
	fk4 := &fakeKeys{ret: 0, leaseErr: redisx.ErrDegraded}
	sl4 := newSelector(&fakeChannels{channels: map[int]*store.Channel{6: ch}}, fk4, fakeSP{st})
	_, err = sl4.Select(context.Background(), SelectReq{ChannelID: 6, EstTokens: 512, AdvanceCursor: true})
	if !errors.Is(err, redisx.ErrDegraded) {
		t.Fatalf("lease set err = %v, want redisx.ErrDegraded", err)
	}
}

// P2-11 回归：look-ahead 命中时 BandInfo 按实际返回 band 计算 Index 与
// EndsAt（EndsAt 是 look-ahead 后 band 的结束时刻，而非当前 band）。
func TestSelectLookaheadBandInfo(t *testing.T) {
	// 4 keys、ActiveCount=1：当前 band 批次的 key 全禁 → look-ahead 一步。
	band := time.Now().Unix() / 60
	cur := int(band % 4)
	disabled := map[int]int{cur: 3}
	ch := multiKeyChannel(8, []string{"k0", "k1", "k2", "k3"}, disabled)
	fk := &fakeKeys{ret: (cur + 1) % 4}
	st := &store.Settings{Rotation: map[int]store.RotationCfg{
		8: {BandSeconds: 60, ActiveCount: 1, Order: "index"},
	}}
	sl := newSelector(&fakeChannels{channels: map[int]*store.Channel{8: ch}}, fk, fakeSP{st})

	resp, err := sl.Select(context.Background(), SelectReq{ChannelID: 8, AdvanceCursor: true})
	if err != nil {
		t.Fatalf("Select err: %v", err)
	}
	if resp.Band == nil {
		t.Fatal("Band should be filled")
	}
	wantIdx := int((band + 1) % 4)
	if resp.Band.Index != wantIdx {
		t.Fatalf("Band.Index = %d, want %d (look-ahead band)", resp.Band.Index, wantIdx)
	}
	wantEnds := (band + 2) * 60
	if resp.Band.EndsAt != wantEnds {
		t.Fatalf("Band.EndsAt = %d, want %d (end of look-ahead band)", resp.Band.EndsAt, wantEnds)
	}
}

// IncludeChannel：请求携带时 SelectResp 附带渠道元数据（来自已加载的渠道，
// 无额外 DB 调用）；未携带时为 nil。
func TestSelectIncludeChannelMeta(t *testing.T) {
	ch := multiKeyChannel(7, []string{"k0", "k1"}, nil)
	ch.Name = "upstream-a"
	ch.Models = "gpt-4o,gpt-4o-mini"
	ch.BaseURL = "https://api.x"
	fc := &fakeChannels{channels: map[int]*store.Channel{7: ch}}
	sl := newSelector(fc, &fakeKeys{ret: 0}, fakeSP{})

	resp, err := sl.Select(context.Background(), SelectReq{ChannelID: 7, AdvanceCursor: true})
	if err != nil {
		t.Fatalf("Select err: %v", err)
	}
	if resp.Channel != nil {
		t.Fatalf("Channel should be nil without IncludeChannel: %+v", resp.Channel)
	}

	resp, err = sl.Select(context.Background(), SelectReq{ChannelID: 7, AdvanceCursor: true, IncludeChannel: true})
	if err != nil {
		t.Fatalf("Select err: %v", err)
	}
	meta := resp.Channel
	if meta == nil {
		t.Fatalf("Channel meta missing")
	}
	if meta.ID != 7 || meta.Name != "upstream-a" || !meta.MultiKey || meta.MultiKeyMode != "polling" ||
		len(meta.Models) != 2 {
		t.Fatalf("meta = %+v", meta)
	}
}

// ---- channel_id + key_index 精确直达 ----

func intPtr(i int) *int { return &i }

// 直达命中：返回指定索引的 key，mode=direct，且完全不调用 Redis 选 key
// （fakeKeys 未被触达）、不返回 band/lease。
func TestSelectKeyIndexDirectHit(t *testing.T) {
	ch := multiKeyChannel(7, []string{"k0", "k1", "k2"}, map[int]int{2: 3})
	fk := &fakeKeys{ret: 999} // 若被调用会返回 999，用于反证未走 Redis
	sl := newSelector(&fakeChannels{channels: map[int]*store.Channel{7: ch}}, fk, fakeSP{})

	resp, err := sl.Select(context.Background(), SelectReq{
		ChannelID: 7, KeyIndex: intPtr(1), AdvanceCursor: true,
	})
	if err != nil {
		t.Fatalf("Select err: %v", err)
	}
	if resp.KeyIndex != 1 || resp.Key != "k1" || resp.ChannelID != 7 {
		t.Fatalf("resp = %+v", resp)
	}
	if resp.Mode != modeDirect {
		t.Fatalf("Mode = %q, want %q", resp.Mode, modeDirect)
	}
	if resp.Epoch != ch.Epoch() {
		t.Fatalf("epoch mismatch")
	}
	if resp.Band != nil || resp.LeaseID != "" {
		t.Fatalf("direct must not return band/lease: %+v", resp)
	}
	if fk.candidates != nil || fk.mode != "" {
		t.Fatalf("direct must not call SelectKey: mode=%q candidates=%v", fk.mode, fk.candidates)
	}
	if got := sl.SnapshotMetrics()["select_direct_total"]; got != 1 {
		t.Fatalf("select_direct_total = %d, want 1", got)
	}

	// key_index=0 是合法索引（指针语义，不被零值遮蔽）
	resp, err = sl.Select(context.Background(), SelectReq{ChannelID: 7, KeyIndex: intPtr(0)})
	if err != nil || resp.KeyIndex != 0 || resp.Key != "k0" {
		t.Fatalf("index 0: resp=%+v err=%v", resp, err)
	}
}

// 直达绕过轮换批次：轮换配置下当前 band 不含目标索引，直达仍命中。
func TestSelectKeyIndexDirectBypassesRotation(t *testing.T) {
	band := time.Now().Unix() / 60
	cur := int(band % 4)
	other := (cur + 2) % 4 // 一定不在当前 band 的批次里（ActiveCount=1）
	ch := multiKeyChannel(8, []string{"k0", "k1", "k2", "k3"}, nil)
	st := &store.Settings{Rotation: map[int]store.RotationCfg{
		8: {BandSeconds: 60, ActiveCount: 1, Order: "index"},
	}}
	sl := newSelector(&fakeChannels{channels: map[int]*store.Channel{8: ch}}, &fakeKeys{ret: cur}, fakeSP{st})

	resp, err := sl.Select(context.Background(), SelectReq{ChannelID: 8, KeyIndex: intPtr(other)})
	if err != nil {
		t.Fatalf("Select err: %v", err)
	}
	if resp.KeyIndex != other || resp.Band != nil {
		t.Fatalf("direct should bypass rotation: resp=%+v (cur band idx=%d)", resp, cur)
	}
}

// 直达忽略 exclude / mode / est_tokens：这些参数在直达路径无效。
func TestSelectKeyIndexDirectIgnoresSchedulingParams(t *testing.T) {
	ch := multiKeyChannel(6, []string{"k0", "k1"}, nil)
	fk := &fakeKeys{ret: 0, leases: map[string]float64{}}
	st := &store.Settings{Balance: map[int]store.BalanceCfg{
		6: {Mode: "usage", Metric: "tokens"},
	}}
	sl := newSelector(&fakeChannels{channels: map[int]*store.Channel{6: ch}}, fk, fakeSP{st})

	resp, err := sl.Select(context.Background(), SelectReq{
		ChannelID: 6, KeyIndex: intPtr(1),
		Exclude:   []KeyRef{{ChannelID: 6, KeyIndex: 1}}, // 被忽略
		Mode:      "usage",
		EstTokens: 512, // 不预扣、不签发租约
	})
	if err != nil {
		t.Fatalf("Select err: %v", err)
	}
	if resp.KeyIndex != 1 || resp.Mode != modeDirect {
		t.Fatalf("resp = %+v", resp)
	}
	if resp.LeaseID != "" || fk.leaseSets != 0 {
		t.Fatalf("direct must not lease: lease_id=%q sets=%d", resp.LeaseID, fk.leaseSets)
	}
}

// 直达的错误语义：越界 → ErrInvalidRequest（400）；被禁用 → ErrNoKey（503）；
// 缺 channel_id / 负索引 → ErrInvalidRequest；渠道不存在 → ErrNoChannel。
func TestSelectKeyIndexDirectErrors(t *testing.T) {
	ch := multiKeyChannel(7, []string{"k0", "k1", "k2"}, map[int]int{2: 3})
	sl := newSelector(&fakeChannels{channels: map[int]*store.Channel{7: ch}}, &fakeKeys{}, fakeSP{})
	ctx := context.Background()

	// 越界 → ErrInvalidRequest（永久性错误，重试无意义）
	_, err := sl.Select(ctx, SelectReq{ChannelID: 7, KeyIndex: intPtr(9)})
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("out of range err = %v, want ErrInvalidRequest", err)
	}
	if errors.Is(err, ErrNoKey) {
		t.Fatalf("out of range must not be ErrNoKey: %v", err)
	}

	// 被禁用的 key → ErrNoKey（可能被重新启用，语义可重试）
	_, err = sl.Select(ctx, SelectReq{ChannelID: 7, KeyIndex: intPtr(2)})
	if !errors.Is(err, ErrNoKey) {
		t.Fatalf("disabled key err = %v, want ErrNoKey", err)
	}

	// 缺 channel_id（走 group+model）→ ErrInvalidRequest
	_, err = sl.Select(ctx, SelectReq{Group: "default", Model: "gpt-x", KeyIndex: intPtr(0)})
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("missing channel_id err = %v, want ErrInvalidRequest", err)
	}

	// 负索引 → ErrInvalidRequest
	_, err = sl.Select(ctx, SelectReq{ChannelID: 7, KeyIndex: intPtr(-1)})
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("negative index err = %v, want ErrInvalidRequest", err)
	}

	// 渠道不存在 → ErrNoChannel（渠道校验先于索引校验）
	sl2 := newSelector(&fakeChannels{channels: map[int]*store.Channel{}}, &fakeKeys{}, fakeSP{})
	_, err = sl2.Select(ctx, SelectReq{ChannelID: 9, KeyIndex: intPtr(0)})
	if !errors.Is(err, ErrNoChannel) {
		t.Fatalf("missing channel err = %v, want ErrNoChannel", err)
	}

	// 渠道被禁用 → ErrNoKey（与常规路径一致）
	chDis := multiKeyChannel(11, []string{"k0"}, nil)
	chDis.Status = store.ChannelStatusManuallyDisabled
	sl3 := newSelector(&fakeChannels{channels: map[int]*store.Channel{11: chDis}}, &fakeKeys{}, fakeSP{})
	_, err = sl3.Select(ctx, SelectReq{ChannelID: 11, KeyIndex: intPtr(0)})
	if !errors.Is(err, ErrNoKey) {
		t.Fatalf("disabled channel err = %v, want ErrNoKey", err)
	}
}

// 直达在 Redis 降级（nil client）下仍可用：零 Redis 依赖。
func TestSelectKeyIndexDirectWorksWhenRedisDegraded(t *testing.T) {
	ch := multiKeyChannel(7, []string{"k0", "k1"}, nil)
	var nilRdb *redisx.Client // SelectKey 会返回 ErrDegraded
	sl := newSelector(&fakeChannels{channels: map[int]*store.Channel{7: ch}}, nilRdb, fakeSP{})

	resp, err := sl.Select(context.Background(), SelectReq{ChannelID: 7, KeyIndex: intPtr(1)})
	if err != nil {
		t.Fatalf("direct should not need Redis: %v", err)
	}
	if resp.KeyIndex != 1 || resp.Key != "k1" {
		t.Fatalf("resp = %+v", resp)
	}
}
