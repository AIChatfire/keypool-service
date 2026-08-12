package api

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"math/rand"
	"time"

	"keypool/internal/store"
)

// rotationStates 为每个 key 计算轮换状态（"active"|"standby"|""）。
//
// 注意：selector 包的批次划分函数（buildBatches/bandCandidates）未导出，
// 此处按 SPEC §4 的同一规则在 api 内重算，必须与 selector/batch.go 保持一致：
//   - 批次基于全部 key 索引 0..n-1；Order=shuffle 用 sha1(fmt.Sprint(cid))
//     的前 8 字节做种子确定性洗牌；按 ActiveCount 连续切分；
//   - 当前 band = floor(now/BandSeconds) % 批数，并入前 OverlapBands 个带批次；
//   - 启用 key 落在当前候选集 → "active"，否则 → "standby"；
//   - 渠道无（合法）轮换配置 → 所有 key 为 ""。
func rotationStates(ch *store.Channel, st *store.Settings, now int64) []string {
	n := len(ch.GetKeys())
	states := make([]string, n)

	cfg, ok := rotationCfgOf(st, ch.Id)
	if !ok || n == 0 {
		return states // ""：未启用轮换
	}

	batches := buildBatchesAPI(cfg, ch.Id, n)
	if len(batches) == 0 {
		return states
	}
	band := now / int64(cfg.BandSeconds)
	active := bandSetAPI(batches, band, cfg.OverlapBands)

	enabled := make(map[int]bool, n)
	for _, i := range ch.EnabledKeyIndexes() {
		enabled[i] = true
	}
	for i := 0; i < n; i++ {
		if !enabled[i] {
			states[i] = "standby" // 禁用 key 不参与当前轮换
			continue
		}
		if active[i] {
			states[i] = "active"
		} else {
			states[i] = "standby"
		}
	}
	return states
}

// rotationCfgOf 与 selector.rotationCfgOf 同规则：缺配置或
// BandSeconds/ActiveCount<=0 视为未启用轮换。
func rotationCfgOf(st *store.Settings, cid int) (store.RotationCfg, bool) {
	if st == nil || st.Rotation == nil {
		return store.RotationCfg{}, false
	}
	cfg, ok := st.Rotation[cid]
	if !ok || cfg.BandSeconds <= 0 || cfg.ActiveCount <= 0 {
		return store.RotationCfg{}, false
	}
	return cfg, true
}

// buildBatchesAPI 复刻 selector.buildBatches（见文件头注释）。
func buildBatchesAPI(cfg store.RotationCfg, cid, keyCount int) [][]int {
	if keyCount <= 0 {
		return nil
	}
	indexes := make([]int, keyCount)
	for i := range indexes {
		indexes[i] = i
	}
	if cfg.Order == "shuffle" {
		sum := sha1.Sum([]byte(fmt.Sprint(cid)))
		seed := int64(binary.BigEndian.Uint64(sum[:8]))
		rand.New(rand.NewSource(seed)).Shuffle(len(indexes), func(i, j int) {
			indexes[i], indexes[j] = indexes[j], indexes[i]
		})
	}
	size := cfg.ActiveCount
	if size <= 0 {
		size = len(indexes)
	}
	var batches [][]int
	for start := 0; start < len(indexes); start += size {
		end := start + size
		if end > len(indexes) {
			end = len(indexes)
		}
		batch := make([]int, end-start)
		copy(batch, indexes[start:end])
		batches = append(batches, batch)
	}
	return batches
}

// bandSetAPI 返回当前 band（含前 OverlapBands 个带回退）覆盖的索引集合，
// 复刻 selector.bandCandidates 的集合语义。
func bandSetAPI(batches [][]int, band int64, overlapBands int) map[int]bool {
	n := int64(len(batches))
	out := make(map[int]bool)
	if n == 0 {
		return out
	}
	if overlapBands < 0 {
		overlapBands = 0
	}
	for back := 0; back <= overlapBands; back++ {
		b := band - int64(back)
		r := b % n
		if r < 0 {
			r += n
		}
		for _, idx := range batches[r] {
			out[idx] = true
		}
	}
	return out
}

// nowUnix 供 handlers 取当前时间（独立变量便于测试替换）。
var nowUnix = func() int64 { return time.Now().Unix() }
