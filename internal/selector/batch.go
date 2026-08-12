package selector

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sort"

	"keypool/internal/store"
)

// 本文件为批次轮换与渠道档位的纯函数实现，全部无副作用、可直接单测。

// splitBatches 把索引序列按 size 连续切分为若干批（SPEC §4：
// "按 key 索引连续切分 ActiveCount 个/批"）。size<=0 时整体为一批。
func splitBatches(indexes []int, size int) [][]int {
	if len(indexes) == 0 {
		return nil
	}
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

// deterministicShuffle 用 sha1(fmt.Sprint(cid)) 的字节作为 math/rand 种子做
// 确定性洗牌（SPEC §4：Order=shuffle）。同一 cid 结果恒定，不同 cid 不同。
// 不修改入参，返回新切片。
func deterministicShuffle(cid int, indexes []int) []int {
	sum := sha1.Sum([]byte(fmt.Sprint(cid)))
	seed := int64(binary.BigEndian.Uint64(sum[:8]))
	out := make([]int, len(indexes))
	copy(out, indexes)
	rand.New(rand.NewSource(seed)).Shuffle(len(out), func(i, j int) {
		out[i], out[j] = out[j], out[i]
	})
	return out
}

// buildBatches 按轮换配置构建批次：Order=shuffle 先确定性洗牌再连续切分，
// 否则按索引升序连续切分。批次基于全部 key 索引 0..keyCount-1。
func buildBatches(cfg store.RotationCfg, cid, keyCount int) [][]int {
	if keyCount <= 0 {
		return nil
	}
	indexes := make([]int, keyCount)
	for i := range indexes {
		indexes[i] = i
	}
	if cfg.Order == "shuffle" {
		indexes = deterministicShuffle(cid, indexes)
	}
	return splitBatches(indexes, cfg.ActiveCount)
}

// bandCandidates 返回当前 band 的批次（并入前 OverlapBands 个 band 的批次）
// 与启用索引集的交集，升序去重。
func bandCandidates(batches [][]int, enabled map[int]bool, band int64, overlapBands int) []int {
	n := int64(len(batches))
	if n == 0 {
		return nil
	}
	if overlapBands < 0 {
		overlapBands = 0
	}
	seen := make(map[int]bool)
	var cands []int
	for back := 0; back <= overlapBands; back++ {
		b := batches[mod(band-int64(back), n)]
		for _, idx := range b {
			if enabled[idx] && !seen[idx] {
				seen[idx] = true
				cands = append(cands, idx)
			}
		}
	}
	sort.Ints(cands)
	return cands
}

// lookahead 当前 band 无候选时向前探查后续批次，最多 len(batches) 步，
// 返回首个非空交集与步数（SPEC §4）。
func lookahead(batches [][]int, enabled map[int]bool, band int64) ([]int, int) {
	n := int64(len(batches))
	for step := int64(1); step <= n; step++ {
		b := batches[mod(band+step, n)]
		var cands []int
		for _, idx := range b {
			if enabled[idx] {
				cands = append(cands, idx)
			}
		}
		if len(cands) > 0 {
			sort.Ints(cands)
			return cands, int(step)
		}
	}
	return nil, 0
}

// mod 是总是非负的取模（band 可能因 OverlapBands 回退为负）。
func mod(a, n int64) int64 {
	r := a % n
	if r < 0 {
		r += n
	}
	return r
}

// priorityOf 取 ability 的 priority，nil 视为 0。
func priorityOf(a store.Ability) int64 {
	if a.Priority == nil {
		return 0
	}
	return *a.Priority
}

// priorityTiers 把 abilities（store 已按 priority desc 排序）按 priority
// 去重分档，返回降序档位序列（SPEC §4："priority 去重降序档位"）。
func priorityTiers(abs []store.Ability) [][]store.Ability {
	var tiers [][]store.Ability
	for _, a := range abs {
		if len(tiers) == 0 || priorityOf(tiers[len(tiers)-1][0]) != priorityOf(a) {
			tiers = append(tiers, []store.Ability{a})
		} else {
			tiers[len(tiers)-1] = append(tiers[len(tiers)-1], a)
		}
	}
	// 防御：store 查询已排序；若输入未排序则按 priority 降序重排档位。
	sort.SliceStable(tiers, func(i, j int) bool {
		return priorityOf(tiers[i][0]) > priorityOf(tiers[j][0])
	})
	return tiers
}

// tierForRetry 按 retry 取档位；越界取最低档（最后一档），负数取最高档。
func tierForRetry(tiers [][]store.Ability, retry int) []store.Ability {
	if len(tiers) == 0 {
		return nil
	}
	if retry < 0 {
		retry = 0
	}
	if retry >= len(tiers) {
		return tiers[len(tiers)-1]
	}
	return tiers[retry]
}

// tierWeights 计算档内各 ability 的权重：weight+10（SPEC §4）。
func tierWeights(tier []store.Ability) []int {
	w := make([]int, len(tier))
	for i, a := range tier {
		w[i] = int(a.Weight) + 10
	}
	return w
}

// pickWeightedIndex 按权重加权随机返回下标（SPEC §4：档内 weight 加权随机）。
func pickWeightedIndex(weights []int, r *rand.Rand) int {
	total := 0
	for _, w := range weights {
		if w > 0 {
			total += w
		}
	}
	if total <= 0 {
		return r.Intn(len(weights))
	}
	x := r.Intn(total)
	for i, w := range weights {
		if w <= 0 {
			continue
		}
		if x < w {
			return i
		}
		x -= w
	}
	return len(weights) - 1
}
