// Package classifier 实现 new-api 的渠道禁用/启用判定语义（SPEC §2.4）。
// 仅依赖 store 的类型，不做任何 IO。
package classifier

import (
	"strings"

	"keypool/internal/store"
)

// ShouldDisable 复刻 new-api ShouldDisableChannel（SPEC §2.4），判定顺序严格如下：
//  1. 全局开关 st.AutoDisableOn == false → false（st 为 nil 视为全关）
//  2. 渠道 auto_ban == false → false
//  3. statusCode 命中 st.DisableCodeRanges 任一区间 → true
//  4. errMsg 小写化后包含 st.DisableKeywords 任一关键词 → true
//  5. 否则 false
func ShouldDisable(st *store.Settings, autoBan bool, statusCode int, errMsg string) bool {
	if st == nil || !st.AutoDisableOn {
		return false
	}
	if !autoBan {
		return false
	}
	if store.MatchCodeRange(st.DisableCodeRanges, statusCode) {
		return true
	}
	lower := strings.ToLower(errMsg)
	for _, kw := range st.DisableKeywords {
		if kw != "" && strings.Contains(lower, kw) {
			return true
		}
	}
	return false
}

// ShouldEnable 复刻 new-api 自动启用判定（SPEC §4）：
// 全局自动启用开关开 且 key/渠道 之前处于自动禁用(3)。
func ShouldEnable(st *store.Settings, prevStatus int) bool {
	return st != nil && st.AutoEnableOn && prevStatus == store.ChannelStatusAutoDisabled
}
