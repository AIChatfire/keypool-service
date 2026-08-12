package store

// 本文件由 api 装配层（feat-api）新增：SPEC §5 要求
// POST /v1/settings/reload 触发快照立即重建，且
// PUT /v1/channels/{id}/balance|rotation 写 options 后需要立即刷新快照，
// 而 OptionsPoller 的 refresh 为私有方法，故暴露最小 Reload。不改变任何既有代码。

// Reload 立即重新 PollOptions 并原子替换 Settings 快照（等同一次定时轮询）。
func (p *OptionsPoller) Reload() error {
	return p.refresh()
}
