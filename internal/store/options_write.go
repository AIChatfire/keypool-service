package store

import (
	"gorm.io/gorm/clause"
)

// 本文件由 api 装配层（feat-api）新增：SPEC §5 要求
// PUT /v1/channels/{id}/balance|rotation 把配置写入 options 表
// （keypool.balance.{cid} / keypool.rotation.{cid}），而 store 原本只有
// PollOptions 读取路径，故补充最小写方法 UpsertOption。不改变任何既有代码。

// UpsertOption 以 PK=key upsert 一行 options（SPEC §2.5：key/value 两列，
// PK=key）。存在即更新 value，不存在则插入。仅用于 keypool.* 扩展键的写入，
// 不涉及 channels/abilities，单写者纪律不受影响。
func (s *Store) UpsertOption(key, value string) error {
	opt := Option{Key: key, Value: value}
	return s.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "key"}},
		DoUpdates: clause.AssignmentColumns([]string{"value"}),
	}).Create(&opt).Error
}
