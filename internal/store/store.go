package store

import (
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"keypool/internal/config"
)

// maxOpenConns is the DB connection-pool size (SPEC §6).
const maxOpenConns = 20

// allKeysDisabledReason is merged into other_info when every key is dead (SPEC §2.3).
const allKeysDisabledReason = "All keys are disabled"

// Store wraps a gorm.DB bound to the new-api schema.
type Store struct {
	db *gorm.DB

	// settings holds *Settings, atomically replaced by the options poller.
	settings atomic.Value
}

// Open connects to the database selected by cfg.DatabaseType
// (mysql | postgres | sqlite), reusing new-api's tables (SPEC §4).
func Open(cfg config.Config) (*Store, error) {
	gormCfg := &gorm.Config{
		SkipDefaultTransaction: true,
	}
	var db *gorm.DB
	var err error
	switch cfg.DatabaseType {
	case "mysql":
		db, err = gorm.Open(mysql.Open(cfg.DatabaseDSN), gormCfg)
	case "postgres":
		db, err = gorm.Open(postgres.Open(cfg.DatabaseDSN), gormCfg)
	case "sqlite":
		db, err = gorm.Open(sqlite.Open(cfg.DatabaseDSN), gormCfg)
	default:
		return nil, fmt.Errorf("unsupported DATABASE_TYPE %q", cfg.DatabaseType)
	}
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", cfg.DatabaseType, err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}
	sqlDB.SetMaxOpenConns(maxOpenConns)
	sqlDB.SetMaxIdleConns(maxOpenConns)
	return &Store{db: db}, nil
}

// GetChannel loads one channel by id.
func (s *Store) GetChannel(id int) (*Channel, error) {
	var ch Channel
	if err := s.db.Where("id = ?", id).First(&ch).Error; err != nil {
		return nil, err
	}
	return &ch, nil
}

// GetChannelsByIDs loads channels by id list.
func (s *Store) GetChannelsByIDs(ids []int) ([]*Channel, error) {
	if len(ids) == 0 {
		return []*Channel{}, nil
	}
	var channels []*Channel
	if err := s.db.Where("id IN ?", ids).Find(&channels).Error; err != nil {
		return nil, err
	}
	return channels, nil
}

// Abilities returns enabled abilities for (group, model), ordered by
// priority desc, weight desc (SPEC §4).
//
// 评审修复（P1-5）：改用 gorm map 条件，由各数据库方言自动为保留字
// `group` 加引号（MySQL 反引号 / PostgreSQL 双引号 / sqlite 双引号），
// 此前硬编码反引号在 PostgreSQL 下是语法错误。
func (s *Store) Abilities(group, model string) ([]Ability, error) {
	var list []Ability
	err := s.db.
		Where(map[string]any{"group": group, "model": model, "enabled": true}).
		Order("priority desc, weight desc").
		Find(&list).Error
	return list, err
}

// GetSettings returns the current Settings snapshot (nil before first poll).
func (s *Store) GetSettings() *Settings {
	if v := s.settings.Load(); v != nil {
		return v.(*Settings)
	}
	return nil
}

// UpdateSettings atomically replaces the Settings snapshot.
func (s *Store) UpdateSettings(st *Settings) {
	s.settings.Store(st)
}

// ApplyKeyStatus persists a key enable/disable, replicating new-api's
// handlerMultiKeyUpdate + UpdateChannelStatus semantics (SPEC §2.3).
// It must be called while holding the per-channel lock (SPEC §4).
//
// status: ChannelStatusEnabled (1) enables the key; 2/3 disable it
// (manual/auto). For multi-key channels the per-key maps in channel_info
// are updated; when no key remains enabled the whole channel is
// auto-disabled and abilities are turned off. Enabling a key while at
// least one key is enabled restores channels.status=1 and
// abilities.enabled=true. Non-multi-key channels directly update
// channels.status + other_info. Everything runs in one transaction, and
// only channel_info/status/other_info and abilities.enabled are written
// (single-writer discipline, SPEC §2.3).
//
// Returns the resulting channels.status and whether all keys are dead.
func (s *Store) ApplyKeyStatus(cid, idx, status int, reason string) (channelStatus int, allDead bool, err error) {
	now := time.Now().Unix()
	err = s.db.Transaction(func(tx *gorm.DB) error {
		var ch Channel
		if err := tx.Where("id = ?", cid).First(&ch).Error; err != nil {
			return err
		}

		if ch.ChannelInfo.IsMultiKey {
			keys := ch.GetKeys()
			if idx < 0 || idx >= len(keys) {
				return fmt.Errorf("key index %d out of range (0..%d) for channel %d", idx, len(keys)-1, cid)
			}
			info := ch.ChannelInfo
			if info.MultiKeyStatusList == nil {
				info.MultiKeyStatusList = map[int]int{}
			}
			if info.MultiKeyDisabledReason == nil {
				info.MultiKeyDisabledReason = map[int]string{}
			}
			if info.MultiKeyDisabledTime == nil {
				info.MultiKeyDisabledTime = map[int]int64{}
			}
			if status == ChannelStatusEnabled {
				// enable: remove idx from status_list (and reason/time)
				delete(info.MultiKeyStatusList, idx)
				delete(info.MultiKeyDisabledReason, idx)
				delete(info.MultiKeyDisabledTime, idx)
			} else {
				// disable: 3=auto / 2=manual, with reason + unix timestamp
				info.MultiKeyStatusList[idx] = status
				info.MultiKeyDisabledReason[idx] = reason
				info.MultiKeyDisabledTime[idx] = now
			}
			ch.ChannelInfo = info

			enabled := ch.EnabledKeyIndexes()
			updates := map[string]interface{}{"channel_info": info}
			switch {
			case len(enabled) == 0:
				// all keys dead: auto-disable the channel, merge other_info,
				// and turn abilities off
				allDead = true
				channelStatus = ChannelStatusAutoDisabled
				updates["status"] = channelStatus
				updates["other_info"] = mergeOtherInfo(ch.OtherInfo, map[string]interface{}{
					"status_reason": allKeysDisabledReason,
					"status_time":   now,
				})
			case status == ChannelStatusEnabled:
				// recovery: at least one enabled key + enable operation
				channelStatus = ChannelStatusEnabled
				updates["status"] = channelStatus
			default:
				// some keys still alive: channel status unchanged
				channelStatus = ch.Status
			}
			if err := tx.Model(&Channel{}).Where("id = ?", cid).Updates(updates).Error; err != nil {
				return err
			}
			if allDead {
				if err := tx.Model(&Ability{}).Where("channel_id = ?", cid).
					Update("enabled", false).Error; err != nil {
					return err
				}
			} else if status == ChannelStatusEnabled {
				if err := tx.Model(&Ability{}).Where("channel_id = ?", cid).
					Update("enabled", true).Error; err != nil {
					return err
				}
			}
			return nil
		}

		// non-multi-key channel: directly set channels.status + other_info
		if idx != 0 {
			return fmt.Errorf("channel %d is not multi-key; only key index 0 is valid", cid)
		}
		channelStatus = status
		allDead = status != ChannelStatusEnabled
		updates := map[string]interface{}{"status": channelStatus}
		if status != ChannelStatusEnabled {
			updates["other_info"] = mergeOtherInfo(ch.OtherInfo, map[string]interface{}{
				"status_reason": reason,
				"status_time":   now,
			})
		}
		if err := tx.Model(&Channel{}).Where("id = ?", cid).Updates(updates).Error; err != nil {
			return err
		}
		if err := tx.Model(&Ability{}).Where("channel_id = ?", cid).
			Update("enabled", status == ChannelStatusEnabled).Error; err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return 0, false, err
	}
	return channelStatus, allDead, nil
}

// mergeOtherInfo merges kv into an existing other_info JSON object
// (merge, not overwrite; SPEC §2.3). A non-object or empty value is
// treated as an empty object.
func mergeOtherInfo(existing string, kv map[string]interface{}) string {
	obj := map[string]interface{}{}
	if existing != "" {
		if err := json.Unmarshal([]byte(existing), &obj); err != nil {
			obj = map[string]interface{}{}
		}
	}
	for k, v := range kv {
		obj[k] = v
	}
	b, err := json.Marshal(obj)
	if err != nil {
		return existing
	}
	return string(b)
}
