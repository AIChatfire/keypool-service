package store

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Option keys reused from new-api (SPEC §2.5).
const (
	OptAutoDisableEnabled   = "AutomaticDisableChannelEnabled"
	OptAutoEnableEnabled    = "AutomaticEnableChannelEnabled"
	OptDisableStatusCodes   = "AutomaticDisableStatusCodes"
	OptDisableKeywords      = "AutomaticDisableKeywords"
	optChannelDisableThresh = "ChannelDisableThreshold" // float, ignored

	// OptExtensionPrefix 是 keypool 全部扩展 options 键的命名空间。
	// UpsertOption 只允许写此前缀下的键，避免误写 new-api 原生配置项。
	OptExtensionPrefix = "keypool."

	// OptPrefixBalance / OptPrefixBalancePrefix namespace keypool extension keys.
	OptBalancePrefix  = "keypool.balance."  // keypool.balance.{cid} -> BalanceCfg JSON
	OptRotationPrefix = "keypool.rotation." // keypool.rotation.{cid} -> RotationCfg JSON
)

// defaultDisableStatusCodes is used when AutomaticDisableStatusCodes is unset (SPEC §2.4).
const defaultDisableStatusCodes = "401"

// defaultDisableKeywords are the new-api defaults (SPEC §2.4), already lowercased.
var defaultDisableKeywords = []string{
	"your credit balance is too low",
	"this organization has been disabled.",
	"you exceeded your current quota",
	"permission denied",
	"the security token included in the request is invalid",
	"operation not allowed",
	"your account is not authorized",
}

// defaultPollInterval is the options polling period (SPEC §6).
const defaultPollInterval = 60 * time.Second

// PollOptions reads the whole options table (SPEC §2.5).
func (s *Store) PollOptions() (map[string]string, error) {
	var opts []Option
	if err := s.db.Find(&opts).Error; err != nil {
		return nil, err
	}
	res := make(map[string]string, len(opts))
	for _, o := range opts {
		res[o.Key] = o.Value
	}
	return res, nil
}

// OptionsPoller polls the options table every interval and atomically
// replaces the Store's Settings snapshot. It implements SettingsProvider.
type OptionsPoller struct {
	store    *Store
	interval time.Duration

	mu      sync.Mutex
	stopCh  chan struct{}
	stopped chan struct{}
	running bool
}

// NewOptionsPoller creates a poller; interval<=0 falls back to 60s.
func NewOptionsPoller(s *Store, interval time.Duration) *OptionsPoller {
	if interval <= 0 {
		interval = defaultPollInterval
	}
	return &OptionsPoller{store: s, interval: interval}
}

// Start performs an initial poll and launches the background loop.
func (p *OptionsPoller) Start() {
	p.mu.Lock()
	if p.running {
		p.mu.Unlock()
		return
	}
	p.running = true
	p.stopCh = make(chan struct{})
	p.stopped = make(chan struct{})
	p.mu.Unlock()

	if err := p.refresh(); err != nil {
		log.Printf("keypool: initial options poll failed: %v", err)
	}
	go func() {
		defer close(p.stopped)
		ticker := time.NewTicker(p.interval)
		defer ticker.Stop()
		for {
			select {
			case <-p.stopCh:
				return
			case <-ticker.C:
				if err := p.refresh(); err != nil {
					log.Printf("keypool: options poll failed: %v", err)
				}
			}
		}
	}()
}

// Stop terminates the background polling loop and waits for it to exit.
func (p *OptionsPoller) Stop() {
	p.mu.Lock()
	if !p.running {
		p.mu.Unlock()
		return
	}
	p.running = false
	close(p.stopCh)
	stopped := p.stopped
	p.mu.Unlock()
	<-stopped
}

// Get implements SettingsProvider: latest snapshot (nil before first poll).
func (p *OptionsPoller) Get() *Settings {
	return p.store.GetSettings()
}

// refresh re-polls options and atomically replaces the snapshot.
// On parse failure of an individual option the previous value is kept.
func (p *OptionsPoller) refresh() error {
	opts, err := p.store.PollOptions()
	if err != nil {
		return err
	}
	p.store.UpdateSettings(parseSettings(opts, p.store.GetSettings()))
	return nil
}

// parseSettings builds a Settings snapshot from raw options. old provides
// fallback values for entries that fail to parse.
func parseSettings(opts map[string]string, old *Settings) *Settings {
	st := &Settings{
		AutoDisableOn: strings.EqualFold(opts[OptAutoDisableEnabled], "true"),
		AutoEnableOn:  strings.EqualFold(opts[OptAutoEnableEnabled], "true"),
		Balance:       map[int]BalanceCfg{},
		Rotation:      map[int]RotationCfg{},
	}

	// status-code ranges: unset -> default "401"; parse error -> keep old
	raw := strings.TrimSpace(opts[OptDisableStatusCodes])
	if raw == "" {
		raw = defaultDisableStatusCodes
	}
	ranges, err := ParseCodeRanges(raw)
	switch {
	case err != nil && old != nil && old.DisableCodeRanges != nil:
		st.DisableCodeRanges = old.DisableCodeRanges
	case err != nil:
		st.DisableCodeRanges, _ = ParseCodeRanges(defaultDisableStatusCodes)
	default:
		st.DisableCodeRanges = ranges
	}

	// keywords: newline-separated, lowercased; unset -> new-api defaults
	if kw, ok := opts[OptDisableKeywords]; ok {
		st.DisableKeywords = parseKeywords(kw)
	} else if old != nil && old.DisableKeywords != nil {
		st.DisableKeywords = old.DisableKeywords
	} else {
		st.DisableKeywords = append([]string(nil), defaultDisableKeywords...)
	}

	// keypool.balance.{cid} / keypool.rotation.{cid} JSON extensions
	for k, v := range opts {
		if cid, ok := strings.CutPrefix(k, OptBalancePrefix); ok {
			id, err := strconv.Atoi(cid)
			if err != nil {
				continue
			}
			var cfg BalanceCfg
			if err := json.Unmarshal([]byte(v), &cfg); err != nil {
				if old != nil && old.Balance != nil {
					if prev, ok := old.Balance[id]; ok {
						st.Balance[id] = prev
					}
				}
				continue
			}
			st.Balance[id] = cfg
			continue
		}
		if cid, ok := strings.CutPrefix(k, OptRotationPrefix); ok {
			id, err := strconv.Atoi(cid)
			if err != nil {
				continue
			}
			var cfg RotationCfg
			if err := json.Unmarshal([]byte(v), &cfg); err != nil {
				if old != nil && old.Rotation != nil {
					if prev, ok := old.Rotation[id]; ok {
						st.Rotation[id] = prev
					}
				}
				continue
			}
			st.Rotation[id] = cfg
		}
	}
	return st
}

// parseKeywords splits on newlines, trims and lowercases, dropping empties.
func parseKeywords(raw string) []string {
	var res []string
	for _, line := range strings.Split(raw, "\n") {
		line = strings.ToLower(strings.TrimSpace(line))
		if line != "" {
			res = append(res, line)
		}
	}
	return res
}

// String renders a Settings snapshot for debugging.
func (s *Settings) String() string {
	if s == nil {
		return "<nil>"
	}
	return fmt.Sprintf("Settings{autoDisable:%v autoEnable:%v ranges:%v keywords:%d balance:%d rotation:%d}",
		s.AutoDisableOn, s.AutoEnableOn, s.DisableCodeRanges, len(s.DisableKeywords), len(s.Balance), len(s.Rotation))
}
