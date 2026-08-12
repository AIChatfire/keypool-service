// Package config loads runtime configuration from environment variables.
// Defaults follow SPEC §7 (.env.example values).
package config

import (
	"os"
	"strconv"
)

// Config is the process-level configuration (SPEC §4/§7).
type Config struct {
	Port            int
	AuthToken       string
	DatabaseType    string // mysql|postgres|sqlite
	DatabaseDSN     string
	RedisAddr       string
	RedisPass       string
	RedisDB         int
	SyncIntervalSec int
}

// Load reads configuration from environment variables, applying SPEC §7 defaults.
func Load() Config {
	return Config{
		Port:            envInt("PORT", 8080),
		AuthToken:       envStr("AUTH_TOKEN", "change-me"),
		DatabaseType:    envStr("DATABASE_TYPE", "mysql"),
		DatabaseDSN:     envStr("DATABASE_DSN", "user:pass@tcp(127.0.0.1:3306)/newapi?parseTime=true"),
		RedisAddr:       envStr("REDIS_ADDR", "127.0.0.1:6379"),
		RedisPass:       envStr("REDIS_PASS", ""),
		RedisDB:         envInt("REDIS_DB", 0),
		SyncIntervalSec: envInt("SYNC_INTERVAL_SEC", 60),
	}
}

func envStr(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v, ok := os.LookupEnv(key); ok {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
