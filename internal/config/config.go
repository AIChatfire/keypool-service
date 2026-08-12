// Package config loads runtime configuration from environment variables.
// Defaults follow SPEC §7 (.env.example values).
package config

import (
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
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

// Load reads configuration from environment variables.
//
// 优先识别 new-api 同款环境变量：
//   - SQL_DSN：new-api 风格数据源，如
//     "user:pass@tcp(host:3306)/newapi?charset=utf8mb4&parseTime=true"（mysql）
//     或 "postgres://user:pass@host:5432/newapi?sslmode=disable"（postgres，
//     据 scheme 自动推断 DatabaseType；显式设置 DATABASE_TYPE 可覆盖推断）
//   - REDIS_CONN_STRING：redis://[:pass@]host:port[/db]
//
// 未设置时回退到 DATABASE_DSN / REDIS_ADDR+REDIS_PASS+REDIS_DB，再退回默认值。
func Load() Config {
	cfg := Config{
		Port:            envInt("PORT", 8080),
		AuthToken:       envStr("AUTH_TOKEN", "change-me"),
		DatabaseType:    envStr("DATABASE_TYPE", ""),
		DatabaseDSN:     envStr("DATABASE_DSN", ""),
		RedisAddr:       envStr("REDIS_ADDR", ""),
		RedisPass:       envStr("REDIS_PASS", ""),
		RedisDB:         envInt("REDIS_DB", 0),
		SyncIntervalSec: envInt("SYNC_INTERVAL_SEC", 60),
	}

	if dsn := envStr("SQL_DSN", ""); dsn != "" {
		cfg.DatabaseDSN = dsn
		// 仅当未显式指定 DATABASE_TYPE 时按 scheme 推断
		if cfg.DatabaseType == "" {
			cfg.DatabaseType = inferDBType(dsn)
		}
	}
	if cfg.DatabaseDSN == "" {
		cfg.DatabaseDSN = "user:pass@tcp(127.0.0.1:3306)/newapi?parseTime=true"
	}
	if cfg.DatabaseType == "" {
		cfg.DatabaseType = "mysql"
	}

	if conn := envStr("REDIS_CONN_STRING", ""); conn != "" {
		addr, pass, db := parseRedisURL(conn)
		cfg.RedisAddr, cfg.RedisPass, cfg.RedisDB = addr, pass, db
	}
	if cfg.RedisAddr == "" {
		cfg.RedisAddr = "127.0.0.1:6379"
	}
	return cfg
}

// inferDBType 按 DSN scheme 推断数据库类型（postgres://|postgresql:// -> postgres；
// sqlite 文件路径 -> sqlite；其余按 mysql，与 new-api 的 SQL_DSN 习惯一致）。
func inferDBType(dsn string) string {
	lower := strings.ToLower(dsn)
	switch {
	case strings.HasPrefix(lower, "postgres://"), strings.HasPrefix(lower, "postgresql://"):
		return "postgres"
	case strings.HasPrefix(lower, "file:"), strings.HasSuffix(lower, ".db"), strings.HasSuffix(lower, ".sqlite"):
		return "sqlite"
	default:
		return "mysql"
	}
}

// parseRedisURL 解析 redis://[:pass@]host:port[/db]（rediss:// 按 redis 处理，
// TLS 由部署层保证；解析失败时按 host:port 原样返回地址，端口缺省 6379）。
func parseRedisURL(raw string) (addr, pass string, db int) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" {
		// 容忍 "host:port" 裸写法
		host, port, err2 := net.SplitHostPort(raw)
		if err2 != nil {
			return raw + ":6379", "", 0
		}
		return host + ":" + port, "", 0
	}
	addr = u.Host
	if !strings.Contains(addr, ":") {
		addr += ":6379"
	}
	if p, ok := u.User.Password(); ok {
		pass = p
	}
	if u.Path != "" && u.Path != "/" {
		if n, err := strconv.Atoi(strings.TrimPrefix(u.Path, "/")); err == nil {
			db = n
		}
	}
	return addr, pass, db
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
