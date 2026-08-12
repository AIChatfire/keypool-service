package config

import "testing"

func TestLoadDefaults(t *testing.T) {
	cfg := Load()
	if cfg.Port != 8080 {
		t.Fatalf("Port: got %d", cfg.Port)
	}
	if cfg.AuthToken != "change-me" {
		t.Fatalf("AuthToken: got %q", cfg.AuthToken)
	}
	if cfg.DatabaseType != "mysql" {
		t.Fatalf("DatabaseType: got %q", cfg.DatabaseType)
	}
	if cfg.RedisAddr != "127.0.0.1:6379" || cfg.RedisDB != 0 {
		t.Fatalf("redis defaults: %+v", cfg)
	}
	if cfg.SyncIntervalSec != 60 {
		t.Fatalf("SyncIntervalSec: got %d", cfg.SyncIntervalSec)
	}
}

func TestLoadFromEnv(t *testing.T) {
	t.Setenv("PORT", "9090")
	t.Setenv("AUTH_TOKEN", "secret")
	t.Setenv("DATABASE_TYPE", "sqlite")
	t.Setenv("DATABASE_DSN", "/tmp/test.db")
	t.Setenv("REDIS_DB", "2")
	cfg := Load()
	if cfg.Port != 9090 || cfg.AuthToken != "secret" || cfg.DatabaseType != "sqlite" ||
		cfg.DatabaseDSN != "/tmp/test.db" || cfg.RedisDB != 2 {
		t.Fatalf("env not honored: %+v", cfg)
	}
}

func TestLoadBadIntFallsBack(t *testing.T) {
	t.Setenv("PORT", "not-a-number")
	if cfg := Load(); cfg.Port != 8080 {
		t.Fatalf("bad PORT should fall back to 8080: got %d", cfg.Port)
	}
}
