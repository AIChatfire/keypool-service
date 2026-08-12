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

func TestSQLDSNPrecedenceAndInfer(t *testing.T) {
	t.Setenv("SQL_DSN", "u:p@tcp(10.0.0.8:3306)/newapi?parseTime=true")
	t.Setenv("DATABASE_DSN", "ignored:x@tcp(0.0.0.0:1)/x")
	c := Load()
	if c.DatabaseDSN != "u:p@tcp(10.0.0.8:3306)/newapi?parseTime=true" || c.DatabaseType != "mysql" {
		t.Fatalf("SQL_DSN precedence/infer failed: %+v", c)
	}
}

func TestSQLDSNPostgresInfer(t *testing.T) {
	t.Setenv("SQL_DSN", "postgres://u:p@10.0.0.8:5432/newapi?sslmode=disable")
	c := Load()
	if c.DatabaseType != "postgres" {
		t.Fatalf("postgres infer failed: %s", c.DatabaseType)
	}
}

func TestDatabaseTypeExplicitOverride(t *testing.T) {
	t.Setenv("SQL_DSN", "u:p@tcp(h:3306)/db")
	t.Setenv("DATABASE_TYPE", "postgres")
	c := Load()
	if c.DatabaseType != "postgres" {
		t.Fatalf("explicit DATABASE_TYPE should win: %s", c.DatabaseType)
	}
}

func TestRedisConnString(t *testing.T) {
	t.Setenv("REDIS_CONN_STRING", "redis://:s3cret@10.0.0.9:6380/2")
	c := Load()
	if c.RedisAddr != "10.0.0.9:6380" || c.RedisPass != "s3cret" || c.RedisDB != 2 {
		t.Fatalf("REDIS_CONN_STRING parse failed: %+v", c)
	}
}

func TestRedisConnStringBareHost(t *testing.T) {
	addr, pass, db := parseRedisURL("10.0.0.9:6379")
	if addr != "10.0.0.9:6379" || pass != "" || db != 0 {
		t.Fatalf("bare host parse failed: %s %s %d", addr, pass, db)
	}
	addr2, _, _ := parseRedisURL("redis://redis:6379")
	_ = addr2
	addr3, _, _ := parseRedisURL("redis://nohost")
	if addr3 != "nohost:6379" {
		t.Fatalf("default port failed: %s", addr3)
	}
}
