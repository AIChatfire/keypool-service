// Command keypool 是装配入口（SPEC §6）：
//
//	Load env → redisx.NewClient（PING 失败仅 log，degraded 继续）
//	→ store.Open → OptionsPoller(60s) Start（defer Stop）
//	→ Selector/Manager → api.NewRouter → http.Server（ReadHeaderTimeout 10s）
//	→ signal.NotifyContext 优雅退出 10s。
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"keypool/internal/api"
	"keypool/internal/config"
	"keypool/internal/redisx"
	"keypool/internal/selector"
	"keypool/internal/state"
	"keypool/internal/store"
)

// shutdownTimeout 是优雅退出窗口（SPEC §6）。
const shutdownTimeout = 10 * time.Second

func main() {
	cfg := config.Load()

	// Redis：PING 失败仅 log，degraded 模式继续运行（SPEC §6）。
	// degraded 下 select/report 的 Redis 操作会失败并由 api 映射为
	// 50001；GET keys 省略 usage 字段；GET usage 返回 50001。
	var rdb *redisx.Client
	if c, err := redisx.NewClient(cfg.RedisAddr, cfg.RedisPass, cfg.RedisDB); err != nil {
		log.Printf("keypool: redis unavailable, running degraded: %v", err)
	} else {
		rdb = c
		defer func() { _ = rdb.Close() }()
	}

	// DB：store.Open（gorm，SkipDefaultTransaction，连接池 20，SPEC §6）。
	s, err := store.Open(cfg)
	if err != nil {
		log.Fatalf("keypool: open store: %v", err)
	}

	// options 轮询器（SYNC_INTERVAL_SEC，缺省 60s），同时作为
	// SettingsProvider 注入 selector/state/api；Reload 能力供
	// /v1/settings/reload 与 PUT balance|rotation 使用。
	poller := store.NewOptionsPoller(s, time.Duration(cfg.SyncIntervalSec)*time.Second)
	poller.Start()
	defer poller.Stop()

	sl := selector.NewSelector(s, rdb, poller)
	m := state.NewManager(s, rdb, poller)

	handler := api.NewRouter(cfg, sl, m, s, poller, rdb)

	srv := &http.Server{
		Addr:              ":" + strconv.Itoa(cfg.Port),
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second, // SPEC §6/任务约束
	}

	// signal.NotifyContext：SIGINT/SIGTERM 触发优雅退出。
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		log.Printf("keypool: listening on %s (degraded=%v)", srv.Addr, rdb == nil)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		log.Fatalf("keypool: server: %v", err)
	case <-ctx.Done():
	}

	shCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shCtx); err != nil {
		log.Printf("keypool: graceful shutdown: %v", err)
	}
	log.Printf("keypool: stopped")
}

// itoa 避免仅为一个转换引入 strconv 的局部包装（保持 main 简洁）。
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
