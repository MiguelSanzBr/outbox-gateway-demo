// gateway: API HTTP + workers da outbox + reaper, num único processo.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/adapters/httpapi"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/adapters/postgres"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/adapters/provider"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/adapters/sse"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/adapters/worker"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/app"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/domain"
	"github.com/jackc/pgx/v5/pgxpool"
)

// exit é variável só pra o teste conseguir ver o código de saída sem matar o processo.
var exit = os.Exit

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, log); err != nil {
		log.Error("encerrando", "err", err)
		exit(1)
	}
}

func run(ctx context.Context, log *slog.Logger) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	cfg := worker.Config{
		Workers:      envInt("WORKERS", 16),
		PollInterval: envDur("POLL_INTERVAL", 50*time.Millisecond),
		ReapInterval: envDur("REAP_INTERVAL", 5*time.Second),
	}
	appCfg := app.Config{
		Lease: envDur("LEASE", 30*time.Second),
		Retry: domain.RetryPolicy{
			MaxAttempts: envInt("MAX_ATTEMPTS", 8),
			BackoffBase: envDur("BACKOFF_BASE", 200*time.Millisecond),
			BackoffMax:  envDur("BACKOFF_MAX", 30*time.Second),
		},
	}

	poolCfg, err := pgxpool.ParseConfig(os.Getenv("DATABASE_URL"))
	if err != nil {
		return err
	}
	poolCfg.MaxConns = int32(cfg.Workers + 16) // workers + API
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return err
	}
	defer pool.Close()

	store := postgres.NewStore(pool)
	if err := store.Migrate(ctx); err != nil {
		return err
	}

	sender := provider.HTTPSender{URL: os.Getenv("PROVIDER_URL"), Client: &http.Client{}}
	// um LISTEN só pro processo, dividido entre os streams SSE
	svc := app.NewService(store, sender, sse.NewHub(store, envInt("SSE_MAX_STREAMS", 32), 256), appCfg)
	fleet := worker.Start(ctx, svc, cfg, log)
	var demo *app.Demo
	if os.Getenv("DEMO_ADMIN") == "1" {
		// derruba worker por HTTP sem autenticação: só pra demo, nunca em produção
		log.Warn("DEMO_ADMIN=1: rotas /v1/admin ligadas")
		demo = app.NewDemo(fleet)
	}

	srv := &http.Server{Addr: envStr("ADDR", ":8080"), Handler: httpapi.New(ctx, svc, demo, log), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	log.Info("ouvindo", "addr", srv.Addr, "workers", cfg.Workers)
	err = srv.ListenAndServe()
	// se o listen falhar, para os workers antes de fechar o pool, senão eles ficam girando em cima de pool fechado
	cancel()
	fleet.Wait()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func envStr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(k)); err == nil {
		return v
	}
	return def
}

func envDur(k string, def time.Duration) time.Duration {
	if v, err := time.ParseDuration(os.Getenv(k)); err == nil {
		return v
	}
	return def
}
