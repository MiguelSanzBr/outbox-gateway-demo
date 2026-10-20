// gateway: API HTTP + workers da outbox + reaper, num único processo.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/adapters/httpapi"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/adapters/postgres"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/adapters/provider"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/adapters/sse"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/adapters/worker"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/app"
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
	cfg, err := loadConfig(os.Getenv)
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	poolCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	var pool *pgxpool.Pool
	if err == nil {
		poolCfg.MaxConns = int32(cfg.Worker.Workers + 16) // workers + API; o LISTEN do SSE é uma conexão à parte
		pool, err = pgxpool.NewWithConfig(ctx, poolCfg)
	}
	if err != nil {
		return fmt.Errorf("pool: %w", err)
	}
	defer pool.Close()

	store := postgres.NewStore(pool)
	if err := store.Migrate(ctx); err != nil {
		return err
	}

	// Transport padrão guarda só 2 conexões ociosas por host: com 16 workers no mesmo
	// provedor quase todo envio abriria TCP novo.
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = cfg.Worker.Workers
	transport.MaxIdleConns = max(transport.MaxIdleConns, cfg.Worker.Workers)
	sender := provider.HTTPSender{URL: cfg.ProviderURL, Client: &http.Client{Transport: transport}}
	// um LISTEN só pro processo, dividido entre os streams SSE
	svc := app.NewService(store, sender, sse.NewHub(store, cfg.MaxStreams, 256), cfg.App)
	fleet := worker.Start(ctx, svc, cfg.Worker, log)
	var demo *app.Demo
	if cfg.DemoAdmin {
		// derruba worker por HTTP sem autenticação: só pra demo, nunca em produção
		log.Warn("DEMO_ADMIN=1: rotas /v1/admin ligadas")
		demo = app.NewDemo(fleet)
	}

	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           httpapi.New(ctx, svc, demo, log),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	log.Info("ouvindo", "addr", srv.Addr, "workers", cfg.Worker.Workers)
	err = srv.ListenAndServe()
	// ListenAndServe volta assim q o Shutdown começa (ou na hora, se o listen falhou):
	// espera as requisições em voo, depois os workers, e só então o defer fecha o pool
	cancel()
	<-drained
	fleet.Wait()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
