// mockprovider: provedor de mensagens simulado, configurado por ambiente.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/adapters/mockprovider"
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

type config struct {
	addr     string
	provider mockprovider.Config
}

func loadConfig(getenv func(string) string) (config, error) {
	var errs []error
	dur := func(k string) time.Duration {
		v := getenv(k)
		if v == "" {
			return 0
		}
		d, err := time.ParseDuration(v)
		if err != nil || d < 0 {
			errs = append(errs, fmt.Errorf("%s=%q precisa ser uma duração >= 0 (ex.: 80ms)", k, v))
		}
		return d
	}
	c := config{addr: getenv("ADDR"), provider: mockprovider.Config{Latency: dur("LATENCY"), Jitter: dur("JITTER")}}
	if c.addr == "" {
		c.addr = ":9090"
	}
	if v := getenv("FAIL_RATE"); v != "" {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil || f < 0 || f > 1 {
			errs = append(errs, fmt.Errorf("FAIL_RATE=%q precisa ficar entre 0 e 1", v))
		}
		c.provider.FailRate = f
	}
	return c, errors.Join(errs...)
}

func run(ctx context.Context, log *slog.Logger) error {
	cfg, err := loadConfig(os.Getenv)
	if err != nil {
		return err
	}
	p := mockprovider.New(cfg.provider)
	log.Info("mockprovider", "addr", cfg.addr, "latency", cfg.provider.Latency, "jitter", cfg.provider.Jitter, "fail_rate", cfg.provider.FailRate)
	srv := &http.Server{Addr: cfg.addr, Handler: p.Handler(), ReadHeaderTimeout: 5 * time.Second}
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	err = srv.ListenAndServe()
	if !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	<-drained
	return nil
}
