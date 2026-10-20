package main

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/adapters/worker"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/app"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/domain"
)

type config struct {
	DatabaseURL string
	ProviderURL string
	Addr        string
	DemoAdmin   bool
	MaxStreams  int
	Worker      worker.Config
	App         app.Config
}

// envReader lê variáveis com padrão e junta os erros de parse, pra o boot mostrar todos de uma vez.
type envReader struct {
	get  func(string) string
	errs []error
}

func (e *envReader) str(k, def string) string {
	if v := e.get(k); v != "" {
		return v
	}
	return def
}

func (e *envReader) int(k string, def int) int {
	v := e.get(k)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s=%q não é um número", k, v))
		return def
	}
	return n
}

func (e *envReader) dur(k string, def time.Duration) time.Duration {
	v := e.get(k)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s=%q não é uma duração (ex.: 30s, 200ms)", k, v))
		return def
	}
	return d
}

func (e *envReader) check(ok bool, format string, args ...any) {
	if !ok {
		e.errs = append(e.errs, fmt.Errorf(format, args...))
	}
}

// loadConfig lê e valida a config. Valor errado falha aqui, antes de abrir o pool,
// em vez de cair no padrão em silêncio ou dar panic com o servidor já no ar.
func loadConfig(getenv func(string) string) (config, error) {
	e := &envReader{get: getenv}
	c := config{
		DatabaseURL: e.str("DATABASE_URL", ""),
		ProviderURL: e.str("PROVIDER_URL", ""),
		Addr:        e.str("ADDR", ":8080"),
		DemoAdmin:   e.get("DEMO_ADMIN") == "1",
		MaxStreams:  e.int("SSE_MAX_STREAMS", 32),
		Worker: worker.Config{
			Workers:      e.int("WORKERS", 16),
			PollInterval: e.dur("POLL_INTERVAL", 50*time.Millisecond),
			ReapInterval: e.dur("REAP_INTERVAL", 5*time.Second),
		},
		App: app.Config{
			Lease: e.dur("LEASE", 30*time.Second),
			Retry: domain.RetryPolicy{
				MaxAttempts: e.int("MAX_ATTEMPTS", 8),
				BackoffBase: e.dur("BACKOFF_BASE", 200*time.Millisecond),
				BackoffMax:  e.dur("BACKOFF_MAX", 30*time.Second),
			},
		},
	}
	r := c.App.Retry
	e.check(c.DatabaseURL != "", "DATABASE_URL é obrigatório")
	u, err := url.Parse(c.ProviderURL)
	e.check(err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Host != "",
		"PROVIDER_URL=%q precisa ser uma URL http(s)", c.ProviderURL)
	e.check(c.Worker.Workers >= 1 && c.Worker.Workers <= 1024, "WORKERS precisa ficar entre 1 e 1024")
	e.check(c.Worker.PollInterval > 0, "POLL_INTERVAL precisa ser maior q zero")
	e.check(c.Worker.ReapInterval > 0, "REAP_INTERVAL precisa ser maior q zero")
	e.check(c.App.Lease > 0, "LEASE precisa ser maior q zero (o envio usa até metade dele)")
	e.check(r.MaxAttempts >= 1, "MAX_ATTEMPTS precisa ser pelo menos 1")
	e.check(r.BackoffBase > 0, "BACKOFF_BASE precisa ser maior q zero")
	e.check(r.BackoffMax >= r.BackoffBase && r.BackoffMax <= 24*time.Hour, "BACKOFF_MAX precisa ficar entre BACKOFF_BASE e 24h")
	e.check(c.MaxStreams >= 1, "SSE_MAX_STREAMS precisa ser pelo menos 1")
	return c, errors.Join(e.errs...)
}
