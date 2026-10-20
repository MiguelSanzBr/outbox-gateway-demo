package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/testdb"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func needDB(t *testing.T) string { return testdb.URL(t) }

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

func waitHealthy(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, err := http.Get("http://" + addr + "/healthz")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusNoContent {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("gateway não ficou de pé em %s: %v", addr, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func env(kv ...string) func(string) string {
	m := map[string]string{"DATABASE_URL": "postgres://x@y/z", "PROVIDER_URL": "http://mock:9090/v1/messages"}
	for i := 0; i < len(kv); i += 2 {
		m[kv[i]] = kv[i+1]
	}
	return func(k string) string { return m[k] }
}

func TestLoadConfigDefaults(t *testing.T) {
	c, err := loadConfig(env())
	if err != nil {
		t.Fatal(err)
	}
	if c.Worker.Workers != 16 || c.App.Lease != 30*time.Second || c.App.Retry.MaxAttempts != 8 ||
		c.Worker.ReapInterval != 5*time.Second || c.Addr != ":8080" || c.DemoAdmin || c.MaxStreams != 32 {
		t.Fatalf("padrões: %+v", c)
	}
	if c, _ := loadConfig(env("WORKERS", "4", "LEASE", "6s", "DEMO_ADMIN", "1")); c.Worker.Workers != 4 || c.App.Lease != 6*time.Second || !c.DemoAdmin {
		t.Fatalf("do ambiente: %+v", c)
	}
}

// Config errada falha no boot, com todos os problemas juntos, em vez de cair no padrão
// em silêncio (WORKERS=abc subia 16) ou dar panic com o servidor no ar (REAP_INTERVAL=0s).
func TestLoadConfigFailsFast(t *testing.T) {
	cases := []struct {
		env  func(string) string
		want []string
	}{
		{env("WORKERS", "abc"), []string{"WORKERS"}},
		{env("WORKERS", "0"), []string{"WORKERS"}},
		{env("REAP_INTERVAL", "0s"), []string{"REAP_INTERVAL"}},
		{env("POLL_INTERVAL", "-1s"), []string{"POLL_INTERVAL"}},
		{env("LEASE", "10"), []string{"LEASE"}}, // sem unidade
		{env("MAX_ATTEMPTS", "0"), []string{"MAX_ATTEMPTS"}},
		{env("BACKOFF_BASE", "0s"), []string{"BACKOFF_BASE"}},
		{env("BACKOFF_BASE", "2s", "BACKOFF_MAX", "1s"), []string{"BACKOFF_MAX"}},
		{env("BACKOFF_MAX", "48h"), []string{"BACKOFF_MAX"}},
		{env("SSE_MAX_STREAMS", "0"), []string{"SSE_MAX_STREAMS"}},
		{env("PROVIDER_URL", ""), []string{"PROVIDER_URL"}},
		{env("PROVIDER_URL", "ftp://mock/x"), []string{"PROVIDER_URL"}},
		{env("PROVIDER_URL", "http://%zz"), []string{"PROVIDER_URL"}},
		{env("DATABASE_URL", ""), []string{"DATABASE_URL"}},
		{env("WORKERS", "x", "LEASE", "0s"), []string{"WORKERS", "LEASE"}},
	}
	for _, c := range cases {
		_, err := loadConfig(c.env)
		if err == nil {
			t.Errorf("%v: sem erro", c.want)
			continue
		}
		for _, w := range c.want {
			if !strings.Contains(err.Error(), w) {
				t.Errorf("erro %q sem %s", err, w)
			}
		}
	}
}

// gatewayEnv é o mínimo pra run subir nos testes.
func gatewayEnv(t *testing.T, addr string) {
	t.Setenv("ADDR", addr)
	t.Setenv("WORKERS", "2")
	t.Setenv("PROVIDER_URL", "http://127.0.0.1:1/v1/messages")
}

func TestRunFailsFast(t *testing.T) {
	cases := []struct{ name, url, workers string }{
		{"config inválida", "postgres://x:x@127.0.0.1:1/x", "abc"},
		{"url que nem parseia", "postgres://%zz", "2"},
		{"banco fora do ar", "postgres://x:x@127.0.0.1:1/x?connect_timeout=1", "2"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			gatewayEnv(t, "127.0.0.1:0")
			t.Setenv("DATABASE_URL", c.url)
			t.Setenv("WORKERS", c.workers)
			if err := run(context.Background(), quiet); err == nil {
				t.Fatal("esperava erro")
			}
		})
	}
}

// Porta inválida: run tem q devolver o erro e só voltar depois de parar os workers.
func TestRunListenErrorStopsWorkers(t *testing.T) {
	needDB(t)
	gatewayEnv(t, "porta-que-nao-existe")
	done := make(chan error)
	go func() { done <- run(context.Background(), quiet) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("esperava erro de listen")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run travou depois do erro de listen")
	}
}

// SIGINT de verdade no próprio processo: main tem q desligar limpo, sem exit(1).
func TestMainGracefulShutdown(t *testing.T) {
	needDB(t)
	addr := freeAddr(t)
	gatewayEnv(t, addr)
	exited := -1
	exit = func(code int) { exited = code }
	t.Cleanup(func() { exit = os.Exit })

	done := make(chan struct{})
	go func() { main(); close(done) }()
	waitHealthy(t, addr)

	if err := syscall.Kill(os.Getpid(), syscall.SIGINT); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("main não terminou depois do SIGINT")
	}
	if exited != -1 {
		t.Fatalf("exit(%d) chamado num shutdown limpo", exited)
	}
}

func TestMainExitsWithOneOnError(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://%zz")
	exited := -1
	exit = func(code int) { exited = code }
	t.Cleanup(func() { exit = os.Exit })

	main()
	if exited != 1 {
		t.Fatalf("exit = %d, esperado 1", exited)
	}
}

// Admin da demo só existe com DEMO_ADMIN=1. E o shutdown fecha os streams SSE abertos.
func TestRunDemoAdminFlagAndSSEShutdown(t *testing.T) {
	needDB(t)
	for _, c := range []struct {
		flag string
		want int
	}{{"", http.StatusNotFound}, {"0", http.StatusNotFound}, {"1", http.StatusOK}} {
		t.Run("DEMO_ADMIN="+c.flag, func(t *testing.T) {
			addr := freeAddr(t)
			gatewayEnv(t, addr)
			t.Setenv("DEMO_ADMIN", c.flag)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error)
			go func() { done <- run(ctx, quiet) }()
			waitHealthy(t, addr)

			resp, err := http.Get("http://" + addr + "/v1/admin/workers")
			if err != nil {
				t.Fatal(err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != c.want {
				t.Fatalf("/v1/admin/workers: HTTP %d, esperado %d", resp.StatusCode, c.want)
			}

			sse, err := http.Get("http://" + addr + "/v1/events")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = sse.Body.Close() }()
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("shutdown preso no stream SSE")
			}
			// o stream tem q terminar junto, não ficar pendurado com a conexão LISTEN aberta
			eof := make(chan struct{})
			go func() { _, _ = io.Copy(io.Discard, sse.Body); close(eof) }()
			select {
			case <-eof:
			case <-time.After(3 * time.Second):
				t.Fatal("stream SSE continuou aberto depois do shutdown")
			}
		})
	}
}

// Shutdown com requisição em voo: ela termina com 202 antes da fleet parar e do pool
// fechar. Antes o run seguia assim q o ListenAndServe voltava e a requisição pegava
// o pool já fechado.
func TestRunWaitsInFlightRequests(t *testing.T) {
	needDB(t)
	addr := freeAddr(t)
	gatewayEnv(t, addr)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error)
	go func() { done <- run(ctx, quiet) }()
	waitHealthy(t, addr)

	body, w := io.Pipe()
	req, _ := http.NewRequest(http.MethodPost, "http://"+addr+"/v1/messages", body)
	req.Header.Set("Idempotency-Key", fmt.Sprintf("drain-%d", time.Now().UnixNano()))
	resp := make(chan *http.Response, 1)
	go func() {
		r, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Error(err)
		}
		resp <- r
	}()
	_, _ = io.WriteString(w, `{"recipient":"drain",`)
	time.Sleep(100 * time.Millisecond) // a requisição já está no handler, lendo o corpo
	cancel()
	time.Sleep(200 * time.Millisecond)
	_, _ = io.WriteString(w, `"body":"oi"}`)
	_ = w.Close()

	r := <-resp
	if r == nil {
		t.Fatal("sem resposta")
	}
	_ = r.Body.Close()
	if r.StatusCode != http.StatusAccepted {
		t.Fatalf("requisição em voo no shutdown: HTTP %d, esperado 202", r.StatusCode)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
