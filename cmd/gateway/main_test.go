package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"syscall"
	"testing"
	"time"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func needDB(t *testing.T) string {
	t.Helper()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL não definido; suba o Postgres com `docker compose up -d postgres`")
	}
	return url
}

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

func TestEnv(t *testing.T) {
	t.Setenv("T_STR", "x")
	t.Setenv("T_INT", "7")
	t.Setenv("T_DUR", "3s")
	t.Setenv("T_RUIM", "abc")

	if got := envStr("T_STR", "d"); got != "x" {
		t.Fatalf("envStr = %q", got)
	}
	if got := envStr("T_NADA", "d"); got != "d" {
		t.Fatalf("envStr sem valor = %q", got)
	}
	if got := envInt("T_INT", 1); got != 7 {
		t.Fatalf("envInt = %d", got)
	}
	if got := envInt("T_RUIM", 1); got != 1 {
		t.Fatalf("envInt inválido = %d", got)
	}
	if got := envDur("T_DUR", time.Second); got != 3*time.Second {
		t.Fatalf("envDur = %s", got)
	}
	if got := envDur("T_RUIM", time.Second); got != time.Second {
		t.Fatalf("envDur inválido = %s", got)
	}
}

func TestRunFailsFast(t *testing.T) {
	cases := []struct{ name, url, workers string }{
		{"url que nem parseia", "postgres://%zz", ""},
		{"pool sem conexão nenhuma", "postgres://x:x@127.0.0.1:1/x", "-16"},
		{"banco fora do ar", "postgres://x:x@127.0.0.1:1/x?connect_timeout=1", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
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
	t.Setenv("ADDR", "porta-que-nao-existe")
	t.Setenv("WORKERS", "2")
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
	t.Setenv("ADDR", addr)
	t.Setenv("WORKERS", "2")
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
