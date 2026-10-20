package main

import (
	"net"
	"net/http"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"
)

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

// SIGINT de verdade no próprio processo: main tem q desligar limpo, sem exit(1).
func TestMainGracefulShutdown(t *testing.T) {
	addr := freeAddr(t)
	t.Setenv("ADDR", addr)
	exited := -1
	exit = func(code int) { exited = code }
	t.Cleanup(func() { exit = os.Exit })

	done := make(chan struct{})
	go func() { main(); close(done) }()

	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := http.Get("http://" + addr + "/stats")
		if err == nil {
			_ = resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("mock não subiu: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	if err := syscall.Kill(os.Getpid(), syscall.SIGINT); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("main não terminou depois do SIGINT")
	}
	if exited != -1 {
		t.Fatalf("exit(%d) chamado num shutdown limpo", exited)
	}
}

func TestMainExitsWithOneOnError(t *testing.T) {
	t.Setenv("ADDR", "porta-que-nao-existe")
	t.Setenv("LATENCY", "")
	exited := -1
	exit = func(code int) { exited = code }
	t.Cleanup(func() { exit = os.Exit })

	main()
	if exited != 1 {
		t.Fatalf("exit = %d, esperado 1", exited)
	}
}

// Com ADDR vazio run tenta o :9090. Seguro a porta antes (ou ela já tá ocupada pelo compose), então tem q dar erro de porta em uso.
func TestDefaultAddr(t *testing.T) {
	t.Setenv("ADDR", "")
	if l, err := net.Listen("tcp", ":9090"); err == nil {
		defer func() { _ = l.Close() }()
	}
	if err := run(t.Context()); err == nil {
		t.Fatal("esperava erro de porta em uso no :9090")
	}
}

func TestLoadConfig(t *testing.T) {
	get := func(kv map[string]string) func(string) string { return func(k string) string { return kv[k] } }
	c, err := loadConfig(get(map[string]string{"LATENCY": "80ms", "JITTER": "120ms", "FAIL_RATE": "0.05"}))
	if err != nil || c.addr != ":9090" || c.provider.Latency != 80*time.Millisecond || c.provider.Jitter != 120*time.Millisecond || c.provider.FailRate != 0.05 {
		t.Fatalf("config: %+v err=%v", c, err)
	}
	if c, err := loadConfig(get(nil)); err != nil || c.provider.Latency != 0 {
		t.Fatalf("vazio: %+v err=%v", c, err)
	}
	_, err = loadConfig(get(map[string]string{"LATENCY": "80", "JITTER": "-1s", "FAIL_RATE": "2"}))
	for _, k := range []string{"LATENCY", "JITTER", "FAIL_RATE"} {
		if err == nil || !strings.Contains(err.Error(), k) {
			t.Errorf("erro %v sem %s", err, k)
		}
	}
	if _, err := loadConfig(get(map[string]string{"FAIL_RATE": "x"})); err == nil {
		t.Error("FAIL_RATE=x sem erro")
	}
}
