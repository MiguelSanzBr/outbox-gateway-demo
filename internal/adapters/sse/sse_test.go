package sse_test

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/adapters/postgres"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/adapters/sse"
	appsvc "github.com/MiguelSanzBr/outbox-gateway-demo/internal/app"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/domain"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/testdb"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// stream abre o SSE e devolve as linhas num canal; o canal fecha quando o servidor encerra o stream.
func stream(t *testing.T, url string) (*http.Response, <-chan string) {
	t.Helper()
	resp, err := http.Get(url + "/v1/events")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("SSE: HTTP %d %q", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	lines := make(chan string, 100)
	go func() {
		defer close(lines)
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			lines <- sc.Text()
		}
	}()
	return resp, lines
}

func waitLine(t *testing.T, lines <-chan string, match func(string) bool) {
	t.Helper()
	timeout := time.After(5 * time.Second)
	for {
		select {
		case l, ok := <-lines:
			if !ok {
				t.Fatal("stream fechou antes da linha esperada")
			}
			if match(l) {
				return
			}
		case <-timeout:
			t.Fatal("linha esperada não chegou")
		}
	}
}

func waitClosed(t *testing.T, lines <-chan string) {
	t.Helper()
	timeout := time.After(5 * time.Second)
	for {
		select {
		case _, ok := <-lines:
			if !ok {
				return
			}
		case <-timeout:
			t.Fatal("stream não fechou")
		}
	}
}

// listeners conta as conexões LISTEN abertas por este teste (application_name = schema).
func listeners(t *testing.T, pool *pgxpool.Pool, app string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*) FROM pg_stat_activity WHERE application_name = $1 AND query = 'LISTEN outbox_events'`,
		app).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func waitListeners(t *testing.T, pool *pgxpool.Pool, app string, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for listeners(t, pool, app) != want {
		if time.Now().After(deadline) {
			t.Fatalf("conexões LISTEN = %d, esperado %d", listeners(t, pool, app), want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Mudança de estado vira evento SSE, com ping no meio pra proxy não derrubar a conexão parada.
// Todos os streams dividem um LISTEN só; acima do teto o stream recebe 503. Cliente q sai
// libera o lugar e, sem ninguém, a conexão do LISTEN; servidor desligando fecha o stream.
func TestEventsStream(t *testing.T) {
	pool, store := testdb.New(t, "t_api_sse")
	app := testdb.AppName(pool)
	ctx, shutdown := context.WithCancel(context.Background())
	defer shutdown()
	svc := appsvc.NewService(store, nil, sse.NewHub(store, 2, 64), appsvc.Config{})
	srv := httptest.NewServer(sse.Handler(ctx, svc, 20*time.Millisecond, quiet))
	t.Cleanup(srv.Close)

	resp, lines := stream(t, srv.URL)
	if resp.Header.Get("Cache-Control") != "no-cache" {
		t.Errorf("Cache-Control = %q", resp.Header.Get("Cache-Control"))
	}
	resp2, _ := stream(t, srv.URL)
	waitListeners(t, pool, app, 1)
	if full, err := http.Get(srv.URL + "/v1/events"); err != nil || full.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("terceiro stream com teto 2: %v %v, esperado 503", full.StatusCode, err)
	} else {
		_ = full.Body.Close()
	}

	if _, _, err := store.Enqueue(context.Background(), domain.Message{IdempotencyKey: "k", Recipient: "sse-ana", Body: "oi"}); err != nil {
		t.Fatal(err)
	}
	waitLine(t, lines, func(l string) bool {
		var e struct {
			Recipient, Status string
			Version           *int64
		}
		data, ok := strings.CutPrefix(l, "data: ")
		return ok && json.Unmarshal([]byte(data), &e) == nil && e.Recipient == "sse-ana" && e.Status == "pending" && e.Version != nil
	})
	waitLine(t, lines, func(l string) bool { return l == ": ping" })

	_ = resp.Body.Close()
	_ = resp2.Body.Close()
	waitListeners(t, pool, app, 0)

	_, lines = stream(t, srv.URL)
	waitListeners(t, pool, app, 1)
	shutdown()
	waitClosed(t, lines)
	waitListeners(t, pool, app, 0)
}

func TestEventsWithDBDown(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), "postgres://x:x@127.0.0.1:1/x?connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	store := postgres.NewStore(pool)
	srv := httptest.NewServer(sse.Handler(context.Background(), appsvc.NewService(store, nil, sse.NewHub(store, 2, 64), appsvc.Config{}), time.Second, quiet))
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/v1/events")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("HTTP %d, esperado 500", resp.StatusCode)
	}
}
