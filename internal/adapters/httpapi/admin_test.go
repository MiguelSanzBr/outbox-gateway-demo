package httpapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/adapters/httpapi"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/adapters/worker"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/domain"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/testdb"
)

type okSender struct{}

func (okSender) Send(context.Context, *domain.OutboxEntry) error { return nil }

func call(t *testing.T, method, url string) (int, []byte) {
	t.Helper()
	req, _ := http.NewRequest(method, url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var raw json.RawMessage
	_ = json.NewDecoder(resp.Body).Decode(&raw)
	return resp.StatusCode, raw
}

// Sem fleet (DEMO_ADMIN desligado) as rotas de admin nem existem.
func TestAdminDisabledByDefault(t *testing.T) {
	_, store := testdb.New(t, "t_api_noadmin")
	srv := httptest.NewServer(httpapi.New(context.Background(), store, nil, quiet))
	t.Cleanup(srv.Close)
	for _, c := range []struct{ method, path string }{
		{http.MethodGet, "/v1/admin/workers"},
		{http.MethodPost, "/v1/admin/workers/0/kill"},
		{http.MethodPost, "/v1/admin/workers/revive"},
	} {
		if code, _ := call(t, c.method, srv.URL+c.path); code != http.StatusNotFound {
			t.Errorf("%s %s: HTTP %d, esperado 404", c.method, c.path, code)
		}
	}
}

func TestAdminKillAndRevive(t *testing.T) {
	_, store := testdb.New(t, "t_api_admin")
	ctx, cancel := context.WithCancel(context.Background())
	fleet := worker.Start(ctx, store, okSender{}, worker.Config{
		Workers: 2, Lease: time.Second, PollInterval: 5 * time.Millisecond, ReapInterval: time.Second,
		MaxAttempts: 3, BackoffBase: time.Millisecond, BackoffMax: time.Millisecond,
	}, quiet)
	t.Cleanup(func() { cancel(); fleet.Wait() })
	srv := httptest.NewServer(httpapi.New(ctx, store, fleet, quiet))
	t.Cleanup(srv.Close)

	steps := []struct {
		method, path string
		code         int
		body         string
	}{
		{http.MethodGet, "/v1/admin/workers", 200, `[{"id":0,"alive":true},{"id":1,"alive":true}]`},
		{http.MethodPost, "/v1/admin/workers/1/kill", 204, ""},
		{http.MethodPost, "/v1/admin/workers/1/kill", 404, `{"error":"worker não existe ou já caiu"}`},
		{http.MethodPost, "/v1/admin/workers/abc/kill", 404, `{"error":"worker não existe ou já caiu"}`},
		{http.MethodGet, "/v1/admin/workers", 200, `[{"id":0,"alive":true},{"id":1,"alive":false}]`},
		{http.MethodPost, "/v1/admin/workers/revive", 200, `{"revived":1}`},
		{http.MethodGet, "/v1/admin/workers", 200, `[{"id":0,"alive":true},{"id":1,"alive":true}]`},
	}
	for i, s := range steps {
		code, body := call(t, s.method, srv.URL+s.path)
		if code != s.code || string(body) != s.body {
			t.Fatalf("passo %d %s %s: HTTP %d %s, esperado %d %s", i, s.method, s.path, code, body, s.code, s.body)
		}
	}
}
