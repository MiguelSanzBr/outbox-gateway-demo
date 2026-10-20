package httpapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/adapters/worker"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/app"
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

// Redrive: dead volta pra fila; o resto responde com o motivo.
func TestAdminRequeueDead(t *testing.T) {
	_, store := testdb.New(t, "t_api_requeue")
	ctx := context.Background()
	dead, _, err := store.Enqueue(ctx, domain.Message{IdempotencyKey: "k1", Recipient: "ana", Body: "oi"})
	if err != nil {
		t.Fatal(err)
	}
	c, err := store.Claim(ctx, 0, time.Minute)
	if err != nil || c == nil {
		t.Fatalf("claim: %v", err)
	}
	tr, _ := c.Fail(domain.Permanent(errors.New("HTTP 400")), domain.RetryPolicy{MaxAttempts: 1})
	if err := store.Finish(ctx, c.ID, tr); err != nil {
		t.Fatal(err)
	}
	pending, _, err := store.Enqueue(ctx, domain.Message{IdempotencyKey: "k2", Recipient: "bia", Body: "oi"})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(api(ctx, store, app.NewDemo(okPool{})))
	t.Cleanup(srv.Close)

	path := func(id any) string { return fmt.Sprintf("%s/v1/admin/messages/%v/requeue", srv.URL, id) }
	for _, s := range []struct {
		url  string
		code int
	}{
		{path(dead), http.StatusNoContent},
		{path(dead), http.StatusConflict}, // já não é mais dead
		{path(pending), http.StatusConflict},
		{path(999), http.StatusNotFound},
		{path("abc"), http.StatusBadRequest},
	} {
		if code, body := call(t, http.MethodPost, s.url); code != s.code {
			t.Fatalf("POST %s: HTTP %d %s, esperado %d", s.url, code, body, s.code)
		}
	}
	if st, _ := store.Get(ctx, dead); st.Status != domain.Pending || st.Attempts != 0 {
		t.Fatalf("depois do redrive: %+v", st)
	}

	down := httptest.NewServer(api(ctx, testdb.Broken(t, "t_api_requeue_down"), app.NewDemo(okPool{})))
	t.Cleanup(down.Close)
	if code, body := call(t, http.MethodPost, down.URL+"/v1/admin/messages/1/requeue"); code != http.StatusInternalServerError {
		t.Fatalf("redrive com banco fora: HTTP %d %s, esperado 500", code, body)
	}
}

// okPool é a fleet de mentira, pra ligar as rotas de admin sem subir worker.
type okPool struct{}

func (okPool) Workers() []app.WorkerInfo { return nil }
func (okPool) Kill(int) bool             { return false }
func (okPool) Revive() int               { return 0 }

// Sem fleet (DEMO_ADMIN desligado) as rotas de admin nem existem.
func TestAdminDisabledByDefault(t *testing.T) {
	_, store := testdb.New(t, "t_api_noadmin")
	srv := httptest.NewServer(api(context.Background(), store, nil))
	t.Cleanup(srv.Close)
	for _, c := range []struct{ method, path string }{
		{http.MethodGet, "/v1/admin/workers"},
		{http.MethodPost, "/v1/admin/workers/0/kill"},
		{http.MethodPost, "/v1/admin/workers/revive"},
		{http.MethodPost, "/v1/admin/messages/1/requeue"},
	} {
		if code, _ := call(t, c.method, srv.URL+c.path); code != http.StatusNotFound {
			t.Errorf("%s %s: HTTP %d, esperado 404", c.method, c.path, code)
		}
	}
}

func TestAdminKillAndRevive(t *testing.T) {
	_, store := testdb.New(t, "t_api_admin")
	ctx, cancel := context.WithCancel(context.Background())
	svc := app.NewService(store, okSender{}, store, app.Config{Lease: time.Second, Retry: domain.RetryPolicy{MaxAttempts: 3}})
	fleet := worker.Start(ctx, svc, worker.Config{Workers: 2, PollInterval: 5 * time.Millisecond, ReapInterval: time.Second}, quiet)
	t.Cleanup(func() { cancel(); fleet.Wait() })
	srv := httptest.NewServer(api(ctx, store, app.NewDemo(fleet)))
	t.Cleanup(srv.Close)

	steps := []struct {
		method, path string
		code         int
		body         string
	}{
		{http.MethodGet, "/v1/admin/workers", 200, `[{"id":0,"alive":true},{"id":1,"alive":true}]`},
		{http.MethodPost, "/v1/admin/workers/1/kill", 204, ""},
		{http.MethodPost, "/v1/admin/workers/1/kill", 404, `{"error":"worker não existe ou já caiu"}`},
		{http.MethodPost, "/v1/admin/workers/abc/kill", 400, `{"error":"id de worker inválido"}`},
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
