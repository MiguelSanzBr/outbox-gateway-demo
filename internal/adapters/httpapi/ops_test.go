package httpapi_test

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/adapters/httpapi"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/app"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/domain"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/testdb"
)

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

// /healthz só diz q o processo está de pé; /readyz toca no banco, pra o orquestrador
// tirar o tráfego quando o Postgres cai.
func TestReadyz(t *testing.T) {
	_, store := testdb.New(t, "t_ready")
	ok := httptest.NewServer(api(context.Background(), store, nil))
	t.Cleanup(ok.Close)
	down := httptest.NewServer(api(context.Background(), testdb.Broken(t, "t_ready_down"), nil))
	t.Cleanup(down.Close)

	if code, _ := get(t, ok.URL+"/readyz"); code != http.StatusNoContent {
		t.Fatalf("readyz com banco: HTTP %d", code)
	}
	if code, _ := get(t, down.URL+"/readyz"); code != http.StatusServiceUnavailable {
		t.Fatalf("readyz sem banco: HTTP %d, esperado 503", code)
	}
	if code, _ := get(t, down.URL+"/healthz"); code != http.StatusNoContent {
		t.Fatalf("healthz sem banco: HTTP %d, esperado 204", code)
	}
}

// /metrics em texto do Prometheus: fila por estado, idade da mais antiga não entregue
// (o SLI de uma outbox) e os contadores de entrega. Sem banco, os contadores continuam.
func TestMetrics(t *testing.T) {
	_, store := testdb.New(t, "t_metrics")
	ctx := context.Background()
	for _, k := range []string{"a", "b"} {
		if _, _, err := store.Enqueue(ctx, domain.Message{IdempotencyKey: k, Recipient: "ana", Body: k}); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(20 * time.Millisecond)
	srv := httptest.NewServer(api(ctx, store, nil))
	t.Cleanup(srv.Close)

	code, body := get(t, srv.URL+"/metrics")
	if code != http.StatusOK {
		t.Fatalf("HTTP %d", code)
	}
	for _, want := range []string{
		`# TYPE outbox_messages gauge`,
		`outbox_messages{status="queued"} 1`,
		`outbox_messages{status="pending"} 1`,
		`outbox_messages{status="sending"} 0`,
		`outbox_messages{status="dead"} 0`,
		`outbox_db_up 1`,
		`# TYPE outbox_deliveries_total counter`,
		`outbox_deliveries_total{result="sent"} 0`,
		`outbox_deliveries_total{result="lease_lost"} 0`,
		`outbox_reaped_total 0`,
	} {
		if !strings.Contains(body, want+"\n") {
			t.Errorf("/metrics sem %q:\n%s", want, body)
		}
	}
	age := regexp.MustCompile(`(?m)^outbox_oldest_unsent_seconds ([0-9.e+-]+)$`).FindStringSubmatch(body)
	if age == nil || age[1] == "0" {
		t.Errorf("idade da mais antiga: %v", age)
	}

	down := httptest.NewServer(api(ctx, testdb.Broken(t, "t_metrics_down"), nil))
	t.Cleanup(down.Close)
	_, body = get(t, down.URL+"/metrics")
	if !strings.Contains(body, "outbox_db_up 0\n") || !strings.Contains(body, `outbox_deliveries_total{result="sent"} 0`) ||
		strings.Contains(body, "outbox_messages{") {
		t.Errorf("/metrics sem banco:\n%s", body)
	}
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// Log de acesso com request id: o do cliente se for razoável, senão um novo aleatório, e volta
// no header da resposta. Sonda de saúde e scrape não entram no log. Erro interno sai
// com o mesmo id, pra dar pra achar a requisição.
func TestAccessLogWithRequestID(t *testing.T) {
	pool, store := testdb.New(t, "t_accesslog")
	logs := &syncBuffer{}
	log := slog.New(slog.NewTextHandler(logs, nil))
	srv := httptest.NewServer(httpapi.New(context.Background(), app.NewService(store, nil, store, app.Config{}), nil, log))
	t.Cleanup(srv.Close)

	send := func(id, path string) http.Header {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+path, nil)
		if id != "" {
			req.Header.Set("X-Request-Id", id)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		return resp.Header
	}
	if got := send("pedido-42", "/v1/messages/999").Get("X-Request-Id"); got != "pedido-42" {
		t.Fatalf("X-Request-Id = %q, esperado o do cliente", got)
	}
	for _, bad := range []string{"", "com espaço", strings.Repeat("x", 65)} {
		if got := send(bad, "/healthz").Get("X-Request-Id"); got == "" || got == bad {
			t.Fatalf("id %q: resposta com %q, esperado um novo", bad, got)
		}
	}
	send("", "/readyz")
	send("", "/metrics")

	out := logs.String()
	if !strings.Contains(out, `request_id=pedido-42 method=GET route="GET /v1/messages/{id}" path=/v1/messages/999 status=404`) {
		t.Fatalf("log de acesso:\n%s", out)
	}
	for _, probe := range []string{"/healthz", "/readyz", "/metrics"} {
		if strings.Contains(out, "path="+probe) {
			t.Fatalf("sonda %s entrou no log:\n%s", probe, out)
		}
	}

	pool.Close()
	send("erro-1", "/v1/messages/1")
	if !strings.Contains(logs.String(), `level=ERROR msg=get request_id=erro-1`) {
		t.Fatalf("erro interno sem o request id:\n%s", logs.String())
	}
}
