package mockprovider_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/adapters/mockprovider"
)

func send(h http.Handler, ctx context.Context, key, seq, body string) int {
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/v1/messages", strings.NewReader(body))
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	if seq != "" {
		req.Header.Set("X-Sequence", seq)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Code
}

func TestRejectsBadPayload(t *testing.T) {
	h := mockprovider.New(mockprovider.Config{}).Handler()
	cases := []struct{ name, key, seq, body string }{
		{"json quebrado", "k", "1", `{`},
		{"sem destinatário", "k", "1", `{"body":"oi"}`},
		{"sem chave", "", "1", `{"to":"ana","body":"oi"}`},
		{"sequência inválida", "k", "x", `{"to":"ana","body":"oi"}`},
	}
	for _, c := range cases {
		if code := send(h, context.Background(), c.key, c.seq, c.body); code != http.StatusBadRequest {
			t.Errorf("%s: HTTP %d, esperado 400", c.name, code)
		}
	}
}

// Sequência menor q a última do destinatário conta como violação de ordem, mesmo com chave nova.
func TestCountsOrderViolation(t *testing.T) {
	p := mockprovider.New(mockprovider.Config{})
	h := p.Handler()
	send(h, context.Background(), "k2", "2", `{"to":"ana","body":"b"}`)
	send(h, context.Background(), "k1", "1", `{"to":"ana","body":"a"}`)

	if st := p.Stats(); st.Accepted != 2 || st.OrderViolations != 1 {
		t.Fatalf("stats: %+v", st)
	}
}

// Cliente desistiu durante a latência: o mock não conta nada como entregue.
func TestClientGaveUpDuringLatency(t *testing.T) {
	p := mockprovider.New(mockprovider.Config{Latency: time.Hour})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	send(p.Handler(), ctx, "k", "1", `{"to":"ana","body":"oi"}`)

	if st := p.Stats(); st.Accepted != 0 {
		t.Fatalf("aceitou com o cliente já fora: %+v", st)
	}
}

func TestStatsEndpoint(t *testing.T) {
	p := mockprovider.New(mockprovider.Config{})
	h := p.Handler()
	send(h, context.Background(), "k", "1", `{"to":"ana","body":"oi"}`)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/stats", nil))
	var st mockprovider.Stats
	if err := json.NewDecoder(rec.Body).Decode(&st); err != nil || st.Accepted != 1 {
		t.Fatalf("GET /stats: %+v err=%v", st, err)
	}
}
