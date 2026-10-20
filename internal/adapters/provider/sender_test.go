package provider_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/adapters/provider"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/domain"
)

var entry = &domain.OutboxEntry{ID: 42, IdempotencyKey: "k", Recipient: "ana", Body: "oi"}

// retryIn é o RetryIn q o domínio tiraria do erro, com backoff zerado pra isolar o Retry-After.
func retryIn(err error) time.Duration {
	tr, _ := domain.OutboxEntry{Status: domain.Sending, Attempts: 1}.Fail(err, domain.RetryPolicy{MaxAttempts: 5})
	return tr.RetryIn
}

// A política do README: 2xx entrega, 408/425/429/5xx e rede tentam de novo, o resto
// dos 4xx é permanente. Retry-After (segundos ou data) vira piso do backoff.
func TestSenderClassifiesResponses(t *testing.T) {
	cases := []struct {
		code       int
		retryAfter string
		ok, perm   bool
		after      time.Duration
	}{
		{code: 200, ok: true},
		{code: 202, ok: true},
		{code: 408},
		{code: 425},
		{code: 429, retryAfter: "7", after: 7 * time.Second},
		{code: 503, retryAfter: time.Now().Add(time.Minute).UTC().Format(http.TimeFormat), after: 50 * time.Second},
		{code: 503, retryAfter: "lixo"},
		{code: 500},
		{code: 400, perm: true},
		{code: 404, perm: true},
		{code: 409, perm: true},
	}
	for _, c := range cases {
		var got http.Header
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got = r.Header.Clone()
			if c.retryAfter != "" {
				w.Header().Set("Retry-After", c.retryAfter)
			}
			w.WriteHeader(c.code)
		}))
		err := provider.HTTPSender{URL: srv.URL, Client: srv.Client()}.Send(context.Background(), entry)
		srv.Close()
		name := strconv.Itoa(c.code) + " " + c.retryAfter
		switch {
		case c.ok && err != nil:
			t.Errorf("%s: err=%v, esperado entregue", name, err)
		case !c.ok && err == nil:
			t.Errorf("%s: sem erro", name)
		case !c.ok && domain.IsPermanent(err) != c.perm:
			t.Errorf("%s: permanente=%v, esperado %v", name, domain.IsPermanent(err), c.perm)
		case c.after > 0 && retryIn(err) < c.after:
			t.Errorf("%s: RetryIn=%s, esperado pelo menos %s", name, retryIn(err), c.after)
		case c.after == 0 && err != nil && retryIn(err) != 0:
			t.Errorf("%s: RetryIn=%s sem Retry-After válido", name, retryIn(err))
		}
		if got.Get("Idempotency-Key") != "k" || got.Get("X-Sequence") != "42" {
			t.Errorf("%s: headers %v, esperado a chave e o id da mensagem", name, got)
		}
	}
}

func TestSenderErrors(t *testing.T) {
	bad := provider.HTTPSender{URL: "http://host com espaço/", Client: http.DefaultClient}
	if err := bad.Send(context.Background(), entry); !domain.IsPermanent(err) {
		t.Errorf("URL inválida: err=%v, esperado permanente", err)
	}

	srv := httptest.NewServer(nil)
	srv.Close()
	down := provider.HTTPSender{URL: srv.URL, Client: srv.Client()}
	if err := down.Send(context.Background(), entry); err == nil || domain.IsPermanent(err) {
		t.Errorf("provedor fora do ar: err=%v, esperado erro temporário", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := down.Send(ctx, entry); !errors.Is(err, context.Canceled) {
		t.Errorf("ctx cancelado: err=%v", err)
	}
}
