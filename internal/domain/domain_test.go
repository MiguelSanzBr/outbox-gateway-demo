package domain_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/domain"
)

func TestStatusMachine(t *testing.T) {
	cases := []struct {
		from, to domain.Status
		ok       bool
	}{
		{domain.Pending, domain.Sending, true},
		{domain.Sending, domain.Sent, true},
		{domain.Sending, domain.Dead, true},
		{domain.Sending, domain.Pending, true}, // retry ou lease vencido
		{domain.Pending, domain.Sent, false},  // não entrega sem claim
		{domain.Pending, domain.Dead, false},
		{domain.Sent, domain.Pending, false}, // sent e dead são finais
		{domain.Dead, domain.Sending, false},
		{domain.Sending, domain.Sending, false},
	}
	for _, c := range cases {
		if got := c.from.CanBecome(c.to); got != c.ok {
			t.Errorf("%s → %s = %v, esperado %v", c.from, c.to, got, c.ok)
		}
	}
}

func TestEntryTransitions(t *testing.T) {
	policy := domain.RetryPolicy{MaxAttempts: 3, BackoffBase: 100 * time.Millisecond, BackoffMax: time.Second}
	inFlight := func(attempts int) domain.OutboxEntry {
		return domain.OutboxEntry{ID: 1, Status: domain.Sending, Attempts: attempts, Lease: domain.Lease{Token: "tok", Worker: 2}}
	}

	e := inFlight(1)
	if tr, err := e.Complete(); err != nil || tr != (domain.Transition{Token: "tok", To: domain.Sent}) {
		t.Fatalf("Complete: %+v err=%v", tr, err)
	}

	tr, err := e.Fail(errors.New("provedor: HTTP 503"), policy)
	if err != nil || tr.Token != "tok" || tr.To != domain.Pending || tr.LastError != "provedor: HTTP 503" {
		t.Fatalf("falha temporária: %+v err=%v", tr, err)
	}
	if tr.RetryIn < 50*time.Millisecond || tr.RetryIn > 100*time.Millisecond {
		t.Fatalf("RetryIn = %s, esperado entre 50 e 100 ms", tr.RetryIn)
	}
	if tr, _ := e.Fail(domain.Permanent(errors.New("HTTP 400")), policy); tr.To != domain.Dead {
		t.Fatalf("erro permanente: %+v, esperado dead", tr)
	}
	last := inFlight(3)
	if tr, _ := last.Fail(errors.New("timeout"), policy); tr.To != domain.Dead {
		t.Fatalf("última tentativa: %+v, esperado dead", tr)
	}

	done := domain.OutboxEntry{Status: domain.Sent}
	if _, err := done.Complete(); !errors.Is(err, domain.ErrInvalidTransition) {
		t.Fatalf("Complete de sent: err=%v", err)
	}
	if _, err := done.Fail(errors.New("x"), policy); !errors.Is(err, domain.ErrInvalidTransition) {
		t.Fatalf("Fail de sent: err=%v", err)
	}
}

// Backoff exponencial com jitter: metade fixa, metade aleatória, com teto.
func TestBackoff(t *testing.T) {
	p := domain.RetryPolicy{BackoffBase: 100 * time.Millisecond, BackoffMax: time.Second}
	for range 100 {
		if d := p.Backoff(2); d < 100*time.Millisecond || d > 200*time.Millisecond {
			t.Fatalf("Backoff(2) = %s", d)
		}
		if d := p.Backoff(1000); d < 500*time.Millisecond || d > time.Second {
			t.Fatalf("Backoff(1000) = %s, esperado no teto", d)
		}
	}
}

func TestNewMessage(t *testing.T) {
	long := func(n int) string { return strings.Repeat("x", n) }
	if m, err := domain.NewMessage("k", "ana", "oi"); err != nil || m != (domain.Message{IdempotencyKey: "k", Recipient: "ana", Body: "oi"}) {
		t.Fatalf("válida: %+v err=%v", m, err)
	}
	cases := []struct {
		name, key, recipient, body string
		want                       error
	}{
		{"sem chave", "", "ana", "oi", domain.ErrInvalidKey},
		{"chave longa", long(201), "ana", "oi", domain.ErrInvalidKey},
		{"sem destinatário", "k", "", "oi", domain.ErrInvalidContent},
		{"destinatário longo", "k", long(65), "oi", domain.ErrInvalidContent},
		{"sem corpo", "k", "ana", "", domain.ErrInvalidContent},
		{"corpo longo", "k", "ana", long(4097), domain.ErrInvalidContent},
	}
	for _, c := range cases {
		if _, err := domain.NewMessage(c.key, c.recipient, c.body); !errors.Is(err, c.want) {
			t.Errorf("%s: err=%v, esperado %v", c.name, err, c.want)
		}
	}
}

func TestRecipientOrder(t *testing.T) {
	var o domain.RecipientOrder
	steps := []struct {
		recipient string
		seq       int64
		ok        bool
	}{{"ana", 1, true}, {"ana", 3, true}, {"bia", 2, true}, {"ana", 2, false}, {"ana", 3, false}, {"ana", 4, true}}
	for _, s := range steps {
		if got := o.Observe(s.recipient, s.seq); got != s.ok {
			t.Errorf("Observe(%s, %d) = %v, esperado %v", s.recipient, s.seq, got, s.ok)
		}
	}
}
