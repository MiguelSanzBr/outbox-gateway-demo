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
		{domain.Queued, domain.Pending, true}, // a cabeça do destinatário saiu, a próxima sobe
		{domain.Queued, domain.Sending, false}, // só a cabeça é reivindicada
		{domain.Pending, domain.Queued, false},
		{domain.Pending, domain.Sending, true},
		{domain.Sending, domain.Sent, true},
		{domain.Sending, domain.Dead, true},
		{domain.Sending, domain.Pending, true}, // retry ou lease vencido
		{domain.Pending, domain.Sent, false},   // não entrega sem claim
		{domain.Pending, domain.Dead, false},
		{domain.Sent, domain.Pending, false}, // sent é final
		{domain.Dead, domain.Sending, false},
		{domain.Dead, domain.Pending, true}, // dead só sai por redrive manual
		{domain.Dead, domain.Queued, true},
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
	if tr, err := e.Complete(); err != nil || tr != (domain.Transition{Token: "tok", To: domain.Sent, Attempts: 1}) {
		t.Fatalf("Complete: %+v err=%v", tr, err)
	}

	tr, err := e.Fail(errors.New("provedor: HTTP 503"), policy)
	if err != nil || tr.Token != "tok" || tr.To != domain.Pending || tr.Attempts != 1 || tr.LastError != "provedor: HTTP 503" {
		t.Fatalf("falha temporária: %+v err=%v", tr, err)
	}
	if tr.RetryIn < 50*time.Millisecond || tr.RetryIn > 100*time.Millisecond {
		t.Fatalf("RetryIn = %s, esperado entre 50 e 100 ms", tr.RetryIn)
	}
	// dead não tem retry: RetryIn só faz sentido voltando pra pending
	if tr, _ := e.Fail(domain.Permanent(errors.New("HTTP 400")), policy); tr.To != domain.Dead || tr.RetryIn != 0 {
		t.Fatalf("erro permanente: %+v, esperado dead sem RetryIn", tr)
	}
	last := inFlight(3)
	if tr, _ := last.Fail(errors.New("timeout"), policy); tr.To != domain.Dead || tr.RetryIn != 0 {
		t.Fatalf("última tentativa: %+v, esperado dead sem RetryIn", tr)
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
	// base de horas: o shift antigo estourava o int64 e o rand.N negativo dava panic
	hours := domain.RetryPolicy{BackoffBase: 3 * time.Hour, BackoffMax: 24 * time.Hour}
	if d := hours.Backoff(30); d < 12*time.Hour || d > 24*time.Hour {
		t.Fatalf("Backoff com base de 3 h = %s, esperado no teto de 24 h", d)
	}
}

// Lease vencido (worker sumiu): mesma régua do Fail. Antes da última tentativa volta
// com backoff; na última vira dead, mas dizendo q o resultado é incerto, pq o provedor
// pode ter recebido antes do worker morrer.
func TestExpire(t *testing.T) {
	policy := domain.RetryPolicy{MaxAttempts: 3, BackoffBase: 100 * time.Millisecond, BackoffMax: time.Second}
	e := domain.OutboxEntry{ID: 1, Status: domain.Sending, Attempts: 1, Lease: domain.Lease{Token: "tok"}}
	tr, err := e.Expire(policy)
	if err != nil || tr.Token != "tok" || tr.To != domain.Pending || tr.Attempts != 1 || tr.LastError != domain.ReasonLeaseExpired {
		t.Fatalf("Expire: %+v err=%v", tr, err)
	}
	if tr.RetryIn < 50*time.Millisecond || tr.RetryIn > 100*time.Millisecond {
		t.Fatalf("RetryIn = %s, esperado o backoff da política", tr.RetryIn)
	}

	e.Attempts = 3
	tr, err = e.Expire(policy)
	if err != nil || tr.To != domain.Dead || tr.RetryIn != 0 ||
		!strings.HasPrefix(tr.LastError, domain.ReasonLeaseExpired) || !strings.Contains(tr.LastError, "incerto") {
		t.Fatalf("Expire na última tentativa: %+v err=%v", tr, err)
	}

	done := domain.OutboxEntry{Status: domain.Sent}
	if _, err := done.Expire(policy); !errors.Is(err, domain.ErrInvalidTransition) {
		t.Fatalf("Expire de sent: err=%v", err)
	}
}

// Shutdown pegou a linha já reivindicada antes do envio: volta pra fila sem gastar
// tentativa e sem backoff, pq nada saiu pro provedor.
func TestRelease(t *testing.T) {
	e := domain.OutboxEntry{ID: 1, Status: domain.Sending, Attempts: 2, Lease: domain.Lease{Token: "tok"}}
	tr, err := e.Release()
	if err != nil || tr.Token != "tok" || tr.To != domain.Pending || tr.Attempts != 1 || tr.RetryIn != 0 {
		t.Fatalf("Release: %+v err=%v", tr, err)
	}
	done := domain.OutboxEntry{Status: domain.Dead}
	if _, err := done.Release(); !errors.Is(err, domain.ErrInvalidTransition) {
		t.Fatalf("Release de dead: err=%v", err)
	}
}

// Retry-After do provedor vira piso do backoff, com teto de 1 h pra um header maluco
// não segurar a mensagem por dias.
func TestRetryAfter(t *testing.T) {
	policy := domain.RetryPolicy{MaxAttempts: 5, BackoffBase: 100 * time.Millisecond, BackoffMax: time.Second}
	e := domain.OutboxEntry{Status: domain.Sending, Attempts: 1}
	cause := errors.New("provedor: HTTP 429")
	cases := []struct {
		after    time.Duration
		min, max time.Duration
	}{
		{10 * time.Second, 10 * time.Second, 10 * time.Second},
		{48 * time.Hour, time.Hour, time.Hour},
		{time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond}, // menor q o backoff: vale o backoff
	}
	for _, c := range cases {
		err := domain.RetryAfter(cause, c.after)
		if !errors.Is(err, cause) || domain.IsPermanent(err) {
			t.Fatalf("RetryAfter perdeu a causa ou virou permanente: %v", err)
		}
		tr, _ := e.Fail(err, policy)
		if tr.To != domain.Pending || tr.RetryIn < c.min || tr.RetryIn > c.max {
			t.Errorf("Retry-After %s: %+v, esperado RetryIn entre %s e %s", c.after, tr, c.min, c.max)
		}
	}
}

func TestNewMessage(t *testing.T) {
	long := func(n int) string { return strings.Repeat("x", n) }
	accents := func(n int) string { return strings.Repeat("é", n) } // 2 bytes cada
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
		// o limite é em bytes, e a mensagem de erro diz isso
		{"chave de 101 acentos (202 bytes)", accents(101), "ana", "oi", domain.ErrInvalidKey},
		// o Postgres recusa NUL e UTF-8 inválido em TEXT: sem essa checagem vira 500
		{"chave com UTF-8 inválido", "\xff\xfe", "ana", "oi", domain.ErrInvalidKey},
		{"chave com NUL", "a\x00b", "ana", "oi", domain.ErrInvalidKey},
		{"destinatário só com espaço", "k", "   ", "oi", domain.ErrInvalidContent},
		{"destinatário com NUL", "k", "a\x00", "oi", domain.ErrInvalidContent},
		{"corpo com NUL", "k", "ana", "x\x00y", domain.ErrInvalidContent},
		{"corpo com UTF-8 inválido", "k", "ana", "\xc3\x28", domain.ErrInvalidContent},
	}
	for _, c := range cases {
		if _, err := domain.NewMessage(c.key, c.recipient, c.body); !errors.Is(err, c.want) {
			t.Errorf("%s: err=%v, esperado %v", c.name, err, c.want)
		}
	}
	if _, err := domain.NewMessage(accents(100), "ana", "oi"); err != nil {
		t.Errorf("chave de 200 bytes: %v", err)
	}
	// o domínio não sabe de HTTP: quem fala de header é o adapter
	for _, err := range []error{domain.ErrInvalidKey, domain.ErrInvalidContent} {
		if msg := err.Error(); strings.Contains(msg, "header") || !strings.Contains(msg, "bytes") {
			t.Errorf("mensagem %q: sem header e com o limite em bytes", msg)
		}
	}
}

func TestRecipientOrder(t *testing.T) {
	var o domain.RecipientOrder
	steps := []struct {
		recipient string
		seq       int64
		ok        bool
	}{{"ana", 1, true}, {"ana", 3, true}, {"bia", 2, true}, {"ana", 2, false}, {"ana", 3, true}, {"ana", 3, false}}
	for _, s := range steps {
		if got := o.Observe(s.recipient, s.seq); got != s.ok {
			t.Errorf("Observe(%s, %d) = %v, esperado %v", s.recipient, s.seq, got, s.ok)
		}
	}
}

func TestPermanentKeepsCause(t *testing.T) {
	cause := errors.New("HTTP 400")
	if !errors.Is(domain.Permanent(cause), cause) {
		t.Fatal("Permanent perdeu o erro original")
	}
}
