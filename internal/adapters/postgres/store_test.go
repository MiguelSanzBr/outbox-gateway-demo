package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/adapters/postgres"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/domain"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/testdb"
)

func finishSent(ctx context.Context, store *postgres.Store, c *domain.OutboxEntry) error {
	tr, err := c.Complete()
	if err != nil {
		return err
	}
	return store.Finish(ctx, c.ID, tr)
}

func TestEnqueueIsIdempotent(t *testing.T) {
	pool, store := testdb.New(t, "t_enqueue")
	ctx := context.Background()

	id1, created1, err := store.Enqueue(ctx, domain.Message{IdempotencyKey: "k1", Recipient: "r1", Body: "oi"})
	if err != nil || !created1 {
		t.Fatalf("primeiro enqueue: id=%d created=%v err=%v", id1, created1, err)
	}
	id2, created2, err := store.Enqueue(ctx, domain.Message{IdempotencyKey: "k1", Recipient: "r1", Body: "oi"})
	if err != nil || created2 || id2 != id1 {
		t.Fatalf("repetição: id=%d created=%v err=%v", id2, created2, err)
	}
	if _, _, err := store.Enqueue(ctx, domain.Message{IdempotencyKey: "k1", Recipient: "r1", Body: "outro corpo"}); !errors.Is(err, domain.ErrKeyConflict) {
		t.Fatalf("conteúdo diferente: err=%v, esperado ErrKeyConflict", err)
	}
	if _, _, err := store.Enqueue(ctx, domain.Message{IdempotencyKey: "k1", Recipient: "outro", Body: "oi"}); !errors.Is(err, domain.ErrKeyConflict) {
		t.Fatalf("outro destinatário: err=%v, esperado ErrKeyConflict", err)
	}

	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox`).Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("outbox tem %d linhas (err=%v), esperado 1", rows, err)
	}
}

// Um worker travado que acorda depois do reaper não consegue finalizar a linha.
func TestStaleLeaseCannotFinish(t *testing.T) {
	_, store := testdb.New(t, "t_fencing")
	ctx := context.Background()
	if _, _, err := store.Enqueue(ctx, domain.Message{IdempotencyKey: "k", Recipient: "bia", Body: "oi"}); err != nil {
		t.Fatal(err)
	}
	stale, err := store.Claim(ctx, 0, time.Millisecond)
	if err != nil || stale == nil {
		t.Fatalf("claim: %v", err)
	}
	time.Sleep(10 * time.Millisecond)
	if n := reap(t, store, noBackoff); n != 1 {
		t.Fatalf("reap: n=%d", n)
	}
	fresh, err := store.Claim(ctx, 0, time.Minute)
	if err != nil || fresh == nil {
		t.Fatalf("novo claim: %v", err)
	}

	if err := finishSent(ctx, store, stale); !errors.Is(err, domain.ErrLeaseLost) {
		t.Fatalf("sent com lease velho: err=%v, esperado ErrLeaseLost", err)
	}
	if err := finishSent(ctx, store, fresh); err != nil {
		t.Fatal(err)
	}
}

func TestStoreSurfacesDBErrors(t *testing.T) {
	store := testdb.Broken(t, "t_broken")
	ctx := context.Background()
	tr := domain.Transition{Token: "00000000-0000-0000-0000-000000000000", To: domain.Sent}

	if _, _, err := store.Enqueue(ctx, domain.Message{IdempotencyKey: "k", Recipient: "ana", Body: "oi"}); err == nil {
		t.Error("Enqueue sem erro com banco fora")
	}
	if _, err := store.Claim(ctx, 0, time.Second); err == nil {
		t.Error("Claim sem erro com banco fora")
	}
	if err := store.Finish(ctx, 1, tr); err == nil || errors.Is(err, domain.ErrLeaseLost) {
		t.Errorf("Finish: %v", err)
	}
	if _, err := store.Expired(ctx, 1); err == nil {
		t.Error("Expired sem erro com banco fora")
	}
	if err := store.Requeue(ctx, 1); err == nil || errors.Is(err, domain.ErrNotFound) {
		t.Errorf("Requeue: %v", err)
	}
	if _, err := store.Get(ctx, 1); err == nil || errors.Is(err, domain.ErrNotFound) {
		t.Errorf("Get: %v", err)
	}
}

// Se qualquer insert falhar, nada fica gravado: mensagem e outbox entram juntas ou não entram.
func TestEnqueueIsAtomic(t *testing.T) {
	cases := []struct{ name, table, body string }{
		{"falha no insert da mensagem", "messages", `RAISE EXCEPTION 'boom';`},
		{"falha no insert da outbox", "outbox", `RAISE EXCEPTION 'boom';`},
		// trigger q descarta a linha: o ON CONFLICT não devolve nada e a busca da chave também não acha
		{"mensagem sumiu entre o insert e a busca", "messages", `RETURN NULL;`},
	}
	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pool, store := testdb.New(t, "t_atomic"+string(rune('a'+i)))
			testdb.Sabotage(t, pool, c.table, c.body)
			if _, _, err := store.Enqueue(context.Background(), domain.Message{IdempotencyKey: "k", Recipient: "ana", Body: "oi"}); err == nil {
				t.Fatal("esperava erro")
			}
			var n int
			if err := pool.QueryRow(context.Background(),
				`SELECT (SELECT count(*) FROM messages) + (SELECT count(*) FROM outbox)`).Scan(&n); err != nil || n != 0 {
				t.Fatalf("sobrou %d linha(s) depois do erro (err=%v)", n, err)
			}
		})
	}
}

func TestGetNotFound(t *testing.T) {
	_, store := testdb.New(t, "t_notfound")
	if _, err := store.Get(context.Background(), 42); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("err=%v, esperado ErrNotFound", err)
	}
}

func TestMarkFailedWithStaleLease(t *testing.T) {
	_, store := testdb.New(t, "t_stalefail")
	ctx := context.Background()
	if _, _, err := store.Enqueue(ctx, domain.Message{IdempotencyKey: "k", Recipient: "ana", Body: "oi"}); err != nil {
		t.Fatal(err)
	}
	c, err := store.Claim(ctx, 0, time.Minute)
	if err != nil || c == nil {
		t.Fatalf("claim: %v", err)
	}
	c.Lease.Token = "00000000-0000-0000-0000-000000000000"
	tr, _ := c.Fail(errors.New("x"), domain.RetryPolicy{MaxAttempts: 5})
	if err := store.Finish(ctx, c.ID, tr); !errors.Is(err, domain.ErrLeaseLost) {
		t.Fatalf("err=%v, esperado ErrLeaseLost", err)
	}
}
