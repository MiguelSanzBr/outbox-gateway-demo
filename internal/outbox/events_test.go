package outbox_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/domain"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/outbox"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/testdb"
)

// O canal do NOTIFY é um só no banco todo, então outros pacotes de teste rodando junto
// também aparecem aqui. Filtra pelo destinatário, q é único por teste.
func nextEvent(t *testing.T, ch <-chan domain.Change, recipient string) domain.Change {
	t.Helper()
	timeout := time.After(5 * time.Second)
	for {
		select {
		case e, ok := <-ch:
			if !ok {
				t.Fatal("canal fechou antes do evento")
			}
			if e.Recipient == recipient {
				return e
			}
		case <-timeout:
			t.Fatalf("nenhum evento de %s", recipient)
		}
	}
}

// Toda mudança de estado vira evento, inclusive a do reaper, q é justamente o q o painel precisa mostrar.
func TestListenStreamsStateChanges(t *testing.T) {
	_, store := testdb.New(t, "t_listen")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := store.Listen(ctx)
	if err != nil {
		t.Fatal(err)
	}

	const who = "listen-ana"
	id, _, err := store.Enqueue(ctx, domain.Message{IdempotencyKey: "k", Recipient: who, Body: "oi"})
	if err != nil {
		t.Fatal(err)
	}
	if c, err := store.Claim(ctx, 7, time.Millisecond); err != nil || c == nil {
		t.Fatalf("claim: %v", err)
	}
	time.Sleep(10 * time.Millisecond)
	if n, err := store.Reap(ctx, 10); err != nil || n != 1 {
		t.Fatalf("reap: n=%d err=%v", n, err)
	}

	for i, want := range []struct {
		status   domain.Status
		attempts int
	}{{domain.Pending, 0}, {domain.Sending, 1}, {domain.Pending, 1}} {
		e := nextEvent(t, ch, who)
		if e.ID != id || e.Status != want.status || e.Attempts != want.attempts {
			t.Fatalf("evento %d: %+v, esperado id=%d %+v", i, e, id, want)
		}
		if i == 1 && (e.Worker == nil || *e.Worker != 7) {
			t.Fatalf("evento do claim sem o worker: %+v", e)
		}
		if i == 2 && (e.LastError == nil || *e.LastError != "lease expirado") {
			t.Fatalf("evento do reaper sem o motivo: %+v", e)
		}
	}

	// ctx acabou: o canal fecha, senão quem consome fica pendurado
	cancel()
	timeout := time.After(5 * time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-timeout:
			t.Fatal("canal não fechou depois do cancel")
		}
	}
}

func TestListenFailsWithDBDown(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), "postgres://x:x@127.0.0.1:1/x?connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err := outbox.NewStore(pool).Listen(context.Background()); err == nil {
		t.Fatal("Listen sem erro com banco fora")
	}
}
