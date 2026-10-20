package worker_test

import (
	"context"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/adapters/worker"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/domain"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/testdb"
)

// Worker derrubado no meio do envio não finaliza: a linha fica em sending com o id dele,
// o reaper devolve depois do lease e, com o worker religado, a mensagem sai.
func TestFleetKillMidSendLeavesRowForReaper(t *testing.T) {
	pool, store := testdb.New(t, "t_fleet")
	ctx := context.Background()
	id, _, err := store.Enqueue(ctx, domain.Message{IdempotencyKey: "k", Recipient: "ana", Body: "oi"})
	if err != nil {
		t.Fatal(err)
	}

	cfg := testConfig()
	cfg.Workers = 1
	cfg.Lease = time.Second
	cfg.ReapInterval = 20 * time.Millisecond
	inSend := make(chan struct{})
	var calls atomic.Int32
	sender := funcSender(func(ctx context.Context, _ *domain.OutboxEntry) error {
		if calls.Add(1) == 1 {
			close(inSend)
			<-ctx.Done() // trava até o worker ser derrubado
			return ctx.Err()
		}
		return nil
	})
	runCtx, cancel := context.WithCancel(ctx)
	fleet := worker.Start(runCtx, store, sender, cfg, quiet)
	t.Cleanup(func() { cancel(); fleet.Wait() })

	select {
	case <-inSend:
	case <-time.After(5 * time.Second):
		t.Fatal("worker não chegou a enviar")
	}
	if !fleet.Kill(0) {
		t.Fatal("Kill(0) não derrubou")
	}
	if fleet.Kill(0) || fleet.Kill(1) || fleet.Kill(-1) {
		t.Fatal("Kill aceitou worker caído ou inexistente")
	}
	if got := fleet.Workers(); !slices.Equal(got, []worker.WorkerInfo{{ID: 0, Alive: false}}) {
		t.Fatalf("workers: %+v", got)
	}

	row := func() (status string, worker *int, lastErr *string) {
		t.Helper()
		if err := pool.QueryRow(ctx, `SELECT status, worker, last_error FROM outbox WHERE message_id = $1`, id).
			Scan(&status, &worker, &lastErr); err != nil {
			t.Fatal(err)
		}
		return
	}
	time.Sleep(100 * time.Millisecond)
	if st, w, _ := row(); st != "sending" || w == nil || *w != 0 {
		t.Fatalf("depois do kill: status=%s worker=%v, esperado sending com worker 0", st, w)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		st, _, lastErr := row()
		if st == "pending" && lastErr != nil && *lastErr == "lease expirado" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("reaper não devolveu: status=%s", st)
		}
		time.Sleep(10 * time.Millisecond)
	}

	if n := fleet.Revive(); n != 1 {
		t.Fatalf("Revive = %d, esperado 1", n)
	}
	if got := fleet.Workers(); !got[0].Alive {
		t.Fatalf("workers depois do revive: %+v", got)
	}
	testdb.WaitAllSent(t, pool, 1)
}

// Depois do shutdown não tem o q religar: senão sobe goroutine em cima de pool fechado.
func TestFleetReviveAfterShutdown(t *testing.T) {
	_, store := testdb.New(t, "t_fleet_down")
	cfg := testConfig()
	cfg.Workers = 2
	ctx, cancel := context.WithCancel(context.Background())
	fleet := worker.Start(ctx, store, funcSender(func(context.Context, *domain.OutboxEntry) error { return nil }), cfg, quiet)
	fleet.Kill(1)
	cancel()
	fleet.Wait()
	if n := fleet.Revive(); n != 0 {
		t.Fatalf("Revive depois do shutdown = %d", n)
	}
}
