package app_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/app"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/domain"
)

// fakeRepo é o repositório em memória do q o caso de uso precisa; o resto entra pelo
// OutboxRepository nil embutido (chamar estoura, de propósito).
type fakeRepo struct {
	app.OutboxRepository
	expired  []domain.OutboxEntry
	finishOf map[int64]error
	finished []domain.Transition
}

func (r *fakeRepo) Expired(context.Context, int) ([]domain.OutboxEntry, error) { return r.expired, nil }

func (r *fakeRepo) Finish(_ context.Context, id int64, t domain.Transition) error {
	if err := r.finishOf[id]; err != nil {
		return err
	}
	r.finished = append(r.finished, t)
	return nil
}

// Transição inválida para no caso de uso, antes de chegar em qualquer porta (por isso as portas nil).
func TestDeliveryRejectsInvalidTransition(t *testing.T) {
	svc := app.NewService(nil, nil, nil, app.Config{})
	done := &domain.OutboxEntry{ID: 1, Status: domain.Sent}
	if err := svc.CompleteDelivery(context.Background(), done); !errors.Is(err, domain.ErrInvalidTransition) {
		t.Fatalf("CompleteDelivery: err=%v", err)
	}
	if _, err := svc.FailDelivery(context.Background(), done, errors.New("x")); !errors.Is(err, domain.ErrInvalidTransition) {
		t.Fatalf("FailDelivery: err=%v", err)
	}
}

// Reaper: linha q o worker finalizou no meio do caminho (lease perdido) não conta nem
// para a rodada; erro de verdade para e volta quantas já foram.
func TestReapSkipsLeaseLostAndStopsOnError(t *testing.T) {
	sending := func(id int64) domain.OutboxEntry {
		return domain.OutboxEntry{ID: id, Status: domain.Sending, Attempts: 1, Lease: domain.Lease{Token: "tok"}}
	}
	boom := errors.New("boom")
	repo := &fakeRepo{
		expired:  []domain.OutboxEntry{sending(1), sending(2), sending(3), sending(4)},
		finishOf: map[int64]error{2: domain.ErrLeaseLost, 4: boom},
	}
	svc := app.NewService(repo, nil, nil, app.Config{Retry: domain.RetryPolicy{MaxAttempts: 5, BackoffBase: time.Second, BackoffMax: time.Second}})
	n, err := svc.ReapExpiredLeases(context.Background())
	if n != 2 || !errors.Is(err, boom) {
		t.Fatalf("n=%d err=%v, esperado 2 e boom", n, err)
	}
	for _, tr := range repo.finished {
		if tr.To != domain.Pending || tr.RetryIn < 500*time.Millisecond || tr.LastError != domain.ReasonLeaseExpired {
			t.Fatalf("transição do reaper: %+v, esperado pending com backoff", tr)
		}
	}
}
