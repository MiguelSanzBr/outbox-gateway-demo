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
	if _, err := svc.CompleteDelivery(context.Background(), done); !errors.Is(err, domain.ErrInvalidTransition) {
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

// recordingRepo guarda o ctx e a transição do Finish.
type recordingRepo struct {
	fakeRepo
	ctxErr   error
	deadline time.Duration
}

func (r *recordingRepo) Finish(ctx context.Context, id int64, t domain.Transition) error {
	r.ctxErr = ctx.Err()
	if d, ok := ctx.Deadline(); ok {
		r.deadline = time.Until(d)
	}
	return r.fakeRepo.Finish(ctx, id, t)
}

type senderFunc func(ctx context.Context, c *domain.OutboxEntry) error

func (f senderFunc) Send(ctx context.Context, c *domain.OutboxEntry) error { return f(ctx, c) }

func inFlight() *domain.OutboxEntry {
	return &domain.OutboxEntry{ID: 7, Status: domain.Sending, Attempts: 1, Lease: domain.Lease{Token: "tok"}}
}

var lease = app.Config{Lease: 2 * time.Second, Retry: domain.RetryPolicy{MaxAttempts: 1}}

// O envio é cortado na metade do lease: o reaper só devolve depois do lease inteiro,
// então nunca pega linha em voo.
func TestDeliverDeadlines(t *testing.T) {
	repo := &recordingRepo{}
	var sendLeft time.Duration
	svc := app.NewService(repo, senderFunc(func(ctx context.Context, _ *domain.OutboxEntry) error {
		d, _ := ctx.Deadline()
		sendLeft = time.Until(d)
		return nil
	}), nil, lease)
	if _, _, err := svc.Deliver(context.Background(), inFlight()); err != nil {
		t.Fatal(err)
	}
	if sendLeft <= 0 || sendLeft > time.Second {
		t.Fatalf("prazo do envio = %s, esperado até Lease/2 (1 s)", sendLeft)
	}
	if repo.deadline <= 0 || repo.deadline > 2*time.Second {
		t.Fatalf("prazo do Finish = %s, esperado até um lease", repo.deadline)
	}
}

// slowRepo demora pra gravar, como o banco sob carga.
type slowRepo struct {
	fakeRepo
	delay time.Duration
}

func (r *slowRepo) Finish(ctx context.Context, id int64, t domain.Transition) error {
	select {
	case <-time.After(r.delay):
		return r.fakeRepo.Finish(ctx, id, t)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Finalização lenta (banco sob carga) ainda grava em vez de desistir. Desistir não evita
// duplicata nenhuma: a linha ficaria em sending, o reaper devolveria e outro worker
// reenviaria. Se o lease vencer e o reaper chegar antes, quem recusa é o fencing.
func TestSlowFinishStillLands(t *testing.T) {
	repo := &slowRepo{delay: 800 * time.Millisecond} // mais q Lease/4 (500 ms), menos q o lease
	svc := app.NewService(repo, senderFunc(func(context.Context, *domain.OutboxEntry) error { return nil }), nil, lease)
	if tr, _, err := svc.Deliver(context.Background(), inFlight()); err != nil || tr.To != domain.Sent || len(repo.finished) != 1 {
		t.Fatalf("finalização lenta: %+v err=%v gravadas=%d", tr, err, len(repo.finished))
	}
}

// SIGTERM com envio em voo: o envio não é cortado (o shutdown espera ele) e a mensagem
// termina sent, mesmo na última tentativa. Antes o cancel virava falha e dead.
func TestShutdownDoesNotCutSend(t *testing.T) {
	repo := &recordingRepo{}
	ctx, shutdown := context.WithCancel(context.Background())
	svc := app.NewService(repo, senderFunc(func(sendCtx context.Context, _ *domain.OutboxEntry) error {
		shutdown()
		time.Sleep(20 * time.Millisecond)
		return sendCtx.Err()
	}), nil, lease)
	tr, sendErr, err := svc.Deliver(ctx, inFlight())
	if sendErr != nil || err != nil || tr.To != domain.Sent || repo.ctxErr != nil {
		t.Fatalf("Deliver no shutdown: %+v sendErr=%v err=%v finishCtx=%v, esperado sent", tr, sendErr, err, repo.ctxErr)
	}
}

// Shutdown chegou entre o claim e o envio: nada sai pro provedor e a linha volta pra
// fila sem gastar a tentativa.
func TestShutdownBeforeSendReleases(t *testing.T) {
	repo := &recordingRepo{}
	ctx, shutdown := context.WithCancel(context.Background())
	shutdown()
	svc := app.NewService(repo, senderFunc(func(context.Context, *domain.OutboxEntry) error {
		t.Error("enviou depois do shutdown")
		return nil
	}), nil, lease)
	tr, sendErr, err := svc.Deliver(ctx, inFlight())
	if sendErr != nil || err != nil || tr.To != domain.Pending || tr.Attempts != 0 || tr.RetryIn != 0 || len(repo.finished) != 1 {
		t.Fatalf("Release: %+v sendErr=%v err=%v gravadas=%d", tr, sendErr, err, len(repo.finished))
	}
}

// Worker derrubado (o kill -9 da demo) corta o envio na hora e não finaliza nada.
func TestKilledWorkerCutsSend(t *testing.T) {
	repo := &recordingRepo{}
	ctx, kill := context.WithCancelCause(context.Background())
	svc := app.NewService(repo, senderFunc(func(sendCtx context.Context, _ *domain.OutboxEntry) error {
		kill(app.ErrWorkerKilled)
		<-sendCtx.Done()
		return sendCtx.Err()
	}), nil, lease)
	if _, _, err := svc.Deliver(ctx, inFlight()); !errors.Is(err, app.ErrWorkerKilled) || len(repo.finished) != 0 {
		t.Fatalf("err=%v gravadas=%d, esperado ErrWorkerKilled sem gravar", err, len(repo.finished))
	}
}

// Contadores de entrega pro /metrics: cada resultado gravado conta uma vez, e o q não
// chegou a ser gravado (erro do banco) não conta.
func TestDeliveryCounters(t *testing.T) {
	repo := &recordingRepo{fakeRepo: fakeRepo{finishOf: map[int64]error{}}}
	fail := errors.New("provedor: HTTP 503")
	var sendErr error
	svc := app.NewService(repo, senderFunc(func(context.Context, *domain.OutboxEntry) error { return sendErr }), nil,
		app.Config{Lease: time.Second, Retry: domain.RetryPolicy{MaxAttempts: 2}})
	deliver := func(attempts int, err error, finishErr error) {
		sendErr = err
		e := inFlight()
		e.Attempts = attempts
		repo.finishOf[e.ID] = finishErr
		_, _, _ = svc.Deliver(context.Background(), e)
	}
	deliver(1, nil, nil)                      // sent
	deliver(1, fail, nil)                     // retry
	deliver(2, fail, nil)                     // dead (última tentativa)
	deliver(1, nil, domain.ErrLeaseLost)      // lease perdido
	deliver(1, nil, errors.New("banco caiu")) // não gravou: não conta
	ctx, shutdown := context.WithCancel(context.Background())
	shutdown()
	repo.finishOf[7] = nil
	_, _, _ = svc.Deliver(ctx, inFlight()) // release

	repo.expired = []domain.OutboxEntry{{ID: 8, Status: domain.Sending, Attempts: 2, Lease: domain.Lease{Token: "t"}}}
	if _, err := svc.ReapExpiredLeases(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := app.DeliveryCounts{Sent: 1, Retried: 1, Dead: 2, LeaseLost: 1, Released: 1, Reaped: 1}
	if got := svc.Deliveries(); got != want {
		t.Fatalf("contadores %+v, esperado %+v", got, want)
	}
}
