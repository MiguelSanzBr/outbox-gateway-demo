package app

import (
	"context"
	"errors"
	"time"

	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/domain"
)

// ErrWorkerKilled é a causa do cancel quando a demo derruba um worker.
var ErrWorkerKilled = errors.New("worker derrubado")

type Config struct {
	Lease time.Duration // precisa cobrir o envio mais lento; o envio usa no máximo Lease/2
	Retry domain.RetryPolicy
}

type Service struct {
	repo   OutboxRepository
	sender Sender
	events EventSource
	cfg    Config
}

func NewService(repo OutboxRepository, sender Sender, events EventSource, cfg Config) *Service {
	return &Service{repo: repo, sender: sender, events: events, cfg: cfg}
}

// EnqueueMessage valida e grava mensagem + outbox atomicamente. Repetir a mesma
// chave com o mesmo conteúdo devolve o id original com created=false.
func (s *Service) EnqueueMessage(ctx context.Context, key, recipient, body string) (id int64, created bool, err error) {
	m, err := domain.NewMessage(key, recipient, body)
	if err != nil {
		return 0, false, err
	}
	return s.repo.Enqueue(ctx, m)
}

func (s *Service) GetMessage(ctx context.Context, id int64) (*MessageState, error) {
	return s.repo.Get(ctx, id)
}

// RecentChanges é a foto inicial do painel; daí pra frente ele segue pelo StreamEvents.
func (s *Service) RecentChanges(ctx context.Context, n int) ([]Change, error) {
	return s.repo.Recent(ctx, n)
}

func (s *Service) StreamEvents(ctx context.Context) (<-chan Change, error) {
	return s.events.Listen(ctx)
}

// ClaimNext reivindica a próxima linha liberada pela regra de ordem, ou nil se não tiver.
func (s *Service) ClaimNext(ctx context.Context, worker int) (*domain.OutboxEntry, error) {
	return s.repo.Claim(ctx, worker, s.cfg.Lease)
}

func (s *Service) CompleteDelivery(ctx context.Context, c *domain.OutboxEntry) error {
	t, err := c.Complete()
	if err != nil {
		return err
	}
	return s.repo.Finish(ctx, c.ID, t)
}

// FailDelivery devolve a linha à fila depois do backoff, ou encerra como dead.
func (s *Service) FailDelivery(ctx context.Context, c *domain.OutboxEntry, cause error) (domain.Transition, error) {
	t, err := c.Fail(cause, s.cfg.Retry)
	if err != nil {
		return t, err
	}
	return t, s.repo.Finish(ctx, c.ID, t)
}

// Deliver envia c e grava o resultado. sendErr é o erro do provedor, err o da gravação.
// Worker derrubado no meio do envio não grava nada: a linha fica em sending pro reaper.
func (s *Service) Deliver(ctx context.Context, c *domain.OutboxEntry) (t domain.Transition, sendErr, err error) {
	// Envio limitado a metade do lease: o reaper não devolve a linha à fila enquanto ela ainda está em voo.
	sendCtx, cancel := context.WithTimeout(ctx, s.cfg.Lease/2)
	sendErr = s.sender.Send(sendCtx, c)
	cancel()
	if errors.Is(context.Cause(ctx), ErrWorkerKilled) {
		return t, sendErr, ErrWorkerKilled
	}

	// Finaliza mesmo durante o shutdown, senão a linha fica presa até o lease expirar.
	doneCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if sendErr == nil {
		return t, nil, s.CompleteDelivery(doneCtx, c)
	}
	t, err = s.FailDelivery(doneCtx, c, sendErr)
	return t, sendErr, err
}

// ReapExpiredLeases devolve à fila as linhas de worker morto ou travado.
func (s *Service) ReapExpiredLeases(ctx context.Context) (int64, error) {
	return s.repo.Reap(ctx, s.cfg.Retry.MaxAttempts)
}
