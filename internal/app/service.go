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
	Lease time.Duration // precisa cobrir o envio mais lento: o envio é cortado em Lease/2
	Retry domain.RetryPolicy
}

type Service struct {
	repo     OutboxRepository
	sender   Sender
	events   EventSource
	cfg      Config
	counters counters
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

// ClaimNext reivindica a próxima cabeça pronta, ou nil se não tiver. O claim não é
// cortado pelo shutdown: um UPDATE cancelado no meio pode ter commitado sem a gente
// saber, e a linha ficaria presa em sending até o reaper. Se o shutdown chegou, o
// Deliver devolve a linha sem gastar a tentativa.
func (s *Service) ClaimNext(ctx context.Context, worker int) (*domain.OutboxEntry, error) {
	claimCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.cfg.Lease)
	defer cancel()
	return s.repo.Claim(claimCtx, worker, s.cfg.Lease)
}

func (s *Service) CompleteDelivery(ctx context.Context, c *domain.OutboxEntry) (domain.Transition, error) {
	t, err := c.Complete()
	if err != nil {
		return t, err
	}
	return t, s.repo.Finish(ctx, c.ID, t)
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
//
// O envio fica solto do ctx do processo: SIGTERM no meio não corta (o shutdown espera
// ele terminar), senão o cancel vira falha e queima tentativa, ou mata a mensagem na
// última. O prazo é metade do lease, pra o reaper nunca devolver linha ainda em voo.
// Só o worker derrubado da demo corta no meio, e aí nada é gravado: a linha fica em
// sending pro reaper, como num kill -9. Se o shutdown chegou antes do envio, a linha
// volta pra fila sem gastar a tentativa.
func (s *Service) Deliver(ctx context.Context, c *domain.OutboxEntry) (t domain.Transition, sendErr, err error) {
	// A finalização também não é cortada pelo shutdown e tem até um lease. Desistir antes
	// não evita duplicata (a linha ficaria em sending até o reaper, e outro worker reenviaria);
	// se o lease vencer e o reaper chegar primeiro, quem recusa a gravação é o fencing.
	doneCtx := func() (context.Context, context.CancelFunc) {
		return context.WithTimeout(context.WithoutCancel(ctx), s.cfg.Lease)
	}
	if ctx.Err() != nil && !killed(ctx) {
		fctx, cancel := doneCtx()
		defer cancel()
		t, err = c.Release()
		if err == nil {
			err = s.repo.Finish(fctx, c.ID, t)
		}
		if err == nil {
			s.counters.released.Add(1)
		}
		return t, nil, err
	}

	sendCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.cfg.Lease/2)
	stop := context.AfterFunc(ctx, func() {
		if killed(ctx) {
			cancel()
		}
	})
	sendErr = s.sender.Send(sendCtx, c)
	stop()
	cancel()
	if killed(ctx) {
		return t, sendErr, ErrWorkerKilled
	}

	fctx, cancelDone := doneCtx()
	defer cancelDone()
	if sendErr == nil {
		t, err = s.CompleteDelivery(fctx, c)
	} else {
		t, err = s.FailDelivery(fctx, c, sendErr)
	}
	s.counters.count(t, err)
	return t, sendErr, err
}

func killed(ctx context.Context) bool { return errors.Is(context.Cause(ctx), ErrWorkerKilled) }

// RequeueDead é o redrive manual: a dead volta pra fila zerada.
func (s *Service) RequeueDead(ctx context.Context, id int64) error { return s.repo.Requeue(ctx, id) }

// reapBatch é quantas linhas o reaper trata por rodada; o resto fica pra próxima.
const reapBatch = 100

// ReapExpiredLeases aplica a regra do domínio (Expire) nas linhas de worker morto ou
// travado: volta com backoff ou, na última tentativa, vira dead com resultado incerto.
func (s *Service) ReapExpiredLeases(ctx context.Context) (int64, error) {
	expired, err := s.repo.Expired(ctx, reapBatch)
	if err != nil {
		return 0, err
	}
	var n int64
	for _, e := range expired {
		t, err := e.Expire(s.cfg.Retry)
		if err == nil {
			err = s.repo.Finish(ctx, e.ID, t)
		}
		// lease perdido: o worker finalizou entre a busca e aqui, não tem o q devolver
		if err != nil && !errors.Is(err, domain.ErrLeaseLost) {
			return n, err
		}
		if err == nil {
			s.counters.count(t, nil)
			s.counters.reaped.Add(1)
			n++
		}
	}
	return n, nil
}
