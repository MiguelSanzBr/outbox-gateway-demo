package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/adapters/postgres"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/domain"
)

// Sender entrega uma mensagem ao provedor. Deve repassar c.IdempotencyKey para
// que o provedor descarte reenvios (o worker garante pelo menos uma vez).
type Sender interface {
	Send(ctx context.Context, c *domain.OutboxEntry) error
}

type Config struct {
	Workers      int
	Lease        time.Duration // precisa cobrir o envio mais lento; o envio usa no máximo Lease/2
	PollInterval time.Duration // espera quando a fila está vazia
	ReapInterval time.Duration
	MaxAttempts  int
	BackoffBase  time.Duration
	BackoffMax   time.Duration
}

// Run sobe cfg.Workers workers e um reaper; bloqueia até ctx ser cancelado.
func Run(ctx context.Context, store *postgres.Store, sender Sender, cfg Config, log *slog.Logger) {
	Start(ctx, store, sender, cfg, log).Wait()
}

var errKilled = errors.New("worker derrubado")

// Fleet são os workers e o reaper de um processo. Guarda o cancel de cada worker
// pra demo conseguir derrubar um no meio do envio e religar depois.
type Fleet struct {
	ctx    context.Context
	store  *postgres.Store
	sender Sender
	cfg    Config
	log    *slog.Logger

	wg    sync.WaitGroup
	mu    sync.Mutex
	kills []context.CancelCauseFunc // nil = worker caído
}

type WorkerInfo struct {
	ID    int  `json:"id"`
	Alive bool `json:"alive"`
}

// Start sobe os workers e o reaper e volta na hora; Wait espera tudo parar depois do cancel de ctx.
func Start(ctx context.Context, store *postgres.Store, sender Sender, cfg Config, log *slog.Logger) *Fleet {
	f := &Fleet{ctx: ctx, store: store, sender: sender, cfg: cfg, log: log, kills: make([]context.CancelCauseFunc, cfg.Workers)}
	f.mu.Lock()
	for i := range f.kills {
		f.start(i)
	}
	f.mu.Unlock()
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		reap(ctx, store, cfg, log.With("component", "reaper"))
	}()
	return f
}

// start sobe o worker i. Chamar com f.mu travado.
func (f *Fleet) start(i int) {
	ctx, kill := context.WithCancelCause(f.ctx)
	f.kills[i] = kill
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		work(ctx, i, f.store, f.sender, f.cfg, f.log.With("worker", i))
	}()
}

func (f *Fleet) Wait() {
	<-f.ctx.Done()
	// segura o lock pra nenhum Revive dar wg.Add com o Wait já rodando
	f.mu.Lock()
	defer f.mu.Unlock()
	f.wg.Wait()
}

// Kill derruba o worker id como um kill -9: se estiver enviando, a linha fica em
// sending sem finalizar e só volta pela mão do reaper. false se já caiu ou não existe.
func (f *Fleet) Kill(id int) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if id < 0 || id >= len(f.kills) || f.kills[id] == nil {
		return false
	}
	f.kills[id](errKilled)
	f.kills[id] = nil
	return true
}

// Revive religa os workers caídos e devolve quantos subiram.
func (f *Fleet) Revive() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.ctx.Err() != nil {
		return 0
	}
	n := 0
	for i, kill := range f.kills {
		if kill == nil {
			f.start(i)
			n++
		}
	}
	return n
}

func (f *Fleet) Workers() []WorkerInfo {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]WorkerInfo, len(f.kills))
	for i, kill := range f.kills {
		out[i] = WorkerInfo{ID: i, Alive: kill != nil}
	}
	return out
}

func work(ctx context.Context, id int, store *postgres.Store, sender Sender, cfg Config, log *slog.Logger) {
	for ctx.Err() == nil {
		c, err := store.Claim(ctx, id, cfg.Lease)
		if err != nil {
			if ctx.Err() == nil {
				log.Error("claim", "err", err)
			}
			sleep(ctx, cfg.PollInterval)
			continue
		}
		if c == nil {
			sleep(ctx, cfg.PollInterval)
			continue
		}
		process(ctx, store, sender, cfg, log, c)
	}
}

func process(ctx context.Context, store *postgres.Store, sender Sender, cfg Config, log *slog.Logger, c *domain.OutboxEntry) {
	// Envio limitado a metade do lease: o reaper não devolve a linha à fila enquanto ela ainda está em voo.
	sendCtx, cancel := context.WithTimeout(ctx, cfg.Lease/2)
	sendErr := sender.Send(sendCtx, c)
	cancel()
	if errors.Is(context.Cause(ctx), errKilled) {
		log.Warn("worker derrubado no meio do envio, a linha fica pro reaper", "id", c.ID)
		return
	}

	// Finaliza mesmo durante o shutdown, senão a linha fica presa até o lease expirar.
	doneCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	var t domain.Transition
	var err error
	if sendErr == nil {
		t, err = c.Complete()
	} else {
		t, err = c.Fail(sendErr, domain.RetryPolicy{MaxAttempts: cfg.MaxAttempts, BackoffBase: cfg.BackoffBase, BackoffMax: cfg.BackoffMax})
		log.Warn("envio falhou", "id", c.ID, "attempt", c.Attempts, "dead", t.To == domain.Dead, "err", sendErr)
	}
	if err == nil {
		err = store.Finish(doneCtx, c.ID, t)
	}
	if errors.Is(err, domain.ErrLeaseLost) {
		// Outro worker assumiu a linha; o provedor deduplica pela chave de idempotência.
		log.Warn("lease perdido", "id", c.ID)
	} else if err != nil {
		log.Error("finalizar", "id", c.ID, "err", err)
	}
}

func reap(ctx context.Context, store *postgres.Store, cfg Config, log *slog.Logger) {
	t := time.NewTicker(cfg.ReapInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		n, err := store.Reap(ctx, cfg.MaxAttempts)
		if err != nil && ctx.Err() == nil {
			log.Error("reap", "err", err)
		}
		if n > 0 {
			log.Warn("leases expirados devolvidos à fila", "rows", n)
		}
	}
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
