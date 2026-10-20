package outbox

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"
)

// Sender entrega uma mensagem ao provedor. Deve repassar c.IdempotencyKey para
// que o provedor descarte reenvios (o worker garante pelo menos uma vez).
type Sender interface {
	Send(ctx context.Context, c *Claim) error
}

type permanentError struct{ error }

func (e permanentError) Unwrap() error { return e.error }

// Permanent marca um erro que não adianta tentar de novo (ex.: HTTP 400).
func Permanent(err error) error { return permanentError{err} }

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
func Run(ctx context.Context, store *Store, sender Sender, cfg Config, log *slog.Logger) {
	var wg sync.WaitGroup
	for i := range cfg.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			work(ctx, i, store, sender, cfg, log.With("worker", i))
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		reap(ctx, store, cfg, log.With("component", "reaper"))
	}()
	wg.Wait()
}

func work(ctx context.Context, id int, store *Store, sender Sender, cfg Config, log *slog.Logger) {
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

func process(ctx context.Context, store *Store, sender Sender, cfg Config, log *slog.Logger, c *Claim) {
	// Envio limitado a metade do lease: o reaper não devolve a linha à fila enquanto ela ainda está em voo.
	sendCtx, cancel := context.WithTimeout(ctx, cfg.Lease/2)
	sendErr := sender.Send(sendCtx, c)
	cancel()

	// Finaliza mesmo durante o shutdown, senão a linha fica presa até o lease expirar.
	doneCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	var err error
	if sendErr == nil {
		err = store.MarkSent(doneCtx, c)
	} else {
		dead := errors.As(sendErr, new(permanentError)) || c.Attempts >= cfg.MaxAttempts
		err = store.MarkFailed(doneCtx, c, sendErr.Error(), backoff(cfg, c.Attempts), dead)
		log.Warn("envio falhou", "id", c.ID, "attempt", c.Attempts, "dead", dead, "err", sendErr)
	}
	if errors.Is(err, ErrLeaseLost) {
		// Outro worker assumiu a linha; o provedor deduplica pela chave de idempotência.
		log.Warn("lease perdido", "id", c.ID)
	} else if err != nil {
		log.Error("finalizar", "id", c.ID, "err", err)
	}
}

// backoff exponencial com jitter: metade fixa, metade aleatória.
func backoff(cfg Config, attempt int) time.Duration {
	d := min(cfg.BackoffBase<<min(attempt-1, 20), cfg.BackoffMax)
	return d/2 + rand.N(d/2+1)
}

func reap(ctx context.Context, store *Store, cfg Config, log *slog.Logger) {
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
