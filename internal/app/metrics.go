package app

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/domain"
)

// QueueStats é a foto da fila pro /metrics.
type QueueStats struct {
	ByStatus     map[domain.Status]int64 // sem sent, q só cresce
	OldestUnsent time.Duration           // idade da mais antiga em queued, pending ou sending
}

// DeliveryCounts são os resultados gravados por este processo desde q subiu.
type DeliveryCounts struct {
	Sent, Retried, Dead, LeaseLost, Released, Reaped int64
}

type counters struct {
	sent, retried, dead, leaseLost, released, reaped atomic.Int64
}

// count registra o resultado de uma transição; erro de gravação não conta, nada mudou.
func (c *counters) count(t domain.Transition, err error) {
	switch {
	case errors.Is(err, domain.ErrLeaseLost):
		c.leaseLost.Add(1)
	case err != nil:
	case t.To == domain.Sent:
		c.sent.Add(1)
	case t.To == domain.Dead:
		c.dead.Add(1)
	default:
		c.retried.Add(1)
	}
}

func (s *Service) Deliveries() DeliveryCounts {
	m := &s.counters
	return DeliveryCounts{
		Sent: m.sent.Load(), Retried: m.retried.Load(), Dead: m.dead.Load(),
		LeaseLost: m.leaseLost.Load(), Released: m.released.Load(), Reaped: m.reaped.Load(),
	}
}

// Ready diz se dá pra receber tráfego: sem banco, o POST só daria 500.
func (s *Service) Ready(ctx context.Context) error { return s.repo.Ping(ctx) }

func (s *Service) QueueStats(ctx context.Context) (QueueStats, error) { return s.repo.QueueStats(ctx) }
