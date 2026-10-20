// Package app são os casos de uso do gateway. Só conhece o domínio e as portas
// abaixo; banco, HTTP e provedor ficam nos adapters.
package app

import (
	"context"
	"time"

	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/domain"
)

// OutboxRepository guarda mensagens e outbox. As garantias de concorrência (SKIP LOCKED,
// uma cabeça por destinatário, fencing pelo token) ficam na implementação, pq só o banco
// enxerga todos os workers ao mesmo tempo. As decisões de estado vêm do domínio.
type OutboxRepository interface {
	Enqueue(ctx context.Context, m domain.Message) (id int64, created bool, err error)
	Claim(ctx context.Context, worker int, lease time.Duration) (*domain.OutboxEntry, error)
	// Finish grava t se a linha ainda tiver o token de t, senão devolve domain.ErrLeaseLost.
	Finish(ctx context.Context, id int64, t domain.Transition) error
	// Expired lista até limit linhas em sending com o lease vencido.
	Expired(ctx context.Context, limit int) ([]domain.OutboxEntry, error)
	// Requeue devolve uma dead à fila (domain.ErrInvalidTransition se ela não for dead).
	Requeue(ctx context.Context, id int64) error
	Get(ctx context.Context, id int64) (*MessageState, error)
	Recent(ctx context.Context, n int) ([]Change, error)
}

// Sender entrega uma mensagem ao provedor. Tem q repassar c.IdempotencyKey pra o
// provedor descartar reenvios (o gateway garante pelo menos uma vez).
type Sender interface {
	Send(ctx context.Context, c *domain.OutboxEntry) error
}

// EventSource entrega as mudanças de estado da outbox. O canal fecha quando ctx acaba
// ou a fonte cai; quem consome tem q ler até fechar.
type EventSource interface {
	Listen(ctx context.Context) (<-chan Change, error)
}

// WorkerPool é o controle dos workers q a demo usa pra derrubar e religar.
type WorkerPool interface {
	Workers() []WorkerInfo
	Kill(id int) bool
	Revive() int
}

type WorkerInfo struct {
	ID    int
	Alive bool
}

// MessageState é a leitura de uma mensagem: o q a API devolve em GET /v1/messages/{id}.
type MessageState struct {
	ID        int64
	Recipient string
	Status    domain.Status
	Attempts  int
	LastError *string
	CreatedAt time.Time
	SentAt    *time.Time
}

// Change é uma mudança de estado da outbox: a foto do painel e cada evento do stream.
type Change struct {
	ID        int64
	Recipient string
	Status    domain.Status
	Attempts  int
	Worker    *int
	Version   int64 // soma 1 a cada mudança da linha: a maior é a mais nova
	LastError *string
}
