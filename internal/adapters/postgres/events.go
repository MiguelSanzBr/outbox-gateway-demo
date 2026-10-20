package postgres

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/app"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/domain"
)

// notification é o JSON q o trigger outbox_notify monta.
type notification struct {
	ID        int64         `json:"id"`
	Recipient string        `json:"recipient"`
	Status    domain.Status `json:"status"`
	Attempts  int           `json:"attempts"`
	Worker    *int          `json:"worker"`
	LastError *string       `json:"last_error"`
}

// Listen assina as mudanças de estado da outbox (JSON montado pelo trigger).
// Usa uma conexão própria, fora do pool, pq ela fica presa no LISTEN.
// O canal fecha quando ctx acaba ou a conexão cai; quem consome tem q ler até fechar.
func (s *Store) Listen(ctx context.Context) (<-chan app.Change, error) {
	cfg := s.pool.Config().ConnConfig
	// LISTEN no AfterConnect: se falhar, o próprio pgx fecha a conexão e devolve o erro
	cfg.AfterConnect = func(ctx context.Context, c *pgconn.PgConn) error {
		return c.Exec(ctx, "LISTEN outbox_events").Close()
	}
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	ch := make(chan app.Change)
	go func() {
		defer close(ch)
		defer func() { _ = conn.Close(context.Background()) }()
		for {
			n, err := conn.WaitForNotification(ctx)
			if err != nil {
				return
			}
			var c notification
			_ = json.Unmarshal([]byte(n.Payload), &c) // payload vem do nosso trigger, sempre nesse formato
			ch <- app.Change(c)
		}
	}()
	return ch, nil
}

// Recent devolve as n linhas mais novas, mais nova primeiro. É a foto inicial do painel;
// daí pra frente ele se atualiza pelo Listen.
func (s *Store) Recent(ctx context.Context, n int) ([]app.Change, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT message_id, recipient, status, attempts, worker, left(last_error, 200)
		FROM outbox ORDER BY id DESC LIMIT $1`, n)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (app.Change, error) {
		var c app.Change
		err := r.Scan(&c.ID, &c.Recipient, &c.Status, &c.Attempts, &c.Worker, &c.LastError)
		return c, err
	})
}
