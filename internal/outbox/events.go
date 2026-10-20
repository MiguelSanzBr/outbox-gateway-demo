package outbox

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Listen assina as mudanças de estado da outbox (JSON montado pelo trigger).
// Usa uma conexão própria, fora do pool, pq ela fica presa no LISTEN.
// O canal fecha quando ctx acaba ou a conexão cai; quem consome tem q ler até fechar.
func (s *Store) Listen(ctx context.Context) (<-chan string, error) {
	cfg := s.pool.Config().ConnConfig
	// LISTEN no AfterConnect: se falhar, o próprio pgx fecha a conexão e devolve o erro
	cfg.AfterConnect = func(ctx context.Context, c *pgconn.PgConn) error {
		return c.Exec(ctx, "LISTEN outbox_events").Close()
	}
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	ch := make(chan string)
	go func() {
		defer close(ch)
		defer func() { _ = conn.Close(context.Background()) }()
		for {
			n, err := conn.WaitForNotification(ctx)
			if err != nil {
				return
			}
			ch <- n.Payload
		}
	}()
	return ch, nil
}
