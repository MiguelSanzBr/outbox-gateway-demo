// Package postgres é o adapter da outbox no PostgreSQL:
// a API grava a mensagem e a linha da outbox na mesma transação, e workers
// reivindicam linhas com FOR UPDATE SKIP LOCKED sob um lease.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/app"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/domain"
)

type Store struct{ pool *pgxpool.Pool }

func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// Enqueue grava a mensagem e a linha da outbox atomicamente. Repetir a mesma
// chave com o mesmo conteúdo devolve o id original com created=false.
func (s *Store) Enqueue(ctx context.Context, m domain.Message) (id int64, created bool, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	err = tx.QueryRow(ctx, `
		INSERT INTO messages (idempotency_key, recipient, body) VALUES ($1, $2, $3)
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING id`, m.IdempotencyKey, m.Recipient, m.Body).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		var r, b string
		if err := tx.QueryRow(ctx,
			`SELECT id, recipient, body FROM messages WHERE idempotency_key = $1`, m.IdempotencyKey).Scan(&id, &r, &b); err != nil {
			return 0, false, err
		}
		if r != m.Recipient || b != m.Body {
			return 0, false, domain.ErrKeyConflict
		}
		return id, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO outbox (message_id, recipient) VALUES ($1, $2)`, id, m.Recipient); err != nil {
		return 0, false, err
	}
	return id, true, tx.Commit(ctx)
}

// Claim reivindica a mensagem pronta mais antiga cujo destinatário não tem
// nenhuma mensagem anterior em aberto. Devolve (nil, nil) se não houver nada.
func (s *Store) Claim(ctx context.Context, worker int, lease time.Duration) (*domain.OutboxEntry, error) {
	c := domain.OutboxEntry{Status: domain.Sending, Lease: domain.Lease{Worker: worker}}
	err := s.pool.QueryRow(ctx, `
		WITH next AS (
			SELECT o.id
			FROM outbox o
			WHERE o.status = 'pending'
			  AND o.next_attempt_at <= now()
			  AND NOT EXISTS (
			      SELECT 1 FROM outbox p
			      WHERE p.recipient = o.recipient
			        AND p.id < o.id
			        AND p.status IN ('pending', 'sending'))
			ORDER BY o.id
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE outbox o
		SET status = 'sending',
		    attempts = o.attempts + 1,
		    lease_token = gen_random_uuid(),
		    lease_until = now() + $1 * interval '1 millisecond',
		    worker = $2
		FROM next, messages m
		WHERE o.id = next.id AND m.id = o.message_id
		RETURNING o.id, o.lease_token::text, o.attempts, m.idempotency_key, m.recipient, m.body`,
		lease.Milliseconds(), worker).Scan(&c.ID, &c.Lease.Token, &c.Attempts, &c.IdempotencyKey, &c.Recipient, &c.Body)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &c, nil
}

// Finish grava a transição decidida pelo domínio. O WHERE pelo lease_token é o fencing:
// se o lease venceu e a linha mudou de mão, nada é gravado e volta ErrLeaseLost.
func (s *Store) Finish(ctx context.Context, id int64, t domain.Transition) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE outbox
		SET status = $3,
		    sent_at = CASE WHEN $3 = 'sent' THEN now() END,
		    next_attempt_at = CASE WHEN $3 = 'sent' THEN next_attempt_at ELSE now() + $4 * interval '1 millisecond' END,
		    last_error = NULLIF($5, ''), lease_token = NULL, lease_until = NULL
		WHERE id = $1 AND lease_token = $2`, id, string(t.Token), string(t.To), t.RetryIn.Milliseconds(), t.LastError)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrLeaseLost
	}
	return nil
}

// Reap devolve à fila as linhas cujo lease expirou (worker morto ou travado).
// Linhas que já gastaram maxAttempts viram 'dead' para não girar para sempre:
// é a RetryPolicy.Exhausted do domínio, aplicada em lote num UPDATE só.
func (s *Store) Reap(ctx context.Context, maxAttempts int) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE outbox
		SET status = CASE WHEN attempts >= $1 THEN 'dead' ELSE 'pending' END,
		    next_attempt_at = now(), last_error = 'lease expirado',
		    lease_token = NULL, lease_until = NULL
		WHERE status = 'sending' AND lease_until < now()`, maxAttempts)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (s *Store) Get(ctx context.Context, id int64) (*app.MessageState, error) {
	var st app.MessageState
	err := s.pool.QueryRow(ctx, `
		SELECT m.id, m.recipient, o.status, o.attempts, o.last_error, m.created_at, o.sent_at
		FROM messages m JOIN outbox o ON o.message_id = m.id
		WHERE m.id = $1`, id).
		Scan(&st.ID, &st.Recipient, &st.Status, &st.Attempts, &st.LastError, &st.CreatedAt, &st.SentAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get %d: %w", id, err)
	}
	return &st, nil
}
