// Package outbox implementa o padrão transactional outbox sobre PostgreSQL:
// a API grava a mensagem e a linha da outbox na mesma transação, e workers
// reivindicam linhas com FOR UPDATE SKIP LOCKED sob um lease.
package outbox

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schema string

var (
	// ErrKeyConflict: a chave de idempotência já existe com outro conteúdo.
	ErrKeyConflict = errors.New("chave de idempotência reutilizada com conteúdo diferente")
	// ErrLeaseLost: o lease expirou e a linha foi devolvida à fila (ou reivindicada por outro worker).
	ErrLeaseLost = errors.New("lease perdido")
	ErrNotFound  = errors.New("mensagem não encontrada")
)

type Store struct{ pool *pgxpool.Pool }

func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

func (s *Store) Migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, schema)
	return err
}

// Enqueue grava a mensagem e a linha da outbox atomicamente. Repetir a mesma
// chave com o mesmo conteúdo devolve o id original com created=false.
func (s *Store) Enqueue(ctx context.Context, key, recipient, body string) (id int64, created bool, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	err = tx.QueryRow(ctx, `
		INSERT INTO messages (idempotency_key, recipient, body) VALUES ($1, $2, $3)
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING id`, key, recipient, body).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		var r, b string
		if err := tx.QueryRow(ctx,
			`SELECT id, recipient, body FROM messages WHERE idempotency_key = $1`, key).Scan(&id, &r, &b); err != nil {
			return 0, false, err
		}
		if r != recipient || b != body {
			return 0, false, ErrKeyConflict
		}
		return id, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO outbox (message_id, recipient) VALUES ($1, $2)`, id, recipient); err != nil {
		return 0, false, err
	}
	return id, true, tx.Commit(ctx)
}

// Claim é uma linha reivindicada por um worker. LeaseToken funciona como
// fencing token: só quem tem o token vigente pode finalizar a linha.
type Claim struct {
	ID             int64
	LeaseToken     string
	Attempts       int
	IdempotencyKey string
	Recipient      string
	Body           string
}

// Claim reivindica a mensagem pronta mais antiga cujo destinatário não tem
// nenhuma mensagem anterior em aberto. Devolve (nil, nil) se não houver nada.
func (s *Store) Claim(ctx context.Context, worker int, lease time.Duration) (*Claim, error) {
	var c Claim
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
		lease.Milliseconds(), worker).Scan(&c.ID, &c.LeaseToken, &c.Attempts, &c.IdempotencyKey, &c.Recipient, &c.Body)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &c, nil
}

func (s *Store) MarkSent(ctx context.Context, c *Claim) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE outbox
		SET status = 'sent', sent_at = now(), lease_token = NULL, lease_until = NULL, last_error = NULL
		WHERE id = $1 AND lease_token = $2`, c.ID, c.LeaseToken)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

// MarkFailed devolve a linha à fila após retryIn, ou a encerra como 'dead'.
func (s *Store) MarkFailed(ctx context.Context, c *Claim, cause string, retryIn time.Duration, dead bool) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE outbox
		SET status = CASE WHEN $3 THEN 'dead' ELSE 'pending' END,
		    next_attempt_at = now() + $4 * interval '1 millisecond',
		    last_error = $5, lease_token = NULL, lease_until = NULL
		WHERE id = $1 AND lease_token = $2`, c.ID, c.LeaseToken, dead, retryIn.Milliseconds(), cause)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

// Reap devolve à fila as linhas cujo lease expirou (worker morto ou travado).
// Linhas que já gastaram maxAttempts viram 'dead' para não girar para sempre.
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

type Status struct {
	ID        int64      `json:"id"`
	Recipient string     `json:"recipient"`
	Status    string     `json:"status"`
	Attempts  int        `json:"attempts"`
	LastError *string    `json:"last_error,omitempty"`
	CreatedAt time.Time  `json:"created_at"`
	SentAt    *time.Time `json:"sent_at,omitempty"`
}

func (s *Store) Get(ctx context.Context, id int64) (*Status, error) {
	var st Status
	err := s.pool.QueryRow(ctx, `
		SELECT m.id, m.recipient, o.status, o.attempts, o.last_error, m.created_at, o.sent_at
		FROM messages m JOIN outbox o ON o.message_id = m.id
		WHERE m.id = $1`, id).
		Scan(&st.ID, &st.Recipient, &st.Status, &st.Attempts, &st.LastError, &st.CreatedAt, &st.SentAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get %d: %w", id, err)
	}
	return &st, nil
}
