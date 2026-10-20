// Package postgres é o adapter da outbox no PostgreSQL:
// a API grava a mensagem e a linha da outbox na mesma transação, e workers
// reivindicam linhas com FOR UPDATE SKIP LOCKED sob um lease.
//
// Os estados vão como literal no SQL de propósito: índice parcial só casa com
// constante. TestSQLStatusesMatchDomain amarra os literais aos do domínio.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/app"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/domain"
)

type Store struct{ pool *pgxpool.Pool }

func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// tx roda fn numa transação e commita se ela não der erro.
func (s *Store) tx(ctx context.Context, op string, fn func(pgx.Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err == nil {
		defer func() { _ = tx.Rollback(ctx) }()
		if err = fn(tx); err == nil {
			err = tx.Commit(ctx)
		}
	}
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	return nil
}

// lockRecipient serializa, por destinatário, tudo q decide quem é a cabeça: Enqueue
// (pending ou queued), Finish pra sent/dead (promove a próxima) e Requeue. Tem q ser um
// statement antes da decisão, pq em READ COMMITTED cada statement enxerga o banco do
// momento em q começou. Colisão de hash só serializa dois destinatários à toa.
func lockRecipient(ctx context.Context, tx pgx.Tx, recipient string) error {
	_, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(7262021, hashtext($1))`, recipient)
	return err
}

// headOrQueued vale pending se o destinatário $2 não tem cabeça, senão queued.
const headOrQueued = `CASE WHEN EXISTS (
	SELECT 1 FROM outbox h WHERE h.recipient = $2 AND h.status IN ('pending', 'sending'))
	THEN 'queued' ELSE 'pending' END`

// Enqueue grava a mensagem e a linha da outbox atomicamente. Repetir a mesma
// chave com o mesmo conteúdo devolve o id original com created=false.
// O lock do destinatário vem antes do insert: o id sai na ordem de commit, então a
// ordem por destinatário é a de commit mesmo com produtores concorrentes.
func (s *Store) Enqueue(ctx context.Context, m domain.Message) (id int64, created bool, err error) {
	err = s.tx(ctx, "enqueue", func(tx pgx.Tx) error {
		err := lockRecipient(ctx, tx, m.Recipient)
		if err == nil {
			err = tx.QueryRow(ctx, `
				INSERT INTO messages (idempotency_key, recipient, body) VALUES ($1, $2, $3)
				ON CONFLICT (idempotency_key) DO NOTHING
				RETURNING id`, m.IdempotencyKey, m.Recipient, m.Body).Scan(&id)
		}
		if errors.Is(err, pgx.ErrNoRows) {
			var r, b string
			if err := tx.QueryRow(ctx,
				`SELECT id, recipient, body FROM messages WHERE idempotency_key = $1`, m.IdempotencyKey).Scan(&id, &r, &b); err != nil {
				return err
			}
			if r != m.Recipient || b != m.Body {
				return domain.ErrKeyConflict
			}
			return nil
		}
		if err == nil {
			created = true
			_, err = tx.Exec(ctx, `INSERT INTO outbox (message_id, recipient, status) VALUES ($1, $2, `+headOrQueued+`)`, id, m.Recipient)
		}
		return err
	})
	if err != nil {
		return 0, false, err
	}
	return id, created, nil
}

// Claim reivindica a cabeça pronta há mais tempo, de qualquer destinatário. Só a cabeça
// fica em pending, então não tem o q checar sobre as outras mensagens do destinatário.
// Devolve (nil, nil) se não houver nada.
func (s *Store) Claim(ctx context.Context, worker int, lease time.Duration) (*domain.OutboxEntry, error) {
	c := domain.OutboxEntry{Status: domain.Sending, Lease: domain.Lease{Worker: worker}}
	err := s.pool.QueryRow(ctx, `
		WITH next AS (
			SELECT id FROM outbox
			WHERE status = 'pending' AND next_attempt_at <= now()
			ORDER BY next_attempt_at, id
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
		RETURNING o.message_id, o.lease_token::text, o.attempts, m.idempotency_key, m.recipient, m.body`,
		lease.Milliseconds(), worker).Scan(&c.ID, &c.Lease.Token, &c.Attempts, &c.IdempotencyKey, &c.Recipient, &c.Body)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim worker %d: %w", worker, err)
	}
	return &c, nil
}

// Finish grava a transição decidida pelo domínio. O WHERE pelo lease_token é o fencing:
// se o lease venceu e a linha mudou de mão, nada é gravado e volta ErrLeaseLost.
// sent e dead liberam o destinatário: a próxima queued sobe na mesma transação.
func (s *Store) Finish(ctx context.Context, id int64, t domain.Transition) error {
	return s.tx(ctx, fmt.Sprintf("finish %d", id), func(tx pgx.Tx) error {
		var recipient string
		err := tx.QueryRow(ctx, `
			UPDATE outbox
			SET status = $3,
			    attempts = $4,
			    sent_at = CASE WHEN $3 = 'sent' THEN now() END,
			    next_attempt_at = CASE WHEN $3 = 'pending' THEN now() + $5 * interval '1 millisecond' ELSE next_attempt_at END,
			    last_error = NULLIF($6, ''), lease_token = NULL, lease_until = NULL
			WHERE message_id = $1 AND lease_token = $2
			RETURNING recipient`,
			id, string(t.Token), string(t.To), t.Attempts, t.RetryIn.Milliseconds(), t.LastError).Scan(&recipient)
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.ErrLeaseLost
		}
		if err != nil || t.To == domain.Pending {
			return err
		}
		return promote(ctx, tx, recipient)
	})
}

// promote sobe a próxima queued do destinatário, a mais velha.
func promote(ctx context.Context, tx pgx.Tx, recipient string) error {
	err := lockRecipient(ctx, tx, recipient)
	if err == nil {
		_, err = tx.Exec(ctx, `
			UPDATE outbox SET status = 'pending', next_attempt_at = now()
			WHERE id = (SELECT id FROM outbox WHERE recipient = $1 AND status = 'queued' ORDER BY id LIMIT 1)`, recipient)
	}
	return err
}

// Expired lista as linhas em sending com lease vencido (worker morto ou travado), as
// mais antigas primeiro. Quem decide o destino é o domínio (OutboxEntry.Expire); a
// gravação passa pelo Finish, com o mesmo fencing do worker.
func (s *Store) Expired(ctx context.Context, limit int) ([]domain.OutboxEntry, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT message_id, attempts, lease_token::text, recipient FROM outbox
		WHERE status = 'sending' AND lease_until < now()
		ORDER BY lease_until LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("expired: %w", err)
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (domain.OutboxEntry, error) {
		e := domain.OutboxEntry{Status: domain.Sending}
		err := r.Scan(&e.ID, &e.Attempts, &e.Lease.Token, &e.Recipient)
		return e, err
	})
}

// Requeue é o redrive manual de uma dead: volta zerada, como cabeça se o destinatário
// estiver livre ou na frente da fila dele (a promoção pega a de menor id).
func (s *Store) Requeue(ctx context.Context, id int64) error {
	return s.tx(ctx, fmt.Sprintf("requeue %d", id), func(tx pgx.Tx) error {
		var recipient string
		err := tx.QueryRow(ctx, `SELECT recipient FROM outbox WHERE message_id = $1`, id).Scan(&recipient)
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.ErrNotFound
		}
		if err == nil {
			err = lockRecipient(ctx, tx, recipient)
		}
		var tag pgconn.CommandTag
		if err == nil {
			tag, err = tx.Exec(ctx, `
				UPDATE outbox SET status = `+headOrQueued+`, attempts = 0, next_attempt_at = now(), last_error = NULL
				WHERE message_id = $1 AND status = 'dead'`, id, recipient)
		}
		if err == nil && tag.RowsAffected() == 0 {
			err = domain.ErrInvalidTransition
		}
		return err
	})
}

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// QueueStats conta as não entregues por estado e mede a idade da mais antiga delas,
// as duas pelo índice outbox_unsent.
func (s *Store) QueueStats(ctx context.Context) (app.QueueStats, error) {
	st := app.QueueStats{ByStatus: map[domain.Status]int64{}}
	var status domain.Status
	var n int64
	rows, err := s.pool.Query(ctx, `SELECT status, count(*) FROM outbox WHERE status <> 'sent' GROUP BY status`)
	if err == nil {
		_, err = pgx.ForEachRow(rows, []any{&status, &n}, func() error {
			st.ByStatus[status] = n
			return nil
		})
	}
	if err == nil {
		var secs float64
		// LEAST de um min por estado: cada um sai do índice na hora, o IN junto varreria tudo
		err = s.pool.QueryRow(ctx, `SELECT coalesce((
			SELECT extract(epoch FROM now() - created_at)::float8 FROM messages WHERE id = LEAST(
				(SELECT min(message_id) FROM outbox WHERE status = 'queued'),
				(SELECT min(message_id) FROM outbox WHERE status = 'pending'),
				(SELECT min(message_id) FROM outbox WHERE status = 'sending'))), 0)`).Scan(&secs)
		st.OldestUnsent = time.Duration(secs * float64(time.Second))
	}
	if err != nil {
		return st, fmt.Errorf("queue stats: %w", err)
	}
	return st, nil
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
