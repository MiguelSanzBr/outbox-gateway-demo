package postgres_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/adapters/postgres"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/domain"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/testdb"
)

// noBackoff devolve na hora: os testes daqui olham ordem e estado, não tempo.
var noBackoff = domain.RetryPolicy{MaxAttempts: 10}

func enqueue(t *testing.T, store *postgres.Store, key, recipient string) int64 {
	t.Helper()
	id, _, err := store.Enqueue(context.Background(), domain.Message{IdempotencyKey: key, Recipient: recipient, Body: key})
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func claim(t *testing.T, store *postgres.Store, lease time.Duration) *domain.OutboxEntry {
	t.Helper()
	c, err := store.Claim(context.Background(), 0, lease)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func claimedKey(c *domain.OutboxEntry) string {
	if c == nil {
		return "<nada>"
	}
	return c.IdempotencyKey
}

func wantClaim(t *testing.T, store *postgres.Store, key string) *domain.OutboxEntry {
	t.Helper()
	c := claim(t, store, time.Minute)
	if claimedKey(c) != key {
		t.Fatalf("claim pegou %s, esperado %s", claimedKey(c), key)
	}
	return c
}

func finish(t *testing.T, store *postgres.Store, c *domain.OutboxEntry, tr domain.Transition, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Finish(context.Background(), c.ID, tr); err != nil {
		t.Fatal(err)
	}
}

func status(t *testing.T, store *postgres.Store, id int64) domain.Status {
	t.Helper()
	st, err := store.Get(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return st.Status
}

// reap faz o q o caso de uso do reaper faz: lista as expiradas e aplica a regra do domínio.
func reap(t *testing.T, store *postgres.Store, p domain.RetryPolicy) int {
	t.Helper()
	ctx := context.Background()
	expired, err := store.Expired(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range expired {
		tr, err := e.Expire(p)
		finish(t, store, &e, tr, err)
	}
	return len(expired)
}

// Só a cabeça do destinatário fica em pending; as outras esperam em queued e só
// sobem quando a cabeça vira sent. Outro destinatário não espera ninguém.
func TestOnlyHeadOfRecipientIsClaimable(t *testing.T) {
	_, store := testdb.New(t, "t_head")
	ana1, ana2, bia1 := enqueue(t, store, "ana1", "ana"), enqueue(t, store, "ana2", "ana"), enqueue(t, store, "bia1", "bia")
	for id, want := range map[int64]domain.Status{ana1: domain.Pending, ana2: domain.Queued, bia1: domain.Pending} {
		if got := status(t, store, id); got != want {
			t.Fatalf("mensagem %d: %s, esperado %s", id, got, want)
		}
	}

	c := wantClaim(t, store, "ana1")
	wantClaim(t, store, "bia1")
	wantClaim(t, store, "<nada>") // ana2 espera a ana1
	tr, err := c.Complete()
	finish(t, store, c, tr, err)
	if st, _ := store.Get(context.Background(), ana1); st.SentAt == nil {
		t.Fatal("sent sem sent_at: a latência do README sai dele")
	}
	if got := status(t, store, ana2); got != domain.Pending {
		t.Fatalf("ana2 depois do sent da ana1: %s, esperado pending", got)
	}
	wantClaim(t, store, "ana2")
}

// Cabeça em backoff segura só o próprio destinatário: o claim pula pra outro e não
// devolve nem ela (antes da hora) nem a sucessora.
func TestHeadInBackoffHoldsOnlyItsRecipient(t *testing.T) {
	_, store := testdb.New(t, "t_backoff")
	enqueue(t, store, "ana1", "ana")
	enqueue(t, store, "ana2", "ana")
	enqueue(t, store, "bia1", "bia")

	c := wantClaim(t, store, "ana1")
	tr, err := c.Fail(errors.New("provedor: HTTP 503"), domain.RetryPolicy{MaxAttempts: 5, BackoffBase: time.Hour, BackoffMax: time.Hour})
	finish(t, store, c, tr, err)
	wantClaim(t, store, "bia1")
	wantClaim(t, store, "<nada>")
}

// dead libera o destinatário: a próxima sobe na mesma transação.
func TestDeadPromotesNextOfRecipient(t *testing.T) {
	_, store := testdb.New(t, "t_deadnext")
	enqueue(t, store, "ana1", "ana")
	enqueue(t, store, "ana2", "ana")
	enqueue(t, store, "ana3", "ana")
	c := wantClaim(t, store, "ana1")
	tr, err := c.Fail(domain.Permanent(errors.New("HTTP 400")), noBackoff)
	finish(t, store, c, tr, err)
	wantClaim(t, store, "ana2") // e não a ana3: sobe a mais velha
}

// Rede de segurança no banco: no máximo uma cabeça (pending ou sending) por destinatário.
func TestDBRefusesTwoHeadsPerRecipient(t *testing.T) {
	pool, store := testdb.New(t, "t_twoheads")
	enqueue(t, store, "ana1", "ana")
	enqueue(t, store, "ana2", "ana")
	_, err := pool.Exec(context.Background(), `UPDATE outbox SET status = 'pending' WHERE status = 'queued'`)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
		t.Fatalf("err=%v, esperado violação do índice único da cabeça", err)
	}
}

// Workers não esperam uns pelos outros: linha travada por outra transação é pulada,
// não esperada (sem o SKIP LOCKED o claim ficaria preso até o timeout).
func TestClaimSkipsLockedRows(t *testing.T) {
	pool, store := testdb.New(t, "t_skiplocked")
	ctx := context.Background()
	ana := enqueue(t, store, "ana1", "ana")
	enqueue(t, store, "bia1", "bia")

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT 1 FROM outbox WHERE message_id = $1 FOR UPDATE`, ana); err != nil {
		t.Fatal(err)
	}
	short, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	c, err := store.Claim(short, 0, time.Minute)
	if err != nil || claimedKey(c) != "bia1" {
		t.Fatalf("claim com a ana travada: %s err=%v, esperado bia1 sem esperar", claimedKey(c), err)
	}
}

// O id da linha reivindicada é o da mensagem (o da API e do painel), mesmo quando os
// dois BIGSERIAL já divergiram por causa de chave repetida.
func TestClaimUsesMessageID(t *testing.T) {
	_, store := testdb.New(t, "t_msgid")
	enqueue(t, store, "k1", "ana")
	enqueue(t, store, "k1", "ana") // repetida: gasta um id de messages sem criar linha na outbox
	want := enqueue(t, store, "k2", "bia")
	wantClaim(t, store, "k1")
	if c := wantClaim(t, store, "k2"); c.ID != want {
		t.Fatalf("claim devolveu id %d, esperado o da mensagem %d", c.ID, want)
	}
}

// Reaper: antes da última tentativa volta com backoff; na última vira dead com o
// motivo de resultado incerto e libera a próxima do destinatário.
func TestReapAppliesDomainRule(t *testing.T) {
	_, store := testdb.New(t, "t_reap")
	ana1 := enqueue(t, store, "ana1", "ana")
	enqueue(t, store, "ana2", "ana")
	bia1 := enqueue(t, store, "bia1", "bia")

	claim(t, store, time.Millisecond) // ana1
	claim(t, store, time.Millisecond) // bia1
	time.Sleep(20 * time.Millisecond)
	if n := reap(t, store, domain.RetryPolicy{MaxAttempts: 1}); n != 2 {
		t.Fatalf("reap devolveu %d, esperado 2", n)
	}
	st, err := store.Get(context.Background(), ana1)
	if err != nil || st.Status != domain.Dead || st.LastError == nil || !strings.Contains(*st.LastError, "incerto") {
		t.Fatalf("ana1 depois do reaper na última tentativa: %+v err=%v", st, err)
	}
	if status(t, store, bia1) != domain.Dead {
		t.Fatal("bia1 devia ter virado dead")
	}
	c := wantClaim(t, store, "ana2")

	// antes da última: volta pra pending, mas só depois do backoff
	tr, err := c.Fail(errors.New("timeout"), noBackoff) // ana2 volta pra pending
	finish(t, store, c, tr, err)
	claim(t, store, time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	reap(t, store, domain.RetryPolicy{MaxAttempts: 5, BackoffBase: time.Hour, BackoffMax: time.Hour})
	wantClaim(t, store, "<nada>")
}

// Release devolve a tentativa: a linha volta pra pending com o attempts de antes do claim
// e pode ser reivindicada na hora.
func TestReleaseGivesAttemptBack(t *testing.T) {
	_, store := testdb.New(t, "t_release")
	id := enqueue(t, store, "ana1", "ana")
	c := wantClaim(t, store, "ana1")
	tr, err := c.Release()
	finish(t, store, c, tr, err)
	if st, _ := store.Get(context.Background(), id); st.Status != domain.Pending || st.Attempts != 0 {
		t.Fatalf("depois do release: %+v, esperado pending com 0 tentativas", st)
	}
	if c := wantClaim(t, store, "ana1"); c.Attempts != 1 {
		t.Fatalf("claim depois do release: attempts=%d, esperado 1", c.Attempts)
	}
}

// Redrive: dead volta zerada, na frente da fila do destinatário se ele já tiver cabeça.
func TestRequeueDead(t *testing.T) {
	_, store := testdb.New(t, "t_requeue")
	ctx := context.Background()
	ana1 := enqueue(t, store, "ana1", "ana")
	ana2 := enqueue(t, store, "ana2", "ana")
	c := wantClaim(t, store, "ana1")
	tr, err := c.Fail(domain.Permanent(errors.New("HTTP 400")), noBackoff)
	finish(t, store, c, tr, err)

	if err := store.Requeue(ctx, ana1); err != nil {
		t.Fatal(err)
	}
	st, _ := store.Get(ctx, ana1)
	if st.Status != domain.Queued || st.Attempts != 0 {
		t.Fatalf("ana1 depois do redrive com a ana2 de cabeça: %+v, esperado queued com 0 tentativas", st)
	}
	c = wantClaim(t, store, "ana2")
	tr, err = c.Complete()
	finish(t, store, c, tr, err)
	wantClaim(t, store, "ana1")

	if err := store.Requeue(ctx, ana2); !errors.Is(err, domain.ErrInvalidTransition) {
		t.Fatalf("redrive de sent: err=%v, esperado ErrInvalidTransition", err)
	}
	if err := store.Requeue(ctx, 999); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("redrive de id q não existe: err=%v, esperado ErrNotFound", err)
	}

	// sem cabeça no destinatário, o redrive já volta como cabeça
	bia := enqueue(t, store, "bia1", "bia")
	c = wantClaim(t, store, "bia1")
	tr, err = c.Fail(domain.Permanent(errors.New("HTTP 400")), noBackoff)
	finish(t, store, c, tr, err)
	if err := store.Requeue(ctx, bia); err != nil || status(t, store, bia) != domain.Pending {
		t.Fatalf("redrive sem cabeça: status=%s err=%v, esperado pending", status(t, store, bia), err)
	}
}

// Os estados vão como literal no SQL (índice parcial só casa com constante): o CHECK
// da tabela tem q aceitar exatamente os do domínio.
func TestSQLStatusesMatchDomain(t *testing.T) {
	pool, _ := testdb.New(t, "t_statuses")
	var def string
	if err := pool.QueryRow(context.Background(), `
		SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'outbox_status_check' AND conrelid = 'outbox'::regclass`).Scan(&def); err != nil {
		t.Fatal(err)
	}
	for _, s := range []domain.Status{domain.Queued, domain.Pending, domain.Sending, domain.Sent, domain.Dead} {
		if !strings.Contains(def, "'"+string(s)+"'") {
			t.Errorf("CHECK %s sem o estado %s", def, s)
		}
	}
	if n := strings.Count(def, "'"); n != 2*5 {
		t.Errorf("CHECK %s com estado q o domínio não tem", def)
	}
}

// Se promover a próxima falha, o sent da cabeça também não fica: senão o destinatário
// ficaria sem cabeça e a fila dele parada pra sempre.
func TestFinishRollsBackWhenPromoteFails(t *testing.T) {
	pool, store := testdb.New(t, "t_promotefail")
	ana1 := enqueue(t, store, "ana1", "ana")
	enqueue(t, store, "ana2", "ana")
	c := wantClaim(t, store, "ana1")
	testdb.Sabotage(t, pool, "outbox", `IF OLD.status = 'queued' THEN RAISE EXCEPTION 'boom'; END IF; RETURN NEW;`)
	tr, _ := c.Complete()
	if err := store.Finish(context.Background(), c.ID, tr); err == nil {
		t.Fatal("Finish sem erro com a promoção quebrada")
	}
	if got := status(t, store, ana1); got != domain.Sending {
		t.Fatalf("ana1 = %s, esperado sending (rollback inteiro)", got)
	}
}

// Redrive com o UPDATE falhando devolve o erro e não muda nada.
func TestRequeueSurfacesDBError(t *testing.T) {
	pool, store := testdb.New(t, "t_requeuefail")
	id := enqueue(t, store, "ana1", "ana")
	c := wantClaim(t, store, "ana1")
	tr, err := c.Fail(domain.Permanent(errors.New("HTTP 400")), noBackoff)
	finish(t, store, c, tr, err)
	testdb.Sabotage(t, pool, "outbox", `RAISE EXCEPTION 'boom';`)
	if err := store.Requeue(context.Background(), id); err == nil || errors.Is(err, domain.ErrInvalidTransition) {
		t.Fatalf("err=%v, esperado o erro do banco", err)
	}
}
