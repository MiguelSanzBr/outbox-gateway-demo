package outbox_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/outbox"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/testdb"
)

// logBuffer guarda o log dos workers pra o teste conferir o q eles fizeram.
type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *logBuffer) waitFor(t *testing.T, msg string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		b.mu.Lock()
		found := strings.Contains(b.buf.String(), msg)
		b.mu.Unlock()
		if found {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("log sem %q", msg)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func runWorkers(t *testing.T, store *outbox.Store, sender outbox.Sender, cfg outbox.Config) *logBuffer {
	logs := &logBuffer{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { outbox.Run(ctx, store, sender, cfg, slog.New(slog.NewTextHandler(logs, nil))); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return logs
}

// Pool fechado = banco fora do ar. Todo método tem q devolver o erro, nunca engolir.
func brokenStore(t *testing.T, schema string) *outbox.Store {
	pool, store := testdb.New(t, schema)
	pool.Close()
	return store
}

// Trigger no schema do teste pra fazer um comando específico falhar dentro da transação.
func sabotage(t *testing.T, pool *pgxpool.Pool, table, body string) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `
		CREATE FUNCTION sabotage() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN `+body+` END $$;
		CREATE TRIGGER sabotage BEFORE INSERT OR UPDATE ON `+pgx.Identifier{table}.Sanitize()+`
		FOR EACH ROW EXECUTE FUNCTION sabotage();`)
	if err != nil {
		t.Fatal(err)
	}
}

type funcSender func(ctx context.Context, c *outbox.Claim) error

func (f funcSender) Send(ctx context.Context, c *outbox.Claim) error { return f(ctx, c) }

func TestStoreSurfacesDBErrors(t *testing.T) {
	store := brokenStore(t, "t_broken")
	ctx := context.Background()
	c := &outbox.Claim{ID: 1, LeaseToken: "00000000-0000-0000-0000-000000000000"}

	if _, _, err := store.Enqueue(ctx, "k", "ana", "oi"); err == nil {
		t.Error("Enqueue sem erro com banco fora")
	}
	if _, err := store.Claim(ctx, time.Second); err == nil {
		t.Error("Claim sem erro com banco fora")
	}
	if err := store.MarkSent(ctx, c); err == nil || errors.Is(err, outbox.ErrLeaseLost) {
		t.Errorf("MarkSent: %v", err)
	}
	if err := store.MarkFailed(ctx, c, "x", 0, false); err == nil || errors.Is(err, outbox.ErrLeaseLost) {
		t.Errorf("MarkFailed: %v", err)
	}
	if _, err := store.Reap(ctx, 1); err == nil {
		t.Error("Reap sem erro com banco fora")
	}
	if _, err := store.Get(ctx, 1); err == nil || errors.Is(err, outbox.ErrNotFound) {
		t.Errorf("Get: %v", err)
	}
}

// Se qualquer insert falhar, nada fica gravado: mensagem e outbox entram juntas ou não entram.
func TestEnqueueIsAtomic(t *testing.T) {
	cases := []struct{ name, table, body string }{
		{"falha no insert da mensagem", "messages", `RAISE EXCEPTION 'boom';`},
		{"falha no insert da outbox", "outbox", `RAISE EXCEPTION 'boom';`},
		// trigger q descarta a linha: o ON CONFLICT não devolve nada e a busca da chave também não acha
		{"mensagem sumiu entre o insert e a busca", "messages", `RETURN NULL;`},
	}
	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pool, store := testdb.New(t, "t_atomic"+string(rune('a'+i)))
			sabotage(t, pool, c.table, c.body)
			if _, _, err := store.Enqueue(context.Background(), "k", "ana", "oi"); err == nil {
				t.Fatal("esperava erro")
			}
			var n int
			if err := pool.QueryRow(context.Background(),
				`SELECT (SELECT count(*) FROM messages) + (SELECT count(*) FROM outbox)`).Scan(&n); err != nil || n != 0 {
				t.Fatalf("sobrou %d linha(s) depois do erro (err=%v)", n, err)
			}
		})
	}
}

func TestGetNotFound(t *testing.T) {
	_, store := testdb.New(t, "t_notfound")
	if _, err := store.Get(context.Background(), 42); !errors.Is(err, outbox.ErrNotFound) {
		t.Fatalf("err=%v, esperado ErrNotFound", err)
	}
}

func TestMarkFailedWithStaleLease(t *testing.T) {
	_, store := testdb.New(t, "t_stalefail")
	ctx := context.Background()
	if _, _, err := store.Enqueue(ctx, "k", "ana", "oi"); err != nil {
		t.Fatal(err)
	}
	c, err := store.Claim(ctx, time.Minute)
	if err != nil || c == nil {
		t.Fatalf("claim: %v", err)
	}
	c.LeaseToken = "00000000-0000-0000-0000-000000000000"
	if err := store.MarkFailed(ctx, c, "x", 0, false); !errors.Is(err, outbox.ErrLeaseLost) {
		t.Fatalf("err=%v, esperado ErrLeaseLost", err)
	}
}

func TestPermanentKeepsCause(t *testing.T) {
	cause := errors.New("HTTP 400")
	if !errors.Is(outbox.Permanent(cause), cause) {
		t.Fatal("Permanent perdeu o erro original")
	}
}

func TestSenderErrors(t *testing.T) {
	c := &outbox.Claim{ID: 1, IdempotencyKey: "k", Recipient: "ana", Body: "oi"}

	bad := outbox.HTTPSender{URL: "http://host com espaço/", Client: http.DefaultClient}
	if err := bad.Send(context.Background(), c); err == nil {
		t.Error("URL inválida não deu erro")
	}

	srv := httptest.NewServer(nil)
	srv.Close()
	down := outbox.HTTPSender{URL: srv.URL, Client: srv.Client()}
	if err := down.Send(context.Background(), c); err == nil {
		t.Error("provedor fora do ar não deu erro")
	}
}

// Banco caiu com os workers rodando: eles logam e continuam tentando, sem panic e sem travar o shutdown.
func TestWorkersSurviveDBOutage(t *testing.T) {
	cfg := testConfig()
	cfg.Workers = 1
	cfg.ReapInterval = 10 * time.Millisecond
	logs := runWorkers(t, brokenStore(t, "t_outage"), funcSender(func(context.Context, *outbox.Claim) error { return nil }), cfg)
	logs.waitFor(t, "msg=claim")
	logs.waitFor(t, "msg=reap")
}

// Worker demorou mais q o lease e o reaper devolveu a linha: ele não pode marcar sent com o token velho.
// A outra tentativa entrega e o attempts mostra as duas.
func TestWorkerThatLostLeaseDoesNotFinish(t *testing.T) {
	pool, store := testdb.New(t, "t_leaselost")
	ctx := context.Background()
	id, _, err := store.Enqueue(ctx, "k", "ana", "oi")
	if err != nil {
		t.Fatal(err)
	}

	cfg := testConfig()
	cfg.Workers = 1
	cfg.Lease = 40 * time.Millisecond
	cfg.ReapInterval = time.Hour
	var once sync.Once
	slow := funcSender(func(context.Context, *outbox.Claim) error {
		once.Do(func() {
			time.Sleep(60 * time.Millisecond)
			if _, err := store.Reap(ctx, 10); err != nil {
				t.Error(err)
			}
		})
		return nil
	})
	logs := runWorkers(t, store, slow, cfg)
	logs.waitFor(t, "lease perdido")
	waitAllSent(t, pool, 1)

	if st, err := store.Get(ctx, id); err != nil || st.Attempts != 2 {
		t.Fatalf("status %+v err=%v, esperado 2 tentativas", st, err)
	}
}

// Envio deu certo mas o UPDATE pra sent falhou: o worker loga e a linha fica pro reaper.
func TestWorkerLogsFinalizeError(t *testing.T) {
	pool, store := testdb.New(t, "t_finalize")
	if _, _, err := store.Enqueue(context.Background(), "k", "ana", "oi"); err != nil {
		t.Fatal(err)
	}
	sabotage(t, pool, "outbox", `IF NEW.status = 'sent' THEN RAISE EXCEPTION 'boom'; END IF; RETURN NEW;`)

	cfg := testConfig()
	cfg.Workers = 1
	cfg.Lease = time.Minute
	logs := runWorkers(t, store, funcSender(func(context.Context, *outbox.Claim) error { return nil }), cfg)
	logs.waitFor(t, "msg=finalizar")
}
