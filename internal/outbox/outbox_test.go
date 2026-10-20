package outbox_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/mockprovider"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/outbox"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/testdb"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func testConfig() outbox.Config {
	return outbox.Config{
		Workers:      8,
		Lease:        2 * time.Second,
		PollInterval: 5 * time.Millisecond,
		ReapInterval: 50 * time.Millisecond,
		MaxAttempts:  50,
		BackoffBase:  2 * time.Millisecond,
		BackoffMax:   20 * time.Millisecond,
	}
}

func startProvider(t *testing.T, cfg mockprovider.Config) (*mockprovider.Provider, outbox.HTTPSender) {
	p := mockprovider.New(cfg)
	srv := httptest.NewServer(p.Handler())
	t.Cleanup(srv.Close)
	return p, outbox.HTTPSender{URL: srv.URL + "/v1/messages", Client: srv.Client()}
}

func startWorkers(t *testing.T, store *outbox.Store, sender outbox.Sender, cfg outbox.Config) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { outbox.Run(ctx, store, sender, cfg, quiet); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
}

func waitAllSent(t *testing.T, pool *pgxpool.Pool, want int) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		var sent, open int
		err := pool.QueryRow(context.Background(), `
			SELECT count(*) FILTER (WHERE status = 'sent'), count(*) FILTER (WHERE status <> 'sent') FROM outbox`).
			Scan(&sent, &open)
		if err != nil {
			t.Fatal(err)
		}
		if sent == want && open == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout: sent=%d outros=%d, esperado sent=%d", sent, open, want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestEnqueueIsIdempotent(t *testing.T) {
	pool, store := testdb.New(t, "t_enqueue")
	ctx := context.Background()

	id1, created1, err := store.Enqueue(ctx, "k1", "r1", "oi")
	if err != nil || !created1 {
		t.Fatalf("primeiro enqueue: id=%d created=%v err=%v", id1, created1, err)
	}
	id2, created2, err := store.Enqueue(ctx, "k1", "r1", "oi")
	if err != nil || created2 || id2 != id1 {
		t.Fatalf("repetição: id=%d created=%v err=%v", id2, created2, err)
	}
	if _, _, err := store.Enqueue(ctx, "k1", "r1", "outro corpo"); !errors.Is(err, outbox.ErrKeyConflict) {
		t.Fatalf("conteúdo diferente: err=%v, esperado ErrKeyConflict", err)
	}

	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox`).Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("outbox tem %d linhas (err=%v), esperado 1", rows, err)
	}
}

// Produtores concorrentes, provedor que falha 20% das vezes e 8 workers:
// tudo entregue, nada duplicado, ordem por destinatário preservada.
func TestDeliveryNoLossNoDuplicatesInOrder(t *testing.T) {
	pool, store := testdb.New(t, "t_delivery")
	provider, sender := startProvider(t, mockprovider.Config{Jitter: 3 * time.Millisecond, FailRate: 0.2})
	startWorkers(t, store, sender, testConfig())

	const recipients, perRecipient = 40, 25
	var wg sync.WaitGroup
	for r := range recipients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perRecipient {
				key := fmt.Sprintf("r%d-m%d", r, i)
				if _, _, err := store.Enqueue(context.Background(), key, fmt.Sprintf("r%d", r), key); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	waitAllSent(t, pool, recipients*perRecipient)

	st := provider.Stats()
	if st.Accepted != recipients*perRecipient || st.Duplicates != 0 || st.OrderViolations != 0 {
		t.Fatalf("stats do provedor: %+v", st)
	}
	if st.FailuresInjected == 0 {
		t.Fatal("nenhuma falha injetada; o teste não exercitou o retry")
	}
	for r := range recipients {
		got := provider.Delivered(fmt.Sprintf("r%d", r))
		want := make([]string, perRecipient)
		for i := range want {
			want[i] = fmt.Sprintf("r%d-m%d", r, i)
		}
		if !slices.Equal(got, want) {
			t.Fatalf("ordem de r%d:\n got %v\nwant %v", r, got, want)
		}
	}
}

// Um worker reivindica a mensagem, o provedor recebe, e o worker morre antes de
// marcar 'sent'. O reaper devolve a linha à fila, outro worker reenvia, o
// provedor descarta o reenvio pela chave e a mensagem seguinte só sai depois.
func TestReaperRecoversWorkerKilledMidSend(t *testing.T) {
	pool, store := testdb.New(t, "t_reaper")
	provider, sender := startProvider(t, mockprovider.Config{})
	ctx := context.Background()

	for _, k := range []string{"m1", "m2"} {
		if _, _, err := store.Enqueue(ctx, k, "ana", k); err != nil {
			t.Fatal(err)
		}
	}

	dying, err := store.Claim(ctx, 0, 200*time.Millisecond)
	if err != nil || dying == nil || dying.IdempotencyKey != "m1" {
		t.Fatalf("claim: %+v err=%v", dying, err)
	}
	if err := sender.Send(ctx, dying); err != nil {
		t.Fatal(err)
	}
	// Enquanto m1 está em voo, m2 (mesmo destinatário) não pode ser reivindicada.
	if c, err := store.Claim(ctx, 0, time.Second); err != nil || c != nil {
		t.Fatalf("m2 reivindicada com m1 em voo: %+v err=%v", c, err)
	}

	startWorkers(t, store, sender, testConfig())
	waitAllSent(t, pool, 2)

	if got := provider.Delivered("ana"); !slices.Equal(got, []string{"m1", "m2"}) {
		t.Fatalf("ordem: %v", got)
	}
	if st := provider.Stats(); st.Accepted != 2 || st.Duplicates != 1 || st.OrderViolations != 0 {
		t.Fatalf("stats do provedor: %+v (esperado 1 reenvio descartado)", st)
	}
	var attempts int
	if err := pool.QueryRow(ctx, `SELECT attempts FROM outbox WHERE id = $1`, dying.ID).Scan(&attempts); err != nil || attempts != 2 {
		t.Fatalf("attempts de m1 = %d (err=%v), esperado 2", attempts, err)
	}
}

// Um worker travado que acorda depois do reaper não consegue finalizar a linha.
func TestStaleLeaseCannotFinish(t *testing.T) {
	_, store := testdb.New(t, "t_fencing")
	ctx := context.Background()
	if _, _, err := store.Enqueue(ctx, "k", "bia", "oi"); err != nil {
		t.Fatal(err)
	}
	stale, err := store.Claim(ctx, 0, time.Millisecond)
	if err != nil || stale == nil {
		t.Fatalf("claim: %v", err)
	}
	time.Sleep(10 * time.Millisecond)
	if n, err := store.Reap(ctx, 10); err != nil || n != 1 {
		t.Fatalf("reap: n=%d err=%v", n, err)
	}
	fresh, err := store.Claim(ctx, 0, time.Minute)
	if err != nil || fresh == nil {
		t.Fatalf("novo claim: %v", err)
	}

	if err := store.MarkSent(ctx, stale); !errors.Is(err, outbox.ErrLeaseLost) {
		t.Fatalf("MarkSent com lease velho: err=%v, esperado ErrLeaseLost", err)
	}
	if err := store.MarkSent(ctx, fresh); err != nil {
		t.Fatal(err)
	}
}

// Erro 4xx do provedor não é repetido: a mensagem vai para 'dead' e libera o destinatário.
func TestPermanentErrorGoesDead(t *testing.T) {
	_, store := testdb.New(t, "t_dead")
	ctx := context.Background()
	reject := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	t.Cleanup(reject.Close)

	id, _, err := store.Enqueue(ctx, "k", "caio", "oi")
	if err != nil {
		t.Fatal(err)
	}
	startWorkers(t, store, outbox.HTTPSender{URL: reject.URL, Client: reject.Client()}, testConfig())

	deadline := time.Now().Add(5 * time.Second)
	for {
		st, err := store.Get(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if st.Status == "dead" {
			if st.Attempts != 1 {
				t.Fatalf("attempts = %d, esperado 1", st.Attempts)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("status = %s", st.Status)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
