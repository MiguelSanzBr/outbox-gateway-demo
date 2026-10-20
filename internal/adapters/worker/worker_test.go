package worker_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/adapters/mockprovider"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/adapters/postgres"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/adapters/provider"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/adapters/worker"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/app"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/domain"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/testdb"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// testCfg junta a config do worker e a dos casos de uso pra o teste mexer num lugar só.
type testCfg struct {
	Workers                    int
	Lease                      time.Duration
	PollInterval, ReapInterval time.Duration
	MaxAttempts                int
	BackoffBase, BackoffMax    time.Duration
}

func testConfig() testCfg {
	return testCfg{
		Workers:      8,
		Lease:        2 * time.Second,
		PollInterval: 5 * time.Millisecond,
		ReapInterval: 50 * time.Millisecond,
		MaxAttempts:  50,
		BackoffBase:  2 * time.Millisecond,
		BackoffMax:   20 * time.Millisecond,
	}
}

func (c testCfg) split(store *postgres.Store, sender app.Sender) (*app.Service, worker.Config) {
	svc := app.NewService(store, sender, store, app.Config{
		Lease: c.Lease,
		Retry: domain.RetryPolicy{MaxAttempts: c.MaxAttempts, BackoffBase: c.BackoffBase, BackoffMax: c.BackoffMax},
	})
	return svc, worker.Config{Workers: c.Workers, PollInterval: c.PollInterval, ReapInterval: c.ReapInterval}
}

func startFleet(ctx context.Context, store *postgres.Store, sender app.Sender, c testCfg) *worker.Fleet {
	svc, wc := c.split(store, sender)
	return worker.Start(ctx, svc, wc, quiet)
}

func startProvider(t *testing.T, cfg mockprovider.Config) (*mockprovider.Provider, provider.HTTPSender) {
	p := mockprovider.New(cfg)
	srv := httptest.NewServer(p.Handler())
	t.Cleanup(srv.Close)
	return p, provider.HTTPSender{URL: srv.URL + "/v1/messages", Client: srv.Client()}
}

func startWorkers(t *testing.T, store *postgres.Store, sender app.Sender, cfg testCfg) {
	svc, wc := cfg.split(store, sender)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { worker.Run(ctx, svc, wc, quiet); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
}

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

func runWorkers(t *testing.T, store *postgres.Store, sender app.Sender, cfg testCfg) *logBuffer {
	svc, wc := cfg.split(store, sender)
	logs := &logBuffer{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { worker.Run(ctx, svc, wc, slog.New(slog.NewTextHandler(logs, nil))); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return logs
}

type funcSender func(ctx context.Context, c *domain.OutboxEntry) error

func (f funcSender) Send(ctx context.Context, c *domain.OutboxEntry) error { return f(ctx, c) }

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
				if _, _, err := store.Enqueue(context.Background(), domain.Message{IdempotencyKey: key, Recipient: fmt.Sprintf("r%d", r), Body: key}); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	testdb.WaitAllSent(t, pool, recipients*perRecipient)

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

// Vários produtores ao mesmo tempo pro mesmo destinatário: os commits chegam em
// qualquer ordem, e mesmo assim nada trava, nada duplica e o provedor recebe na ordem
// de commit (X-Sequence só cresce). Antes, um commit invertido fazia todo claim bater
// no índice único e a entrega parava pra todo mundo.
func TestConcurrentProducersSameRecipient(t *testing.T) {
	pool, store := testdb.New(t, "t_samerecipient")
	provider, sender := startProvider(t, mockprovider.Config{Jitter: 2 * time.Millisecond})
	cfg := testConfig()
	cfg.Workers = 4
	logs := runWorkers(t, store, sender, cfg)

	const producers, each = 10, 10
	var wg sync.WaitGroup
	for p := range producers {
		wg.Go(func() {
			for i := range each {
				key := fmt.Sprintf("p%d-m%d", p, i)
				if _, _, err := store.Enqueue(context.Background(), domain.Message{IdempotencyKey: key, Recipient: "solo", Body: key}); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()
	testdb.WaitAllSent(t, pool, producers*each)

	if st := provider.Stats(); st.Accepted != producers*each || st.Duplicates != 0 || st.OrderViolations != 0 {
		t.Fatalf("stats do provedor: %+v", st)
	}
	// a ordem de cada produtor também se mantém: o commit dele é sequencial
	last := map[string]int{}
	for _, body := range provider.Delivered("solo") {
		var p, i int
		_, _ = fmt.Sscanf(body, "p%d-m%d", &p, &i)
		if prev, ok := last[fmt.Sprint(p)]; ok && i <= prev {
			t.Fatalf("produtor %d fora de ordem: m%d depois de m%d", p, i, prev)
		}
		last[fmt.Sprint(p)] = i
	}
	logs.mu.Lock()
	defer logs.mu.Unlock()
	if strings.Contains(logs.buf.String(), "msg=claim") {
		t.Fatalf("claim deu erro com produtores concorrentes:\n%s", logs.buf.String())
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
		if _, _, err := store.Enqueue(ctx, domain.Message{IdempotencyKey: k, Recipient: "ana", Body: k}); err != nil {
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
	testdb.WaitAllSent(t, pool, 2)

	if got := provider.Delivered("ana"); !slices.Equal(got, []string{"m1", "m2"}) {
		t.Fatalf("ordem: %v", got)
	}
	if st := provider.Stats(); st.Accepted != 2 || st.Duplicates != 1 || st.OrderViolations != 0 {
		t.Fatalf("stats do provedor: %+v (esperado 1 reenvio descartado)", st)
	}
	var attempts int
	if err := pool.QueryRow(ctx, `SELECT attempts FROM outbox WHERE message_id = $1`, dying.ID).Scan(&attempts); err != nil || attempts != 2 {
		t.Fatalf("attempts de m1 = %d (err=%v), esperado 2", attempts, err)
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

	id, _, err := store.Enqueue(ctx, domain.Message{IdempotencyKey: "k", Recipient: "caio", Body: "oi"})
	if err != nil {
		t.Fatal(err)
	}
	startWorkers(t, store, provider.HTTPSender{URL: reject.URL, Client: reject.Client()}, testConfig())

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

// Banco caiu com os workers rodando: eles logam e continuam tentando, sem panic e sem travar o shutdown.
func TestWorkersSurviveDBOutage(t *testing.T) {
	cfg := testConfig()
	cfg.Workers = 1
	cfg.ReapInterval = 10 * time.Millisecond
	logs := runWorkers(t, testdb.Broken(t, "t_outage"), funcSender(func(context.Context, *domain.OutboxEntry) error { return nil }), cfg)
	logs.waitFor(t, "msg=claim")
	logs.waitFor(t, "msg=reap")
}

// Com o banco fora, o worker espera mais a cada erro seguido em vez de martelar o banco
// e o log no ritmo do poll.
func TestClaimErrorsBackOff(t *testing.T) {
	cfg := testConfig()
	cfg.Workers = 1
	cfg.PollInterval = time.Millisecond
	cfg.ReapInterval = time.Hour
	logs := runWorkers(t, testdb.Broken(t, "t_claimbackoff"), funcSender(func(context.Context, *domain.OutboxEntry) error { return nil }), cfg)
	time.Sleep(300 * time.Millisecond)
	logs.mu.Lock()
	n := strings.Count(logs.buf.String(), "msg=claim")
	logs.mu.Unlock()
	if n == 0 || n > 20 {
		t.Fatalf("%d erros de claim em 300 ms com poll de 1 ms, esperado poucos (backoff)", n)
	}
}

// Worker demorou mais q o lease e o reaper devolveu a linha: ele não pode marcar sent com o token velho.
// A outra tentativa entrega e o attempts mostra as duas.
func TestWorkerThatLostLeaseDoesNotFinish(t *testing.T) {
	pool, store := testdb.New(t, "t_leaselost")
	ctx := context.Background()
	id, _, err := store.Enqueue(ctx, domain.Message{IdempotencyKey: "k", Recipient: "ana", Body: "oi"})
	if err != nil {
		t.Fatal(err)
	}

	cfg := testConfig()
	cfg.Workers = 1
	cfg.Lease = 200 * time.Millisecond
	cfg.ReapInterval = time.Hour
	reaper := app.NewService(store, nil, store, app.Config{Retry: domain.RetryPolicy{MaxAttempts: 10}})
	var once sync.Once
	slow := funcSender(func(context.Context, *domain.OutboxEntry) error {
		once.Do(func() {
			time.Sleep(300 * time.Millisecond)
			if _, err := reaper.ReapExpiredLeases(ctx); err != nil {
				t.Error(err)
			}
		})
		return nil
	})
	logs := runWorkers(t, store, slow, cfg)
	logs.waitFor(t, "lease perdido")
	testdb.WaitAllSent(t, pool, 1)

	if st, err := store.Get(ctx, id); err != nil || st.Attempts != 2 {
		t.Fatalf("status %+v err=%v, esperado 2 tentativas", st, err)
	}
}

// Envio deu certo mas o UPDATE pra sent falhou: o worker loga e a linha fica pro reaper.
func TestWorkerLogsFinalizeError(t *testing.T) {
	pool, store := testdb.New(t, "t_finalize")
	if _, _, err := store.Enqueue(context.Background(), domain.Message{IdempotencyKey: "k", Recipient: "ana", Body: "oi"}); err != nil {
		t.Fatal(err)
	}
	testdb.Sabotage(t, pool, "outbox", `IF NEW.status = 'sent' THEN RAISE EXCEPTION 'boom'; END IF; RETURN NEW;`)

	cfg := testConfig()
	cfg.Workers = 1
	cfg.Lease = time.Minute
	logs := runWorkers(t, store, funcSender(func(context.Context, *domain.OutboxEntry) error { return nil }), cfg)
	logs.waitFor(t, "msg=finalizar")
}
