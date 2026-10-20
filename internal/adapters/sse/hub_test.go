package sse_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/adapters/sse"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/app"
)

// upstream é o LISTEN de mentira: conta quantas vezes abriu e deixa o teste empurrar
// mudança ou derrubar a conexão.
type upstream struct {
	mu    sync.Mutex
	opens int
	ch    chan app.Change
	ctx   context.Context
	fail  error
}

func (u *upstream) Listen(ctx context.Context) (<-chan app.Change, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.fail != nil {
		return nil, u.fail
	}
	u.opens++
	u.ch, u.ctx = make(chan app.Change), ctx
	ch := u.ch
	go func() {
		<-ctx.Done()
		u.mu.Lock()
		defer u.mu.Unlock()
		if u.ch == ch {
			close(ch)
			u.ch = nil
		}
	}()
	return ch, nil
}

func (u *upstream) push(c app.Change) {
	u.mu.Lock()
	ch := u.ch
	u.mu.Unlock()
	ch <- c
}

// drop simula a conexão do LISTEN caindo.
func (u *upstream) drop() {
	u.mu.Lock()
	defer u.mu.Unlock()
	close(u.ch)
	u.ch = nil
}

func (u *upstream) count() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.opens
}

func recv(t *testing.T, ch <-chan app.Change) (app.Change, bool) {
	t.Helper()
	select {
	case c, ok := <-ch:
		return c, ok
	case <-time.After(5 * time.Second):
		t.Fatal("nada chegou no stream")
	}
	return app.Change{}, false
}

func waitChanClosed(t *testing.T, ch <-chan app.Change) {
	t.Helper()
	for {
		if _, ok := recv(t, ch); !ok {
			return
		}
	}
}

// Um LISTEN só pra todos os streams do processo, cada mudança pra todos eles.
func TestHubFansOutOneListen(t *testing.T) {
	up := &upstream{}
	hub := sse.NewHub(up, 10, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a, errA := hub.Listen(ctx)
	b, errB := hub.Listen(ctx)
	if errA != nil || errB != nil {
		t.Fatal(errA, errB)
	}
	up.push(app.Change{ID: 1})
	if ca, _ := recv(t, a); ca.ID != 1 {
		t.Fatalf("a recebeu %+v", ca)
	}
	if cb, _ := recv(t, b); cb.ID != 1 {
		t.Fatalf("b recebeu %+v", cb)
	}
	if n := up.count(); n != 1 {
		t.Fatalf("abriu %d LISTEN, esperado 1", n)
	}
}

// Acima do teto o stream é recusado em vez de abrir mais coisa.
func TestHubCapsStreams(t *testing.T) {
	hub := sse.NewHub(&upstream{}, 2, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for range 2 {
		if _, err := hub.Listen(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := hub.Listen(ctx); !errors.Is(err, sse.ErrTooManyStreams) {
		t.Fatalf("terceiro stream: err=%v, esperado ErrTooManyStreams", err)
	}
}

// Cliente q não lê é derrubado quando o buffer enche: ele reconecta e recarrega a foto.
// Os outros seguem recebendo, e quem empurra nunca fica preso esperando o lento.
func TestHubDropsSlowStream(t *testing.T) {
	up := &upstream{}
	hub := sse.NewHub(up, 10, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	slow, _ := hub.Listen(ctx)
	fast, _ := hub.Listen(ctx)
	for i := range 5 {
		up.push(app.Change{ID: int64(i)})
		if c, _ := recv(t, fast); c.ID != int64(i) {
			t.Fatalf("rápido recebeu %+v, esperado %d", c, i)
		}
	}
	got := 0
	for range slow {
		got++
	}
	if got != 2 {
		t.Fatalf("lento recebeu %d antes de cair, esperado o buffer (2)", got)
	}
}

// Sem ninguém ouvindo o hub larga a conexão; o próximo stream abre outra.
func TestHubReleasesListenWhenIdle(t *testing.T) {
	up := &upstream{}
	hub := sse.NewHub(up, 10, 8)
	ctx, cancel := context.WithCancel(context.Background())
	ch, _ := hub.Listen(ctx)
	first := up.ctx
	cancel()
	waitChanClosed(t, ch)
	select {
	case <-first.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("LISTEN continuou aberto sem nenhum stream")
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	if _, err := hub.Listen(ctx2); err != nil || up.count() != 2 {
		t.Fatalf("novo stream: err=%v LISTEN abertos=%d, esperado 2", err, up.count())
	}
}

// LISTEN caiu: os streams fecham (o EventSource reconecta) e o próximo abre outro.
// Se abrir falhar, o erro volta pro handler e nada fica registrado.
func TestHubReconnectsAfterListenDrops(t *testing.T) {
	up := &upstream{}
	hub := sse.NewHub(up, 1, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, _ := hub.Listen(ctx)
	up.drop()
	waitChanClosed(t, ch)

	up.mu.Lock()
	up.fail = errors.New("banco fora")
	up.mu.Unlock()
	if _, err := hub.Listen(ctx); err == nil {
		t.Fatal("Listen sem erro com o LISTEN falhando")
	}
	up.mu.Lock()
	up.fail = nil
	up.mu.Unlock()
	// teto 1: se o stream q falhou tivesse ficado registrado, este daria ErrTooManyStreams
	ch, err := hub.Listen(ctx)
	if err != nil {
		t.Fatal(err)
	}
	up.push(app.Change{ID: 9})
	if c, _ := recv(t, ch); c.ID != 9 || up.count() != 2 {
		t.Fatalf("depois de reconectar: %+v, LISTEN abertos=%d", c, up.count())
	}
}
