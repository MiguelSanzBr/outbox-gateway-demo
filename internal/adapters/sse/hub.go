package sse

import (
	"context"
	"errors"
	"sync"

	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/app"
)

// ErrTooManyStreams: o processo já tem o máximo de streams abertos.
var ErrTooManyStreams = errors.New("limite de streams SSE atingido")

// Hub é a porta app.EventSource por cima de outra (o LISTEN do postgres): um LISTEN só
// pro processo inteiro, repassado pra cada stream. Sem isso cada aba do painel segurava
// uma conexão de banco e umas 80 abas derrubavam a entrega.
//
// Cada stream tem um buffer; quem não lê a tempo é derrubado em vez de segurar os outros
// (e, pior, a conexão do LISTEN: a fila de NOTIFY do banco enche e todo commit q
// notifica passa a falhar). Derrubado, o EventSource reconecta e recarrega a foto.
type Hub struct {
	src    app.EventSource
	max    int
	buffer int

	mu   sync.Mutex
	subs map[chan app.Change]uint64 // stream → geração do LISTEN q ele ouve
	gen  uint64
	stop context.CancelFunc // nil = LISTEN fechado
}

func NewHub(src app.EventSource, maxStreams, buffer int) *Hub {
	return &Hub{src: src, max: maxStreams, buffer: buffer, subs: map[chan app.Change]uint64{}}
}

// Listen abre um stream. O LISTEN sobe no primeiro e fecha quando sai o último.
func (h *Hub) Listen(ctx context.Context) (<-chan app.Change, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.subs) >= h.max {
		return nil, ErrTooManyStreams
	}
	if h.stop == nil {
		upCtx, stop := context.WithCancel(context.Background())
		up, err := h.src.Listen(upCtx)
		if err != nil {
			stop()
			return nil, err
		}
		h.gen++
		h.stop = stop
		go h.pump(up, h.gen)
	}
	ch := make(chan app.Change, h.buffer)
	h.subs[ch] = h.gen
	context.AfterFunc(ctx, func() { h.leave(ch) })
	return ch, nil
}

func (h *Hub) pump(up <-chan app.Change, gen uint64) {
	for c := range up {
		h.mu.Lock()
		for ch := range h.subs {
			select {
			case ch <- c:
			default:
				h.drop(ch) // lento: cai e reconecta
			}
		}
		h.mu.Unlock()
	}
	// LISTEN caiu ou foi fechado: quem ainda ouvia essa geração reconecta
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch, g := range h.subs {
		if g == gen {
			h.drop(ch)
		}
	}
	if h.gen == gen && h.stop != nil {
		h.stop()
		h.stop = nil
	}
}

// drop fecha o stream. Chamar com h.mu travado.
func (h *Hub) drop(ch chan app.Change) {
	delete(h.subs, ch)
	close(ch)
}

func (h *Hub) leave(ch chan app.Change) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.subs[ch]; ok {
		h.drop(ch)
	}
	if len(h.subs) == 0 && h.stop != nil {
		h.stop() // ninguém ouvindo: larga a conexão
		h.stop = nil
	}
}
