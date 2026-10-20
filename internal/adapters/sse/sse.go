// Package sse é o adapter q empurra as mudanças de estado da outbox pro navegador
// em Server-Sent Events.
package sse

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/app"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/domain"
)

// Event é o JSON de uma mudança, igual no stream e na foto de GET /v1/messages
// (o httpapi usa este mesmo formato pra os dois não divergirem).
type Event struct {
	ID        int64         `json:"id"`
	Recipient string        `json:"recipient"`
	Status    domain.Status `json:"status"`
	Attempts  int           `json:"attempts"`
	Worker    *int          `json:"worker"`
	Version   int64         `json:"version"`
	LastError *string       `json:"last_error"`
}

func EventOf(c app.Change) Event { return Event(c) }

// Streams é o q o handler precisa do caso de uso.
type Streams interface {
	StreamEvents(ctx context.Context) (<-chan app.Change, error)
}

// writeTimeout: cliente q não lê nem o TCP por esse tempo é largado, em vez de prender o handler.
const writeTimeout = 10 * time.Second

// Handler manda cada mudança como um evento SSE. Não guarda nada: quem reconecta
// busca a foto de novo em GET /v1/messages. ctx é a vida do servidor; quando acaba,
// os streams fecham. heartbeat é o ping: proxy e load balancer derrubam conexão
// parada (o nginx corta em 60 s).
func Handler(ctx context.Context, s Streams, heartbeat time.Duration, log *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		streamCtx, cancel := context.WithCancel(r.Context())
		defer cancel()
		defer context.AfterFunc(ctx, cancel)()

		ch, err := s.StreamEvents(streamCtx)
		if err != nil {
			code, msg := http.StatusInternalServerError, "erro interno"
			if errors.Is(err, ErrTooManyStreams) {
				code, msg = http.StatusServiceUnavailable, err.Error()
			} else {
				log.Error("listen", "err", err)
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(code)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
			return
		}
		// o canal só fecha depois de soltar o stream; drenar garante q nada fica pendurado
		defer func() {
			cancel()
			for range ch {
			}
		}()

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.WriteHeader(http.StatusOK)
		rc := http.NewResponseController(w)
		write := func(s string) error {
			_ = rc.SetWriteDeadline(time.Now().Add(writeTimeout))
			_, err := io.WriteString(w, s)
			if err == nil {
				err = rc.Flush()
			}
			return err
		}
		tick := time.NewTicker(heartbeat)
		defer tick.Stop()
		for err = rc.Flush(); err == nil; {
			select {
			case c, ok := <-ch:
				if !ok {
					return // cliente saiu, servidor desligando, cliente lento ou LISTEN caiu; o EventSource reconecta
				}
				p, _ := json.Marshal(EventOf(c)) // Event só tem tipo simples, Marshal não falha
				err = write("data: " + string(p) + "\n\n")
			case <-tick.C:
				err = write(": ping\n\n")
			}
		}
	})
}
