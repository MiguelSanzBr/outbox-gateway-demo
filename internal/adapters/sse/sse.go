// Package sse é o adapter q empurra as mudanças de estado da outbox pro navegador
// em Server-Sent Events.
package sse

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/app"
)

// ping do SSE: proxy e load balancer derrubam conexão parada (o nginx corta em 60 s)
var heartbeat = 15 * time.Second

// Handler manda cada mudança como um evento SSE. Não guarda nada: quem reconecta
// busca a foto de novo em GET /v1/messages. ctx é a vida do servidor; quando acaba,
// os streams fecham, senão ficam pendurados com a conexão LISTEN aberta.
func Handler(ctx context.Context, svc *app.Service, log *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		streamCtx, cancel := context.WithCancel(r.Context())
		defer cancel()
		defer context.AfterFunc(ctx, cancel)()

		ch, err := svc.StreamEvents(streamCtx)
		if err != nil {
			log.Error("listen", "err", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = fmt.Fprintln(w, `{"error":"erro interno"}`)
			return
		}
		// o canal só fecha depois de largar a conexão; drenar garante q ela não vaza
		defer func() {
			cancel()
			for range ch {
			}
		}()

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.WriteHeader(http.StatusOK)
		rc := http.NewResponseController(w)
		_ = rc.Flush()
		tick := time.NewTicker(heartbeat)
		defer tick.Stop()
		for {
			select {
			case c, ok := <-ch:
				if !ok {
					return // cliente saiu, servidor desligando ou LISTEN caiu; o EventSource reconecta sozinho
				}
				p, _ := json.Marshal(c) // Change só tem tipo simples, Marshal não falha
				_, _ = fmt.Fprintf(w, "data: %s\n\n", p)
			case <-tick.C:
				_, _ = fmt.Fprint(w, ": ping\n\n")
			}
			_ = rc.Flush()
		}
	})
}
