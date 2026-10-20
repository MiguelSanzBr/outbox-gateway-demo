// Package api expõe a entrada HTTP: valida, grava na outbox e responde 202.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/outbox"
)

const (
	maxKeyLen       = 200
	maxRecipientLen = 64
	maxBodyLen      = 4096
	recentLimit     = 500
)

// ping do SSE: proxy e load balancer derrubam conexão parada (o nginx corta em 60 s)
var heartbeat = 15 * time.Second

// New monta as rotas. ctx é a vida do servidor: quando acaba, os streams SSE fecham,
// senão o Shutdown fica esperando eles pra sempre. fleet nil desliga /v1/admin.
func New(ctx context.Context, store *outbox.Store, fleet *outbox.Fleet, log *slog.Logger) http.Handler {
	h := handler{ctx: ctx, store: store, log: log}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/messages", h.create)
	mux.HandleFunc("GET /v1/messages", h.list)
	mux.HandleFunc("GET /v1/messages/{id}", h.get)
	mux.HandleFunc("GET /v1/events", h.events)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	return mux
}

type handler struct {
	ctx   context.Context
	store *outbox.Store
	log   *slog.Logger
}

func (h handler) create(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" || len(key) > maxKeyLen {
		writeError(w, http.StatusBadRequest, "header Idempotency-Key obrigatório (até 200 caracteres)")
		return
	}
	var in struct {
		Recipient string `json:"recipient"`
		Body      string `json:"body"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		writeError(w, http.StatusBadRequest, "JSON inválido")
		return
	}
	if in.Recipient == "" || len(in.Recipient) > maxRecipientLen || in.Body == "" || len(in.Body) > maxBodyLen {
		writeError(w, http.StatusUnprocessableEntity, "recipient (até 64) e body (até 4096) são obrigatórios")
		return
	}

	id, created, err := h.store.Enqueue(r.Context(), key, in.Recipient, in.Body)
	switch {
	case errors.Is(err, outbox.ErrKeyConflict):
		writeError(w, http.StatusConflict, err.Error())
	case err != nil:
		h.log.Error("enqueue", "err", err)
		writeError(w, http.StatusInternalServerError, "erro interno")
	case created:
		writeJSON(w, http.StatusAccepted, map[string]any{"id": id, "status": "queued"})
	default:
		writeJSON(w, http.StatusOK, map[string]any{"id": id, "status": "duplicate"})
	}
}

func (h handler) get(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "id inválido")
		return
	}
	st, err := h.store.Get(r.Context(), id)
	switch {
	case errors.Is(err, outbox.ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case err != nil:
		h.log.Error("get", "err", err)
		writeError(w, http.StatusInternalServerError, "erro interno")
	default:
		writeJSON(w, http.StatusOK, st)
	}
}

func (h handler) list(w http.ResponseWriter, r *http.Request) {
	rows, err := h.store.Recent(r.Context(), recentLimit)
	if err != nil {
		h.log.Error("list", "err", err)
		writeError(w, http.StatusInternalServerError, "erro interno")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(rows)
}

// events manda cada mudança de estado da outbox como evento SSE. Não guarda nada:
// quem reconecta busca a foto de novo em GET /v1/messages.
func (h handler) events(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	defer context.AfterFunc(h.ctx, cancel)()

	ch, err := h.store.Listen(ctx)
	if err != nil {
		h.log.Error("listen", "err", err)
		writeError(w, http.StatusInternalServerError, "erro interno")
		return
	}
	// Listen só fecha o canal depois de largar a conexão; drenar garante q ela não vaza
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
		case p, ok := <-ch:
			if !ok {
				return // cliente saiu, servidor desligando ou LISTEN caiu; o EventSource reconecta sozinho
			}
			_, _ = fmt.Fprintf(w, "data: %s\n\n", p)
		case <-tick.C:
			_, _ = fmt.Fprint(w, ": ping\n\n")
		}
		_ = rc.Flush()
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
