// Package httpapi expõe a entrada HTTP: valida, grava na outbox e responde 202.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/adapters/sse"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/app"
	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/domain"
)

const (
	maxBody      = 16 << 10
	bodyTimeout  = 5 * time.Second
	recentLimit  = 500
	sseHeartbeat = 15 * time.Second // abaixo dos 60 s de read timeout do nginx
)

// New monta as rotas. ctx é a vida do servidor (fecha os streams SSE no shutdown).
// demo nil desliga /v1/admin.
func New(ctx context.Context, svc *app.Service, demo *app.Demo, log *slog.Logger) http.Handler {
	h := handler{svc: svc, log: log}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/messages", h.create)
	mux.HandleFunc("GET /v1/messages", h.list)
	mux.HandleFunc("GET /v1/messages/{id}", h.get)
	mux.Handle("GET /v1/events", sse.Handler(ctx, svc, sseHeartbeat, log))
	if demo != nil {
		mux.HandleFunc("GET /v1/admin/workers", func(w http.ResponseWriter, _ *http.Request) {
			type worker struct {
				ID    int  `json:"id"`
				Alive bool `json:"alive"`
			}
			out := []worker{}
			for _, w := range demo.ListWorkers() {
				out = append(out, worker(w))
			}
			writeJSON(w, http.StatusOK, out)
		})
		mux.HandleFunc("POST /v1/admin/workers/{id}/kill", func(w http.ResponseWriter, r *http.Request) {
			id, err := strconv.Atoi(r.PathValue("id"))
			if err != nil {
				writeError(w, http.StatusBadRequest, "id de worker inválido")
				return
			}
			if err := demo.KillWorker(id); err != nil {
				writeError(w, http.StatusNotFound, err.Error())
				return
			}
			w.WriteHeader(http.StatusNoContent)
		})
		mux.HandleFunc("POST /v1/admin/messages/{id}/requeue", h.requeue)
		mux.HandleFunc("POST /v1/admin/workers/revive", func(w http.ResponseWriter, _ *http.Request) {
			writeJSON(w, http.StatusOK, map[string]int{"revived": demo.ReviveWorkers()})
		})
	}
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	return mux
}

type handler struct {
	svc *app.Service
	log *slog.Logger
}

var errTrailing = errors.New("lixo depois do JSON")

// decodeStrict lê um objeto JSON só, sem campo desconhecido e sem nada depois.
func decodeStrict(r io.Reader, v any) error {
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	err := dec.Decode(v)
	if err == nil {
		if _, next := dec.Token(); next != io.EOF {
			err = errTrailing
		}
	}
	return err
}

func (h handler) create(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Recipient string `json:"recipient"`
		Body      string `json:"body"`
	}
	// prazo pra ler o corpo: cliente mandando 1 byte por segundo não segura o handler
	_ = http.NewResponseController(w).SetReadDeadline(time.Now().Add(bodyTimeout))
	err := decodeStrict(http.MaxBytesReader(w, r.Body, maxBody), &in)
	var tooBig *http.MaxBytesError
	switch {
	case errors.As(err, &tooBig):
		writeError(w, http.StatusRequestEntityTooLarge, "corpo maior q 16 KiB")
		return
	case err != nil:
		writeError(w, http.StatusBadRequest, "JSON inválido: só recipient e body, um objeto")
		return
	}
	// a validação é uma só, no caso de uso; aqui é só traduzir pra HTTP
	id, created, err := h.svc.EnqueueMessage(r.Context(), r.Header.Get("Idempotency-Key"), in.Recipient, in.Body)
	switch {
	case errors.Is(err, domain.ErrInvalidKey):
		writeError(w, http.StatusBadRequest, "header Idempotency-Key obrigatório: até 200 bytes, UTF-8 sem NUL")
	case errors.Is(err, domain.ErrInvalidContent):
		writeError(w, http.StatusUnprocessableEntity, domain.ErrInvalidContent.Error())
	case errors.Is(err, domain.ErrKeyConflict):
		writeError(w, http.StatusConflict, domain.ErrKeyConflict.Error())
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
	st, err := h.svc.GetMessage(r.Context(), id)
	switch {
	case errors.Is(err, domain.ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case err != nil:
		h.log.Error("get", "err", err)
		writeError(w, http.StatusInternalServerError, "erro interno")
	default:
		writeJSON(w, http.StatusOK, messageJSON{
			ID: st.ID, Recipient: st.Recipient, Status: st.Status, Attempts: st.Attempts,
			LastError: st.LastError, CreatedAt: st.CreatedAt, SentAt: st.SentAt,
		})
	}
}

func (h handler) requeue(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "id inválido")
		return
	}
	err = h.svc.RequeueDead(r.Context(), id)
	switch {
	case errors.Is(err, domain.ErrNotFound):
		writeError(w, http.StatusNotFound, domain.ErrNotFound.Error())
	case errors.Is(err, domain.ErrInvalidTransition):
		writeError(w, http.StatusConflict, "só mensagem dead volta pra fila")
	case err != nil:
		h.log.Error("requeue", "id", id, "err", err)
		writeError(w, http.StatusInternalServerError, "erro interno")
	default:
		w.WriteHeader(http.StatusNoContent)
	}
}

func (h handler) list(w http.ResponseWriter, r *http.Request) {
	rows, err := h.svc.RecentChanges(r.Context(), recentLimit)
	if err != nil {
		h.log.Error("list", "err", err)
		writeError(w, http.StatusInternalServerError, "erro interno")
		return
	}
	out := make([]sse.Event, len(rows))
	for i, c := range rows {
		out[i] = sse.EventOf(c)
	}
	writeJSON(w, http.StatusOK, out)
}

// messageJSON é o formato de GET /v1/messages/{id}.
type messageJSON struct {
	ID        int64         `json:"id"`
	Recipient string        `json:"recipient"`
	Status    domain.Status `json:"status"`
	Attempts  int           `json:"attempts"`
	LastError *string       `json:"last_error,omitempty"`
	CreatedAt time.Time     `json:"created_at"`
	SentAt    *time.Time    `json:"sent_at,omitempty"`
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
