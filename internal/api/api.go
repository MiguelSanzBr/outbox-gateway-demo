// Package api expõe a entrada HTTP: valida, grava na outbox e responde 202.
package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/outbox"
)

const (
	maxKeyLen       = 200
	maxRecipientLen = 64
	maxBodyLen      = 4096
)

func New(store *outbox.Store, log *slog.Logger) http.Handler {
	h := handler{store: store, log: log}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/messages", h.create)
	mux.HandleFunc("GET /v1/messages/{id}", h.get)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	return mux
}

type handler struct {
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

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
