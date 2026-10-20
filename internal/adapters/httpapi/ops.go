package httpapi

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/MiguelSanzBr/outbox-gateway-demo/internal/domain"
)

const requestIDHeader = "X-Request-Id"

var validRequestID = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

// probes não entram no log de acesso: sonda de 1 em 1 s afoga o resto.
var probes = map[string]bool{"GET /healthz": true, "GET /readyz": true, "GET /metrics": true}

type recorder struct {
	http.ResponseWriter
	code int
}

func (r *recorder) WriteHeader(code int) {
	r.code = code
	r.ResponseWriter.WriteHeader(code)
}

// Unwrap deixa o http.ResponseController (Flush do SSE, prazos) chegar no writer de verdade.
func (r *recorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

// withAccessLog loga cada requisição com um request id: o do cliente se for razoável,
// senão um novo. O id volta no header da resposta e os handlers logam erro com ele.
func withAccessLog(next http.Handler, log *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(requestIDHeader)
		if !validRequestID.MatchString(id) {
			id = rand.Text()
			r.Header.Set(requestIDHeader, id)
		}
		w.Header().Set(requestIDHeader, id)
		rec := &recorder{ResponseWriter: w, code: http.StatusOK}
		start := time.Now()
		next.ServeHTTP(rec, r)
		if !probes[r.Pattern] {
			log.Info("http", "request_id", id, "method", r.Method, "route", r.Pattern, "path", r.URL.Path,
				"status", rec.code, "ms", time.Since(start).Milliseconds())
		}
	})
}

func (h handler) internalError(w http.ResponseWriter, r *http.Request, op string, err error) {
	h.log.Error(op, "request_id", r.Header.Get(requestIDHeader), "err", err)
	writeError(w, http.StatusInternalServerError, "erro interno")
}

func (h handler) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	if err := h.svc.Ready(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "banco fora do ar")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// metrics escreve o formato de texto do Prometheus na mão: são meia dúzia de séries,
// não vale uma dependência. A fila vem do banco (é a mesma em todas as réplicas, use
// max); os contadores são deste processo (some as réplicas).
func (h handler) metrics(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	st, err := h.svc.QueueStats(ctx)
	var b strings.Builder
	up := 0
	if err == nil {
		up = 1
		b.WriteString("# HELP outbox_messages Linhas da outbox por estado, sem sent (essas contam em outbox_deliveries_total).\n")
		b.WriteString("# TYPE outbox_messages gauge\n")
		for _, s := range []domain.Status{domain.Queued, domain.Pending, domain.Sending, domain.Dead} {
			fmt.Fprintf(&b, "outbox_messages{status=%q} %d\n", s, st.ByStatus[s])
		}
		b.WriteString("# HELP outbox_oldest_unsent_seconds Idade da mensagem mais antiga ainda não entregue.\n")
		b.WriteString("# TYPE outbox_oldest_unsent_seconds gauge\n")
		fmt.Fprintf(&b, "outbox_oldest_unsent_seconds %g\n", st.OldestUnsent.Seconds())
	} else {
		h.log.Warn("metrics", "err", err)
	}
	b.WriteString("# HELP outbox_db_up 1 se a consulta da fila respondeu.\n# TYPE outbox_db_up gauge\n")
	fmt.Fprintf(&b, "outbox_db_up %d\n", up)

	d := h.svc.Deliveries()
	b.WriteString("# HELP outbox_deliveries_total Resultados gravados por este processo.\n")
	b.WriteString("# TYPE outbox_deliveries_total counter\n")
	for _, c := range []struct {
		result string
		n      int64
	}{{"sent", d.Sent}, {"retry", d.Retried}, {"dead", d.Dead}, {"lease_lost", d.LeaseLost}, {"released", d.Released}} {
		fmt.Fprintf(&b, "outbox_deliveries_total{result=%q} %d\n", c.result, c.n)
	}
	b.WriteString("# HELP outbox_reaped_total Linhas devolvidas pelo reaper (lease vencido).\n")
	fmt.Fprintf(&b, "# TYPE outbox_reaped_total counter\noutbox_reaped_total %d\n", d.Reaped)

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = io.WriteString(w, b.String())
}
