// Package mockprovider simula a API de um provedor de mensagens: latência e
// falhas configuráveis, deduplicação por Idempotency-Key e contadores que os
// testes e o teste de carga usam para checar perda, duplicação e ordem.
package mockprovider

import (
	"encoding/json"
	"math/rand/v2"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type Config struct {
	Latency  time.Duration // latência fixa por requisição
	Jitter   time.Duration // latência extra aleatória em [0, Jitter)
	FailRate float64       // fração de requisições respondidas com 503 antes de processar
}

type Stats struct {
	Accepted         int64 `json:"accepted"`          // chaves distintas aceitas
	Duplicates       int64 `json:"duplicates"`        // reenvios de chave já aceita (descartados)
	FailuresInjected int64 `json:"failures_injected"` // 503 simulados
	OrderViolations  int64 `json:"order_violations"`  // X-Sequence fora de ordem por destinatário
}

type Provider struct {
	cfg Config

	mu        sync.Mutex
	stats     Stats
	seen      map[string]bool
	lastSeq   map[string]int64
	delivered map[string][]string // tudo em memória: serve pra teste e carga de alguns minutos, não pra rodar dias
}

func New(cfg Config) *Provider {
	return &Provider{cfg: cfg, seen: map[string]bool{}, lastSeq: map[string]int64{}, delivered: map[string][]string{}}
}

func (p *Provider) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/messages", p.send)
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(p.Stats())
	})
	return mux
}

func (p *Provider) send(w http.ResponseWriter, r *http.Request) {
	d := p.cfg.Latency
	if p.cfg.Jitter > 0 {
		d += rand.N(p.cfg.Jitter)
	}
	select {
	case <-r.Context().Done():
		return
	case <-time.After(d):
	}

	var msg struct{ To, Body string }
	key := r.Header.Get("Idempotency-Key")
	seq, seqErr := strconv.ParseInt(r.Header.Get("X-Sequence"), 10, 64)
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil || msg.To == "" || key == "" || seqErr != nil {
		http.Error(w, "payload inválido", http.StatusBadRequest)
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if rand.Float64() < p.cfg.FailRate {
		p.stats.FailuresInjected++
		http.Error(w, "falha simulada", http.StatusServiceUnavailable)
		return
	}
	if p.seen[key] {
		p.stats.Duplicates++
		w.WriteHeader(http.StatusOK)
		return
	}
	p.seen[key] = true
	p.stats.Accepted++
	if seq <= p.lastSeq[msg.To] {
		p.stats.OrderViolations++
	}
	p.lastSeq[msg.To] = seq
	p.delivered[msg.To] = append(p.delivered[msg.To], msg.Body)
	w.WriteHeader(http.StatusAccepted)
}

func (p *Provider) Stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stats
}

// Delivered devolve os corpos aceitos para o destinatário, na ordem de chegada.
func (p *Provider) Delivered(recipient string) []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.delivered[recipient]...)
}
