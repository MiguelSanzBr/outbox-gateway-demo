// Package domain guarda as regras do gateway sem saber de banco nem de HTTP:
// estados da outbox e suas transições, lease, política de retry, validação da
// mensagem e a regra de ordem por destinatário.
package domain

import "errors"

var (
	// ErrKeyConflict: a chave de idempotência já existe com outro conteúdo.
	ErrKeyConflict = errors.New("chave de idempotência reutilizada com conteúdo diferente")
	// ErrLeaseLost: o lease expirou e a linha foi devolvida à fila (ou reivindicada por outro worker).
	ErrLeaseLost         = errors.New("lease perdido")
	ErrNotFound          = errors.New("mensagem não encontrada")
	ErrInvalidKey        = errors.New("chave de idempotência obrigatória, até 200 bytes, UTF-8 sem NUL")
	ErrInvalidContent    = errors.New("recipient (até 64 bytes) e body (até 4096 bytes) são obrigatórios, UTF-8 sem NUL")
	ErrInvalidTransition = errors.New("transição de estado inválida")
)

type Status string

const (
	Queued  Status = "queued" // atrás de outra mensagem do mesmo destinatário
	Pending Status = "pending"
	Sending Status = "sending"
	Sent    Status = "sent"
	Dead    Status = "dead"
)

// CanBecome é a máquina de estados: queued → pending → sending → sent | dead | pending
// (retry ou lease vencido). Por destinatário só uma linha fica em pending ou sending
// (a cabeça); as outras esperam em queued até a cabeça virar sent ou dead.
// sent é final; dead só sai por redrive manual.
func (s Status) CanBecome(next Status) bool {
	switch s {
	case Queued:
		return next == Pending
	case Pending:
		return next == Sending
	case Sending:
		return next == Sent || next == Dead || next == Pending
	case Dead:
		return next == Pending || next == Queued
	}
	return false
}
