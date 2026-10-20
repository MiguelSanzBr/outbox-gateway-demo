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
	ErrInvalidKey        = errors.New("header Idempotency-Key obrigatório (até 200 caracteres)")
	ErrInvalidContent    = errors.New("recipient (até 64) e body (até 4096) são obrigatórios")
	ErrInvalidTransition = errors.New("transição de estado inválida")
)

type Status string

const (
	Pending Status = "pending"
	Sending Status = "sending"
	Sent    Status = "sent"
	Dead    Status = "dead"
)

// CanBecome é a máquina de estados: pending → sending → sent | dead | pending
// (retry ou lease vencido). sent e dead são finais.
func (s Status) CanBecome(next Status) bool {
	switch s {
	case Pending:
		return next == Sending
	case Sending:
		return next == Sent || next == Dead || next == Pending
	}
	return false
}
