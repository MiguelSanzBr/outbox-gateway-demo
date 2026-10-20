package domain

import (
	"errors"
	"math/rand/v2"
	"time"
)

// LeaseToken é o fencing token: só quem tem o token vigente finaliza a linha.
type LeaseToken string

type Lease struct {
	Token  LeaseToken
	Worker int
}

// OutboxEntry é uma linha da outbox reivindicada por um worker.
type OutboxEntry struct {
	ID             int64
	Status         Status
	Attempts       int
	Lease          Lease
	IdempotencyKey string
	Recipient      string
	Body           string
}

// Transition é o próximo estado decidido pelo domínio. Quem grava tem q exigir Token:
// é isso q impede um worker q perdeu o lease de sobrescrever o estado.
type Transition struct {
	Token     LeaseToken
	To        Status
	RetryIn   time.Duration
	LastError string
}

func (e OutboxEntry) Complete() (Transition, error) {
	if !e.Status.CanBecome(Sent) {
		return Transition{}, ErrInvalidTransition
	}
	return Transition{Token: e.Lease.Token, To: Sent}, nil
}

// Fail decide entre tentar de novo depois do backoff ou encerrar como dead.
func (e OutboxEntry) Fail(cause error, p RetryPolicy) (Transition, error) {
	if !e.Status.CanBecome(Dead) {
		return Transition{}, ErrInvalidTransition
	}
	to := Pending
	if IsPermanent(cause) || p.Exhausted(e.Attempts) {
		to = Dead
	}
	return Transition{Token: e.Lease.Token, To: to, RetryIn: p.Backoff(e.Attempts), LastError: cause.Error()}, nil
}

type RetryPolicy struct {
	MaxAttempts int
	BackoffBase time.Duration
	BackoffMax  time.Duration
}

// Exhausted vale também pro reaper: o adapter postgres aplica a mesma regra em lote.
func (p RetryPolicy) Exhausted(attempts int) bool { return attempts >= p.MaxAttempts }

// Backoff exponencial com jitter: metade fixa, metade aleatória.
func (p RetryPolicy) Backoff(attempt int) time.Duration {
	d := min(p.BackoffBase<<min(attempt-1, 20), p.BackoffMax)
	return d/2 + rand.N(d/2+1)
}

type permanentError struct{ error }

func (e permanentError) Unwrap() error { return e.error }

// Permanent marca um erro que não adianta tentar de novo (ex.: HTTP 400).
func Permanent(err error) error { return permanentError{err} }

func IsPermanent(err error) bool { return errors.As(err, new(permanentError)) }
