package domain

import (
	"errors"
	"math/rand/v2"
	"time"
)

// Motivos gravados em last_error. O painel reconhece a devolução pelo reaper pelo
// prefixo ReasonLeaseExpired (dashboard/src/domain/message.ts), então mudar o texto
// aqui pede mudar lá também.
const (
	ReasonLeaseExpired = "lease expirado"
	reasonUncertain    = ReasonLeaseExpired + " na última tentativa: resultado incerto, o provedor pode ter recebido"
	reasonReleased     = "devolvida no desligamento, antes de enviar"
	// maxRetryAfter é o teto do Retry-After do provedor, pra um header maluco não segurar a mensagem por dias.
	maxRetryAfter = time.Hour
)

// LeaseToken é o fencing token: só quem tem o token vigente finaliza a linha.
type LeaseToken string

type Lease struct {
	Token  LeaseToken
	Worker int
}

// OutboxEntry é uma linha da outbox reivindicada por um worker. ID é o id público
// da mensagem (o mesmo da API e do painel).
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
// Attempts é o valor a gravar (o Release devolve a tentativa q não chegou a sair).
type Transition struct {
	Token     LeaseToken
	To        Status
	Attempts  int
	RetryIn   time.Duration
	LastError string
}

// to monta a transição da linha reivindicada: toda decisão daqui parte de sending
// (dead também vira pending, mas só pelo redrive, q não passa por aqui).
func (e OutboxEntry) to(next Status) (Transition, error) {
	if e.Status != Sending || !e.Status.CanBecome(next) {
		return Transition{}, ErrInvalidTransition
	}
	return Transition{Token: e.Lease.Token, To: next, Attempts: e.Attempts}, nil
}

func (e OutboxEntry) Complete() (Transition, error) { return e.to(Sent) }

// Fail decide entre tentar de novo depois do backoff ou encerrar como dead.
// Retry-After do provedor vira piso do backoff.
func (e OutboxEntry) Fail(cause error, p RetryPolicy) (Transition, error) {
	next := Pending
	if IsPermanent(cause) || p.Exhausted(e.Attempts) {
		next = Dead
	}
	t, err := e.to(next)
	if err != nil {
		return t, err
	}
	t.LastError = cause.Error()
	if next == Pending {
		t.RetryIn = p.Backoff(e.Attempts)
		var ra retryAfterError
		if errors.As(cause, &ra) {
			t.RetryIn = max(t.RetryIn, min(ra.after, maxRetryAfter))
		}
	}
	return t, nil
}

// Expire é a decisão do reaper pra uma linha cujo lease venceu (worker morto ou travado).
// Mesma régua do Fail. Na última tentativa vira dead, mas o motivo diz q o resultado
// é incerto: o worker pode ter morrido depois de o provedor aceitar.
func (e OutboxEntry) Expire(p RetryPolicy) (Transition, error) {
	if p.Exhausted(e.Attempts) {
		t, err := e.to(Dead)
		t.LastError = reasonUncertain
		return t, err
	}
	t, err := e.to(Pending)
	t.RetryIn, t.LastError = p.Backoff(e.Attempts), ReasonLeaseExpired
	return t, err
}

// Release devolve à fila uma linha reivindicada q nem chegou a ser enviada (shutdown):
// sem gastar tentativa e sem backoff.
func (e OutboxEntry) Release() (Transition, error) {
	t, err := e.to(Pending)
	t.Attempts, t.LastError = e.Attempts-1, reasonReleased
	return t, err
}

type RetryPolicy struct {
	MaxAttempts int
	BackoffBase time.Duration
	BackoffMax  time.Duration
}

func (p RetryPolicy) Exhausted(attempts int) bool { return attempts >= p.MaxAttempts }

// Backoff exponencial com jitter: metade fixa, metade aleatória. Dobra até o teto em
// vez de fazer shift, q estoura o int64 com base de horas.
func (p RetryPolicy) Backoff(attempt int) time.Duration {
	d := p.BackoffBase
	for i := 1; i < attempt && d < p.BackoffMax; i++ {
		d *= 2
	}
	d = min(d, p.BackoffMax)
	return d/2 + rand.N(d/2+1)
}

type permanentError struct{ error }

func (e permanentError) Unwrap() error { return e.error }

// Permanent marca um erro que não adianta tentar de novo (ex.: HTTP 400).
func Permanent(err error) error { return permanentError{err} }

func IsPermanent(err error) bool { return errors.As(err, new(permanentError)) }

type retryAfterError struct {
	error
	after time.Duration
}

func (e retryAfterError) Unwrap() error { return e.error }

// RetryAfter marca um erro temporário em q o provedor disse quanto esperar (Retry-After).
func RetryAfter(err error, after time.Duration) error { return retryAfterError{err, after} }
