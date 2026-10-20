package app

import "errors"

var ErrNoWorker = errors.New("worker não existe ou já caiu")

// Demo são os casos de uso de derrubar e religar worker. Só existe com DEMO_ADMIN=1.
type Demo struct{ pool WorkerPool }

func NewDemo(pool WorkerPool) *Demo { return &Demo{pool: pool} }

func (d *Demo) ListWorkers() []WorkerInfo { return d.pool.Workers() }

// KillWorker derruba o worker como um kill -9: se ele estiver enviando, a linha
// fica em sending sem finalizar e só volta pela mão do reaper.
func (d *Demo) KillWorker(id int) error {
	if !d.pool.Kill(id) {
		return ErrNoWorker
	}
	return nil
}

func (d *Demo) ReviveWorkers() int { return d.pool.Revive() }
