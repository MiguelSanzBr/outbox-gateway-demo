package domain

// RecipientOrder confere a regra de ordem do lado de quem recebe: por destinatário,
// a sequência só cresce. No gateway quem garante a regra é o claim no adapter
// postgres (NOT EXISTS + índice único parcial), pq só o banco vê os outros workers.
type RecipientOrder struct{ last map[string]int64 }

// Observe registra seq e devolve false se ela veio fora de ordem.
func (o *RecipientOrder) Observe(recipient string, seq int64) bool {
	if o.last == nil {
		o.last = map[string]int64{}
	}
	ok := seq > o.last[recipient]
	o.last[recipient] = seq
	return ok
}
