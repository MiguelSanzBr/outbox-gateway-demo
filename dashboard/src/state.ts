export type Status = 'pending' | 'sending' | 'sent' | 'dead'

// Mesmo formato do outbox_event() no Postgres, tanto na lista quanto no SSE.
export interface Msg {
  id: number
  recipient: string
  status: Status
  attempts: number
  worker: number | null
  last_error: string | null
}

export interface Note {
  id: number
  text: string
}

export interface Board {
  msgs: Map<number, Msg>
  notes: Note[]
  seq: number
}

export type Action = { type: 'rows'; rows: Msg[]; live: boolean } | { type: 'note'; text: string }

export const emptyBoard: Board = { msgs: new Map(), notes: [], seq: 0 }

const maxNotes = 8

// Posição do estado na vida da linha: todo claim soma 1 em attempts e o sending vem antes
// do resultado da mesma tentativa. A foto (GET) e o SSE chegam em qualquer ordem, então
// só aplica o q for mais novo.
const rank = (m: Msg) => m.attempts * 2 + (m.status === 'sending' ? 0 : 1)

export const reaped = (m: Msg) => m.last_error === 'lease expirado'

function noteFor(m: Msg): string | null {
  if (m.status === 'dead') return `#${m.id} dead${m.last_error ? `: ${m.last_error}` : ''}`
  if (m.status === 'pending' && reaped(m)) return `#${m.id} devolvida pelo reaper`
  return null
}

function addNote(b: Board, text: string): Board {
  return { ...b, notes: [{ id: b.seq, text }, ...b.notes].slice(0, maxNotes), seq: b.seq + 1 }
}

export function reducer(b: Board, a: Action): Board {
  if (a.type === 'note') return addNote(b, a.text)
  let next = b
  for (const m of a.rows) {
    const prev = next.msgs.get(m.id)
    if (prev && rank(m) <= rank(prev)) continue
    next = { ...next, msgs: new Map(next.msgs).set(m.id, m) }
    const text = a.live ? noteFor(m) : null
    if (text) next = addNote(next, text)
  }
  return next
}

export function counts(msgs: Iterable<Msg>): Record<Status, number> {
  const c = { pending: 0, sending: 0, sent: 0, dead: 0 }
  for (const m of msgs) c[m.status]++
  return c
}

// Mensagens por destinatário em ordem de id, destinatário com atividade mais nova primeiro.
export function lanes(msgs: Iterable<Msg>, max: number): [string, Msg[]][] {
  const by = new Map<string, Msg[]>()
  for (const m of msgs) by.set(m.recipient, [...(by.get(m.recipient) ?? []), m])
  const out = [...by].map(([r, l]): [string, Msg[]] => [r, l.sort((a, b) => a.id - b.id)])
  const last = (l: Msg[]) => Math.max(...l.map((m) => m.id))
  return out.sort((a, b) => last(b[1]) - last(a[1])).slice(0, max)
}
