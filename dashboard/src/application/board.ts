import { isNewer, reaped, type Msg } from '../domain/message'

export interface Note {
  id: number
  text: string
}

// Board é o q o painel sabe: as mensagens e os avisos pro leitor de tela.
export interface Board {
  msgs: Map<number, Msg>
  notes: Note[]
  seq: number
}

export type Action = { type: 'rows'; rows: Msg[]; live: boolean } | { type: 'note'; text: string }

export const emptyBoard: Board = { msgs: new Map(), notes: [], seq: 0 }

const maxNotes = 8
// Mesma janela da foto (GET /v1/messages devolve as 500 mais novas): com o painel aberto
// durante o k6 o estado não cresce sem fim.
export const maxMsgs = 500

// Só avisa o q importa: avisar cada sent numa rajada afoga o leitor de tela.
function noteFor(m: Msg): string | null {
  if (m.status === 'dead') return `#${m.id} dead${m.last_error ? `: ${m.last_error}` : ''}`
  if (m.status === 'pending' && reaped(m)) return `#${m.id} devolvida pelo reaper`
  return null
}

function addNote(b: Board, text: string): Board {
  return { ...b, notes: [{ id: b.seq, text }, ...b.notes].slice(0, maxNotes), seq: b.seq + 1 }
}

// reducer junta a foto inicial (live=false) e os eventos ao vivo sem voltar estado pra trás.
// Copia o Map uma vez por ação, não uma por linha.
export function reducer(b: Board, a: Action): Board {
  if (a.type === 'note') return addNote(b, a.text)
  let next = b
  let msgs: Map<number, Msg> | null = null
  for (const m of a.rows) {
    if (!isNewer(m, (msgs ?? b.msgs).get(m.id))) continue
    msgs ??= new Map(b.msgs)
    msgs.set(m.id, m)
    const text = a.live ? noteFor(m) : null
    if (text) next = addNote(next, text)
  }
  if (!msgs) return b
  while (msgs.size > maxMsgs) msgs.delete(Math.min(...msgs.keys()))
  return { ...next, msgs }
}
