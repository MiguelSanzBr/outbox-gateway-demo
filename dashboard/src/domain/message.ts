// Domínio do painel: o estado de uma mensagem e como ele anda. Sem React, sem fetch.

export type Status = 'pending' | 'sending' | 'sent' | 'dead'

// Mesmo formato do domain.Change do gateway, tanto na lista quanto no SSE.
export interface Msg {
  id: number
  recipient: string
  status: Status
  attempts: number
  worker: number | null
  last_error: string | null
}

// Posição do estado na vida da linha: todo claim soma 1 em attempts e o sending vem antes
// do resultado da mesma tentativa. A foto (GET) e o SSE chegam em qualquer ordem, então
// só vale o q for mais novo.
const rank = (m: Msg) => m.attempts * 2 + (m.status === 'sending' ? 0 : 1)

export const isNewer = (next: Msg, prev: Msg | undefined) => !prev || rank(next) > rank(prev)

export const reaped = (m: Msg) => m.last_error === 'lease expirado'

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
