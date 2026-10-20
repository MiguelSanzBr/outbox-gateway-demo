// Domínio do painel: o estado de uma mensagem e como ele anda. Sem React, sem fetch.

export type Status = 'queued' | 'pending' | 'sending' | 'sent' | 'dead'

export const statuses: readonly Status[] = ['queued', 'pending', 'sending', 'sent', 'dead']

// Mesmo formato do sse.Event do gateway, tanto na lista quanto no SSE.
export interface Msg {
  id: number
  recipient: string
  status: Status
  attempts: number
  worker: number | null
  // soma 1 a cada mudança da linha (trigger no banco): a maior é a mais nova
  version: number
  last_error: string | null
}

const isNum = (v: unknown) => typeof v === 'number'

// isMsg confere o JSON q vem do gateway antes de ele entrar no estado.
export function isMsg(x: unknown): x is Msg {
  if (typeof x !== 'object' || x === null) return false
  const m = x as Record<string, unknown>
  return (
    isNum(m.id) &&
    typeof m.recipient === 'string' &&
    statuses.includes(m.status as Status) &&
    isNum(m.attempts) &&
    isNum(m.version) &&
    (m.worker === null || isNum(m.worker)) &&
    (m.last_error === null || typeof m.last_error === 'string')
  )
}

// A foto (GET) e o SSE chegam em qualquer ordem, então só vale o q for mais novo.
// attempts não serve de relógio: o release do shutdown e o redrive voltam ele.
export const isNewer = (next: Msg, prev: Msg | undefined) => !prev || next.version > prev.version

// Prefixo de domain.ReasonLeaseExpired (internal/domain/entry.go): mudou lá, muda aqui.
export const reaped = (m: Msg) => m.last_error?.startsWith('lease expirado') ?? false

export function counts(msgs: Iterable<Msg>): Record<Status, number> {
  const c = { queued: 0, pending: 0, sending: 0, sent: 0, dead: 0 }
  for (const m of msgs) c[m.status]++
  return c
}

// Mensagens por destinatário em ordem de id, destinatário com atividade mais nova primeiro.
export function lanes(msgs: Iterable<Msg>, max: number): [string, Msg[]][] {
  const by = new Map<string, Msg[]>()
  for (const m of msgs) {
    const lane = by.get(m.recipient)
    if (lane) lane.push(m)
    else by.set(m.recipient, [m])
  }
  const out = [...by].map(([r, l]): [string, Msg[]] => [r, l.sort((a, b) => a.id - b.id)])
  const last = (l: Msg[]) => Math.max(...l.map((m) => m.id))
  return out.sort((a, b) => last(b[1]) - last(a[1])).slice(0, max)
}
