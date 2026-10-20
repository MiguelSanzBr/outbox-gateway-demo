import { describe, expect, it } from 'vitest'
import { counts, isNewer, lanes, type Msg } from './message'

const msg = (over: Partial<Msg>): Msg => ({
  id: 1,
  recipient: 'ana',
  status: 'pending',
  attempts: 0,
  worker: null,
  last_error: null,
  ...over,
})

describe('counts', () => {
  it('conta por estado, zerado quando não tem', () => {
    const ms = [msg({ id: 1 }), msg({ id: 2, status: 'sent' }), msg({ id: 3, status: 'sent' })]
    expect(counts(ms)).toEqual({ pending: 1, sending: 0, sent: 2, dead: 0 })
  })
})

describe('lanes', () => {
  it('agrupa por destinatário em ordem de id, destinatário com atividade mais nova primeiro', () => {
    const ms = [
      msg({ id: 4, recipient: 'bia' }),
      msg({ id: 1, recipient: 'ana' }),
      msg({ id: 3, recipient: 'ana' }),
      msg({ id: 2, recipient: 'caio' }),
    ]
    expect(lanes(ms, 10).map(([r, l]) => [r, l.map((m) => m.id)])).toEqual([
      ['bia', [4]],
      ['ana', [1, 3]],
      ['caio', [2]],
    ])
    expect(lanes(ms, 2).map(([r]) => r)).toEqual(['bia', 'ana'])
  })
})

describe('isNewer', () => {
  it('segue a vida da linha: claim soma tentativa e o sending vem antes do resultado', () => {
    const pending = msg({})
    const sending1 = msg({ status: 'sending', attempts: 1 })
    const reapedBack = msg({ status: 'pending', attempts: 1, last_error: 'lease expirado' })
    const sending2 = msg({ status: 'sending', attempts: 2 })
    const sent = msg({ status: 'sent', attempts: 2 })
    const life: [Msg, Msg][] = [
      [pending, sending1],
      [sending1, reapedBack],
      [reapedBack, sending2],
      [sending2, sent],
    ]
    for (const [prev, next] of life) {
      expect(isNewer(next, prev)).toBe(true)
      expect(isNewer(prev, next)).toBe(false)
    }
    expect(isNewer(sent, sent)).toBe(false)
    expect(isNewer(pending, undefined)).toBe(true)
  })
})
