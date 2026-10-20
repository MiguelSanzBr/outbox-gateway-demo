import { describe, expect, it } from 'vitest'
import { counts, isMsg, isNewer, lanes, reaped, type Msg } from './message'

const msg = (over: Partial<Msg>): Msg => ({
  id: 1,
  recipient: 'ana',
  status: 'pending',
  attempts: 0,
  worker: null,
  version: 0,
  last_error: null,
  ...over,
})

describe('counts', () => {
  it('conta por estado, zerado quando não tem', () => {
    const ms = [msg({ id: 1 }), msg({ id: 2, status: 'sent' }), msg({ id: 3, status: 'sent' }), msg({ id: 4, status: 'queued' })]
    expect(counts(ms)).toEqual({ queued: 1, pending: 1, sending: 0, sent: 2, dead: 0 })
  })
})

describe('isMsg', () => {
  it('aceita o formato do gateway e recusa o resto, em vez de estourar dentro do handler', () => {
    expect(isMsg(msg({}))).toBe(true)
    expect(isMsg(msg({ worker: 3, last_error: 'x' }))).toBe(true)
    for (const bad of [
      null,
      'x',
      { ...msg({}), status: 'perdida' },
      { ...msg({}), id: '1' },
      { ...msg({}), version: undefined },
      { ...msg({}), recipient: 3 },
      { ...msg({}), attempts: null },
      { ...msg({}), worker: 'w0' },
      { ...msg({}), last_error: 4 },
    ]) {
      expect(isMsg(bad)).toBe(false)
    }
  })
})

describe('reaped', () => {
  it('reconhece a devolução pelo reaper pelo prefixo do motivo, inclusive a dead da última tentativa', () => {
    expect(reaped(msg({ last_error: 'lease expirado' }))).toBe(true)
    expect(reaped(msg({ last_error: 'lease expirado na última tentativa: resultado incerto' }))).toBe(true)
    expect(reaped(msg({ last_error: 'provedor: HTTP 503' }))).toBe(false)
    expect(reaped(msg({}))).toBe(false)
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
  it('a versão da linha decide, mesmo quando attempts volta (release no shutdown, redrive)', () => {
    const queued = msg({ status: 'queued' })
    const pending = msg({ version: 1 })
    const sending1 = msg({ status: 'sending', attempts: 1, version: 2 })
    const released = msg({ status: 'pending', attempts: 0, version: 3 })
    const sending2 = msg({ status: 'sending', attempts: 1, version: 4 })
    const dead = msg({ status: 'dead', attempts: 1, version: 5 })
    const redriven = msg({ status: 'pending', attempts: 0, version: 6 })
    const sent = msg({ status: 'sent', attempts: 1, version: 8 })
    const life: [Msg, Msg][] = [
      [queued, pending],
      [pending, sending1],
      [sending1, released],
      [released, sending2],
      [sending2, dead],
      [dead, redriven],
      [redriven, sent],
    ]
    for (const [prev, next] of life) {
      expect(isNewer(next, prev)).toBe(true)
      expect(isNewer(prev, next)).toBe(false)
    }
    expect(isNewer(sent, sent)).toBe(false)
    expect(isNewer(pending, undefined)).toBe(true)
  })
})
