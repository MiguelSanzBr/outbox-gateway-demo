import { describe, expect, it } from 'vitest'
import { counts, emptyBoard, lanes, reducer, type Board, type Msg } from './state'

const msg = (over: Partial<Msg>): Msg => ({
  id: 1,
  recipient: 'ana',
  status: 'pending',
  attempts: 0,
  worker: null,
  last_error: null,
  ...over,
})

const rows = (b: Board, live: boolean, ...ms: Msg[]) => reducer(b, { type: 'rows', rows: ms, live })

describe('reducer', () => {
  it('foto inicial entra sem gerar aviso', () => {
    const b = rows(emptyBoard, false, msg({ id: 1 }), msg({ id: 2, status: 'sent', attempts: 1 }))
    expect([...b.msgs.keys()]).toEqual([1, 2])
    expect(b.notes).toEqual([])
  })

  it('evento atrasado não volta o estado pra trás', () => {
    // foto já tem sent; o evento do sending da mesma tentativa chegou depois
    let b = rows(emptyBoard, false, msg({ status: 'sent', attempts: 1 }))
    b = rows(b, true, msg({ status: 'sending', attempts: 1, worker: 3 }))
    expect(b.msgs.get(1)?.status).toBe('sent')
    // o mesmo estado repetido (foto + evento) também não muda nada
    expect(rows(b, true, msg({ status: 'sent', attempts: 1 }))).toBe(b)
  })

  it('segue a vida da linha: pending, sending, devolvida pelo reaper, sending de novo, sent', () => {
    let b = rows(emptyBoard, true, msg({ id: 5 }))
    b = rows(b, true, msg({ id: 5, status: 'sending', attempts: 1, worker: 0 }))
    b = rows(b, true, msg({ id: 5, status: 'pending', attempts: 1, worker: 0, last_error: 'lease expirado' }))
    b = rows(b, true, msg({ id: 5, status: 'sending', attempts: 2, worker: 1, last_error: 'lease expirado' }))
    b = rows(b, true, msg({ id: 5, status: 'sent', attempts: 2, worker: 1 }))
    expect(b.msgs.get(5)).toMatchObject({ status: 'sent', attempts: 2 })
    expect(b.notes.map((n) => n.text)).toEqual(['#5 devolvida pelo reaper'])
  })

  it('avisa dead com o motivo, quando tem', () => {
    let b = rows(emptyBoard, true, msg({ id: 7, status: 'dead', attempts: 1, last_error: 'provedor: HTTP 400' }))
    b = rows(b, true, msg({ id: 8, status: 'dead', attempts: 1 }))
    expect(b.notes.map((n) => n.text)).toEqual(['#8 dead', '#7 dead: provedor: HTTP 400'])
  })

  it('guarda só os 8 avisos mais novos, cada um com id próprio', () => {
    let b = emptyBoard
    for (let i = 0; i < 10; i++) b = reducer(b, { type: 'note', text: `aviso ${i}` })
    expect(b.notes.map((n) => n.text)).toEqual(['aviso 9', 'aviso 8', 'aviso 7', 'aviso 6', 'aviso 5', 'aviso 4', 'aviso 3', 'aviso 2'])
    expect(new Set(b.notes.map((n) => n.id)).size).toBe(8)
  })
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
