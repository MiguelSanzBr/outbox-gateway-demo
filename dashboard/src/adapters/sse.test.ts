import { afterEach, beforeEach, expect, it, vi } from 'vitest'
import type { Msg } from '../domain/message'
import { sseEvents } from './sse'

class FakeEventSource {
  static readonly CONNECTING = 0
  static readonly OPEN = 1
  static readonly CLOSED = 2
  static all: FakeEventSource[] = []
  onopen: (() => void) | null = null
  onerror: (() => void) | null = null
  onmessage: ((e: { data: string }) => void) | null = null
  readyState = FakeEventSource.CONNECTING
  closed = false
  constructor(readonly url: string) {
    FakeEventSource.all.push(this)
  }
  close() {
    this.closed = true
    this.readyState = FakeEventSource.CLOSED
  }
}

const last = () => FakeEventSource.all.at(-1) as FakeEventSource
const handlers = () => ({ onChange: vi.fn(), onOpen: vi.fn(), onError: vi.fn() })
// o navegador desiste (resposta != 200, como o 502 do nginx com o gateway fora): CLOSED
const giveUp = (es: FakeEventSource) => {
  es.readyState = FakeEventSource.CLOSED
  es.onerror?.()
}

beforeEach(() => {
  FakeEventSource.all = []
  vi.stubGlobal('EventSource', FakeEventSource)
  vi.useFakeTimers()
})
afterEach(() => vi.useRealTimers())

it('repassa abertura, mudança e erro, e fecha no unsubscribe', () => {
  const h = handlers()
  const stop = sseEvents.subscribe(h)
  const es = last()
  expect(es.url).toBe('/v1/events')

  const m: Msg = { id: 3, recipient: 'ana', status: 'sending', attempts: 1, worker: 0, version: 1, last_error: null }
  es.onopen?.()
  es.onmessage?.({ data: JSON.stringify(m) })
  es.onerror?.() // CONNECTING: o próprio EventSource reconecta, não abre outro
  expect(h.onOpen).toHaveBeenCalledOnce()
  expect(h.onChange).toHaveBeenCalledWith(m)
  expect(h.onError).toHaveBeenCalledOnce()
  vi.advanceTimersByTime(60_000)
  expect(FakeEventSource.all).toHaveLength(1)

  stop()
  expect(es.closed).toBe(true)
})

it('evento fora do formato vira erro em vez de estourar dentro do handler', () => {
  const h = handlers()
  sseEvents.subscribe(h)
  last().onmessage?.({ data: '{"id":1}' })
  last().onmessage?.({ data: 'não é json' })
  expect(h.onChange).not.toHaveBeenCalled()
  expect(h.onError).toHaveBeenCalledTimes(2)
})

it('quando o EventSource desiste (CLOSED), abre outro com backoff q dobra e zera ao conectar', () => {
  vi.spyOn(Math, 'random').mockReturnValue(0.99) // jitter no máximo: a espera é o próprio backoff
  const h = handlers()
  sseEvents.subscribe(h)

  giveUp(last())
  expect(h.onError).toHaveBeenCalledOnce()
  vi.advanceTimersByTime(900)
  expect(FakeEventSource.all).toHaveLength(1)
  vi.advanceTimersByTime(100)
  expect(FakeEventSource.all).toHaveLength(2) // 1 s

  giveUp(last())
  vi.advanceTimersByTime(1500)
  expect(FakeEventSource.all).toHaveLength(2)
  vi.advanceTimersByTime(500)
  expect(FakeEventSource.all).toHaveLength(3) // 2 s

  last().onopen?.() // conectou: o próximo backoff volta pra 1 s
  giveUp(last())
  vi.advanceTimersByTime(1000)
  expect(FakeEventSource.all).toHaveLength(4)

  for (let i = 0; i < 10; i++) {
    giveUp(last())
    vi.advanceTimersByTime(30_000) // teto de 30 s
  }
  expect(FakeEventSource.all).toHaveLength(14)
})

it('unsubscribe durante o backoff não abre mais nenhum', () => {
  const h = handlers()
  const stop = sseEvents.subscribe(h)
  giveUp(last())
  stop()
  vi.advanceTimersByTime(60_000)
  expect(FakeEventSource.all).toHaveLength(1)
})
