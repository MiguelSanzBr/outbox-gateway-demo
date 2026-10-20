import { expect, it, vi } from 'vitest'
import type { Msg } from '../domain/message'
import { sseEvents } from './sse'

class FakeEventSource {
  static last: FakeEventSource
  onopen: (() => void) | null = null
  onerror: (() => void) | null = null
  onmessage: ((e: { data: string }) => void) | null = null
  closed = false
  constructor(readonly url: string) {
    FakeEventSource.last = this
  }
  close() {
    this.closed = true
  }
}

it('sseEvents repassa abertura, mudança e erro, e fecha no unsubscribe', () => {
  vi.stubGlobal('EventSource', FakeEventSource)
  const h = { onChange: vi.fn(), onOpen: vi.fn(), onError: vi.fn() }
  const stop = sseEvents.subscribe(h)
  const es = FakeEventSource.last
  expect(es.url).toBe('/v1/events')

  const m: Msg = { id: 3, recipient: 'ana', status: 'sending', attempts: 1, worker: 0, last_error: null }
  es.onopen?.()
  es.onmessage?.({ data: JSON.stringify(m) })
  es.onerror?.()
  expect(h.onOpen).toHaveBeenCalledOnce()
  expect(h.onChange).toHaveBeenCalledWith(m)
  expect(h.onError).toHaveBeenCalledOnce()

  stop()
  expect(es.closed).toBe(true)
})
