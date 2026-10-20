import type { Msg } from '../domain/message'
import type { GatewayEvents } from '../application/ports'

// sseEvents é o adapter da porta GatewayEvents em cima do EventSource,
// q já reconecta sozinho quando a conexão cai.
export const sseEvents: GatewayEvents = {
  subscribe(h) {
    const es = new EventSource('/v1/events')
    es.onopen = () => h.onOpen()
    es.onerror = () => h.onError()
    es.onmessage = (e: MessageEvent<string>) => h.onChange(JSON.parse(e.data) as Msg)
    return () => es.close()
  },
}
