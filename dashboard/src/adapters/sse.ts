import { isMsg } from '../domain/message'
import type { GatewayEvents } from '../application/ports'

const minDelay = 1000
const maxDelay = 30_000

// sseEvents é o adapter da porta GatewayEvents em cima do EventSource. Ele mesmo
// reconecta quando a conexão cai, mas desiste de vez (CLOSED) se a resposta não for
// 200 text/event-stream: 502 do nginx com o gateway fora, 500 com o banco fora, 503
// acima do teto de streams. Aí quem tenta de novo é a gente, com backoff e jitter.
export const sseEvents: GatewayEvents = {
  subscribe(h) {
    let es: EventSource
    let timer: ReturnType<typeof setTimeout> | undefined
    let delay = minDelay
    const connect = () => {
      es = new EventSource('/v1/events')
      es.onopen = () => {
        delay = minDelay
        h.onOpen()
      }
      es.onerror = () => {
        h.onError()
        if (es.readyState !== EventSource.CLOSED) return
        timer = setTimeout(connect, delay * (0.5 + Math.random() / 2))
        delay = Math.min(delay * 2, maxDelay)
      }
      es.onmessage = (e: MessageEvent<string>) => {
        let m: unknown
        try {
          m = JSON.parse(e.data)
        } catch {
          m = null
        }
        if (isMsg(m)) h.onChange(m)
        else h.onError()
      }
    }
    connect()
    return () => {
      clearTimeout(timer)
      es.close()
    }
  },
}
