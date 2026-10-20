import type { Msg } from '../domain/message'

export interface Worker {
  id: number
  alive: boolean
}

// GatewayApi é a porta pro REST do gateway.
export interface GatewayApi {
  listMessages(): Promise<Msg[]>
  // null = gateway sem DEMO_ADMIN=1
  listWorkers(): Promise<Worker[] | null>
  killWorker(id: number): Promise<void>
  reviveWorkers(): Promise<number>
  sendMessage(key: string, recipient: string, body: string): Promise<void>
}

export interface EventHandlers {
  onChange(m: Msg): void
  // conectou ou reconectou: o q passou enquanto estava fora só vem na foto
  onOpen(): void
  onError(): void
}

// GatewayEvents é a porta pro stream de mudanças. subscribe devolve o unsubscribe.
export interface GatewayEvents {
  subscribe(h: EventHandlers): () => void
}
