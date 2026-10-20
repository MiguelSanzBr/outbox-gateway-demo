import { describe, expect, it } from 'vitest'
import { enqueueBurst } from './burst'
import type { GatewayApi } from './ports'

// api em memória: só o sendMessage interessa aqui
function fakeApi(failAt = -1) {
  const sent: { key: string; recipient: string; body: string }[] = []
  const api = {
    async sendMessage(key: string, recipient: string, body: string) {
      if (sent.length === failAt) throw new Error('HTTP 500')
      sent.push({ key, recipient, body })
    },
  } as GatewayApi
  return { api, sent }
}

describe('enqueueBurst', () => {
  it('enqueueBurst manda em sequência, revezando destinatários, com chave única', async () => {
    const { api, sent } = fakeApi()
    await expect(enqueueBurst(api, 6, 3)).resolves.toBe(6)
    expect(sent.map((m) => m.recipient)).toEqual(['demo-1', 'demo-2', 'demo-3', 'demo-1', 'demo-2', 'demo-3'])
    expect(new Set(sent.map((m) => m.key)).size).toBe(6)
  })

  it('enqueueBurst para no primeiro erro', async () => {
    const { api, sent } = fakeApi(1)
    await expect(enqueueBurst(api, 5, 2)).rejects.toThrow('HTTP 500')
    expect(sent).toHaveLength(1)
  })
})
