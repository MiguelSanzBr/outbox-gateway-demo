import type { GatewayApi } from './ports'

// Uma requisição por vez: a ordem garantida por destinatário é a de commit, então em paralelo não teria ordem.
export async function enqueueBurst(api: GatewayApi, n: number, recipients: number): Promise<number> {
  const run = crypto.randomUUID().slice(0, 8)
  for (let i = 0; i < n; i++) {
    await api.sendMessage(`demo-${run}-${i}`, `demo-${(i % recipients) + 1}`, `demo ${run} #${i}`)
  }
  return n
}
