import type { Msg } from './state'

export interface Worker {
  id: number
  alive: boolean
}

export class HttpError extends Error {
  constructor(readonly status: number) {
    super(`HTTP ${status}`)
  }
}

async function ok(res: Response): Promise<Response> {
  if (!res.ok) throw new HttpError(res.status)
  return res
}

const json = async <T>(res: Response): Promise<T> => (await ok(res)).json() as Promise<T>

export const listMessages = async () => json<Msg[]>(await fetch('/v1/messages'))

// 404 = gateway sem DEMO_ADMIN=1; o painel mostra isso em vez de erro
export async function listWorkers(): Promise<Worker[] | null> {
  const res = await fetch('/v1/admin/workers')
  return res.status === 404 ? null : json<Worker[]>(res)
}

export async function killWorker(id: number): Promise<void> {
  await ok(await fetch(`/v1/admin/workers/${id}/kill`, { method: 'POST' }))
}

export const reviveWorkers = async () =>
  (await json<{ revived: number }>(await fetch('/v1/admin/workers/revive', { method: 'POST' }))).revived

// Uma requisição por vez: a ordem garantida por destinatário é a de commit, então em paralelo não teria ordem.
export async function enqueueBurst(n: number, recipients: number): Promise<number> {
  const run = crypto.randomUUID().slice(0, 8)
  for (let i = 0; i < n; i++) {
    await ok(
      await fetch('/v1/messages', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Idempotency-Key': `demo-${run}-${i}` },
        body: JSON.stringify({ recipient: `demo-${(i % recipients) + 1}`, body: `demo ${run} #${i}` }),
      }),
    )
  }
  return n
}
