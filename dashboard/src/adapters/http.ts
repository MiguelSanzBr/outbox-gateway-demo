import type { Msg } from '../domain/message'
import type { GatewayApi, Worker } from '../application/ports'

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

// httpApi é o adapter da porta GatewayApi em cima do fetch.
export const httpApi: GatewayApi = {
  listMessages: async () => json<Msg[]>(await fetch('/v1/messages')),

  async listWorkers() {
    const res = await fetch('/v1/admin/workers')
    return res.status === 404 ? null : json<Worker[]>(res)
  },

  async killWorker(id) {
    await ok(await fetch(`/v1/admin/workers/${id}/kill`, { method: 'POST' }))
  },

  reviveWorkers: async () =>
    (await json<{ revived: number }>(await fetch('/v1/admin/workers/revive', { method: 'POST' }))).revived,

  async sendMessage(key, recipient, body) {
    await ok(
      await fetch('/v1/messages', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key },
        body: JSON.stringify({ recipient, body }),
      }),
    )
  },
}
