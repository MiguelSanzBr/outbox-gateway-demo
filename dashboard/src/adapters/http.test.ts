import { describe, expect, it, vi } from 'vitest'
import { HttpError, httpApi } from './http'

const reply = (status: number, body?: unknown) =>
  new Response(body === undefined ? null : JSON.stringify(body), { status })

function stubFetch(...replies: Response[]) {
  const fn = vi.fn<typeof fetch>()
  for (const r of replies) fn.mockResolvedValueOnce(r)
  vi.stubGlobal('fetch', fn)
  return fn
}

describe('httpApi', () => {
  it('listMessages devolve a foto e falha com o status HTTP', async () => {
    const rows = [{ id: 1, recipient: 'ana', status: 'pending', attempts: 0, worker: null, last_error: null }]
    const fetch = stubFetch(reply(200, rows), reply(500, { error: 'erro interno' }))
    await expect(httpApi.listMessages()).resolves.toEqual(rows)
    expect(fetch).toHaveBeenCalledWith('/v1/messages')
    await expect(httpApi.listMessages()).rejects.toEqual(new HttpError(500))
  })

  it('listWorkers devolve null quando o admin está desligado (404)', async () => {
    stubFetch(reply(200, [{ id: 0, alive: true }]), reply(404, { error: 'x' }), reply(502))
    await expect(httpApi.listWorkers()).resolves.toEqual([{ id: 0, alive: true }])
    await expect(httpApi.listWorkers()).resolves.toBeNull()
    await expect(httpApi.listWorkers()).rejects.toHaveProperty('status', 502)
  })

  it('killWorker e reviveWorkers fazem POST nas rotas de admin', async () => {
    const fetch = stubFetch(reply(204), reply(404, { error: 'worker não existe ou já caiu' }), reply(200, { revived: 2 }))
    await expect(httpApi.killWorker(3)).resolves.toBeUndefined()
    expect(fetch).toHaveBeenLastCalledWith('/v1/admin/workers/3/kill', { method: 'POST' })
    await expect(httpApi.killWorker(3)).rejects.toHaveProperty('message', 'HTTP 404')
    await expect(httpApi.reviveWorkers()).resolves.toBe(2)
    expect(fetch).toHaveBeenLastCalledWith('/v1/admin/workers/revive', { method: 'POST' })
  })

  it('sendMessage faz POST com a chave no header e falha com o status', async () => {
    const fetch = stubFetch(reply(202, { id: 1, status: 'queued' }), reply(409, { error: 'x' }))
    await expect(httpApi.sendMessage('k1', 'ana', 'oi')).resolves.toBeUndefined()
    expect(fetch).toHaveBeenLastCalledWith('/v1/messages', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Idempotency-Key': 'k1' },
      body: JSON.stringify({ recipient: 'ana', body: 'oi' }),
    })
    await expect(httpApi.sendMessage('k1', 'ana', 'tchau')).rejects.toEqual(new HttpError(409))
  })
})
