import { describe, expect, it, vi } from 'vitest'
import { enqueueBurst, HttpError, killWorker, listMessages, listWorkers, reviveWorkers } from './api'

const reply = (status: number, body?: unknown) =>
  new Response(body === undefined ? null : JSON.stringify(body), { status })

function stubFetch(...replies: Response[]) {
  const fn = vi.fn<typeof fetch>()
  for (const r of replies) fn.mockResolvedValueOnce(r)
  vi.stubGlobal('fetch', fn)
  return fn
}

describe('api', () => {
  it('listMessages devolve a foto e falha com o status HTTP', async () => {
    const rows = [{ id: 1, recipient: 'ana', status: 'pending', attempts: 0, worker: null, last_error: null }]
    const fetch = stubFetch(reply(200, rows), reply(500, { error: 'erro interno' }))
    await expect(listMessages()).resolves.toEqual(rows)
    expect(fetch).toHaveBeenCalledWith('/v1/messages')
    await expect(listMessages()).rejects.toEqual(new HttpError(500))
  })

  it('listWorkers devolve null quando o admin está desligado (404)', async () => {
    stubFetch(reply(200, [{ id: 0, alive: true }]), reply(404, { error: 'x' }), reply(502))
    await expect(listWorkers()).resolves.toEqual([{ id: 0, alive: true }])
    await expect(listWorkers()).resolves.toBeNull()
    await expect(listWorkers()).rejects.toHaveProperty('status', 502)
  })

  it('killWorker e reviveWorkers fazem POST nas rotas de admin', async () => {
    const fetch = stubFetch(reply(204), reply(404, { error: 'worker não existe ou já caiu' }), reply(200, { revived: 2 }))
    await expect(killWorker(3)).resolves.toBeUndefined()
    expect(fetch).toHaveBeenLastCalledWith('/v1/admin/workers/3/kill', { method: 'POST' })
    await expect(killWorker(3)).rejects.toHaveProperty('message', 'HTTP 404')
    await expect(reviveWorkers()).resolves.toBe(2)
    expect(fetch).toHaveBeenLastCalledWith('/v1/admin/workers/revive', { method: 'POST' })
  })

  it('enqueueBurst manda em sequência, revezando destinatários, com chave única', async () => {
    const fetch = stubFetch(...Array.from({ length: 6 }, () => reply(202, { id: 1, status: 'queued' })))
    await expect(enqueueBurst(6, 3)).resolves.toBe(6)
    const calls = fetch.mock.calls.map(([url, init]) => {
      const headers = init?.headers as Record<string, string>
      return { url, key: headers['Idempotency-Key'], body: JSON.parse(String(init?.body)) as { recipient: string } }
    })
    expect(calls.map((c) => c.url)).toEqual(Array(6).fill('/v1/messages'))
    expect(calls.map((c) => c.body.recipient)).toEqual(['demo-1', 'demo-2', 'demo-3', 'demo-1', 'demo-2', 'demo-3'])
    expect(new Set(calls.map((c) => c.key)).size).toBe(6)
  })

  it('enqueueBurst para no primeiro erro', async () => {
    const fetch = stubFetch(reply(202, {}), reply(500, {}))
    await expect(enqueueBurst(5, 2)).rejects.toEqual(new HttpError(500))
    expect(fetch).toHaveBeenCalledTimes(2)
  })
})
