import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { act, render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it, vi } from 'vitest'
import type { EventHandlers, GatewayApi, GatewayEvents, Worker } from '../../application/ports'
import type { Msg } from '../../domain/message'
import { App } from './App'

const msg = (over: Partial<Msg>): Msg => ({
  id: 1,
  recipient: 'demo-1',
  status: 'pending',
  attempts: 0,
  worker: null,
  last_error: null,
  ...over,
})

function fakeApi(over: Partial<GatewayApi> = {}): GatewayApi {
  return {
    listMessages: vi.fn(async () => [msg({ id: 1 }), msg({ id: 2, recipient: 'demo-2', status: 'sent', attempts: 1, worker: 1 })]),
    listWorkers: vi.fn(async (): Promise<Worker[] | null> => [
      { id: 0, alive: true },
      { id: 1, alive: false },
    ]),
    killWorker: vi.fn(async () => undefined),
    reviveWorkers: vi.fn(async () => 1),
    sendMessage: vi.fn(async () => undefined),
    ...over,
  }
}

// stream controlado pelo teste: guarda os handlers pra empurrar eventos na mão
function fakeEvents() {
  const s = { h: null as EventHandlers | null, stopped: false }
  const events: GatewayEvents = {
    subscribe(h) {
      s.h = h
      return () => {
        s.stopped = true
      }
    },
  }
  const push = (fn: (h: EventHandlers) => void) =>
    act(() => {
      if (s.h) fn(s.h)
    })
  return { events, push, s }
}

function setup(api = fakeApi()) {
  const ev = fakeEvents()
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  const view = render(
    <QueryClientProvider client={client}>
      <App api={api} events={ev.events} />
    </QueryClientProvider>,
  )
  return { api, ...ev, view, user: userEvent.setup() }
}

const item = (id: number) => screen.findByRole('listitem', { name: new RegExp(`^mensagem ${id}:`) })

describe('App', () => {
  it('mostra a foto por destinatário, os contadores e o estado da conexão', async () => {
    const { push, api } = setup()
    expect(screen.getByRole('status')).toHaveTextContent('conectando')
    expect(await item(1)).toHaveAccessibleName('mensagem 1: pending')
    expect(await item(2)).toHaveAccessibleName('mensagem 2: sent, tentativa 1')
    expect(screen.getByRole('region', { name: 'demo-1' })).toContainElement(await item(1))

    const counters = screen.getByRole('region', { name: 'Estados' })
    expect(within(counters).getByText('pending').nextSibling).toHaveTextContent('1')
    expect(within(counters).getByText('sent').nextSibling).toHaveTextContent('1')

    // conectou: busca a foto de novo pra cobrir o q passou antes do LISTEN
    push((h) => h.onOpen())
    expect(screen.getByRole('status')).toHaveTextContent('ao vivo')
    await waitFor(() => expect(api.listMessages).toHaveBeenCalledTimes(2))
    push((h) => h.onError())
    expect(screen.getByRole('status')).toHaveTextContent('reconectando')
  })

  it('segue os eventos ao vivo e avisa quando o reaper devolve a linha', async () => {
    const { push } = setup()
    await item(1)
    push((h) => h.onChange(msg({ id: 1, status: 'sending', attempts: 1, worker: 0 })))
    expect(await item(1)).toHaveAccessibleName('mensagem 1: sending, tentativa 1, worker 0')
    expect(screen.getByRole('listitem', { name: /^worker 0/ })).toHaveTextContent('enviando #1')

    push((h) => h.onChange(msg({ id: 1, status: 'pending', attempts: 1, worker: 0, last_error: 'lease expirado' })))
    expect(await item(1)).toHaveAccessibleName('mensagem 1: pending, tentativa 1, devolvida pelo reaper')
    expect(screen.getByRole('log')).toHaveTextContent('#1 devolvida pelo reaper')
  })

  it('derruba e religa worker pelo admin da demo', async () => {
    const { api, user } = setup()
    const w0 = await screen.findByRole('listitem', { name: /^worker 0/ })
    expect(w0).toHaveTextContent('livre')
    expect(screen.getByRole('listitem', { name: /^worker 1/ })).toHaveTextContent('caído')
    expect(screen.getByRole('button', { name: 'Derrubar worker 1' })).toBeDisabled()

    await user.click(screen.getByRole('button', { name: 'Derrubar worker 0' }))
    expect(api.killWorker).toHaveBeenCalledWith(0)
    expect(await screen.findByRole('log')).toHaveTextContent('worker 0 derrubado')

    await user.click(screen.getByRole('button', { name: 'Religar workers' }))
    expect(api.reviveWorkers).toHaveBeenCalled()
    await waitFor(() => expect(screen.getByRole('log')).toHaveTextContent('1 worker(s) religado(s)'))
  })

  it('avisa quando derrubar falha', async () => {
    const { user } = setup(fakeApi({ killWorker: vi.fn(async () => Promise.reject(new Error('HTTP 404'))) }))
    await user.click(await screen.findByRole('button', { name: 'Derrubar worker 0' }))
    await waitFor(() => expect(screen.getByRole('log')).toHaveTextContent('não deu pra derrubar o worker 0: HTTP 404'))
  })

  it('sem DEMO_ADMIN mostra como ligar, sem botão de derrubar', async () => {
    setup(fakeApi({ listWorkers: vi.fn(async () => null) }))
    expect(await screen.findByText(/DEMO_ADMIN=1/)).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /Derrubar/ })).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Religar workers' })).not.toBeInTheDocument()
  })

  it('enfileira a rajada e avisa, inclusive quando falha', async () => {
    const { api, user } = setup()
    await user.click(screen.getByRole('button', { name: 'Enfileirar 20 mensagens' }))
    await waitFor(() => expect(screen.getByRole('log')).toHaveTextContent('20 mensagens enfileiradas'))
    expect(api.sendMessage).toHaveBeenCalledTimes(20)

    vi.mocked(api.sendMessage).mockRejectedValueOnce(new Error('HTTP 500'))
    await user.click(screen.getByRole('button', { name: 'Enfileirar 20 mensagens' }))
    await waitFor(() => expect(screen.getByRole('log')).toHaveTextContent('falha ao enfileirar: HTTP 500'))
  })

  it('mostra erro de carga das mensagens e dos workers', async () => {
    setup(
      fakeApi({
        listMessages: vi.fn(async () => Promise.reject(new Error('HTTP 500'))),
        listWorkers: vi.fn(async () => Promise.reject(new Error('HTTP 502'))),
      }),
    )
    expect(await screen.findByText('não deu pra carregar as mensagens: HTTP 500')).toBeInTheDocument()
    expect(await screen.findByText('não deu pra ler os workers: HTTP 502')).toBeInTheDocument()
  })

  it('para de ouvir o stream ao desmontar', () => {
    const { view, s } = setup()
    view.unmount()
    expect(s.stopped).toBe(true)
  })
})
