import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useEffect, useReducer, useState } from 'react'
import { emptyBoard, reducer } from '../../application/board'
import { enqueueBurst } from '../../application/burst'
import type { GatewayApi, GatewayEvents } from '../../application/ports'
import { counts, lanes, reaped, type Msg, type Status } from '../../domain/message'
import './app.css'

const burstSize = 20
const burstRecipients = 4
const maxLanes = 12
const statuses: Status[] = ['pending', 'sending', 'sent', 'dead']

function label(m: Msg): string {
  const parts = [`mensagem ${m.id}: ${m.status}`]
  if (m.attempts > 0) parts.push(`tentativa ${m.attempts}`)
  if (m.status === 'sending' && m.worker !== null) parts.push(`worker ${m.worker}`)
  if (reaped(m)) parts.push('devolvida pelo reaper')
  return parts.join(', ')
}

const message = (e: Error) => e.message

// App é o adapter de entrada: React dirigindo os casos de uso pelas portas.
export function App({ api, events }: { api: GatewayApi; events: GatewayEvents }) {
  const qc = useQueryClient()
  const [board, dispatch] = useReducer(reducer, emptyBoard)
  const [conn, setConn] = useState<'conectando' | 'ao vivo' | 'reconectando'>('conectando')
  const note = (text: string) => dispatch({ type: 'note', text })

  const snapshot = useQuery({ queryKey: ['messages'], queryFn: () => api.listMessages() })
  const workers = useQuery({ queryKey: ['workers'], queryFn: () => api.listWorkers(), refetchInterval: 1000 })

  useEffect(() => {
    if (snapshot.data) dispatch({ type: 'rows', rows: snapshot.data, live: false })
  }, [snapshot.data])

  useEffect(
    () =>
      events.subscribe({
        onChange: (m) => dispatch({ type: 'rows', rows: [m], live: true }),
        onOpen: () => {
          setConn('ao vivo')
          // o SSE não guarda nada: o q mudou antes do LISTEN só vem na foto
          void qc.invalidateQueries({ queryKey: ['messages'] })
        },
        onError: () => setConn('reconectando'),
      }),
    [events, qc],
  )

  const refreshWorkers = () => qc.invalidateQueries({ queryKey: ['workers'] })
  const kill = useMutation({
    mutationFn: (id: number) => api.killWorker(id),
    onSuccess: (_, id) => note(`worker ${id} derrubado`),
    onError: (e, id) => note(`não deu pra derrubar o worker ${id}: ${message(e)}`),
    onSettled: refreshWorkers,
  })
  const revive = useMutation({
    mutationFn: () => api.reviveWorkers(),
    onSuccess: (n) => note(`${n} worker(s) religado(s)`),
    onSettled: refreshWorkers,
  })
  const burst = useMutation({
    mutationFn: () => enqueueBurst(api, burstSize, burstRecipients),
    onSuccess: (n) => note(`${n} mensagens enfileiradas`),
    onError: (e) => note(`falha ao enfileirar: ${message(e)}`),
  })

  const msgs = [...board.msgs.values()]
  const total = counts(msgs)
  const busy = (id: number) => msgs.find((m) => m.status === 'sending' && m.worker === id)

  return (
    <>
      <header>
        <h1>Outbox ao vivo</h1>
        <p role="status" className={`conn conn-${conn.replace(' ', '-')}`}>
          {conn}
        </p>
        <button type="button" onClick={() => burst.mutate()} disabled={burst.isPending}>
          Enfileirar {burstSize} mensagens
        </button>
      </header>
      <main>
        {snapshot.error && <p role="alert">não deu pra carregar as mensagens: {message(snapshot.error)}</p>}

        <section aria-labelledby="h-estados">
          <h2 id="h-estados">Estados</h2>
          <dl className="counters">
            {statuses.map((s) => (
              <div key={s} className={`chip st-${s}`}>
                <dt>{s}</dt>
                <dd>{total[s]}</dd>
              </div>
            ))}
          </dl>
        </section>

        <section aria-labelledby="h-workers">
          <h2 id="h-workers">Workers</h2>
          {workers.error && <p role="alert">não deu pra ler os workers: {message(workers.error)}</p>}
          {workers.data === null && (
            <p>Admin desligado. Suba o gateway com DEMO_ADMIN=1 pra derrubar worker (nunca em produção).</p>
          )}
          {workers.data && (
            <>
              <ul className="workers">
                {workers.data.map((w) => {
                  const m = busy(w.id)
                  const state = !w.alive ? 'caído' : m ? `enviando #${m.id}` : 'livre'
                  return (
                    <li key={w.id} aria-label={`worker ${w.id}: ${state}`}>
                      <span>
                        w{w.id} · {state}
                      </span>
                      <button
                        type="button"
                        aria-label={`Derrubar worker ${w.id}`}
                        disabled={!w.alive}
                        onClick={() => kill.mutate(w.id)}
                      >
                        Derrubar
                      </button>
                    </li>
                  )
                })}
              </ul>
              {workers.data.some((w) => !w.alive) && (
                <button type="button" onClick={() => revive.mutate()}>
                  Religar workers
                </button>
              )}
            </>
          )}
        </section>

        <section aria-labelledby="h-avisos">
          <h2 id="h-avisos">Avisos</h2>
          <ol role="log" aria-live="polite" className="notes">
            {board.notes.map((n) => (
              <li key={n.id}>{n.text}</li>
            ))}
          </ol>
        </section>

        <section aria-labelledby="h-raias">
          <h2 id="h-raias">Por destinatário</h2>
          <div className="lanes">
            {lanes(msgs, maxLanes).map(([r, list]) => (
              <section key={r} aria-label={r} className="lane">
                <h3>{r}</h3>
                <ol>
                  {list.map((m) => (
                    <li key={m.id} aria-label={label(m)} className={`chip st-${m.status}`}>
                      #{m.id} {m.status}
                      {reaped(m) && <small aria-hidden="true"> ↺</small>}
                    </li>
                  ))}
                </ol>
              </section>
            ))}
          </div>
        </section>
      </main>
    </>
  )
}
