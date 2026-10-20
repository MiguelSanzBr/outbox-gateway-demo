# outbox-gateway-demo

Gateway de mensagens em Go com *transactional outbox* no PostgreSQL: recebe pedidos de envio por HTTP e entrega a um provedor externo lento e instável sem perder mensagem, sem entregar duas vezes e sem trocar a ordem das mensagens de um mesmo destinatário, mesmo se o processo morrer no meio do envio.

Fiz esse repo pq o gateway de WhatsApp que construí em produção é privado e não dá pra mostrar o código. Então reescrevi o mesmo padrão do zero, com código novo, e aproveitei pra fechar o buraco que ficou lá: aqui tem reaper. O provedor é um mock HTTP com latência e falhas configuráveis.

[English version](README.en.md)

## Como funciona

```
POST /v1/messages ──► BEGIN; INSERT messages; INSERT outbox; COMMIT ──► 202
                                        │
             N workers ◄── claim: FOR UPDATE SKIP LOCKED + lease ──┘
                 │
                 ├─ envia ao provedor com Idempotency-Key
                 ├─ 2xx            → sent
                 ├─ 5xx/429/timeout → pending, backoff exponencial com jitter
                 └─ outros 4xx     → dead

             reaper: sending com lease vencido → pending
```

1. **Entrada idempotente.** `POST /v1/messages` exige `Idempotency-Key`. A mensagem e a linha da outbox são gravadas na mesma transação. Repetir a chave devolve `200` com o mesmo id; a mesma chave com outro conteúdo devolve `409`.
2. **Claim.** Cada worker reivindica uma linha com `SELECT ... FOR UPDATE SKIP LOCKED`, marca `sending` e grava um `lease_until` e um `lease_token` novo. Workers não esperam uns pelos outros.
3. **Ordem por destinatário.** O claim só aceita a mensagem mais antiga de um destinatário que não tenha nenhuma anterior em aberto (`NOT EXISTS`). Um índice único parcial (`recipient WHERE status = 'sending'`) garante no banco, no máximo, uma mensagem em voo por destinatário.
4. **Lease, reaper e fencing.** Se o worker morre, a linha fica em `sending` até o lease vencer; o reaper a devolve à fila. Toda finalização exige o `lease_token` vigente, então um worker que "acorda" depois de perder o lease não consegue sobrescrever o estado. O gateway original não tinha reaper; essa é a melhoria desta versão.
5. **Entrega efetivamente única.** O gateway garante *pelo menos uma vez*; a chave de idempotência vai no header para o provedor descartar reenvios. Sem esse apoio do provedor, um crash entre o envio e o commit gera duplicata. Não há como evitar isso só do lado de quem envia.

## Arquitetura

Hexagonal, sem cerimônia: o domínio no meio, os casos de uso em volta e tudo q fala com o mundo de fora (Postgres, HTTP, provedor, SSE, o próprio loop dos workers) vira adapter.

```
   adapters de entrada                                    adapters de saída
   httpapi (REST) ──┐                                 ┌──► postgres (OutboxRepository, EventSource)
   sse ─────────────┼──► app (casos de uso) ──portas──┼──► provider (Sender HTTP)
   worker (loop) ───┘            │                    └──► worker.Fleet (WorkerPool, só na demo)
                                 ▼
                              domain
```

- `internal/domain`: estados e transições (`pending → sending → sent | dead | pending`), `OutboxEntry` com `Complete` e `Fail`, `LeaseToken` como fencing token, `RetryPolicy` (backoff com jitter e limite de tentativas), validação da mensagem e a regra de ordem por destinatário. Go puro.
- `internal/app`: os casos de uso (`EnqueueMessage`, `ClaimNext`, `Deliver`, `CompleteDelivery`, `FailDelivery`, `ReapExpiredLeases`, `StreamEvents`, `KillWorker`) e as portas q eles usam (`OutboxRepository`, `Sender`, `EventSource`, `WorkerPool`).
- `internal/adapters`: `postgres`, `provider`, `httpapi`, `sse`, `worker` e o `mockprovider`.

A regra de dependência é teste, não combinado: `TestDependencyRule` lê os imports de `domain` e `app` com `go/parser` e falha se aparecer pgx, `net/http` ou adapter. No painel quem faz isso é o `no-restricted-imports` do ESLint.

**Por que as invariantes de concorrência moram no adapter postgres.** O domínio decide o quê: qual a próxima transição, se é retry ou dead, quanto esperar. Quem garante o quando é o banco. O `FOR UPDATE SKIP LOCKED`, o `NOT EXISTS` da ordem por destinatário, o índice único parcial de uma mensagem em voo por destinatário e o `WHERE lease_token = $2` do fencing só valem se forem atômicos com a escrita, e só o Postgres enxerga todos os workers ao mesmo tempo. Trazer isso pra memória viraria ler, decidir e gravar em passos separados, com corrida no meio. Por isso o `domain.RecipientOrder` existe pra conferir a ordem do lado de quem recebe (o mock usa), mas a garantia em si fica no SQL do claim. Pelo mesmo motivo não tem porta de `Clock`: lease e reaper usam o `now()` do Postgres de propósito, pra não depender do relógio de cada worker.

## Garantias e testes

Testes de integração contra Postgres real (`internal/adapters/`, `cmd/`) e unitários no domínio e nos casos de uso:

| Garantia | Teste |
|---|---|
| Sem perda, sem duplicação, ordem preservada com 40 produtores concorrentes, 8 workers e 20% de falhas do provedor | `TestDeliveryNoLossNoDuplicatesInOrder` |
| Worker morto depois de o provedor receber: reaper devolve, outro worker reenvia, provedor descarta o reenvio e a mensagem seguinte do destinatário só sai depois | `TestReaperRecoversWorkerKilledMidSend` |
| Worker com lease vencido não finaliza a linha | `TestStaleLeaseCannotFinish` |
| Erro 4xx vai para `dead` sem retry | `TestPermanentErrorGoesDead` |
| Idempotência na entrada (store e HTTP) | `TestEnqueueIsIdempotent`, `TestCreateMessage` |
| Mensagem e outbox entram juntas ou não entram: falha em qualquer insert não deixa linha pela metade | `TestEnqueueIsAtomic` |
| Worker que demorou mais que o lease não marca `sent` com token velho; a outra tentativa entrega | `TestWorkerThatLostLeaseDoesNotFinish` |
| Banco fora do ar: store devolve o erro, workers logam e seguem tentando, API responde 500 sem vazar detalhe | `TestStoreSurfacesDBErrors`, `TestWorkersSurviveDBOutage`, `TestReadEndpointsAndErrors` |
| `SIGINT` desliga gateway e mock limpo; se o listen falhar, os workers param antes do pool fechar | `TestMainGracefulShutdown`, `TestRunListenErrorStopsWorkers` |
| Toda mudança de estado vira evento no `LISTEN`, inclusive a devolução pelo reaper | `TestListenStreamsStateChanges` |
| SSE com ping; cliente q sai libera a conexão do `LISTEN`; desligar o gateway fecha o stream | `TestEventsStream`, `TestRunDemoAdminFlagAndSSEShutdown` |
| Worker derrubado no meio do envio não finaliza: a linha fica em `sending` até o reaper e, religado, ele volta a entregar | `TestFleetKillMidSendLeavesRowForReaper` |
| Rotas de admin só existem com `DEMO_ADMIN=1` | `TestAdminDisabledByDefault`, `TestRunDemoAdminFlagAndSSEShutdown` |
| Máquina de estados e decisão de retry ou dead no domínio, sem banco | `TestStatusMachine`, `TestEntryTransitions` |
| Domínio e casos de uso não importam pgx, `net/http` nem adapter | `TestDependencyRule` |
| Painel: evento atrasado não volta o estado pra trás; reaper e dead viram aviso | `board.test.ts`, `message.test.ts` (Vitest) |
| No navegador, contra a stack de verdade: mensagem enfileirada chega em `sent`; worker derrubado no meio do envio volta pelo reaper e é entregue | `e2e/demo.spec.ts` (Playwright) |

Cobertura de 100% das instruções (fora o helper `internal/testdb`), com gate no CI. O painel tem o mesmo gate no Vitest (instruções, ramos, funções e linhas), fora o `main.tsx`, q só liga os adapters reais e é exercitado pelo E2E. Os ramos de erro do banco rodam contra Postgres de verdade, sem mock: pool fechado pra simular banco fora e trigger no schema do teste pra derrubar um comando específico no meio da transação.

Para conferir que os testes de ordem não passam por acaso, removi a condição de ordem da query de claim: os dois primeiros testes da tabela falharam.

Fiz o mesmo com o painel e o admin, quebrando de propósito e voltando depois: ligar as rotas de admin sem olhar o `DEMO_ADMIN` derruba o `TestRunDemoAdminFlagAndSSEShutdown`; um `WHEN` no trigger q ignora a volta pra `pending` deixa o `TestListenStreamsStateChanges` sem o evento do reaper; deixar o worker derrubado finalizar a linha quebra o `TestFleetKillMidSendLeavesRowForReaper`; aceitar evento atrasado no painel quebra dois testes do Vitest. Colocar import de `net/http` em `app` ou de `app` em `domain` derruba o `TestDependencyRule`.

## Números do teste de carga

Medidos com k6 nesta configuração, tudo na mesma máquina: AMD Ryzen 5 5600GT (6 núcleos/12 threads), ~11 GiB de RAM, Docker 29, Postgres 17 sem tuning. Mock: 80 ms + 0–120 ms de latência e 5% de respostas 503. Lease de 10 s, reaper a cada 2 s, 1.000 destinatários. A saída bruta de cada rodada está em [`load/results/`](load/results/).

| Cenário (60 s de carga) | Aceitas | Entregues | Reenvios descartados pelo provedor | Violações de ordem | Ponta a ponta p50 / p95 / p99 |
|---|---:|---:|---:|---:|---|
| 100 req/s, 16 workers | 6.001 | 6.001 | 0 | 0 | 162 / 246 / 519 ms |
| 300 req/s, 64 workers | 17.814 | 17.814 | 0 | 0 | 172 / 1.864 / 2.013 ms |
| 100 req/s, 32 workers, `kill -9` no gateway aos 20 s, religado aos 25 s | 5.590 | 5.590 | 19 | 0 | 151 / 325 / 523 ms |
| Mesmo crash com 16 workers | 5.599 | 5.599 | 14 | 0 | 2.718 / 4.137 / 4.337 ms |

"Ponta a ponta" vai da gravação na API até o `sent` no banco, ou seja, inclui fila, retries e a latência do mock. As retentativas por 503 aparecem em `load/results/` como "tentativas extras".

O que os números mostram:

- **Nenhuma perda e nenhuma violação de ordem em todas as rodadas**, inclusive com `kill -9`. No crash com 32 workers, 21 linhas estavam em voo: voltaram à fila pelo reaper depois do lease e 19 delas já tinham chegado ao provedor, que descartou o reenvio pela chave. Essas mensagens esperaram até ~11,5 s (lease + intervalo do reaper).
- **Com 16 workers, o crash afetou o resto do teste**: a latência subiu para ~4 s após o restart e não voltou antes do fim. Com 32 workers, o mesmo crash não aparece no p50. Minha leitura é que 16 workers × ~140 ms por envio deixam pouca folga sobre 100 req/s, mas não investiguei a fundo por que a fila não drenou.
- **A 300 req/s o gargalo passou a ser o Postgres:** o p95 da API subiu de ~2 ms para 111 ms, o k6 deixou de disparar 187 iterações e a vazão de entrega ficou em 288 msg/s. Cada mensagem custa três commits (entrada, claim e finalização); num teste à parte drenando um backlog com 64 workers, as esperas no `pg_stat_activity` eram de WAL (`WalSync`, `WALWrite`).

## Trade-offs: Postgres como fila vs. broker

**A favor:** a mensagem e a intenção de enviá-la são gravadas atomicamente junto do dado de negócio, sem *dual write*; uma peça a menos para operar; status consultável com SQL; ordem por destinatário expressa numa query.

**Contra:** três commits por mensagem põem o teto no WAL; os workers fazem polling (50 ms) em vez de receber push; a tabela cresce e gera trabalho de vacuum; a query de claim fica mais cara quando muitos destinatários têm fila bloqueada.

**Quando trocar:** carga sustentada de milhares de mensagens por segundo, ou vários consumidores do mesmo evento. Aí a outbox continua útil como ponte (um *relay* publica no Kafka/NATS/RabbitMQ), mas deixa de ser a fila de trabalho.

## Limites conhecidos

- **Head-of-line blocking:** uma mensagem em backoff segura as seguintes do mesmo destinatário. Uma mensagem `dead` libera o destinatário: escolhi não travar a conversa para sempre.
- A ordem garantida é a de commit. Requisições concorrentes para o mesmo destinatário não têm ordem definida.
- Linhas `sent` nunca são apagadas. Em produção, isso pede particionamento por data ou arquivamento.
- Os workers fazem polling (o `LISTEN/NOTIFY` só alimenta o painel) e não tem rate limit por provedor; um 429 é tratado só como retry.
- O lease precisa ser maior que o envio mais lento; o envio é cortado em metade do lease. O relógio usado é o do Postgres (`now()`), não o dos workers.
- O mock guarda tudo em memória e não persiste nada.

## Painel em tempo real

`dashboard/` é um painel em Vite, React 19 e TypeScript strict q mostra as mensagens andando entre os estados ao vivo: raias por destinatário (dá pra ver a ordem sendo respeitada), contadores por estado, a lista de workers com botão de derrubar e um aviso quando o reaper devolve uma linha. Segue a mesma divisão do Go: `domain` (estado da mensagem e contadores), `application` (casos de uso sobre as portas `GatewayApi` e `GatewayEvents`) e `adapters` (fetch, `EventSource` e a tela em React).

Como funciona:

1. Um trigger `AFTER INSERT OR UPDATE` na `outbox` faz `pg_notify` com id, destinatário, status, tentativas, worker e último erro. Como o reaper é só mais um `UPDATE`, a devolução dele aparece igual a qualquer outra mudança.
2. `GET /v1/events` abre uma conexão própria com `LISTEN` e repassa cada notificação como evento SSE, com ping a cada 15 s pra proxy não derrubar a conexão parada. Cliente q sai libera a conexão do `LISTEN`; desligar o gateway fecha todos os streams.
3. O painel busca a foto em `GET /v1/messages` (as 500 mais novas) com TanStack Query e aplica os eventos do `EventSource` por cima. Toda vez q o SSE conecta ou reconecta ele busca a foto de novo, pq o NOTIFY não guarda nada.
4. Foto e evento chegam em qualquer ordem. Em vez de coluna de versão, o painel ordena pelo ciclo de vida: cada claim soma 1 em `attempts` e o `sending` vem antes do resultado da mesma tentativa, então `attempts * 2 + (sending ? 0 : 1)` só cresce. Evento com posição menor ou igual à atual é ignorado.

**Por que LISTEN/NOTIFY e não polling.** Polling por `updated_at` ou por coluna de versão perde evento: duas transações podem gravar numa ordem e commitar na outra, e o cursor passa por cima da q commitou depois. O NOTIFY só sai no commit, na ordem dos commits, pega qualquer escritor (worker, reaper, até um `UPDATE` na mão) e não precisa de coluna nova. O custo:

- Não é durável: quem não estava ouvindo perde o evento. Por isso a foto é recarregada a cada reconexão.
- Todo commit q notifica passa por um lock global da fila de notificação. Os números do teste de carga acima foram medidos antes do trigger existir e não medi de novo; com carga alta vale tirar o trigger ou trocar o painel pra polling.
- Payload de no máximo 8000 bytes, por isso o `last_error` vai cortado em 200 caracteres.
- Cada cliente SSE segura uma conexão de banco. Pra demo serve; pra muitos clientes o certo é um listener só no processo distribuindo pros streams.

**Derrube um worker de verdade.** Com `DEMO_ADMIN=1` o gateway liga `GET /v1/admin/workers`, `POST /v1/admin/workers/{id}/kill` e `POST /v1/admin/workers/revive`. O kill cancela o contexto daquele worker com uma causa própria: se ele estiver enviando, o envio é cortado e a linha não é finalizada. Ela fica em `sending` com o id do worker até o lease vencer e o reaper devolver, e no painel dá pra ver a linha parada, o aviso "devolvida pelo reaper" e outro worker entregando. O botão "Enfileirar 20 mensagens" manda uma rajada pra 4 destinatários pela própria API, uma requisição por vez pra ordem de commit ser a ordem de envio.

Essas rotas vêm desligadas e **nunca podem ser ligadas em produção**: não têm autenticação, então qualquer um na rede derruba os workers. Aliás `GET /v1/events` e `GET /v1/messages` mostram o destinatário de todas as mensagens, igual ao `GET /v1/messages/{id}` q já existia; num gateway de verdade tudo isso fica atrás de autenticação.

Acessibilidade: os avisos (reaper, dead, ações) ficam num `role="log"` com `aria-live`, sem anunciar cada `sent` pra não afogar o leitor de tela; cada mensagem tem nome acessível com estado, tentativa e worker; o estado nunca é só cor; tudo é botão nativo e funciona no teclado; tema claro e escuro pelo `prefers-color-scheme`; o grid cabe em 375 px.

## Como rodar

Testes (precisam de Postgres):

```bash
docker compose up -d postgres
DATABASE_URL=postgres://outbox:outbox@localhost:5432/outbox go test -race ./...
```

Com cobertura (o mesmo comando do CI):

```bash
DATABASE_URL=postgres://outbox:outbox@localhost:5432/outbox \
  go test -race -coverpkg=$(go list ./... | grep -v /internal/testdb | paste -sd,) -coverprofile=cover.out ./...
go tool cover -func=cover.out | tail -1
```

Stack completa e teste de carga:

```bash
docker compose down -v && WORKERS=16 docker compose up -d --build
RATE=100 DURATION=60s docker compose run --rm k6
./load/report.sh
```

`report.sh` espera a outbox esvaziar e imprime aceitas, entregues, latência ponta a ponta e os contadores do mock (`duplicates`, `order_violations`). Variáveis do mock: `MOCK_LATENCY`, `MOCK_JITTER`, `MOCK_FAIL_RATE`. Do gateway: `WORKERS`, `LEASE`, `MAX_ATTEMPTS`, `BACKOFF_BASE`, `BACKOFF_MAX`, `DEMO_ADMIN`.

Chamada manual:

```bash
curl -i -X POST localhost:8080/v1/messages \
  -H 'Idempotency-Key: pedido-42' \
  -d '{"recipient":"5511999990000","body":"Seu pedido saiu para entrega"}'
curl localhost:8080/v1/messages/1
```

Painel (sobe junto no compose, em http://localhost:5180):

```bash
DEMO_ADMIN=1 docker compose up -d --build
```

Sem `DEMO_ADMIN=1` o painel funciona igual, só sem os botões de derrubar worker. Pra mexer no painel com o gateway rodando em `:8080`:

```bash
cd dashboard && npm ci
npm run dev        # Vite em :5180, repassa /v1 pro gateway
npm run lint && npm run typecheck && npm run coverage && npm run build
```

E2E com Playwright contra a stack de verdade. Com 2 workers e 1 s por envio sempre tem worker no meio de um envio pra derrubar:

```bash
docker compose down -v
DEMO_ADMIN=1 WORKERS=2 LEASE=6s MOCK_LATENCY=1s MOCK_JITTER=0s MOCK_FAIL_RATE=0 docker compose up -d --build
cd dashboard && npx playwright install chromium && npm run e2e
```

## Estrutura

```
cmd/gateway                    raiz de composição: API + workers + reaper num processo
cmd/mockprovider               provedor simulado
internal/domain                estados, transições, lease, retry, validação e ordem (Go puro)
internal/app                   casos de uso e portas
internal/adapters/postgres     schema, repositório (claim, lease, reaper) e LISTEN/NOTIFY
internal/adapters/httpapi      handlers HTTP
internal/adapters/sse          stream SSE pro painel
internal/adapters/provider     sender HTTP pro provedor
internal/adapters/worker       loop dos workers e do reaper, kill e revive da demo
internal/adapters/mockprovider latência, falhas, deduplicação e checagem de ordem
internal/archtest              regra de dependência entre as camadas
dashboard/                     painel (domain, application, adapters) e E2E
load/                          script k6, relatório e resultados brutos
```
