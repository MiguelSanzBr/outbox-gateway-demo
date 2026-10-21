# outbox-gateway-demo

[![ci](https://github.com/MiguelSanzBr/outbox-gateway-demo/actions/workflows/ci.yml/badge.svg)](https://github.com/MiguelSanzBr/outbox-gateway-demo/actions/workflows/ci.yml)
![Go 1.26](https://img.shields.io/badge/go-1.26-00ADD8)
![cobertura 100% com gate no CI](https://img.shields.io/badge/cobertura-100%25%20(gate%20no%20CI)-brightgreen)
[![licença MIT](https://img.shields.io/badge/licen%C3%A7a-MIT-blue)](LICENSE)

Gateway de mensagens em Go com *transactional outbox* no PostgreSQL. Gravar a mensagem no banco e chamar o provedor não cabem na mesma transação: se o processo cai entre uma coisa e outra, a mensagem some ou sai duas vezes.

Aqui a API grava a mensagem e a intenção de enviar na mesma transação, e workers entregam ao provedor **pelo menos uma vez**, na ordem de commit por destinatário, mesmo com `kill -9` no meio do envio. A `Idempotency-Key` vai junto pro provedor descartar o reenvio; com esse apoio dele, o efeito é de entrega única.

Fiz esse repo pq o gateway de WhatsApp que construí em produção é privado e não dá pra mostrar o código. Então reescrevi o mesmo padrão do zero, com código novo, e aproveitei pra fechar o buraco que ficou lá: aqui tem reaper. O provedor é um mock HTTP com latência e falhas configuráveis.

[English version](README.en.md)

![Painel ao vivo: contadores por estado, workers com um derrubado, aviso do reaper e as raias por destinatário](docs/painel.png)

## Como funciona

```
POST /v1/messages ──► BEGIN; lock do destinatário; INSERT messages; INSERT outbox; COMMIT ──► 202
                        (pending se o destinatário está livre, queued se já tem uma na frente)
                                        │
             N workers ◄── claim da cabeça pronta: FOR UPDATE SKIP LOCKED + lease ──┘
                 │
                 ├─ envia ao provedor com Idempotency-Key
                 ├─ 2xx                      → sent, a próxima queued do destinatário sobe
                 ├─ 408/425/429/5xx/rede     → pending, backoff com jitter (Retry-After como piso)
                 └─ outros 4xx ou sem tentativa → dead, a próxima sobe

             reaper: sending com lease vencido → pending com backoff
                     (na última tentativa: dead, com "resultado incerto" no motivo)
```

### Ver rodando

```bash
DEMO_ADMIN=1 docker compose up -d --build
```

Abra http://localhost:5180, clique em **Enfileirar 20 mensagens** e derrube um worker no meio do envio. A linha fica parada em `sending`, o reaper devolve depois do lease e outro worker entrega. As portas só abrem em `127.0.0.1`.

## Garantias

1. **Entrada idempotente.** `POST /v1/messages` exige `Idempotency-Key`. A mensagem e a linha da outbox são gravadas na mesma transação. Repetir a chave devolve `200` com o mesmo id; a mesma chave com outro conteúdo devolve `409`.
2. **Claim sem espera.** Cada worker reivindica uma linha com `SELECT ... FOR UPDATE SKIP LOCKED`, marca `sending` e grava um `lease_until` e um `lease_token` novo. Linha travada por outro é pulada, não esperada.
3. **Ordem por destinatário, a de commit.** Por destinatário só uma mensagem fica de cabeça (`pending` ou `sending`); as outras esperam em `queued` e sobem uma a uma quando a cabeça vira `sent` ou `dead`. Quem decide isso (o `Enqueue` e a promoção) passa por um `pg_advisory_xact_lock` do destinatário, então o id sai na ordem de commit mesmo com produtores concorrentes. Um índice único parcial garante no banco uma cabeça por destinatário. Uma mensagem `dead` libera o destinatário: escolhi não travar a conversa pra sempre.
4. **Lease, reaper e fencing.** Se o worker morre, a linha fica em `sending` até o lease vencer; o reaper devolve com o mesmo backoff de uma falha. Na última tentativa ela vira `dead` com o motivo "resultado incerto", pq o provedor pode ter recebido antes do worker morrer. Toda gravação exige o `lease_token` vigente, então um worker que "acorda" depois de perder o lease não sobrescreve nada. O gateway original não tinha reaper; essa é a melhoria desta versão.
5. **Pelo menos uma vez, não exatamente uma.** A chave de idempotência vai no header pro provedor descartar reenvios. Sem esse apoio do provedor, um crash entre o envio e o commit gera duplicata. Não há como evitar isso só do lado de quem envia.
6. **Deploy não mata mensagem.** No `SIGTERM` o envio em voo termina (é cortado só em metade do lease) e a linha é finalizada; se o shutdown chega entre o claim e o envio, ela volta pra fila sem gastar tentativa. As requisições HTTP em voo terminam antes de o pool fechar.

## Arquitetura

Hexagonal, sem cerimônia: o domínio no meio, os casos de uso em volta e tudo q fala com o mundo de fora (Postgres, HTTP, provedor, SSE, o próprio loop dos workers) vira adapter.

```
   adapters de entrada                                    adapters de saída
   httpapi (REST) ──┐                                 ┌──► postgres (OutboxRepository, EventSource)
   sse ─────────────┼──► app (casos de uso) ──portas──┼──► sse.Hub (EventSource: um LISTEN por processo)
   worker (loop) ───┘            │                    ├──► provider (Sender HTTP)
                                 ▼                    └──► worker.Fleet (WorkerPool, só na demo)
                              domain
```

- `internal/domain`: estados e transições (`queued → pending → sending → sent | dead | pending`), `OutboxEntry` com `Complete`, `Fail`, `Expire` (a decisão do reaper) e `Release` (o shutdown), `LeaseToken` como fencing token, `RetryPolicy` (backoff com jitter, limite de tentativas, `Retry-After` como piso), validação da mensagem e a regra de ordem do lado de quem recebe. Go puro, sem tag JSON e sem palavra de HTTP.
- `internal/app`: os casos de uso (`EnqueueMessage`, `ClaimNext`, `Deliver`, `ReapExpiredLeases`, `RequeueDead`, `StreamEvents`, `KillWorker`), os modelos de leitura (`MessageState`, `Change`), os contadores de entrega e as portas q eles usam (`OutboxRepository`, `Sender`, `EventSource`, `WorkerPool`).
- `internal/adapters`: `postgres`, `provider`, `httpapi`, `sse`, `worker` e o `mockprovider`.

A regra de dependência é teste, não combinado: `TestDependencyRule` pega o fecho transitivo de cada pacote com `go list -deps` e confere contra uma lista do q cada um pode usar. Domínio e casos de uso não chegam em pgx, rede, banco nem processo nem de longe; cada adapter tem a sua lista (o `httpapi` usar o `sse` está escrito lá como decisão) e pacote novo sem regra falha. `TestDependencyRuleCatchesViolations` prova q a regra pega dependência indireta, subpacote e adapter conhecendo outro. No painel quem faz isso é o `no-restricted-imports` do ESLint.

**Por que as invariantes de concorrência moram no adapter postgres.** O domínio decide o quê: qual a próxima transição, se é retry ou dead, quanto esperar, inclusive pro reaper. Quem garante o quando é o banco. O `FOR UPDATE SKIP LOCKED`, o lock do destinatário, o índice único da cabeça e o `WHERE lease_token = $2` do fencing só valem se forem atômicos com a escrita, e só o Postgres enxerga todos os workers ao mesmo tempo. Trazer isso pra memória viraria ler, decidir e gravar em passos separados, com corrida no meio. Pelo mesmo motivo não tem porta de `Clock`: lease e reaper usam o `now()` do Postgres de propósito, pra não depender do relógio de cada worker. Os estados vão como literal no SQL (índice parcial só casa com constante) e `TestSQLStatusesMatchDomain` amarra os literais aos do domínio.

## Garantias e testes

Testes de integração contra Postgres real (`internal/adapters/`, `cmd/`) e unitários no domínio, nos casos de uso e no hub do SSE:

| Garantia | Teste |
|---|---|
| Sem perda, sem duplicação, ordem preservada com 40 produtores concorrentes, 8 workers e 20% de falhas do provedor | `TestDeliveryNoLossNoDuplicatesInOrder` |
| 10 produtores ao mesmo tempo no mesmo destinatário: nada trava, nada duplica, o provedor recebe na ordem de commit | `TestConcurrentProducersSameRecipient` |
| Só a cabeça do destinatário sai; cabeça em backoff segura só ela; `dead` promove a próxima; o banco recusa duas cabeças | `TestOnlyHeadOfRecipientIsClaimable`, `TestHeadInBackoffHoldsOnlyItsRecipient`, `TestDeadPromotesNextOfRecipient`, `TestDBRefusesTwoHeadsPerRecipient` |
| Linha travada por outra transação é pulada, não esperada | `TestClaimSkipsLockedRows` |
| Worker morto depois de o provedor receber: reaper devolve, outro worker reenvia, provedor descarta o reenvio e a mensagem seguinte do destinatário só sai depois | `TestReaperRecoversWorkerKilledMidSend` |
| Reaper com a regra do domínio: backoff antes da última tentativa, `dead` com resultado incerto nela | `TestReapAppliesDomainRule`, `TestExpire` |
| Worker com lease vencido não finaliza a linha; a outra tentativa entrega | `TestStaleLeaseCannotFinish`, `TestWorkerThatLostLeaseDoesNotFinish` |
| `SIGTERM` com envio em voo termina `sent` mesmo na última tentativa; shutdown antes do envio devolve sem gastar tentativa; envio cortado em metade do lease | `TestFleetShutdownMidSendDelivers`, `TestShutdownDoesNotCutSend`, `TestShutdownBeforeSendReleases`, `TestDeliverDeadlines` |
| Shutdown espera a requisição HTTP em voo antes de fechar o pool | `TestRunWaitsInFlightRequests` |
| 4 réplicas subindo juntas migram sem erro, cada versão uma vez; banco de antes das migrações versionadas sobe | `TestMigrateConcurrentReplicas`, `TestMigrateUpgradesPreVersioningDB` |
| Erro 4xx vai para `dead` sem retry; 408/425/429/5xx tentam de novo; `Retry-After` vira piso | `TestPermanentErrorGoesDead`, `TestSenderClassifiesResponses`, `TestRetryAfter` |
| Idempotência na entrada (store e HTTP), inclusive a mesma chave com outro destinatário | `TestEnqueueIsIdempotent`, `TestCreateMessage` |
| Entrada inválida é erro do cliente: corpo grande (413), campo desconhecido, lixo depois do JSON, NUL e UTF-8 inválido | `TestCreateMessage`, `TestNewMessage` |
| Config errada falha no boot, com todos os erros juntos | `TestLoadConfigFailsFast` |
| Mensagem e outbox entram juntas ou não entram; se a promoção falhar, o `sent` da cabeça também não fica | `TestEnqueueIsAtomic`, `TestFinishRollsBackWhenPromoteFails` |
| Banco fora do ar: store devolve o erro, workers logam com backoff e seguem tentando, API responde 500 sem vazar detalhe, `/readyz` responde 503 | `TestStoreSurfacesDBErrors`, `TestWorkersSurviveDBOutage`, `TestClaimErrorsBackOff`, `TestReadEndpointsAndErrors`, `TestReadyz` |
| Um `LISTEN` só por processo, teto de streams com 503, cliente lento derrubado sem travar os outros | `TestHubFansOutOneListen`, `TestHubCapsStreams`, `TestHubDropsSlowStream`, `TestEventsStream` |
| Toda mudança de estado vira evento, inclusive a do reaper, com versão crescente | `TestListenStreamsStateChanges` |
| `/metrics` com fila por estado, idade da mais antiga e entregas por resultado; log de acesso com request id | `TestMetrics`, `TestDeliveryCounters`, `TestAccessLogWithRequestID` |
| Redrive de `dead` e rotas de admin só com `DEMO_ADMIN=1` | `TestAdminRequeueDead`, `TestRequeueDead`, `TestAdminDisabledByDefault`, `TestRunDemoAdminFlagAndSSEShutdown` |
| Regra de dependência entre as camadas | `TestDependencyRule`, `TestDependencyRuleCatchesViolations` |
| Painel: evento atrasado não volta o estado pra trás, janela das 500 mais novas, reconexão do SSE com backoff | `board.test.ts`, `message.test.ts`, `sse.test.ts` (Vitest) |
| No navegador, contra a stack de verdade: worker derrubado no meio do envio volta pelo reaper e é entregue; gateway parado e religado, o painel volta pra "ao vivo" sozinho | `e2e/demo.spec.ts` (Playwright) |

Cobertura de 100% das instruções (fora o helper `internal/testdb`), com gate no CI. O painel tem o mesmo gate no Vitest (instruções, ramos, funções e linhas), fora o `main.tsx`, q só liga os adapters reais e é exercitado pelo E2E. Os ramos de erro do banco rodam contra Postgres de verdade, sem mock: pool fechado pra simular banco fora e trigger no schema do teste pra derrubar um comando específico no meio da transação. Cada teste usa um schema só dele, apagado no fim, então duas execuções no mesmo banco não se atrapalham.

Cobertura é de Go, não de SQL: um `WHERE` inteiro conta como uma linha. Por isso testei 23 mutantes à mão, trocando um trecho por vez e rodando a suíte inteira, e todos caíram. Os q a revisão achou sobrevivendo na versão anterior, e quem pega cada um agora:

| Mutante | Pego por |
|---|---|
| claim ignora o `next_attempt_at` (backoff) | `TestHeadInBackoffHoldsOnlyItsRecipient`, `TestReapAppliesDomainRule` |
| `Finish` grava backoff zero | `TestHeadInBackoffHoldsOnlyItsRecipient`, `TestReapAppliesDomainRule` |
| a regra de ordem (promoção pega a mais nova, cabeça q só olha `sending`) | `TestDeadPromotesNextOfRecipient`, `TestConcurrentProducersSameRecipient`, `TestDBRefusesTwoHeadsPerRecipient` |
| reaper nunca manda pra `dead` | `TestReapAppliesDomainRule`, `TestExpire` |
| envio cortado em `Lease*2` em vez de `Lease/2` | `TestDeliverDeadlines` |
| envio ou finalização presos ao ctx do processo (o `SIGTERM`) | `TestShutdownDoesNotCutSend`, `TestFleetShutdownMidSendDelivers` |
| sem `SKIP LOCKED` | `TestClaimSkipsLockedRows` |
| 429 tratado como permanente | `TestSenderClassifiesResponses` |
| sem o índice único da cabeça | `TestDBRefusesTwoHeadsPerRecipient` |
| `sent_at` nunca gravado | `TestOnlyHeadOfRecipientIsClaimable` |
| conflito de chave ignora o destinatário | `TestCreateMessage`, `TestEnqueueIsIdempotent` |
| sem `MaxBytesReader` | `TestCreateMessage` |

Os outros: lock do destinatário virando no-op (`TestConcurrentProducersSameRecipient`), migração sem advisory lock (`TestMigrateConcurrentReplicas`), hub q não derruba cliente lento (`TestHubDropsSlowStream` trava até o timeout), `Finish` sem fencing, reaper ignorando `lease_until`, `Exhausted` ignorado no `Fail`, release gastando a tentativa, `Retry-After` ignorado, `dead` sem promover a próxima e o claim fora da ordem de prontidão.

## Números do teste de carga

Medidos com k6 1.3.0, tudo na mesma máquina: AMD Ryzen 5 5600GT (6 núcleos/12 threads), ~11 GiB de RAM, Docker 29, Postgres 17 sem tuning. Mock: 80 ms + 0 a 120 ms de latência e 5% de respostas 503. Lease de 10 s, reaper a cada 2 s, 1.000 destinatários. Todas as rodadas são do commit `f9b2b53`, com o trigger de NOTIFY ligado, e saíram do `load/run.sh`; a saída bruta, com o commit, a config e o load average de antes no cabeçalho, está em [`load/results/`](load/results/) (de `05` em diante; `01` a `04` são da primeira versão, de antes do trigger e das correções).

| Cenário (60 s de carga) | Aceitas | Sem 202 (erro ou não disparadas pelo k6) | Entregues | Reenvios descartados pelo provedor | Violações de ordem | Ponta a ponta p50 / p95 / p99 |
|---|---:|---:|---:|---:|---:|---|
| 100 req/s, 16 workers | 6.000 | 0 | 6.000 | 0 | 0 | 163 / 248 / 530 ms |
| 300 req/s, 64 workers | 18.001 | 0 | 18.001 | 0 | 0 | 151 / 214 / 506 ms |
| 300 req/s, 64 workers, outra rodada com a máquina mais ocupada | 17.989 | 11 | 17.989 | 0 | 0 | 2.925 / 7.197 / 9.923 ms |
| 100 req/s, 16 workers, `kill -9` no gateway aos 20 s, religado aos 25 s | 5.593 | 22 + 387 | 5.593 | 14 | 0 | 601 / 1.805 / 3.429 ms |
| Mesmo crash com 32 workers | 5.597 | 20 + 384 | 5.597 | 16 | 0 | 157 / 634 / 888 ms |

"Ponta a ponta" vai da gravação na API até o `sent` no banco, ou seja, inclui fila, retries e a latência do mock. As retentativas por 503 aparecem em `load/results/` como "tentativas extras".

O que os números mostram:

- **Nenhuma perda e nenhuma violação de ordem em nenhuma rodada**, inclusive com `kill -9`; o `report.sh` sai com erro se uma das duas acontecer. No crash com 32 workers, 16 linhas estavam em voo: voltaram pelo reaper depois do lease e as 16 já tinham chegado ao provedor, q descartou o reenvio pela chave. É o *at-least-once* aparecendo, com a deduplicação do provedor fechando a conta.
- **Requisição sem 202 na queda não é perda.** Sem 202 o cliente não recebeu promessa nenhuma e reenvia com a mesma chave. O k6 não reenvia, só conta: erro de conexão com o gateway fora e iterações q ele nem disparou pq as VUs ficaram presas.
- **16 workers deixam pouca folga.** Cada envio leva em média 80 + 60 ms (a média do jitter), uns 140 ms, então 16 workers dão uns 114 msg/s de capacidade pra 100 req/s de chegada: ρ ≈ 0,88. Com a fila trabalhando perto do limite, qualquer perturbação demora pra drenar, e por isso o crash aparece no p50 com 16 workers e quase some com 32. Na primeira versão deste README eu disse q não tinha investigado por que a fila não drenava; a conta é essa.
- **A 300 req/s o sistema está no limite.** Com a máquina livre a entrega acompanha a chegada (~298 msg/s, p95 de 214 ms). Numa rodada com a máquina mais ocupada a vazão caiu pra 269 msg/s e a fila cresceu até o fim do teste. O gargalo segue sendo o Postgres: cada mensagem custa três commits (entrada, claim e finalização). Antes das correções esse cenário deu 172 / 1.864 / 2.013 ms, 288 msg/s e 187 iterações descartadas.
- **Claim com backlog grande.** Com 100 mil linhas na fila e a cabeça de cada um dos 1.000 destinatários em backoff (o provedor fora do ar), o claim antigo fazia um anti-join por linha: 99.000 sondagens, 398 mil buffers e ~350 ms por claim só pra descobrir q não tinha nada, com 16 workers repetindo isso a cada 50 ms. Com só a cabeça em `pending` é um range no índice: 2 buffers e 0,06 ms. Os dois planos estão em [`load/results/claim-100k-backlog.txt`](load/results/claim-100k-backlog.txt).
- **A máquina não é dedicada** (é a minha máquina de trabalho, rodando outras coisas), então cada arquivo grava o load average de antes. Pra separar ruído de regressão rodei o código de antes das correções e o novo alternando, 100 req/s com 16 workers: antes 572 / 1.517 / 1.639 e 165 / 323 / 537 ms, novo 163 / 248 / 534 e 183 / 648 / 996 ms ([`ab-antes-vs-novo-100rps.txt`](load/results/ab-antes-vs-novo-100rps.txt)). A diferença entre duas rodadas do mesmo código é maior q entre os dois códigos.

## Trade-offs: Postgres como fila vs. broker

**A favor:** a mensagem e a intenção de enviá-la são gravadas atomicamente junto do dado de negócio, sem *dual write*; uma peça a menos para operar; status consultável com SQL; ordem por destinatário expressa no próprio schema.

**Contra:** três commits por mensagem põem o teto no WAL; os workers fazem polling (50 ms) em vez de receber push; a tabela cresce e gera trabalho de vacuum; o lock do destinatário serializa os produtores de um mesmo destinatário.

**Quando trocar:** carga sustentada de milhares de mensagens por segundo, ou vários consumidores do mesmo evento. Aí a outbox continua útil como ponte (um *relay* publica no Kafka/NATS/RabbitMQ), mas deixa de ser a fila de trabalho.

## Limites conhecidos

- **Head-of-line blocking:** uma mensagem em backoff segura as seguintes do mesmo destinatário. Uma mensagem `dead` libera o destinatário, e o redrive dela volta na frente da fila dele, depois de mensagens mais novas já terem saído.
- A ordem garantida é a de commit. Requisições concorrentes para o mesmo destinatário saem na ordem em q commitaram, q não é necessariamente a ordem em q o cliente mandou.
- Linhas `sent` nunca são apagadas. Em produção, isso pede particionamento por data ou arquivamento.
- Os workers fazem polling (o `LISTEN/NOTIFY` só alimenta o painel) e não tem rate limit por provedor; um 429 é retry com o `Retry-After` como piso.
- O lease precisa ser maior que o envio mais lento; o envio é cortado em metade do lease. O relógio usado é o do Postgres (`now()`), não o dos workers.
- O mock guarda tudo em memória e não persiste nada.

## Operação

- `GET /healthz` diz só q o processo está de pé; `GET /readyz` faz ping no banco e responde 503 sem ele, pra o orquestrador tirar o tráfego.
- `GET /metrics` no formato de texto do Prometheus, escrito à mão (são meia dúzia de séries): `outbox_messages{status}` sem as `sent`, `outbox_oldest_unsent_seconds` (a idade da mais antiga ainda não entregue, o SLI de uma outbox), `outbox_db_up`, `outbox_deliveries_total{result="sent|retry|dead|lease_lost|released"}` e `outbox_reaped_total`. A fila vem do banco (igual em todas as réplicas, use `max`); os contadores são do processo (some as réplicas).
- Log JSON pelo `slog`, com uma linha por requisição (método, rota, status, duração e `request_id`, q vem do `X-Request-Id` do cliente ou é um novo aleatório, e volta no header). Erro interno sai com o mesmo id.
- Config por ambiente, validada no boot: `WORKERS` (1 a 1024), `LEASE`, `POLL_INTERVAL`, `REAP_INTERVAL`, `MAX_ATTEMPTS`, `BACKOFF_BASE`, `BACKOFF_MAX` (até 24 h), `SSE_MAX_STREAMS`, `PROVIDER_URL` (http ou https), `DATABASE_URL`, `ADDR`, `DEMO_ADMIN`. Valor errado não cai no padrão em silêncio: o processo sai com todos os erros de uma vez.
- Migrações versionadas em `internal/adapters/postgres/migrations`, aplicadas no boot numa transação atrás de um advisory lock: réplicas subindo juntas esperam a primeira.
- Redrive de uma `dead` (com `DEMO_ADMIN=1`): `curl -X POST localhost:8080/v1/admin/messages/42/requeue`. Ela volta zerada, como cabeça se o destinatário estiver livre ou na frente da fila dele.

## Painel em tempo real

`dashboard/` é um painel em Vite, React 19 e TypeScript strict q mostra as mensagens andando entre os estados ao vivo: raias por destinatário (dá pra ver a ordem sendo respeitada), contadores por estado nas últimas 500 mensagens, a lista de workers com botão de derrubar e um aviso quando o reaper devolve uma linha. Segue a mesma divisão do Go: `domain` (estado da mensagem e contadores), `application` (casos de uso sobre as portas `GatewayApi` e `GatewayEvents`) e `adapters` (fetch, `EventSource` e a tela em React).

Como funciona:

1. Um trigger `AFTER INSERT OR UPDATE` na `outbox` faz `pg_notify` com id, destinatário, status, tentativas, worker, versão e último erro. Como o reaper é só mais um `UPDATE`, a devolução dele aparece igual a qualquer outra mudança.
2. O gateway abre um `LISTEN` só por processo (o `sse.Hub`), quando chega o primeiro stream, e larga quando sai o último. Cada `GET /v1/events` recebe as mudanças por um buffer próprio; cliente q não lê a tempo é derrubado em vez de segurar os outros, e acima de `SSE_MAX_STREAMS` o stream recebe 503. Ping a cada 15 s pra proxy não derrubar a conexão parada; desligar o gateway fecha todos os streams.
3. O painel busca a foto em `GET /v1/messages` (as 500 mais novas) com TanStack Query e aplica os eventos do `EventSource` por cima, guardando só as 500 mais novas. Toda vez q o SSE conecta ou reconecta ele busca a foto de novo, pq o NOTIFY não guarda nada. Se a resposta não for 200 (502 do nginx com o gateway fora, 503 acima do teto) o `EventSource` desiste de vez; aí o próprio adapter abre outro, com backoff de 1 a 30 s e jitter.
4. Foto e evento chegam em qualquer ordem. Um trigger `BEFORE UPDATE` soma 1 na coluna `version` a cada mudança, e o painel só aplica o q tiver versão maior q a q já tem. Antes eu ordenava por `attempts * 2 + fase`, sem coluna nova, mas o release do shutdown e o redrive voltam o `attempts`, e aí a conta deixa de só crescer.

**Por que LISTEN/NOTIFY e não polling.** Polling por `updated_at` ou por coluna de versão perde evento: duas transações podem gravar numa ordem e commitar na outra, e o cursor passa por cima da q commitou depois. O NOTIFY só sai no commit, na ordem dos commits, e pega qualquer escritor (worker, reaper, até um `UPDATE` na mão). O custo:

- Não é durável: quem não estava ouvindo perde o evento. Por isso a foto é recarregada a cada reconexão.
- Todo commit q notifica passa por um lock global da fila de notificação. Os números de carga acima já são com o trigger.
- Payload de no máximo 8000 bytes, por isso o `last_error` vai cortado em 200 caracteres.

**Derrube um worker de verdade.** Com `DEMO_ADMIN=1` o gateway liga `GET /v1/admin/workers`, `POST /v1/admin/workers/{id}/kill`, `POST /v1/admin/workers/revive` e o redrive. O kill cancela o contexto daquele worker com uma causa própria: se ele estiver enviando, o envio é cortado e a linha não é finalizada. Ela fica em `sending` com o id do worker até o lease vencer e o reaper devolver, e no painel dá pra ver a linha parada, o aviso "devolvida pelo reaper" e outro worker entregando. O botão "Enfileirar 20 mensagens" manda uma rajada pra 4 destinatários pela própria API, uma requisição por vez pra ordem de commit ser a ordem de envio.

Essas rotas vêm desligadas e **nunca podem ser ligadas em produção**: não têm autenticação. No compose as portas só abrem em `127.0.0.1`, então nem a rede local alcança. Aliás `GET /v1/events` e `GET /v1/messages` mostram o destinatário de todas as mensagens, igual ao `GET /v1/messages/{id}`; num gateway de verdade tudo isso fica atrás de autenticação.

Acessibilidade: os avisos (reaper, dead, ações) ficam num `role="log"` com `aria-live`, sem anunciar cada `sent` pra não afogar o leitor de tela; cada mensagem tem nome acessível com estado, tentativa e worker; o estado nunca é só cor; tudo é botão nativo e funciona no teclado; tema claro e escuro pelo `prefers-color-scheme`; o grid cabe em 375 px.

## Como rodar

Testes (precisam de Postgres; sem `DATABASE_URL` eles pulam, menos no CI, onde falham):

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

Lint e vulnerabilidades, como no CI:

```bash
golangci-lint run ./...
go run golang.org/x/vuln/cmd/govulncheck@v1.5.0 ./...
```

Teste de carga: `load/run.sh` sobe a stack do zero, roda o k6, espera a fila esvaziar e grava o resultado em `load/results/` com o commit e a config no cabeçalho. Sai com 1 se perder mensagem ou trocar a ordem. Com `KILL_AT` ele dá `SIGKILL` no gateway e religa 5 s depois.

```bash
RATE=100 WORKERS=16 ./load/run.sh 05-100rps-16workers
RATE=100 WORKERS=16 KILL_AT=20 ./load/run.sh 07-100rps-kill9-gateway-16workers
```

Variáveis do mock: `MOCK_LATENCY`, `MOCK_JITTER`, `MOCK_FAIL_RATE`. Do Postgres no compose: `PG_PORT`, se a 5432 já estiver ocupada.

Chamada manual:

```bash
curl -i -X POST localhost:8080/v1/messages \
  -H 'Idempotency-Key: pedido-42' \
  -d '{"recipient":"5511999990000","body":"Seu pedido saiu para entrega"}'
curl localhost:8080/v1/messages/1
```

Painel em modo dev, com o gateway rodando em `:8080`:

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
cmd/gateway                    raiz de composição e config: API + workers + reaper num processo
cmd/mockprovider               provedor simulado
internal/domain                estados, transições, lease, retry, validação e ordem (Go puro)
internal/app                   casos de uso, portas, modelos de leitura e contadores
internal/adapters/postgres     migrações, repositório (claim, lease, promoção, reaper) e LISTEN/NOTIFY
internal/adapters/httpapi      handlers HTTP, /readyz, /metrics e log de acesso
internal/adapters/sse          hub de um LISTEN por processo e stream SSE pro painel
internal/adapters/provider     sender HTTP pro provedor
internal/adapters/worker       loop dos workers e do reaper, kill e revive da demo
internal/adapters/mockprovider latência, falhas, deduplicação e checagem de ordem
internal/archtest              regra de dependência entre as camadas
dashboard/                     painel (domain, application, adapters) e E2E
load/                          script k6, run.sh, relatório e resultados brutos
docs/                          screenshot do painel
```
