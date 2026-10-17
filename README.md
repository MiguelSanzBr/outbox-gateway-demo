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

## Garantias e testes

Testes de integração contra Postgres real (`internal/outbox/`, `internal/api/`, `cmd/`):

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

Cobertura de 100% das instruções (fora o helper `internal/testdb`), com gate no CI. Os ramos de erro do banco rodam contra Postgres de verdade, sem mock: pool fechado pra simular banco fora e trigger no schema do teste pra derrubar um comando específico no meio da transação.

Para conferir que os testes de ordem não passam por acaso, removi a condição de ordem da query de claim: os dois primeiros testes da tabela falharam.

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
- Sem `LISTEN/NOTIFY` (polling) e sem rate limit por provedor; um 429 é tratado só como retry.
- O lease precisa ser maior que o envio mais lento; o envio é cortado em metade do lease. O relógio usado é o do Postgres (`now()`), não o dos workers.
- O mock guarda tudo em memória e não persiste nada.

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

`report.sh` espera a outbox esvaziar e imprime aceitas, entregues, latência ponta a ponta e os contadores do mock (`duplicates`, `order_violations`). Variáveis do mock: `MOCK_LATENCY`, `MOCK_JITTER`, `MOCK_FAIL_RATE`. Do gateway: `WORKERS`, `LEASE`, `MAX_ATTEMPTS`, `BACKOFF_BASE`, `BACKOFF_MAX`.

Chamada manual:

```bash
curl -i -X POST localhost:8080/v1/messages \
  -H 'Idempotency-Key: pedido-42' \
  -d '{"recipient":"5511999990000","body":"Seu pedido saiu para entrega"}'
curl localhost:8080/v1/messages/1
```

## Estrutura

```
cmd/gateway          API + workers + reaper num processo
cmd/mockprovider     provedor simulado
internal/outbox      schema, store (claim, lease, reaper), workers, sender HTTP
internal/api         handlers HTTP e validação
internal/mockprovider latência, falhas, deduplicação e checagem de ordem
load/                script k6, relatório e resultados brutos
```
