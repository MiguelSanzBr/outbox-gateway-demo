#!/bin/sh
# Espera a outbox esvaziar e imprime o resultado do teste de carga: o que entrou, o que
# foi entregue, latência ponta a ponta e contadores do provedor. Sai com 1 se perdeu
# mensagem (aceita q não virou sent nem dead, ou sem linha na outbox) ou se o provedor
# viu ordem trocada. Reenvio descartado pelo provedor não é falha: é o at-least-once.
set -eu

psql() { docker compose exec -T postgres psql -U outbox -d outbox -At "$@"; }

timeout=${DRAIN_TIMEOUT:-300}
while [ "$(psql -c "SELECT count(*) FROM outbox WHERE status IN ('queued','pending','sending')")" != "0" ]; do
  timeout=$((timeout - 1))
  [ "$timeout" -gt 0 ] || { echo "a outbox não esvaziou em ${DRAIN_TIMEOUT:-300} s"; exit 1; }
  sleep 1
done

psql -F ' | ' <<'SQL'
SELECT 'mensagens aceitas',   count(*) FROM messages;
SELECT 'entregues (sent)',    count(*) FROM outbox WHERE status = 'sent';
SELECT 'dead',                count(*) FROM outbox WHERE status = 'dead';
SELECT 'sem linha na outbox', count(*) FROM messages m LEFT JOIN outbox o ON o.message_id = m.id WHERE o.id IS NULL;
SELECT 'tentativas extras',   sum(attempts - 1) FROM outbox;
SELECT 'janela de entrega (s)', round(extract(epoch FROM max(o.sent_at) - min(m.created_at))::numeric, 1)
  FROM outbox o JOIN messages m ON m.id = o.message_id;
SELECT 'vazão de entrega (msg/s)', round((count(*) / extract(epoch FROM max(o.sent_at) - min(m.created_at)))::numeric, 1)
  FROM outbox o JOIN messages m ON m.id = o.message_id WHERE o.status = 'sent';
SELECT 'ponta a ponta p50/p95/p99 (ms)',
  array_to_string(array(SELECT round(x::numeric) FROM unnest(
    percentile_cont(ARRAY[0.5, 0.95, 0.99]) WITHIN GROUP (ORDER BY extract(epoch FROM o.sent_at - m.created_at) * 1000)) x), ' / ')
  FROM outbox o JOIN messages m ON m.id = o.message_id WHERE o.status = 'sent';
SQL

stats=$(curl -s localhost:9090/stats)
echo "provedor: $stats"

lost=$(psql -c "SELECT (SELECT count(*) FROM messages) - (SELECT count(*) FROM outbox WHERE status IN ('sent','dead'))
  + (SELECT count(*) FROM messages m LEFT JOIN outbox o ON o.message_id = m.id WHERE o.id IS NULL)")
violations=$(echo "$stats" | sed -n 's/.*"order_violations":\([0-9]*\).*/\1/p')
[ "$lost" = "0" ] || { echo "FALHOU: $lost mensagem(ns) perdida(s)"; exit 1; }
[ "$violations" = "0" ] || { echo "FALHOU: $violations violação(ões) de ordem"; exit 1; }
echo "ok: nada perdido, ordem preservada"
