#!/bin/sh
# Espera a outbox esvaziar e imprime o resultado do teste de carga:
# o que entrou, o que foi entregue, latência ponta a ponta e contadores do provedor.
set -eu

psql() { docker compose exec -T postgres psql -U outbox -d outbox -At "$@"; }

while [ "$(psql -c "SELECT count(*) FROM outbox WHERE status IN ('pending','sending')")" != "0" ]; do
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

echo "provedor: $(curl -s localhost:9090/stats)"
