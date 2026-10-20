#!/bin/sh
# Roda um cenário de carga do zero e grava em load/results/<nome>.txt, com o commit,
# a config e a versão do k6 no cabeçalho. KILL_AT=20 dá SIGKILL no gateway aos 20 s e
# religa 5 s depois.
#
#   RATE=100 WORKERS=16 ./load/run.sh 05-100rps-16workers
#   RATE=100 WORKERS=16 KILL_AT=20 ./load/run.sh 07-100rps-kill9-gateway
set -eu
name=$1
out=load/results/$name.txt
: "${RATE:=100}" "${DURATION:=60s}" "${WORKERS:=16}" "${RECIPIENTS:=1000}"
export RATE DURATION WORKERS RECIPIENTS

docker compose down -v >/dev/null 2>&1
docker compose up -d --build --wait postgres mockprovider gateway >/dev/null 2>&1
timeout 60 sh -c 'until curl -sf localhost:8080/readyz; do sleep 1; done'

dirty=$(git diff --quiet HEAD -- . ':!load/results' || echo ' (com mudança local)')
{
  echo "# $name: RATE=$RATE DURATION=$DURATION WORKERS=$WORKERS RECIPIENTS=$RECIPIENTS LEASE=${LEASE:-10s} KILL_AT=${KILL_AT:-nenhum}"
  echo "# commit $(git rev-parse --short HEAD)$dirty; mock LATENCY=${MOCK_LATENCY:-80ms} JITTER=${MOCK_JITTER:-120ms} FAIL_RATE=${MOCK_FAIL_RATE:-0.05}; $(docker compose run --rm -T k6 version | head -1)"
} > "$out"

if [ -n "${KILL_AT:-}" ]; then
  (sleep "$KILL_AT"; docker compose kill -s KILL gateway >/dev/null 2>&1; sleep 5; docker compose start gateway >/dev/null 2>&1) &
fi
# no cenário de kill os thresholds falham de propósito (requisições recusadas na queda)
docker compose run --rm -T k6 2>&1 |
  grep -E 'ingest:|checks_|✗|✓|↳|http_req_duration\.|http_req_failed|dropped_iterations|iterations\.' | grep -v '\[ *100% \]' >> "$out" || true
wait
./load/report.sh >> "$out" || { cat "$out"; exit 1; }
docker compose logs gateway 2>&1 | grep 'devolvidos' | sed -E 's/^gateway-[0-9]+ *\| //' >> "$out" || true
cat "$out"
