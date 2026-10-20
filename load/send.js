// Carga de entrada: RATE requisições/s durante DURATION.
// Iterações i e i+RECIPIENTS vão para o mesmo destinatário. Com RECIPIENTS=1000
// a 100-300 req/s elas ficam segundos de distância, então a ordem de chegada por
// destinatário é bem definida e o mock consegue verificá-la.
// 1 em cada 50 iterações reenvia a mesma chave para exercitar a idempotência da API.
import http from 'k6/http';
import { check } from 'k6';
import exec from 'k6/execution';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const RUN = __ENV.RUN || `${Date.now()}`;
const RECIPIENTS = Number(__ENV.RECIPIENTS || 1000);

export const options = {
  scenarios: {
    ingest: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.RATE || 200),
      timeUnit: '1s',
      duration: __ENV.DURATION || '60s',
      preAllocatedVUs: 50,
      maxVUs: 200,
    },
  },
  // dropped_iterations > 0 quer dizer q o k6 não conseguiu manter a taxa: o número de
  // req/s da tabela seria mentira
  thresholds: { checks: ['rate>0.99'], dropped_iterations: ['count==0'] },
};

export default function () {
  const i = exec.scenario.iterationInTest;
  const key = `${RUN}-${i}`;
  const recipient = `r-${i % RECIPIENTS}`;
  const params = { headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key } };
  const payload = JSON.stringify({ recipient, body: `msg ${i}` });

  const res = http.post(`${BASE_URL}/v1/messages`, payload, params);
  check(res, { 'aceita (202)': (r) => r.status === 202 });

  if (i % 50 === 0) {
    const again = http.post(`${BASE_URL}/v1/messages`, payload, params);
    check(again, { 'repetição reconhecida (200)': (r) => r.status === 200 });
  }
}
