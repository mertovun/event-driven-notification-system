// k6 idempotency test.
// Sends the same Idempotency-Key with the same body N times; asserts that
// every response (after the first) returns Idempotency-Replayed: true.
// Catches the case where the cache layer breaks under load.
//
// Run: k6 run loadtest/k6_idempotency.js
import http from 'k6/http';
import { check } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8090';
const API_KEY  = __ENV.API_KEY  || 'dev1234-do-not-use-in-prod-xxxxxxxxxx';
const IDEM_KEY = __ENV.IDEM_KEY || 'k6-idem-key-fixed';

export const options = {
  scenarios: {
    replay: {
      executor: 'constant-arrival-rate',
      rate: 20,
      timeUnit: '1s',
      duration: '15s',
      preAllocatedVUs: 10,
      maxVUs: 20,
    },
  },
  thresholds: {
    'checks{type:replayed_or_201}': ['rate>0.99'],
    'http_req_failed': ['rate<0.01'],
  },
};

export default function () {
  const url = `${BASE_URL}/v1/notifications`;
  const payload = JSON.stringify({
    channel: 'sms',
    recipient: '+905551234567',
    content: 'idempotency load test fixed body',
    priority: 'normal',
  });
  const res = http.post(url, payload, {
    headers: {
      Authorization: `Bearer ${API_KEY}`,
      'Content-Type': 'application/json',
      'Idempotency-Key': IDEM_KEY,
    },
  });
  check(res, {
    'replayed or 201': (r) =>
      r.status === 201 && (
        r.headers['Idempotency-Replayed'] === 'true' ||
        __ITER === 0
      ),
  }, { type: 'replayed_or_201' });
}
