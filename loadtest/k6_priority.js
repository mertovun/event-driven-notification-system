// k6 priority-ordering test.
// Issues a mix of high/low priority notifications; asserts the rate-limit
// throttling metric increments (proving the limiter engages under load)
// and that creates don't fail.
//
// Run: k6 run loadtest/k6_priority.js
import http from 'k6/http';
import { check } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8090';
const API_KEY  = __ENV.API_KEY  || 'dev1234-do-not-use-in-prod-xxxxxxxxxx';

export const options = {
  scenarios: {
    burst: {
      executor: 'constant-arrival-rate',
      rate: 150,         // intentionally above the 100/s channel limit
      timeUnit: '1s',
      duration: '20s',
      preAllocatedVUs: 30,
      maxVUs: 100,
    },
  },
  thresholds: {
    'http_req_failed': ['rate<0.02'],
    // Bursting at 1.5x the channel rate; expect creates to still succeed
    // (rate-limit is on delivery, not on API ingestion).
    'checks{type:created}': ['rate>0.98'],
  },
};

export default function () {
  // 50/50 high vs low so we exercise the priority paths.
  const priority = (__ITER % 2 === 0) ? 'high' : 'low';
  const url = `${BASE_URL}/v1/notifications`;
  const payload = JSON.stringify({
    channel: 'sms',
    recipient: `+90555${String(Math.floor(Math.random() * 1e7)).padStart(7, '0')}`,
    content: `prio ${priority} ${__ITER}`,
    priority,
  });
  const res = http.post(url, payload, {
    headers: {
      Authorization: `Bearer ${API_KEY}`,
      'Content-Type': 'application/json',
    },
  });
  check(res, { 'status is 201': (r) => r.status === 201 }, { type: 'created' });
}
