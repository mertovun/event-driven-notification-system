// k6 baseline load test for notifyd.
// Hammers POST /v1/notifications at a fixed RPS and asserts p99 latency + success rate.
//
// Run: k6 run loadtest/k6_baseline.js
// Env: BASE_URL (default http://localhost:8090), API_KEY (default dev seed)
import http from 'k6/http';
import { check } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8090';
const API_KEY  = __ENV.API_KEY  || 'dev1234-do-not-use-in-prod-xxxxxxxxxx';

export const options = {
  scenarios: {
    baseline: {
      executor: 'constant-arrival-rate',
      rate: 50,           // 50 req/s sustained
      timeUnit: '1s',
      duration: '30s',
      preAllocatedVUs: 20,
      maxVUs: 50,
    },
  },
  thresholds: {
    'http_req_duration{status:201}': ['p(99)<500'],   // create p99 under 500ms
    'http_req_failed': ['rate<0.01'],                  // <1% failures
    'checks{type:created}': ['rate>0.99'],
  },
};

export default function () {
  const url = `${BASE_URL}/v1/notifications`;
  const payload = JSON.stringify({
    channel: 'sms',
    recipient: `+90555${String(Math.floor(Math.random() * 1e7)).padStart(7, '0')}`,
    content: `baseline load test ${__ITER}`,
    priority: 'normal',
  });
  const params = {
    headers: {
      Authorization: `Bearer ${API_KEY}`,
      'Content-Type': 'application/json',
    },
  };
  const res = http.post(url, payload, params);
  check(res, { 'status is 201': (r) => r.status === 201 }, { type: 'created' });
}
