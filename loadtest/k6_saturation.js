// k6 saturation test — ramp from 100 RPS to 1500 RPS in steps.
// Goal: find the new ceiling after the verified-key auth cache landed.
//
// Run: k6 run loadtest/k6_saturation.js
import http from 'k6/http';
import { check } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8090';
const API_KEY  = __ENV.API_KEY  || 'dev1234-do-not-use-in-prod-xxxxxxxxxx';

export const options = {
  scenarios: {
    ramp: {
      executor: 'ramping-arrival-rate',
      startRate: 500,
      timeUnit: '1s',
      stages: [
        { duration: '10s', target: 500 },
        { duration: '10s', target: 1000 },
        { duration: '10s', target: 2000 },
        { duration: '10s', target: 3000 },
        { duration: '10s', target: 4000 },
        { duration: '5s',  target: 0 },
      ],
      preAllocatedVUs: 500,
      maxVUs: 5000,
    },
  },
  thresholds: {
    'http_req_failed': ['rate<0.05'],
    'checks{type:created}': ['rate>0.95'],
  },
};

export default function () {
  const url = `${BASE_URL}/v1/notifications`;
  const payload = JSON.stringify({
    channel: 'sms',
    recipient: `+90555${String(Math.floor(Math.random() * 1e7)).padStart(7, '0')}`,
    content: `sat ${__ITER}`,
  });
  const res = http.post(url, payload, {
    headers: {
      Authorization: `Bearer ${API_KEY}`,
      'Content-Type': 'application/json',
    },
  });
  check(res, { 'status is 201': (r) => r.status === 201 }, { type: 'created' });
}
