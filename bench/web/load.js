// k6 driver. MODE=vus is closed loop saturation (VUS concurrent clients);
// MODE=rate is open loop at RATE requests per second, so a slow server shows
// up as latency and dropped iterations instead of silently lowering the load.
import http from "k6/http";

const scenario =
  __ENV.MODE === "rate"
    ? {
        executor: "constant-arrival-rate",
        rate: Number(__ENV.RATE),
        timeUnit: "1s",
        duration: __ENV.DURATION,
        preAllocatedVUs: 200,
        maxVUs: 2000,
      }
    : { executor: "constant-vus", vus: Number(__ENV.VUS), duration: __ENV.DURATION };

export const options = {
  scenarios: { load: scenario },
  summaryTrendStats: ["p(50)", "p(90)", "p(99)", "max"],
  discardResponseBodies: true,
};

export default function () {
  http.request(__ENV.METHOD, __ENV.URL);
}
