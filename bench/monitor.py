"""Poll the completion counter and compute drain rate over the middle 80% of
completions (the last 10% carries startup ramp-up, the first 10% carries any
straggler tail -- see RESULTS.md for why naive N/elapsed is unsafe here).
"""

import json
import sys
import time

import redis

from common import DONE_KEY, REDIS_URL


def main():
    n = int(sys.argv[1])
    timeout_s = float(sys.argv[2])
    r = redis.Redis.from_url(REDIS_URL)

    samples = []  # (t, count)
    t_start = time.perf_counter()
    while True:
        raw = r.get(DONE_KEY)
        count = int(raw) if raw else 0
        t = time.perf_counter()
        samples.append((t, count))
        if count >= n:
            break
        if t - t_start > timeout_s:
            break
        time.sleep(0.02)

    final_t, final_count = samples[-1]
    elapsed = final_t - t_start

    lo_target = 0.1 * n
    hi_target = 0.9 * n

    def interp(target):
        for i in range(1, len(samples)):
            t_prev, c_prev = samples[i - 1]
            t_cur, c_cur = samples[i]
            if c_prev <= target <= c_cur:
                if c_cur == c_prev:
                    return t_cur
                frac = (target - c_prev) / (c_cur - c_prev)
                return t_prev + frac * (t_cur - t_prev)
        return None

    t_lo = interp(lo_target)
    t_hi = interp(hi_target)

    result = {
        "n": n,
        "final_count": final_count,
        "elapsed_s": elapsed,
        "timed_out": final_count < n,
        "naive_rate": final_count / elapsed if elapsed > 0 else None,
        "mid80_rate": None,
    }
    if t_lo is not None and t_hi is not None and t_hi > t_lo:
        result["mid80_rate"] = (hi_target - lo_target) / (t_hi - t_lo)

    print(json.dumps(result))


if __name__ == "__main__":
    main()
