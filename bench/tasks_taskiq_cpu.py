"""CPU-bound task, taskiq, sync def -- needs --use-process-pool at run time
for real multicore (taskiq's default thread pool GIL-serializes a busy loop
like this same as any other Python threads). Run with:
taskiq worker tasks_taskiq_cpu:broker --use-process-pool --max-process-pool-processes ...
"""

import redis
from taskiq_redis import ListQueueBroker

from common import DONE_KEY, REDIS_URL
from workloads import cpu_burn

broker = ListQueueBroker(url=REDIS_URL, queue_name="fwbench_cpu")
_r = redis.Redis.from_url(REDIS_URL)


@broker.task
def burn(ms):
    cpu_burn(ms)
    _r.incr(DONE_KEY)
