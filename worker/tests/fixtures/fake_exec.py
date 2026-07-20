#!/usr/bin/env python3
"""Standalone stand-in for `python3 -m rupy._exec` (PROTOCOL §5.1).

Used by e2e tests via the documented RUPY_EXEC_CMD env hook so the cpu pool
can be exercised without the real rupy package. Ready line, then one JSON
request per line on stdin, one JSON response per line on stdout. Never crashes
on task exceptions; reports them. Hard timeouts are enforced worker-side
(SIGKILL), which the fx.cpu_slow task exists to provoke.
"""
import json
import os
import sys
import time


def handle(task, args, kwargs):
    if task == "fx.cpu_echo":
        return {"args": args, "kwargs": kwargs, "pid": os.getpid()}
    if task == "fx.cpu_slow":
        time.sleep(float(args[0]) if args else 30.0)
        return "slow-done"
    if task == "fx.cpu_fail":
        raise ValueError("cpu boom")
    raise KeyError("unknown fake task %r" % (task,))


def main():
    # tolerate --app and any other argv
    sys.stdout.write(json.dumps({"ready": True, "pid": os.getpid()}) + "\n")
    sys.stdout.flush()
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
        except Exception:
            continue
        rid = req.get("id")
        try:
            result = handle(req.get("task"), req.get("args") or [], req.get("kwargs") or {})
            json.dumps(result)  # serializability check
            out = {"id": rid, "ok": True, "result": result}
        except BaseException as e:
            out = {"id": rid, "ok": False,
                   "error": {"type": type(e).__name__, "message": str(e), "traceback": ""}}
        sys.stdout.write(json.dumps(out) + "\n")
        sys.stdout.flush()


if __name__ == "__main__":
    main()
