"""Sum PSS (proportional set size) across all OS processes matching a
pgrep pattern. PSS, not RSS: Celery prefork forks after import, so its
children share copy-on-write interpreter/module pages -- summing RSS across
children counts those shared pages once per child, inflating the total.
PSS divides shared pages among sharers, so it is the fair "real physical
memory cost" number for a many-small-processes stack like Celery, and costs
nothing to also apply to cauli for consistency (cauli's --procs are spawned
fresh, not forked, so cauli's PSS and RSS are already close).

Usage: memory_report.py <pgrep_pattern>
"""

import subprocess
import sys


def pss_kb(pid):
    try:
        with open(f"/proc/{pid}/smaps_rollup") as f:
            for line in f:
                if line.startswith("Pss:"):
                    return int(line.split()[1])
    except (FileNotFoundError, ProcessLookupError):
        return 0
    return 0


def main():
    pattern = sys.argv[1]
    out = subprocess.run(["pgrep", "-f", pattern], capture_output=True, text=True)
    pids = [int(p) for p in out.stdout.split()]
    if not pids:
        print(f"no processes matched {pattern!r}", file=sys.stderr)
        raise SystemExit(1)

    total_kb = sum(pss_kb(pid) for pid in pids)
    print(f"processes: {len(pids)}")
    print(f"total PSS: {total_kb / 1024:.1f} MiB")
    print(f"PSS per process: {total_kb / len(pids) / 1024:.2f} MiB")


if __name__ == "__main__":
    main()
