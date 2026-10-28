"""python -m bench <benchmark>: run one benchmark and print its table."""

import argparse

from bench import chaos, drain, memory, web

BENCHMARKS = {
    "dispatch": (
        drain.dispatch,
        drain.add_arguments,
        "no-op drain rate against Celery, taskiq, Dramatiq and arq",
    ),
    "cpu": (
        drain.cpu,
        drain.add_arguments,
        "CPU bound task sizes against Celery prefork",
    ),
    "free-threaded": (
        drain.free_threaded,
        drain.add_arguments,
        "GIL build against free threaded build",
    ),
    "memory": (memory.run, memory.add_arguments, "PSS per in flight task"),
    "chaos": (
        chaos.run,
        chaos.add_arguments,
        "kill -9 loss and duplicates, segfault blast radius",
    ),
    "web": (
        web.run,
        web.add_arguments,
        "enqueue inside Django and FastAPI requests under k6",
    ),
}


def main() -> None:
    parser = argparse.ArgumentParser(prog="python -m bench", description=__doc__)
    commands = parser.add_subparsers(dest="benchmark", required=True)
    for name, (run, add_arguments, summary) in BENCHMARKS.items():
        sub = commands.add_parser(name, help=summary, description=summary)
        add_arguments(sub)
        sub.set_defaults(run=run)
    args = parser.parse_args()
    args.run(args)


main()
