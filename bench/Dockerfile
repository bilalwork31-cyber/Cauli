# Reproduces this benchmark suite in one command. python:3.12-slim: cauli-worker
# needs a CPython built with --enable-shared (README's own install
# requirement), which the official python:* images satisfy.
FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl redis-tools postgresql-client git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /repo
COPY . .

RUN cd worker && cargo build --release --bin cauli-worker
ENV CAULI_WORKER_BIN=/repo/worker/target/release/cauli-worker

RUN pip install --no-cache-dir -e py \
    && pip install --no-cache-dir -r bench/requirements.txt

WORKDIR /repo/bench
CMD ["python3", "campaign.py", "--reps", "3"]
