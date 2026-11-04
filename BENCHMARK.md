# Single-node key/value benchmark — 2026-11-04

Measured on a Fedora desktop with an Intel Core i9-12900KF; other desktop work
was not suspended. These are local closed-loop microbenchmarks, not a claim
about production or general Redis performance.

## Method

* Baseline: original key-val commit `71163ef`, compiled with `gcc -O3 -DNDEBUG
  -pthread`. Refactored server: the same optimization level, default four I/O
  workers, 64 store shards. This separates code improvements from the previous
  default unoptimized build.
* Redis 8.2.1, built from the official release with `MALLOC=libc OPT=-O3`, TLS
  disabled. A single process, `--io-threads 1`, persistence disabled, loopback
  TCP. Redis requests use RESP2, not the slower inline command parser.
* Same C client engine, 64-byte values, 9-byte keys, a preloaded 10,000-key
  working set, uniform deterministic key selection, and 31,536,000-second TTLs
  on writes for both servers (`SET ... EX` / `write ... ttl`). All reads hit;
  mixed workload is approximately 50% reads and 50% overwrites. These SET
  numbers include expiry bookkeeping; they are not plain no-expiry Redis SET.
* Three repetitions with target order rotated. Connections and command buffers
  are prepared before timing. Each case is warmed up with 10,000 operations;
  measured cases use 200,000 operations without pipelining and 5,000,000 with
  pipeline depth 32. Each target starts fresh for each repetition and receives
  the same prefill. The same sequence is used across targets.
* Each server gets either one CPU or the same four-CPU affinity allowance;
  clients get eight separate CPUs. Affinity is not exclusive CPU isolation.
  Redis retains its single command-execution thread in both configurations;
  the four-CPU comparison shows key-val's parallel execution advantage too.
* Every response is checked, including returned values. All runs below had
  zero response errors. Throughput is completed operations divided by elapsed
  wall time. Tables show the median of three complete runs.
* Raw JSON includes batch p50/p95/p99 in microseconds. At pipeline depth 1,
  these represent individual request latency. At depth 32, they measure full
  batch completion, not per-command latency. This closed-loop test does not
  measure open-loop tail latency under an independently arriving workload.

Redis's [benchmark guidance](https://redis.io/docs/latest/operate/oss_and_stack/management/optimization/benchmarks/)
discusses client bottlenecks, pipelining, and matching the benchmark to the
application. This intentionally small test does not cover larger datasets,
misses, large values, deletion-heavy workloads, persistence, replication,
long-running expiry pressure, memory efficiency, or networked deployment.

## 1-CPU server allowance

Server CPU IDs: `[0]`; client CPU IDs: `[1, 2, 3, 4, 5, 6, 7, 8]`.

| Clients | Pipeline | Workload | Original ops/s | Refactored ops/s | Redis ops/s | Refactored/original | Refactored/Redis |
|---:|---:|:---|---:|---:|---:|---:|---:|
| 1 | 1 | SET | 129,815 | 138,145 | 122,145 | 1.06× | 1.13× |
| 1 | 1 | GET | 127,130 | 133,669 | 127,399 | 1.05× | 1.05× |
| 1 | 1 | MIXED | 128,106 | 136,375 | 123,644 | 1.06× | 1.10× |
| 8 | 1 | SET | 262,771 | 287,917 | 265,260 | 1.10× | 1.09× |
| 8 | 1 | GET | 253,372 | 263,571 | 286,419 | 1.04× | 0.92× |
| 8 | 1 | MIXED | 258,009 | 264,018 | 272,199 | 1.02× | 0.97× |
| 8 | 32 | SET | 2,813,478 | 4,531,817 | 1,777,173 | 1.61× | 2.55× |
| 8 | 32 | GET | 2,609,998 | 3,938,961 | 3,137,141 | 1.51× | 1.26× |
| 8 | 32 | MIXED | 2,666,477 | 3,999,381 | 2,150,737 | 1.50× | 1.86× |

Median of per-run p99 batch completion latencies for eight clients (µs):

| Pipeline | Workload | Original | Refactored | Redis |
|---:|:---|---:|---:|---:|
| 1 | SET | 636.99 | 516.62 | 73.11 |
| 1 | GET | 647.54 | 166.70 | 66.58 |
| 1 | MIXED | 646.18 | 296.38 | 70.58 |
| 32 | SET | 3126.04 | 888.04 | 302.24 |
| 32 | GET | 417.89 | 202.87 | 148.02 |
| 32 | MIXED | 3133.81 | 325.00 | 254.62 |

[Raw samples](benchmarks/2026-11-04-1-cpu.json).

## 4-CPU server allowance

Server CPU IDs: `[0, 1, 2, 3]`; client CPU IDs: `[4, 5, 6, 7, 8, 9, 10, 11]`.

| Clients | Pipeline | Workload | Original ops/s | Refactored ops/s | Redis ops/s | Refactored/original | Refactored/Redis |
|---:|---:|:---|---:|---:|---:|---:|---:|
| 1 | 1 | SET | 132,600 | 141,680 | 126,066 | 1.07× | 1.12× |
| 1 | 1 | GET | 129,645 | 137,168 | 129,590 | 1.06× | 1.06× |
| 1 | 1 | MIXED | 130,840 | 138,915 | 126,855 | 1.06× | 1.10× |
| 8 | 1 | SET | 785,405 | 850,461 | 292,670 | 1.08× | 2.91× |
| 8 | 1 | GET | 766,553 | 781,538 | 314,682 | 1.02× | 2.48× |
| 8 | 1 | MIXED | 771,713 | 800,296 | 300,940 | 1.04× | 2.66× |
| 8 | 32 | SET | 3,991,145 | 11,439,712 | 1,854,512 | 2.87× | 6.17× |
| 8 | 32 | GET | 4,389,729 | 9,201,875 | 3,224,227 | 2.10× | 2.85× |
| 8 | 32 | MIXED | 3,603,887 | 10,496,720 | 2,235,516 | 2.91× | 4.70× |

Median of per-run p99 batch completion latencies for eight clients (µs):

| Pipeline | Workload | Original | Refactored | Redis |
|---:|:---|---:|---:|---:|
| 1 | SET | 16.11 | 14.60 | 45.95 |
| 1 | GET | 16.87 | 15.38 | 42.67 |
| 1 | MIXED | 16.32 | 14.99 | 42.46 |
| 32 | SET | 99.99 | 35.30 | 211.65 |
| 32 | GET | 90.91 | 41.78 | 127.47 |
| 32 | MIXED | 109.65 | 37.87 | 190.31 |

[Raw samples](benchmarks/2026-11-04-4-cpu.json).

## Interpretation

The refactor improves the measured pipelined workloads on both CPU budgets.
With four CPUs, removing the global store mutex allows parallel command
execution; that comparison includes a concurrency advantage over Redis's
single command-execution thread. On one CPU, eight-client non-pipelined GET
and mixed throughput still trail Redis. One-CPU p99 latency is also worse:
four key-val workers compete for a single CPU. Higher aggregate throughput
does not imply better tail latency in that configuration. The small, resident,
overwrite-heavy working set and matching TTLs are material to these results.

## Reproduce

```sh
sh configure
make all test smoke-test network-test
python3 tests/compare_redis.py --redis-server /path/to/redis-server \
  --baseline /path/to/original-optimized-server --server-cpus 1 \
  --output benchmark-one-cpu.json
python3 tests/compare_redis.py --redis-server /path/to/redis-server \
  --baseline /path/to/original-optimized-server --server-cpus 4 \
  --output benchmark-four-cpu.json
```

The runner refuses occupied benchmark ports (8080 and 16379), starts disposable
servers in a temporary directory, and terminates them on completion or failure.
It does not install Redis or alter any existing Redis instance.

## Verification

* `sh configure && make all test` passed in the actual repository.
* Smoke, network regression, and existing performance tests passed. Network
  checks cover fragmented input, CRLF, pipelines, half-close, quit ordering,
  oversized commands/arguments, concurrent clients and resize, TTL replacement,
  connection resets, and server liveness while a client sends without reading.
* Store tests check data after resize, concurrent ADD exclusivity, updates with
  growing/shrinking values, deletes, and whole-store snapshots during writes.
* `-Wall -Wextra -Werror` builds and GCC `-fanalyzer` passed for the server/store.
* ASan/UBSan could not be run: this host lacks their runtime/static libraries.
* `git diff --check` passed. No commit, push, or service installation was made.
