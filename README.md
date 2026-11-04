# key-val

## About

Simple server in C that sits at 8080 port, serves like in-memory key-value db. It has folowing commands:

* "write key value ttl" writes or overwrites key with value, saving timestamp, ttl optional
* "add key value ttl"  writes key with value, saving timestamp, but not overwrites it, ttl optional
* "update key value ttl" overwrites key with value, saving timestamp, but not add it, ttl optional
* "search key" searches value by the key
* "dump index offset" dupms what is in table from index to offset, if there are buckets there. Just "dump" returns first INITIAL_CAPACITY indexes.
* "wipe" drops everyting
* "size" show amount of key-values pairs and current hash table reserved
* "delete key" deletes the key
* "quit" close the connection.

## Data model

64 independently locked hash tables store keys, values, timestamps, and TTLs.
Each key uses one shard; shards resize independently. Whole-store operations
(`wipe`, `size`, `dump`) lock all shards in order. Capacity is rounded up to a
multiple of 64 initially; dump bucket indexes span the shards in order.

The TCP server uses four epoll workers, in-place line parsing, and bounded
read batches. Replies remain ordered within a connection. Keys are limited to
255 bytes and values to 767 bytes; whitespace separates tokens. Oversized
arguments are rejected rather than truncated. It is an in-memory server with
no persistence or replication.

## Build and verify

```sh
sh configure
make all
make test smoke-test network-test
```

The default build uses `-O3`; override `CFLAGS` for other configurations.
Only a C compiler, pthreads, and Python 3 (network tests/benchmark runner) are
required. The server listens on port 8080. Tests refuse to use an occupied port.

## Redis comparison

```sh
python3 tests/compare_redis.py --redis-server /path/to/redis-server
# Also measure an original, separately compiled server:
python3 tests/compare_redis.py --redis-server /path/to/redis-server \
  --baseline /path/to/original-server --server-cpus 4
```

See [BENCHMARK.md](BENCHMARK.md) for measured results and limitations. The runner
builds one C load generator for both protocols, starts disposable instances,
and writes raw JSON samples with throughput and batch p50/p95/p99 latency.
It requires Linux `taskset` and separate server/client CPUs. Redis persistence
is disabled, and the same 64-byte values, 10,000-key working set, TTL, client
counts, and pipeline depths are used for both servers. Every reply is checked.

## ToDo

* Configfile, cli keys
* Persistence (yet need to choose strategy, Periodic Snapshots + WAL maybe?)
* Simple master-master replication with Last-Write-Wins (LWW) strategy
* Dokerise
* Consistent logging.

## Known problems

* Slow readers exceeding a 1 MiB userspace reply backlog are disconnected;
  clients must read replies while pipelining. `dump` materializes its snapshot
  before queueing it, so large dumps still require memory proportional to output.
* No memory eviction policy or proactive background expiry; expiry is lazy on
  key access and collected by `size`, `dump`, or the store collection API.
* No authentication, transport encryption, persistence, or replication.
