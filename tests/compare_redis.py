#!/usr/bin/env python3
"""Single-node, loopback, closed-loop GET/SET comparison; no Python in hot path."""
import argparse
import json
import os
from pathlib import Path
import platform
import shutil
import socket
import statistics
import subprocess
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--redis-server', default=shutil.which('redis-server'))
    p.add_argument('--server', default=str(ROOT / 'server'))
    p.add_argument('--baseline', help='optional original key-val executable')
    p.add_argument('--operations', type=int, default=200000)
    p.add_argument('--pipeline-operations', type=int, default=5000000)
    p.add_argument('--repeats', type=int, default=3)
    p.add_argument('--server-cpus', type=int, default=1)
    p.add_argument('--output', default='benchmark-results.json')
    a = p.parse_args()
    if not a.redis_server: p.error('redis-server is required (pass --redis-server PATH)')
    if a.operations < 10000 or a.pipeline_operations < 10000 or a.repeats < 1 or a.server_cpus < 1: p.error('invalid count')
    cpus = sorted(os.sched_getaffinity(0))
    if len(cpus) <= a.server_cpus: p.error('need separate server and client CPUs')
    server_cpus = cpus[:a.server_cpus]
    client_cpus = cpus[a.server_cpus:a.server_cpus + 8]
    results = []
    targets = [('keyval', a.server, 'keyval', 8080), ('redis', a.redis_server, 'redis', 16379)]
    if a.baseline: targets.insert(0, ('baseline', a.baseline, 'keyval', 8080))
    for _, _, _, port in targets:
        with socket.socket() as s:
            if s.connect_ex(('127.0.0.1', port)) == 0:
                p.error(f'port {port} is already occupied; refusing to touch existing data')
    with tempfile.TemporaryDirectory(prefix='keyval-bench-') as temp:
        bench = str(Path(temp) / 'kv_bench')
        subprocess.run(['cc', '-O3', '-Wall', '-Wextra', '-Werror', '-pthread',
                        str(ROOT / 'tests/kv_bench.c'), '-o', bench], check=True)
        def client(protocol, port, clients, operations, pipeline, workload):
            cmd = ['taskset', '-c', ','.join(map(str, client_cpus)), bench, protocol,
                   str(port), str(clients), str(operations), str(pipeline), workload]
            return json.loads(subprocess.check_output(cmd, timeout=120, text=True))
        for repeat in range(a.repeats):
            # Rotate target order to reduce systematic warm-cache/thermal ordering bias.
            order = targets[repeat % len(targets):] + targets[:repeat % len(targets)]
            for name, binary, protocol, port in order:
                command = [str(Path(binary).resolve())]
                if protocol == 'redis':
                    command += ['--bind', '127.0.0.1', '--port', str(port), '--save', '',
                                '--appendonly', 'no', '--io-threads', '1', '--protected-mode', 'yes',
                                '--dir', temp]
                command = ['taskset', '-c', ','.join(map(str, server_cpus))] + command
                with open(Path(temp) / f'{name}.log', 'w+') as log:
                    process = subprocess.Popen(command, stdout=log, stderr=log, cwd=temp)
                    try:
                        for _ in range(100):
                            if process.poll() is not None:
                                log.seek(0); raise RuntimeError(log.read())
                            with socket.socket() as s:
                                if s.connect_ex(('127.0.0.1', port)) == 0: break
                            time.sleep(.02)
                        else: raise RuntimeError('startup timeout')
                        client(protocol, port, 1, 10000, 32, 'load')
                        for clients, pipeline in [(1, 1), (8, 1), (8, 32)]:
                            for workload in ['set', 'get', 'mixed']:
                                client(protocol, port, clients, 10000, pipeline, workload)
                                row = client(protocol, port, clients, max(a.operations, a.pipeline_operations) if pipeline > 1 else a.operations, pipeline, workload)
                                if process.poll() is not None: raise RuntimeError('server exited')
                                row.update(target=name, repeat=repeat + 1)
                                results.append(row)
                                print(f'{name} r{repeat+1} c{clients} p{pipeline} {workload}: '
                                      f'{row["ops_per_second"]:,.0f} ops/s', flush=True)
                    finally:
                        process.terminate()
                        try: process.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            process.kill(); process.wait()
    report = dict(metadata=dict(host=platform.platform(), cpu_model=next((line.split(':',1)[1].strip() for line in Path('/proc/cpuinfo').read_text().splitlines() if line.startswith('model name')), 'unknown'), compiler=subprocess.check_output(['cc','--version'],text=True).splitlines()[0],
                 redis_version=subprocess.check_output([a.redis_server,'--version'],text=True).strip(),
                 server_cpus=server_cpus, client_cpus=client_cpus, value_bytes=64, keyspace=10000,
                 ttl_seconds=31536000, persistence=False, repeats=a.repeats,
                 latency='closed-loop batch completion; individual latency only at pipeline=1',
                 redis_request_protocol='RESP2', allocator='record Redis build allocator from redis_version'), results=results)
    Path(a.output).write_text(json.dumps(report, indent=2) + '\n')
    for clients, pipeline in [(1,1),(8,1),(8,32)]:
        for workload in ['set','get','mixed']:
            medians = {name: round(statistics.median(r['ops_per_second'] for r in results
                       if r['target']==name and r['clients']==clients and r['pipeline']==pipeline
                       and r['workload']==workload)) for name, *_ in targets}
            print(f'c{clients} p{pipeline} {workload}: {medians}')

if __name__ == '__main__':
    main()
