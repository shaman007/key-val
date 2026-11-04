#!/usr/bin/env python3
"""Start a disposable key-val server and exercise protocol/concurrency regressions."""
import socket
import struct
import time
from concurrent.futures import ThreadPoolExecutor

def connect():
    return socket.create_connection(('127.0.0.1', 8080), timeout=5)

def exchange(command, expected):
    with connect() as s:
        s.sendall(command)
        f = s.makefile('rb')
        for reply in expected:
            actual = f.readline()
            assert actual.startswith(reply), (actual, reply)

def run_checks():
    exchange(b'wipe\n', [b'All clean!\n'])
    # Fragmented lines, CRLF, pipelined writes/reads, half-close, quit ordering.
    with connect() as s:
        s.sendall(b'write fragment ')
        time.sleep(.01)
        s.sendall(b'value\r\nsearch fragment\r\n')
        s.shutdown(socket.SHUT_WR)
        f = s.makefile('rb')
        assert f.readline() == b'OK\n'
        assert f.readline().startswith(b'Found: value, timestamp: ')
        assert f.readline() == b''
    exchange(b'write q v\nquit\nwrite never v\n', [b'OK\n', b'Goodbye!\n'])
    exchange(b'search never\n', [b'Not found\n'])
    exchange(b'write ' + b'k'*256 + b' v\n', [b'Error: invalid command arguments\n'])
    exchange(b'x'*5000 + b'\n', [b'Error: command too long\n'])
    # Concurrent writers/readers exercising independent shards and resizing.
    def concurrent(i):
        with connect() as s:
            f = s.makefile('rb')
            commands = b''.join(f'write t{i}-{j} v{j}\n'.encode() for j in range(1000))
            s.sendall(commands)
            for _ in range(1000): assert f.readline() == b'OK\n'
            s.sendall(b''.join(f'search t{i}-{j}\n'.encode() for j in range(1000)))
            for j in range(1000): assert f.readline().startswith(f'Found: v{j}, timestamp: '.encode())
    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(concurrent, range(16)))
    # RST during output must never terminate the process with SIGPIPE.
    for _ in range(30):
        s = connect()
        s.sendall(b'search fragment\n'*1000)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
        s.close()
    # A non-reading pipeline is disconnected at a bounded output backlog.
    with connect() as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
        s.settimeout(.2)
        payload = b'search fragment\n' * 4096
        for _ in range(128):
            try: s.sendall(payload)
            except (OSError, TimeoutError): break
        exchange(b'write live yes\nsearch live\n', [b'OK\n', b'Found: yes, timestamp: '])
    exchange(b'write expiry old 1\n', [b'OK\n'])
    time.sleep(2)
    exchange(b'add expiry new\nsearch expiry\n', [b'OK\n', b'Found: new, timestamp: '])
    print('network regression tests passed')

if __name__ == '__main__':
    import argparse
    from pathlib import Path
    import subprocess
    import tempfile
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--server', default=str(Path(__file__).resolve().parents[1] / 'server'))
    args = parser.parse_args()
    with socket.socket() as probe:
        if probe.connect_ex(('127.0.0.1', 8080)) == 0:
            parser.error('port 8080 is occupied; refusing to touch existing data')
    with tempfile.TemporaryFile() as log:
        process = subprocess.Popen([args.server], stdout=log, stderr=log)
        try:
            for _ in range(100):
                if process.poll() is not None:
                    log.seek(0)
                    raise RuntimeError(log.read().decode())
                try:
                    with connect(): break
                except OSError: time.sleep(.02)
            else: raise RuntimeError('server startup timeout')
            run_checks()
            assert process.poll() is None, 'server exited unexpectedly'
        finally:
            process.terminate()
            try: process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill(); process.wait()
