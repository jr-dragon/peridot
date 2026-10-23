"""Compare Peridot and Redis 8.10.2 under identical AOF workloads on macOS."""

import argparse
import hashlib
import csv
import datetime
import io
import json
import os
import pathlib
import platform
import signal
import shutil
import socket
import statistics
import subprocess
import time

parser = argparse.ArgumentParser()
parser.add_argument('--output', type=pathlib.Path, required=True)
parser.add_argument('--candidate', required=True)
parser.add_argument('--label', required=True)
parser.add_argument('--policies', nargs='+', choices=['off', 'everysec', 'no', 'always'], default=['off', 'everysec', 'no', 'always'])
parser.add_argument('--rounds', type=int, default=3)
parser.add_argument('--redis-server', default=shutil.which('redis-server'))
parser.add_argument('--benchmark', default=shutil.which('redis-benchmark'))
parser.add_argument('--requests', type=int, default=1000000)
parser.add_argument('--always-requests', type=int, default=20000)
parser.add_argument('--engines', nargs='+', choices=['candidate', 'redis'], default=['candidate', 'redis'])
parser.add_argument('--pipelines', type=int, nargs='+', default=[1, 16])
parser.add_argument('--operations', nargs='+', choices=['SET', 'GET'], default=['SET', 'GET'])
options = parser.parse_args()
ROOT = options.output.resolve()
ROOT.mkdir(parents=True, exist_ok=True)
REPO = pathlib.Path(__file__).resolve().parents[1]
VALUE = 'x' * 32
KEYS = 10000
REQUESTS = options.requests
ROUNDS = options.rounds
CLIENTS = 50
SEED = 42
BENCH = options.benchmark
REDIS = options.redis_server
RESULTS = []
EVENTS = []

def capture(args):
    p = subprocess.run(args, capture_output=True, text=True, check=True)
    return p.stdout.strip()

def encode(args):
    parts = [str(a).encode() for a in args]
    return b'*%d\r\n' % len(parts) + b''.join(b'$%d\r\n' % len(p) + p + b'\r\n' for p in parts)

def reply(f):
    line = f.readline()
    if not line.endswith(b'\r\n'):
        raise RuntimeError('Truncated RESP reply: %r' % line)
    kind, body = line[:1], line[1:-2]
    if kind == b'-':
        raise RuntimeError(line.decode(errors='replace'))
    if kind == b'+':
        return body
    if kind == b':':
        return int(body)
    if kind == b'$':
        n = int(body)
        if n == -1:
            return None
        data = f.read(n + 2)
        assert len(data) == n + 2 and data[-2:] == b'\r\n'
        return data[:-2]
    if kind == b'*':
        return [reply(f) for _ in range(int(body))]
    raise RuntimeError('Unexpected RESP reply: %r' % line)

def command(port, *args):
    with socket.create_connection(('127.0.0.1', port), timeout=5) as s:
        s.sendall(encode(args))
        return reply(s.makefile('rb'))

def dataset(port, populate):
    with socket.create_connection(('127.0.0.1', port), timeout=10) as s:
        f = s.makefile('rb')
        for start in range(0, KEYS, 256):
            keys = ['bench:%012d' % i for i in range(start, min(start + 256, KEYS))]
            commands = [('SET', key, VALUE) if populate else ('GET', key) for key in keys]
            s.sendall(b''.join(encode(args) for args in commands))
            for key in keys:
                expected = b'OK' if populate else VALUE.encode()
                actual = reply(f)
                assert actual == expected, (key, actual, expected)

def benchmark(engine, port, round_number, pipeline, operation, warmup=False):
    n = (2000 if warmup else options.always_requests) if POLICY == 'always' else (100000 if warmup else REQUESTS)
    args = [BENCH, '-h', '127.0.0.1', '-p', str(port), '-c', str(CLIENTS),
            '-n', str(n), '-r', str(KEYS), '-P', str(pipeline),
            '--seed', str(SEED), '--csv', operation, 'bench:__rand_int__']
    if operation == 'SET':
        args.append(VALUE)
    suffix = '-warmup' if warmup else ''
    name = '%s-%s-round%d-p%d-%s%s' % (engine, POLICY, round_number, pipeline, operation.lower(), suffix)
    p = subprocess.run(args, capture_output=True, text=True, timeout=180)
    (ROOT / (name + '.csv')).write_text(p.stdout)
    (ROOT / (name + '.stderr')).write_text(p.stderr)
    event = dict(name=name, command=args, returncode=p.returncode)
    EVENTS.append(event)
    (ROOT / 'commands.json').write_text(json.dumps(EVENTS, indent=2))
    if p.returncode or 'error' in p.stderr.lower():
        raise RuntimeError('%s failed: %s' % (name, p.stderr))
    rows = list(csv.DictReader(io.StringIO(p.stdout)))
    assert len(rows) == 1, p.stdout
    row = rows[0]
    if not warmup:
        RESULTS.append(dict(engine=engine, policy=POLICY, round=round_number, requests=n, pipeline=pipeline,
                            operation=operation, **{k: v for k, v in row.items() if k != 'test'}))
        (ROOT / 'results.json').write_text(json.dumps(RESULTS, indent=2))
        print(name + ': ' + json.dumps(row), flush=True)

def run_server(engine, round_number):
    # Start only the server under test; keep reference and candidate workloads serial.
    port = 16681 if engine == 'redis' else 16679
    with socket.socket() as probe:
        probe.bind(('127.0.0.1', port))
    name = '%s-%s-round%d' % (engine, POLICY, round_number)
    data_dir = ROOT / (name + '-data')
    data_dir.mkdir()
    config = ROOT / (name + '.conf')
    config.write_text('bind 127.0.0.1\nport %d\nappendonly %s\nappendfsync %s\nsave ""\nauto-aof-rewrite-percentage 0\ndir "%s"\n' % (port, 'no' if POLICY == 'off' else 'yes', 'everysec' if POLICY == 'off' else POLICY, data_dir))
    binary = REDIS if engine == 'redis' else options.candidate
    args = [binary, str(config)]
    if engine != 'redis':
        args.extend(['--pprof-addr', '127.0.0.1:0'])
    with (ROOT / (name + '.log')).open('w') as log:
        process = subprocess.Popen(args, stdout=log, stderr=subprocess.STDOUT)
        EVENTS.append(dict(name='start-%s-round%d' % (engine, round_number), command=args, pid=process.pid))
        try:
            deadline = time.monotonic() + 15
            while True:
                if process.poll() is not None:
                    raise RuntimeError('Server exited early: %s' % args)
                try:
                    if command(port, 'PING') == b'PONG':
                        break
                except (OSError, RuntimeError):
                    pass
                if time.monotonic() > deadline:
                    raise RuntimeError('Server did not become ready')
                time.sleep(0.05)
            if engine == 'redis':
                effective = command(port, 'CONFIG', 'GET', 'appendonly', 'appendfsync', 'save', 'io-threads')
                decoded = [v.decode() for v in effective]
                assert dict(zip(decoded[::2], decoded[1::2]))['appendonly'] == ('no' if POLICY == 'off' else 'yes')
                assert dict(zip(decoded[::2], decoded[1::2]))['appendfsync'] == ('everysec' if POLICY == 'off' else POLICY)
                assert dict(zip(decoded[::2], decoded[1::2]))['save'] == ''
                assert dict(zip(decoded[::2], decoded[1::2]))['io-threads'] == '1'
                (ROOT / (name + '-effective-config.json')).write_text(json.dumps(decoded, indent=2))
                (ROOT / (name + '-info.txt')).write_bytes(command(port, 'INFO'))
            dataset(port, populate=True)
            dataset(port, populate=False)
            for pipeline in options.pipelines:
                for operation in options.operations:
                    benchmark(engine, port, round_number, pipeline, operation, warmup=True)
                    benchmark(engine, port, round_number, pipeline, operation)
            dataset(port, populate=False)
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            assert process.returncode == 0, ('Server exit', engine, process.returncode)
            if POLICY != 'off':
                manifest = data_dir / 'appendonlydir' / 'appendonly.aof.manifest'
                assert manifest.exists(), manifest
                increments = list((data_dir / 'appendonlydir').glob('*.incr.aof'))
                assert increments and sum(p.stat().st_size for p in increments) > 0
            else:
                assert not (data_dir / 'appendonlydir').exists()
            print(name + ': all 10,000 keys verified; persistence files checked', flush=True)

metadata = dict(commit=capture(['git', '-C', str(REPO), 'rev-parse', 'HEAD']),
                redis=capture([REDIS, '--version']), go=capture(['go', 'version']),
                build=capture(['go', 'version', '-m', options.candidate]),
                hardware=capture(['sysctl', 'machdep.cpu.brand_string', 'hw.ncpu', 'hw.memsize']),
                os=capture(['sw_vers']), architecture=platform.machine(), candidate=options.candidate,
                sha256=hashlib.sha256(pathlib.Path(options.candidate).read_bytes()).hexdigest(),
                clients=CLIENTS, keyspace=KEYS, value_bytes=len(VALUE), rounds=ROUNDS,
                requests=REQUESTS, always_requests=options.always_requests, seed=SEED, client_threads=1,
                pipelines=options.pipelines, operations=options.operations, engines=options.engines,
                gomaxprocs=os.environ.get('GOMAXPROCS', 'unset'), protocol='RESP2',
                started=datetime.datetime.now(datetime.timezone.utc).isoformat())
assert 'v=8.10.2 ' in metadata['redis'], metadata['redis']
(ROOT / 'metadata.json').write_text(json.dumps(metadata, indent=2))
for round_number in range(1, ROUNDS+1):
    for POLICY in options.policies:
        engines = [options.label, 'redis'] if round_number % 2 else ['redis', options.label]
        engines = [e for e in engines if ('redis' if e == 'redis' else 'candidate') in options.engines]
        for engine in engines:
            run_server(engine, round_number)
metadata['finished'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
(ROOT / 'metadata.json').write_text(json.dumps(metadata, indent=2))
with (ROOT / 'results.csv').open('w') as f:
    writer = csv.DictWriter(f, fieldnames=list(RESULTS[0]), lineterminator='\n')
    writer.writeheader()
    writer.writerows(RESULTS)
summary = []
groups = sorted({(r['engine'], r['policy'], r['pipeline'], r['operation']) for r in RESULTS})
for engine, policy, pipeline, operation in groups:
    rows = [r for r in RESULTS if (r['engine'], r['policy'], r['pipeline'], r['operation']) == (engine, policy, pipeline, operation)]
    summary.append(dict(engine=engine, policy=policy, pipeline=pipeline, operation=operation,
                        **{metric: statistics.median(float(r[metric]) for r in rows)
                           for metric in ['rps', 'avg_latency_ms', 'p50_latency_ms', 'p99_latency_ms', 'max_latency_ms']}))
(ROOT / 'summary.json').write_text(json.dumps(summary, indent=2))
print('BENCHMARK COMPLETE: %d measured runs' % len(RESULTS), flush=True)
