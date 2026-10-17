#!/usr/bin/env python3
"""Regret gate: how often the planner's default is not the fastest exact strategy, and by how much.

For every cell (a study dataset x query, or a ClickBench query over sub16), the strategies the `ZPQ_*` knobs can force
that change this cell's plan are each run, warm, and timed; every alternative must give the byte-identical answer.
Report: sum(default)/sum(fastest), cells whose default is >10% and >5 ms slower than the fastest, and any answer that
differs (a hard failure: the knobs only choose between exact strategies).

    python3 -I benchmarks/regret_gate.py --bin zig-out/bin/zpq --out /tmp/regret            # full: ~1.5 h
    python3 -I benchmarks/regret_gate.py --bin zig-out/bin/zpq --out /tmp/regret --cells 'q0[345]_|cb16/Q1[0-9]$'
    python3 -I benchmarks/regret_gate.py --report /tmp/regret                               # re-print a finished run

Needs the study datasets (~/bench/zpq-general/data, with their harness's queries.py) and ClickBench sub16
(~/bench/zpq-fair/data/clickbench/sub16). Timing goes through `zpq-bench-run` when it is on PATH (one cell and knob per
call), each zpq run inside a 24G scope. Resumable: rows already in OUT/warm.jsonl are skipped. Fails (exit 1) on an
answer difference, or when sum(default)/sum(fastest) exceeds --max-regret (default 1.10).
"""
import argparse, glob, hashlib, json, os, re, shutil, subprocess, sys, time

STUDY = os.path.expanduser('~/bench/zpq-general')
SUB16 = os.path.expanduser('~/bench/zpq-fair/data/clickbench/sub16/*.parquet')
CB_QUERIES = os.path.expanduser('~/bench/zpq-fair/repos/ClickBench/duckdb-parquet/queries.sql')
KNOBS = {
    'default': {},
    'sketch_off': {'ZPQ_SKETCH_TOPK': '0'},
    'census_off': {'ZPQ_SKETCH_CENSUS': '0'},
    'partagg': {'ZPQ_PARTAGG': '1'},
    'spill_off': {'ZPQ_SPILL': '0'},
    'dense_off': {'ZPQ_DENSE': '0'},
    'union_off': {'ZPQ_NDV_UNION': '0'},
    'pairs_on': {'ZPQ_DISTINCT_PAIRS': '1'},
    'pairs_off': {'ZPQ_DISTINCT_PAIRS': '0'},
}


def cells():
    sys.path.insert(0, os.path.join(STUDY, 'harness'))
    from queries import queries
    out = []
    for mp in sorted(glob.glob(os.path.join(STUDY, 'data/meta/*.json'))):
        m = json.load(open(mp))
        strings = any(m['columns'][k]['type'] == 'BYTE_ARRAY' for k in m['kcols'])
        for qid, sql, kind in queries(m):
            argv = ['query', '--query', sql.replace('{T}', f"'{m['path']}'")]
            if kind == 'full': argv += ['--codec', 'snappy']
            out.append({'label': f"study/{m['ds']}/{qid}", 'argv': argv, 'parquet': kind == 'full', 'strings': strings})
    for i, line in enumerate(open(CB_QUERIES)):
        sql = re.sub(r'\bhits\b', f"'{SUB16}'", line.strip().rstrip(';'), count=1)
        out.append({'label': f'cb16/Q{i}', 'argv': ['query', '--query', sql], 'parquet': False, 'strings': True})
    return out


def explain(binary, cell, env):
    env = dict(os.environ, ZPQ_IO_PROFILE='mmap', **env)
    p = subprocess.run([binary] + cell['argv'][:3] + ['--explain'], env=env, capture_output=True, timeout=600)
    try:
        return json.loads(p.stdout)
    except ValueError:
        return None


def applicable(binary, cell):
    """The knobs that change this cell's plan: visible in explain, or acting on the shape explain shows."""
    d = explain(binary, cell, {})
    if d is None or d.get('strategy') in (None, 'scalar'): return ['default']
    strat, ndv, sql = d['strategy'], d.get('ndv') or {}, cell['argv'][2].upper()
    out = ['default']
    if strat in ('sketch', 'first_keys'): out.append('sketch_off')
    census = (d.get('sketch') or {}).get('census_allowed')
    if strat == 'first_keys' or (strat == 'sketch' and census): out.append('census_off')
    forced = (explain(binary, cell, KNOBS['partagg']) or {}).get('strategy')
    if strat != 'partitioned_hash' and forced == 'partitioned_hash': out.append('partagg')
    if ndv.get('union_attempted'): out.append('union_off')
    if ndv.get('union_complete'): out.append('dense_off')
    if strat in ('plain_hash', 'sketch') and cell['strings']: out.append('spill_off')
    if 'COUNT(DISTINCT' in sql: out += ['pairs_on', 'pairs_off']
    return out


def timed(binary, cell, knob, reps, out_dir):
    """One cell under one knob: a warm-up, then `reps` timed runs (this process is what zpq-bench-run pins)."""
    tmp = os.path.join(out_dir, 'out.parquet')
    def run():
        argv = [binary] + cell['argv'] + (['-o', tmp] if cell['parquet'] else [])
        t = time.monotonic_ns()
        p = subprocess.run(argv, env=dict(os.environ, **KNOBS[knob]), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        ms = (time.monotonic_ns() - t) / 1e6
        body = (open(tmp, 'rb').read() if p.returncode == 0 else b'') if cell['parquet'] else p.stdout
        return ms, p.returncode, hashlib.md5(body).hexdigest()[:12]
    run()
    rs = [run() for _ in range(reps)]
    ts = sorted(r[0] for r in rs)
    return {'label': cell['label'], 'config': knob, 'median': ts[len(ts) // 2], 'rc': rs[0][1], 'md5': rs[0][2]}


def report(out_dir, max_regret):
    by = {}
    for w in map(json.loads, open(os.path.join(out_dir, 'warm.jsonl'))): by.setdefault(w['label'], {})[w['config']] = w
    diffs, rows = [], []
    for label, m in by.items():
        d = m.get('default')
        if not d or d['rc'] != 0: continue
        ok = {k: v for k, v in m.items() if v['rc'] == 0}
        diffs += [(label, k) for k, v in ok.items() if v['md5'] != d['md5']]
        best_k, best = min(ok.items(), key=lambda kv: kv[1]['median'])
        if len(ok) > 1: rows.append((d['median'] / best['median'], label, best_k, d['median'], best['median']))
    rows.sort(reverse=True)
    tot = sum(r[3] for r in rows) / max(1e-9, sum(r[4] for r in rows))
    miss = [r for r in rows if r[0] > 1.10 and r[3] - r[4] > 5]
    print(f'cells with alternatives {len(rows)}; default not fastest by >10% and >5 ms: {len(miss)}; '
          f'sum default / sum fastest {tot:.3f}')
    for r in miss: print(f'  {r[0]:5.2f}x {r[1]:34s} fastest={r[2]:11s} default={r[3]:8.1f} ms fastest={r[4]:8.1f} ms')
    for label, knob in diffs: print(f'  ANSWER DIFFERS: {label} under {knob}')
    return 1 if diffs or tot > max_regret else 0


def main():
    ap = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    ap.add_argument('--bin'); ap.add_argument('--out'); ap.add_argument('--report')
    ap.add_argument('--cells', default='.'); ap.add_argument('--reps', type=int, default=3)
    ap.add_argument('--max-regret', type=float, default=1.10)
    a = ap.parse_args()
    if a.report: sys.exit(report(a.report, a.max_regret))
    if not a.bin or not a.out: ap.error('--bin and --out, or --report')
    binary = os.path.abspath(a.bin); os.makedirs(a.out, exist_ok=True)
    path = os.path.join(a.out, 'warm.jsonl')
    done = {(w['label'], w['config']) for w in map(json.loads, open(path))} if os.path.exists(path) else set()
    pat = re.compile(a.cells)
    bench = shutil.which('zpq-bench-run')
    for cell in cells():
        if not pat.search(cell['label']): continue
        for knob in applicable(binary, cell):
            if (cell['label'], knob) in done: continue
            cmd = [sys.executable, '-I', os.path.abspath(__file__), '--one', json.dumps(cell), knob, str(a.reps),
                   binary, a.out]
            scope = ['systemd-run', '--user', '--scope', '--quiet', '--collect',
                     f'--unit=regret-{os.getpid()}-{len(done)}', '-p', 'MemoryMax=24G', '-p', 'MemorySwapMax=0']
            p = subprocess.run(([bench] if bench else []) + scope + cmd, stdout=subprocess.PIPE,
                               stderr=subprocess.DEVNULL)
            line = p.stdout.decode().strip().splitlines()[-1:] or ['']
            if line[0].startswith('{'):
                with open(path, 'a') as f: f.write(line[0] + '\n')
            done.add((cell['label'], knob))
    sys.exit(report(a.out, a.max_regret))


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == '--one':
        cell, knob, binary, out_dir = json.loads(sys.argv[2]), sys.argv[3], sys.argv[5], sys.argv[6]
        reps = int(sys.argv[4])
        print(json.dumps(timed(binary, cell, knob, reps, out_dir)), flush=True)
    else:
        main()
