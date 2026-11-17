#!/usr/bin/env python3
"""The joins suite: 104 analyst join queries (plus 24 workaround rewrites) over public multi-table datasets in the
S3 bucket zpq-public-data, checked against DuckDB 2.0.0's answers in cases.json. Off-tier; no gate runs it.

It runs every case through zpq on local copies of the data and over S3, and prints one score line per place:
correct, refused by name, refused without naming a feature, wrong, crash.

Usage:
  python3 tools/joins_suite/suite.py --zpq zig-out/bin/zpq [--local] [--s3] [--data DIR]
        [--set analyst|workaround|all] [--ids ds01,tl08] [--tlc-single] [--threads N] [--reps N]
        [--json OUT] [--fail-unnamed] [-v]
  python3 tools/joins_suite/suite.py --fetch-only [--data DIR]          # pull the data once
  python3 tools/joins_suite/suite.py --engine duckdb [--s3] ...          # score DuckDB itself (a self-check)
  python3 tools/joins_suite/suite.py --record [--duckdb PATH]            # re-record the answers with DuckDB

Data: --data, else $ZPQ_JOINS_DATA, else $ZPQ_TEST_PARQUET_DIR/zpq-public-data. Local runs fetch whatever is
missing (about 5 GB for every dataset; only the tables the chosen cases read). S3 credentials and endpoint come from
the environment only: S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY, S3_ENDPOINT_URL, S3_REGION (or the AWS_* names).
Without them the S3 pass is skipped, and local runs need the data already present.

Exit status: 1 when any case is wrong or crashes (or, with --fail-unnamed, is refused without a name), else 0.
"""
import argparse, csv, fnmatch, glob, io, json, os, re, statistics, sys
from decimal import Decimal, ROUND_HALF_UP

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
CASES = os.path.join(HERE, 'cases.json')
BUCKET = 'zpq-public-data'
TLC = {'yellow': 'nyc-tlc/yellow_tripdata/*.parquet',
       'yellow01': 'nyc-tlc/yellow_tripdata/yellow_tripdata_2024-01.parquet',
       'green': 'nyc-tlc/green_tripdata/*.parquet',
       'green01': 'nyc-tlc/green_tripdata/green_tripdata_2024-01.parquet',
       'fhvhv': 'nyc-tlc/fhvhv_tripdata/fhvhv_tripdata_2024-01.parquet',
       'zones': 'nyc-tlc/taxi_zone_lookup/taxi_zone_lookup.parquet',
       'weather': 'noaa-ghcnd/central_park_daily/noaa_central_park_daily.parquet'}
ROOTS = {'ds': 'tpcds-sf10', 'lh': 'lahman', 'ch': 'chinook'}
TOKEN = re.compile(r'\{(\w+)\.(\w+)\}')


def key_of(ns, table, tlc_single):
    """The bucket key (or glob) a {ns.table} token reads."""
    if ns == 'tlc':
        if tlc_single and table in ('yellow', 'green'):
            table += '01'
        return TLC[table]
    return f'{ROOTS[ns]}/{table}/{table}.parquet'


def sql_for(case, where, args):
    """where: 'local' (paths under the data dir) or 's3' (s3://bucket/...)."""
    def sub(m):
        k = key_of(m.group(1), m.group(2), args.tlc_single)
        return "'" + (os.path.join(args.data, k) if where == 'local' else f's3://{args.bucket}/{k}') + "'"
    return TOKEN.sub(sub, case['sql'])


def expected(case, args):
    answer = case['answer_tlc_single'] if args.tlc_single and 'answer_tlc_single' in case else case['answer']
    scales = case.get('decimal_avg_scale', {}) if args.engine == 'zpq' else {}
    if not scales:
        return answer
    rows = list(csv.reader(io.StringIO(answer)))
    for name, scale in scales.items():
        col = rows[0].index(name)
        quantum = Decimal(1).scaleb(-scale)
        for row in rows[1:]:
            if row[col] not in ('', 'NULL'):
                row[col] = str(Decimal(row[col]).quantize(quantum, rounding=ROUND_HALF_UP))
    out = io.StringIO(newline='')
    csv.writer(out, lineterminator='\n').writerows(rows)
    return out.getvalue()


def keys_needed(cases, args):
    return sorted({key_of(ns, t, args.tlc_single) for c in cases for ns, t in TOKEN.findall(c['sql'])})


def fetch(cases, args):
    """Download every object the cases read that is missing locally or has another size."""
    pats = keys_needed(cases, args)
    s3 = common.S3()
    have, got, nbytes = 0, 0, 0
    for root in sorted({p.split('/')[0] for p in pats}):
        for key, size in s3.list(args.bucket, root + '/'):
            if not key.endswith('.parquet') or not any(fnmatch.fnmatchcase(key, p) for p in pats):
                continue
            dest = os.path.join(args.data, key)
            if os.path.exists(dest) and os.path.getsize(dest) == size:
                have += 1
                continue
            print(f'fetch s3://{args.bucket}/{key} ({size / 1e6:.1f} MB)', flush=True)
            s3.get(args.bucket, key, dest)
            got += 1
            nbytes += size
    print(f'data: {args.data}: {have} present, {got} fetched ({nbytes / 1e9:.2f} GB)', flush=True)


def data_present(cases, args):
    missing = [p for p in keys_needed(cases, args) if not glob.glob(os.path.join(args.data, p))]
    return missing


def run_case(case, where, args):
    sql = sql_for(case, where, args)
    runs = []
    for _ in range(max(1, args.reps)):
        if args.engine == 'zpq':
            r = common.run_zpq(args.zpq, sql, threads=args.threads, timeout=args.timeout)
        else:
            r = common.run_duckdb(args.duckdb, sql, s3=(where == 's3'), threads=args.threads, timeout=args.timeout)
        runs.append(r)
        if r['rc'] != 0:
            break  # refusals and errors are deterministic: time only answers
    cls, detail = common.classify(case['sql'], expected(case, args), runs[0])
    return {'class': cls, 'detail': detail, 'ms': round(statistics.median(r['t'] for r in runs) * 1000, 1),
            'rc': runs[0]['rc'], 'err': common.first_line(runs[0]['err'])[:400]}


def score_line(res):
    c = {k: sum(1 for r in res.values() if r['class'] == k) for k in common.CLASSES}
    ref = c['refused-named'] + c['refused-unnamed']
    return (f"{c['correct']} correct, {ref} refused ({c['refused-named']} named, {c['refused-unnamed']} unnamed), "
            f"{c['wrong']} wrong, {c['crash']} crash"), c


def record(cases, args):
    """Re-record DuckDB's answers (local data) into cases.json."""
    doc = json.load(open(CASES))
    by_id = {c['id']: c for c in doc['cases']}
    single = args.tlc_single
    for case in cases:
        for variant in ([False, True] if 'answer_tlc_single' in case else [False]):
            args.tlc_single = variant
            sql = sql_for(case, 'local', args)
            runs = [common.run_duckdb(args.duckdb, sql, threads=args.threads, timeout=args.timeout) for _ in range(3)]
            if runs[0]['rc'] != 0:
                raise SystemExit(f"{case['id']}: DuckDB failed: {common.first_line(runs[0]['err'])}")
            by_id[case['id']]['answer_tlc_single' if variant else 'answer'] = runs[0]['out']
            if not variant:
                by_id[case['id']]['duckdb_ms'] = round(statistics.median(r['t'] for r in runs) * 1000, 1)
        print('recorded', case['id'], flush=True)
    args.tlc_single = single
    doc['duckdb'] = common.run_cmd([args.duckdb, '--version'])['out'].strip()
    open(CASES, 'w').write(json.dumps(doc, indent=1, ensure_ascii=False) + '\n')


def main():
    ap = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    ap.add_argument('--zpq', default='zig-out/bin/zpq')
    ap.add_argument('--engine', choices=('zpq', 'duckdb'), default='zpq')
    ap.add_argument('--duckdb', default=os.environ.get('ZPQ_DUCKDB', common.DEFAULT_DUCKDB))
    ap.add_argument('--data', default=None)
    ap.add_argument('--bucket', default=BUCKET)
    ap.add_argument('--local', action='store_true', help='local pass only (default: local, and S3 when credentials are set)')
    ap.add_argument('--s3', action='store_true', help='S3 pass only')
    ap.add_argument('--set', default='analyst', choices=('analyst', 'workaround', 'all'))
    ap.add_argument('--ids', default=None, help='comma-separated case ids')
    ap.add_argument('--tlc-single', action='store_true', help='TLC six-month globs read the January file instead')
    ap.add_argument('--threads', type=int, default=None)
    ap.add_argument('--reps', type=int, default=1, help='runs per answering case; ms is their median')
    ap.add_argument('--timeout', type=int, default=600)
    ap.add_argument('--json', default=None, help='write every result here')
    ap.add_argument('--fetch-only', action='store_true')
    ap.add_argument('--record', action='store_true')
    ap.add_argument('--fail-unnamed', action='store_true')
    ap.add_argument('-v', '--verbose', action='store_true', help='a line per case, not just the failures')
    args = ap.parse_args()
    if args.data is None:
        base = os.environ.get('ZPQ_JOINS_DATA')
        if base is None and os.environ.get('ZPQ_TEST_PARQUET_DIR'):
            base = os.path.join(os.environ['ZPQ_TEST_PARQUET_DIR'], 'zpq-public-data')
        if base is None:
            raise SystemExit('set --data, ZPQ_JOINS_DATA or ZPQ_TEST_PARQUET_DIR for the local copy of the data')
        args.data = base
    args.data = os.path.abspath(args.data)

    cases = [c for c in json.load(open(CASES))['cases'] if args.set in ('all', c['set'])]
    if args.ids:
        want = set(args.ids.split(','))
        cases = [c for c in cases if c['id'] in want]
        unknown = want - {c['id'] for c in cases}
        if unknown:
            raise SystemExit('unknown case ids: ' + ','.join(sorted(unknown)))
    if args.tlc_single:
        cases = [c for c in cases if 'answer_tlc_single' in c or not c['id'].startswith('tl')
                 or not re.search(r'\{tlc\.(yellow|green)\}', c['sql'])]

    have_creds = common.s3_credentials() is not None
    places = ['s3'] if args.s3 else ['local'] if args.local else ['local'] + (['s3'] if have_creds else [])
    if args.fetch_only or args.record or 'local' in places:
        missing = data_present(cases, args)
        if missing or args.fetch_only:
            if not have_creds:
                raise SystemExit(f'{len(missing)} tables missing under {args.data} and no S3 credentials to fetch them')
            fetch(cases, args)
    if args.fetch_only:
        return 0
    if args.record:
        record(cases, args)
        return 0
    if 's3' in places and not have_creds:
        raise SystemExit('--s3 needs S3 credentials in the environment')

    eng = common.zpq_version(args.zpq) if args.engine == 'zpq' else common.run_cmd([args.duckdb, '--version'])['out'].strip()
    out = {'engine': eng, 'set': args.set, 'tlc_single': args.tlc_single, 'places': {}}
    lines, bad = [], 0
    for where in places:
        res = {}
        for case in cases:
            r = res[case['id']] = run_case(case, where, args)
            if args.verbose or r['class'] in ('wrong', 'crash'):
                print(f"{where:5} {case['id']:7} {r['class']:15} {r['ms']:9.1f} ms  {r['detail']}", flush=True)
        line, c = score_line(res)
        out['places'][where] = {'score': c, 'cases': res}
        lines.append(f'{where} {line}')
        bad += c['wrong'] + c['crash'] + (c['refused-unnamed'] if args.fail_unnamed else 0)
        ok = [r['ms'] for r in res.values() if r['class'] == 'correct']
        if ok:
            out['places'][where]['correct_ms_total'] = round(sum(ok), 1)
    if 's3' not in places and not args.local:
        lines.append('s3 skipped (no S3 credentials in the environment)')
    label = f"joins-suite {args.set}{' tlc-single' if args.tlc_single else ''} ({len(cases)}) {eng}"
    print(label + ': ' + ' | '.join(lines))
    if args.json:
        json.dump(out, open(args.json, 'w'), indent=1)
    return 1 if bad else 0


if __name__ == '__main__':
    sys.exit(main())
