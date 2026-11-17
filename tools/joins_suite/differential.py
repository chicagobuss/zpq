#!/usr/bin/env python3
"""Randomized join differential: zpq against DuckDB 2.0.0 on generated tables. Off-tier; no gate runs it.

Each seed builds its own tables with DuckDB (deterministic: one thread, setseed) and its own queries, runs each query
through DuckDB (the oracle) and zpq, and classifies zpq's outcome as in the suite: correct, refused by name, refused
without a name, wrong, crash. A refusal is never a failure here; a wrong answer or a crash is.

Shapes (pick with --shapes; default all):
  inner, star, snow, left, semi, semidup, anti  integer keys, the review harness's shapes (joins-p2 rand.py)
  derived      HAVING on one derived column, ORDER BY another, LIMIT/OFFSET (join-fixes sib.py)
  nullpay      build columns NULL at 0-90%: in SELECT, GROUP BY, WHERE, a residual against the probe, LEFT misses
  strkey       string keys: empty, multibyte, shared prefixes, trailing spaces, case; inner, LEFT, IN, NOT IN
  composite    2-3 key columns of mixed types (INTEGER, VARCHAR, DATE) with NULL parts on both sides
  self         a table joined to itself: parent lookups, LEFT misses, same-named columns compared
  rows         row output with duplicate rows under ORDER BY, LIMIT and OFFSET, and DISTINCT
  exists       [NOT] EXISTS with NULLs on both sides, beside the matching [NOT] IN
  globfk       multi-file probe grouped by the FK, with varied row groups and result ordering

Usage:
  python3 tools/joins_suite/differential.py --zpq zig-out/bin/zpq [--duckdb PATH] [--seeds 1-12] [--queries 40]
        [--shapes nullpay,strkey] [--dir DIR] [--json OUT] [--keep] [-v]

Tables go under --dir, else $ZPQ_TEST_PARQUET_DIR/joins-differential, and are removed afterwards unless --keep.
Exit status 1 when any query is wrong or crashes.
"""
import argparse, json, os, random, shutil, sys
from collections import Counter, defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common  # noqa: E402

STRS = ['', 'a', 'A', 'ab', 'abc', 'abd', 'a ', ' a', 'key', 'key0', 'key00', 'key000', 'key1', 'kéy', 'kéy0',
        'ключ', 'ключи', '日本', '日本語', 'ß', 'ss', 'é', 'é', 'emoji😀', 'emoji😀😀']  # é and é: NFC vs NFD
SHAPES = ['inner', 'star', 'snow', 'left', 'semi', 'semidup', 'anti', 'derived', 'nullpay', 'strkey', 'composite',
          'self', 'rows', 'exists', 'globfk']


def lit(s):
    return "'" + s.replace("'", "''") + "'"


def params(seed):
    r = random.Random(seed)
    lo = r.choice([0, -50, 1, 2147483647 - 400, 1 << 40, -2147483648])
    int32 = -2147483648 <= lo and lo + 400 <= 2147483647 and r.random() < 0.6  # drawn here so seeds keep their tables
    p = {'lo': lo, 'kt': None,
         'n': r.randint(200, 9000), 'nd': r.randint(5, 400), 'rg': r.choice([63, 64, 65, 100, 777, 1000, 4096]),
         'drg': r.choice([17, 64, 128, 500]), 'pfk': r.choice([0, 0.05, 0.3, 0.9]), 'hole': r.choice([2, 3, 7, 1000]),
         'ppay': r.choice([0, 0.05, 0.3, 0.6, 0.9]), 'ppart': r.choice([0, 0.1, 0.4]),
         'strs': r.sample(STRS, r.randint(6, len(STRS)))}
    # INTEGER only when every generated key fits: an FK spans lo - 3 .. lo + nd + 3, every other key lies inside that.
    fits = -2147483648 <= lo - 3 and lo + p['nd'] + 3 <= 2147483647
    p['kt'] = 'INTEGER' if int32 and fits else 'BIGINT'
    return p


def gen_sql(seed, p, T):
    lo, kt, n, nd, rg, drg = p['lo'], p['kt'], p['n'], p['nd'], p['rg'], p['drg']
    pfk, ppay, ppart, hole = p['pfk'], p['ppay'], p['ppart'], p['hole']
    dom = p['strs']
    present = dom[: max(1, len(dom) * 2 // 3)]   # string-dim keys; the rest are probe-only misses
    allstr = 'list_value(' + ', '.join(lit(s) for s in dom) + ')'
    pres = 'list_value(' + ', '.join(lit(s) for s in present) + ')'
    c2 = ['', 'x', 'xé', 'y']
    c2l = 'list_value(' + ', '.join(lit(s) for s in c2) + ')'
    pq = f"(FORMAT PARQUET, ROW_GROUP_SIZE {drg})"
    nul = lambda pr, e: f"CASE WHEN random() < {pr} THEN NULL ELSE {e} END"
    return f"""SET memory_limit='4GB'; SET threads=1; SELECT setseed({(seed % 97) / 100});
COPY (SELECT i AS id,
  {nul(pfk, f'({lo} + (random() * {nd + 6})::BIGINT - 3)')}::{kt} AS fk,
  {nul(pfk, f'({lo} + (random() * {nd + 6})::BIGINT - 3)')}::{kt} AS fk2,
  (random() * 10)::BIGINT AS q, (random() * 1000)::BIGINT AS v,
  {nul(0.2, '(random() * 50)::INTEGER')} AS x,
  (i % 7)::BIGINT AS dup,
  {nul(pfk, f'list_extract({allstr}, 1 + (random() * {len(dom)})::INTEGER % {len(dom)})')} AS sk,
  {nul(ppart, '(random() * 5)::INTEGER')} AS c1,
  {nul(ppart, f'list_extract({c2l}, 1 + (random() * 4)::INTEGER % 4)')} AS c2,
  {nul(ppart, "DATE '2024-01-01' + (random() * 3)::INTEGER")} AS c3,
  {nul(0.1, f'({lo} + (random() * {nd})::BIGINT)')}::{kt} AS ek
 FROM range(0, {n}) t(i)) TO '{T}/f.parquet' (FORMAT PARQUET, ROW_GROUP_SIZE {rg});
COPY (SELECT * FROM read_parquet('{T}/f.parquet') WHERE id < {n // 4}) TO '{T}/glob_a.parquet' (FORMAT PARQUET, ROW_GROUP_SIZE 8192);
COPY (SELECT * FROM read_parquet('{T}/f.parquet') WHERE id >= {n // 4}) TO '{T}/glob_b.parquet' (FORMAT PARQUET, ROW_GROUP_SIZE 2048);
COPY (SELECT ({lo} + k)::{kt} AS pk, 'p' || k || repeat('ß', (k % 5)::INTEGER) AS name, (k % 4)::BIGINT AS grp,
  (k % 30)::INTEGER AS w, {nul(ppay, f'({lo} + ((k * 7) % {nd}))')}::{kt} AS nk,
  {nul(ppay, '(k % 13)::INTEGER')} AS pn, {nul(ppay, "'s' || (k % 6)")} AS ps,
  {nul(ppay, '((k % 50) * 1.25 - 20)::DECIMAL(9,2)')} AS pdec
 FROM range(0, {nd}) t(k) WHERE k % {hole} <> 1) TO '{T}/d.parquet' {pq};
COPY (SELECT * FROM read_parquet('{T}/d.parquet') LIMIT 10) TO '{T}/gdim.parquet' {pq};
COPY (SELECT ({lo} + (k % {max(nd // 2, 1)}))::{kt} AS dpk, (k % 3)::BIGINT AS dg FROM range(0, {nd * 2}) t(k)) TO '{T}/dup.parquet' {pq};
COPY (SELECT ({lo} + k)::{kt} AS pk3, 'z' || (k * 31 % 997) AS nm3, (k % 5)::BIGINT AS g3, {nul(ppay, '(k % 9)::INTEGER')} AS pn3
 FROM range(0, {nd}) t(k) WHERE k % 5 <> 2) TO '{T}/s.parquet' {pq};
COPY (SELECT list_extract({pres}, (k + 1)::INTEGER) AS spk, (k % 3)::BIGINT AS sg, {nul(ppay, '(k * 5 % 11)::INTEGER')} AS spn,
  'sv' || k AS snm FROM range(0, {len(present)}) t(k)) TO '{T}/ds.parquet' {pq};
COPY (SELECT c1, c2, c3, (row_number() OVER ())::BIGINT AS ca, {nul(ppay, '(c1 * 3 + length(c2))::INTEGER')} AS cpn FROM (
  SELECT a.c1, b.c2, DATE '2024-01-01' + c.d::INTEGER AS c3 FROM range(0, 5) a(c1), (SELECT unnest({c2l}) AS c2) b, range(0, 3) c(d)
  WHERE hash(a.c1, b.c2, c.d) % 4 <> 0
  UNION ALL SELECT NULL, 'x', DATE '2024-01-01' UNION ALL SELECT 1, NULL, DATE '2024-01-02' UNION ALL SELECT 2, 'y', NULL)
 ) TO '{T}/dc.parquet' {pq};
COPY (SELECT ({lo} + k)::{kt} AS eid, {nul(0.15, f'({lo} + (k * 3 % {nd + 3}))')}::{kt} AS boss, 'e' || (k % 17) AS ename,
  (k % 4)::BIGINT AS egrp, {nul(ppay, '(k * 37 % 100)::INTEGER')} AS esal FROM range(0, {nd}) t(k)) TO '{T}/e.parquet' {pq};
"""


def queries(seed, p, T, count, shapes):
    r = random.Random(seed * 7919 + 1)
    F, D, DU, S, DS, DC, E, GD = (f"'{T}/{x}.parquet'" for x in ('f', 'd', 'dup', 's', 'ds', 'dc', 'e', 'gdim'))
    pf = ['', 'f.q > 4', 'f.x IS NOT NULL', 'f.v < 300', 'f.id % 64 = 63', 'f.q <> 3 AND f.v > 100']
    lim = lambda: r.choice([1, 3, 7, 50, 100000])
    out = []
    for i in range(count):
        shape = r.choice(shapes)
        w = [c for c in [r.choice(pf)] if c]
        wh = lambda: (' WHERE ' + ' AND '.join(w)) if w else ''
        if shape == 'globfk':
            keys = r.choice(['f.fk', 'd.pk', 'f.fk, d.grp'])
            order = ', '.join(k + ' NULLS FIRST' for k in keys.split(', '))
            having = r.choice(['', ' HAVING count(*) > 1'])
            sql = (f"SELECT {keys}, count(*) AS n, sum(f.v) AS sv FROM '{T}/glob_*.parquet' f "
                   f"JOIN '{T}/gdim.parquet' d ON f.fk = d.pk{wh()} GROUP BY {keys}{having} "
                   f"ORDER BY n DESC, {order} LIMIT {lim()} OFFSET {r.randint(0, 3)}")
        elif shape in ('inner', 'star', 'snow', 'left', 'semi', 'semidup', 'anti'):
            if shape == 'inner': frm = f'{F} f, {D} d'; w.append('f.fk = d.pk')
            elif shape == 'star': frm = f'{F} f, {D} d, {S} s'; w += ['f.fk = d.pk', 'f.fk2 = s.pk3']
            elif shape == 'snow': frm = f'{F} f, {D} d, {S} s'; w += ['f.fk = d.pk', 'd.nk = s.pk3']
            elif shape == 'left': frm = f'{F} f LEFT JOIN {D} d ON f.fk = d.pk'
            elif shape == 'semi': frm = f'{F} f'; w.append(f"f.fk IN (SELECT pk FROM {D} WHERE grp {r.choice(['<', '<>', '>='])} {r.randint(0, 3)})")
            elif shape == 'semidup': frm = f'{F} f'; w.append(f"f.fk2 IN (SELECT dpk FROM {DU}{r.choice(['', ' WHERE dg = 1'])})")
            else: frm = f'{F} f'; w.append(f'f.fk NOT IN (SELECT pk FROM {D} WHERE grp = 1)')
            if shape in ('inner', 'star', 'snow') and r.random() < 0.4:
                w.append(r.choice(['(d.grp = 1 OR f.q = 2)', 'd.w > f.x', 'd.w < 20']))
            if shape == 'snow' and r.random() < 0.6:
                sql = (f'SELECT d.nk, s.nm3, count(*) AS n, sum(f.v) AS sv FROM {frm}{wh()} GROUP BY d.nk, s.nm3 '
                       f'ORDER BY n DESC, d.nk NULLS FIRST, s.nm3 NULLS FIRST LIMIT {lim()}')
            elif shape in ('inner', 'star', 'left') and r.random() < 0.5:
                sql = f'SELECT d.grp, count(*) AS n, sum(f.v) AS sv FROM {frm}{wh()} GROUP BY d.grp ORDER BY d.grp NULLS FIRST'
            else:
                sql = f'SELECT count(*) AS n, sum(f.v) AS sv, min(f.id) AS lo, max(f.id) AS hi FROM {frm}{wh()}'
        elif shape == 'derived':
            frm, keys = r.choice([(f'{F} f, {D} d', ['f.fk', 'd.grp']), (f'{F} f, {D} d', ['d.grp']),
                                  (f'{F} f, {D} d, {S} s', ['d.nk', 's.g3']), (f'{F} f LEFT JOIN {D} d ON f.fk = d.pk', ['f.fk', 'd.w'])])
            if 'LEFT' not in frm:
                w += ['f.fk = d.pk'] + (['d.nk = s.pk3'] if 's.g3' in keys else [])
            k = ', '.join(keys); kn = ', '.join(x + ' NULLS FIRST' for x in keys)
            tail = r.choice([f'HAVING sum(f.v) - count(*) > {r.choice([100, 3000])} ORDER BY m DESC, {kn} LIMIT {lim()}',
                             f'HAVING sum(f.v) / count(*) < 500 ORDER BY kk DESC, {kn} LIMIT {lim()} OFFSET {r.randint(0, 3)}',
                             f'ORDER BY m DESC, {kn} LIMIT {lim()}',
                             f'HAVING count(*) > 2 ORDER BY {kn} LIMIT {lim()} OFFSET {r.randint(1, 4)}'])
            sql = f'SELECT {k}, sum(f.v) / count(*) AS m, sum(f.v) - count(*) AS kk, count(*) AS n FROM {frm}{wh()} GROUP BY {k} {tail}'
        elif shape == 'nullpay':
            left = r.random() < 0.35
            frm = f'{F} f LEFT JOIN {D} d ON f.fk = d.pk' if left else f'{F} f, {D} d'
            if not left: w.append('f.fk = d.pk')
            col = r.choice(['d.pn', 'd.ps', 'd.pdec'])
            form = r.choice(['group', 'agg', 'filter', 'residual', 'count', 'snow'])
            if form == 'group':
                sql = f'SELECT {col}, count(*) AS n, sum(f.v) AS sv FROM {frm}{wh()} GROUP BY {col} ORDER BY {col} NULLS FIRST'
            elif form == 'agg':
                agg = 'min(d.ps), max(d.ps)' if col == 'd.ps' else f'sum({col}), min({col}), max({col}), avg({col})'
                sql = f'SELECT count(*) AS n, {agg} FROM {frm}{wh()}'
            elif form == 'filter':
                cond = r.choice({'d.pn': ['d.pn > 5', 'd.pn IS NULL', 'd.pn IS NOT NULL', 'd.pn IN (1, 2, 3)', 'NOT (d.pn = 4)'],
                                 'd.ps': ["d.ps = 's2'", 'd.ps IS NULL', "d.ps <> 's1'", "d.ps LIKE 's%'"],
                                 'd.pdec': ['d.pdec > 0', 'd.pdec IS NULL', 'd.pdec BETWEEN -5 AND 5']}[col])
                w.append(cond)
                sql = f'SELECT d.grp, count(*) AS n, sum(f.v) AS sv FROM {frm}{wh()} GROUP BY d.grp ORDER BY d.grp NULLS FIRST'
            elif form == 'residual':
                w.append(r.choice(['f.x > d.pn', 'f.x = d.pn OR f.q = 1', 'f.q < d.pn', 'NOT (f.x < d.pn)']))
                sql = f'SELECT count(*) AS n, sum(f.v) AS sv FROM {frm}{wh()}'
            elif form == 'count':
                sql = f'SELECT count(*) AS n, count({col}) AS nn, count(d.pk) AS matched, count(d.name) AS named FROM {frm}{wh()}'
            else:
                frm = f'{F} f, {D} d, {S} s'
                w = [c for c in w if not c.startswith('f.fk = ')] + ['f.fk = d.pk', 'd.nk = s.pk3']
                sql = f'SELECT s.pn3, count(*) AS n, sum(f.v) AS sv FROM {frm}{wh()} GROUP BY s.pn3 ORDER BY s.pn3 NULLS FIRST'
        elif shape == 'strkey':
            form = r.choice(['inner', 'left', 'in', 'notin', 'group', 'like'])
            if form == 'inner':
                sql = f'SELECT ds.sg, count(*) AS n, sum(f.v) AS sv FROM {F} f JOIN {DS} ds ON f.sk = ds.spk{wh()} GROUP BY ds.sg ORDER BY ds.sg'
            elif form == 'left':
                sql = (f'SELECT count(*) AS n, count(ds.spk) AS matched, count(ds.spn) AS nn, sum(ds.spn) AS s '
                       f'FROM {F} f LEFT JOIN {DS} ds ON f.sk = ds.spk{wh()}')
            elif form == 'in':
                w.append(f"f.sk IN (SELECT spk FROM {DS} WHERE sg {r.choice(['=', '<>'])} {r.randint(0, 2)})")
                sql = f'SELECT count(*) AS n, sum(f.v) AS sv FROM {F} f{wh()}'
            elif form == 'notin':
                w.append(f'f.sk NOT IN (SELECT spk FROM {DS})')
                sql = f'SELECT count(*) AS n, sum(f.v) AS sv FROM {F} f{wh()}'
            elif form == 'group':
                sql = (f'SELECT ds.spk, ds.snm, count(*) AS n FROM {F} f JOIN {DS} ds ON f.sk = ds.spk{wh()} '
                       f'GROUP BY ds.spk, ds.snm ORDER BY ds.spk, ds.snm')
            else:
                w.append(r.choice(["ds.spk LIKE 'key%'", "ds.spk = ''", "length(ds.spk) > 2", "ds.spk > 'k'"]))
                sql = f'SELECT count(*) AS n, sum(f.v) AS sv FROM {F} f JOIN {DS} ds ON f.sk = ds.spk{wh()}'
        elif shape == 'composite':
            on = ['f.c1 = dc.c1', 'f.c2 = dc.c2'] + (['f.c3 = dc.c3'] if r.random() < 0.6 else [])
            r.shuffle(on)
            form = r.choice(['inner', 'inner-where', 'left', 'group', 'exists'])
            if form == 'inner':
                sql = f"SELECT count(*) AS n, sum(f.v) AS sv, sum(dc.ca) AS sa FROM {F} f JOIN {DC} dc ON {' AND '.join(on)}{wh()}"
            elif form == 'inner-where':
                frm = f'{F} f, {DC} dc'; w += on
                sql = f'SELECT dc.c1, count(*) AS n, sum(dc.cpn) AS sp FROM {frm}{wh()} GROUP BY dc.c1 ORDER BY dc.c1 NULLS FIRST'
            elif form == 'left':
                sql = (f"SELECT count(*) AS n, count(dc.ca) AS matched, count(dc.cpn) AS nn FROM {F} f LEFT JOIN {DC} dc "
                       f"ON {' AND '.join(on)}{wh()}")
            elif form == 'group':
                sql = (f"SELECT dc.c2, dc.c3, count(*) AS n FROM {F} f JOIN {DC} dc ON {' AND '.join(on)}{wh()} "
                       f"GROUP BY dc.c2, dc.c3 ORDER BY dc.c2 NULLS FIRST, dc.c3 NULLS FIRST")
            else:
                w.append(f"{r.choice(['', 'NOT '])}EXISTS (SELECT 1 FROM {DC} dc WHERE {' AND '.join(on)})")
                sql = f'SELECT count(*) AS n, sum(f.v) AS sv FROM {F} f{wh()}'
        elif shape == 'self':
            form = r.choice(['parent-group', 'parent-left', 'compare', 'rows', 'two-level', 'both-copies', 'role', 'role-rows', 'name-in-both'])
            if form == 'parent-group':
                sql = f'SELECT m.egrp, count(*) AS n FROM {E} e JOIN {E} m ON e.boss = m.eid GROUP BY m.egrp ORDER BY m.egrp'
            elif form == 'parent-left':
                sql = f'SELECT count(*) AS n, count(m.eid) AS with_boss, count(m.esal) AS boss_sal FROM {E} e LEFT JOIN {E} m ON e.boss = m.eid'
            elif form == 'compare':
                sql = (f"SELECT count(*) AS n FROM {E} e JOIN {E} m ON e.boss = m.eid "
                       f"WHERE {r.choice(['e.esal > m.esal', 'e.egrp = m.egrp', 'e.ename <> m.ename', 'e.esal IS NULL OR m.esal IS NULL'])}")
            elif form == 'rows':
                sql = (f'SELECT e.ename, m.ename AS boss_name FROM {E} e LEFT JOIN {E} m ON e.boss = m.eid '
                       f'ORDER BY e.ename, boss_name NULLS FIRST LIMIT {lim()}')
            elif form == 'both-copies':   # each copy's column beside the other's: a reference bound to the wrong copy shows
                sql = (f'SELECT e.egrp, m.egrp AS boss_grp, e.ename, m.ename AS boss_name, count(*) AS n FROM {E} e JOIN {E} m ON e.boss = m.eid '
                       f'GROUP BY e.egrp, m.egrp, e.ename, m.ename ORDER BY 1, 2, 3, 4 LIMIT {lim()}')
            elif form == 'role':   # one dimension in two roles
                cmp = r.choice(['a.w < b.w', 'a.grp = b.grp', 'a.name <> b.name', 'a.w + b.w > 20'])
                sql = (f'SELECT a.grp AS ag, b.grp AS bg, count(*) AS n, sum(a.w - b.w) AS dw FROM {F} f JOIN {D} a ON f.fk = a.pk '
                       f'JOIN {D} b ON f.fk2 = b.pk WHERE {cmp} GROUP BY a.grp, b.grp ORDER BY ag, bg')
            elif form == 'role-rows':
                sql = (f'SELECT a.grp, b.grp AS bg, b.name AS bn FROM {F} f JOIN {D} a ON f.fk = a.pk JOIN {D} b ON f.fk2 = b.pk{wh()} '
                       f'ORDER BY a.grp, b.grp, b.name, a.w LIMIT {lim()} OFFSET {r.choice([0, 3, 30])}')
            elif form == 'name-in-both':   # the probe's column and a build's of one name (the probe is the larger file)
                sql = (f'SELECT x.grp, count(*) AS n, sum(x.w) AS big, sum(y.w) AS small FROM {D} x JOIN {GD} y ON x.grp = y.pk '
                       f"WHERE {r.choice(['x.w <> y.w', 'x.name > y.name', 'x.grp = y.grp OR x.w > 3'])} GROUP BY x.grp ORDER BY x.grp")
            else:
                sql = (f'SELECT g.egrp, count(*) AS n FROM {E} e JOIN {E} m ON e.boss = m.eid JOIN {E} g ON m.boss = g.eid '
                       f'GROUP BY g.egrp ORDER BY g.egrp')
        elif shape == 'rows':
            form = r.choice(['probe-cols', 'build-cols', 'distinct', 'left', 'semi', 'group-offset',
                             'hidden-order', 'anti', 'expr', 'unordered', 'star', 'strings', 'alias'])
            frm = f'{F} f JOIN {D} d ON f.fk = d.pk'
            L, O = lim(), r.choice([0, 0, 1, 5, 40])
            if form == 'probe-cols':
                sql = f'SELECT f.dup, f.q FROM {frm}{wh()} ORDER BY f.dup, f.q LIMIT {L} OFFSET {O}'
            elif form == 'build-cols':
                sql = f'SELECT f.dup, d.grp, d.ps FROM {frm}{wh()} ORDER BY f.dup, d.grp, d.ps NULLS FIRST LIMIT {L} OFFSET {O}'
            elif form == 'distinct':
                sql = f'SELECT DISTINCT f.dup, d.grp FROM {frm}{wh()} ORDER BY f.dup, d.grp'
            elif form == 'left':
                sql = (f'SELECT f.dup, d.w FROM {F} f LEFT JOIN {D} d ON f.fk = d.pk{wh()} '
                       f'ORDER BY f.dup, d.w NULLS FIRST LIMIT {L} OFFSET {O}')
            elif form == 'semi':
                w.append(f'f.fk IN (SELECT pk FROM {D} WHERE grp = {r.randint(0, 3)})')
                sql = f'SELECT f.dup, f.q FROM {F} f{wh()} ORDER BY f.dup, f.q LIMIT {L} OFFSET {O}'
            elif form == 'hidden-order':   # ranked by a column the rows do not show
                sql = f'SELECT f.dup, d.grp FROM {frm}{wh()} ORDER BY f.q {r.choice(["", "DESC"])}, f.dup, d.grp LIMIT {L} OFFSET {O}'
            elif form == 'anti':
                w.append(r.choice([f'f.fk NOT IN (SELECT pk FROM {D} WHERE grp = {r.randint(0, 3)})',
                                   f'NOT EXISTS (SELECT 1 FROM {D} d WHERE d.pk = f.fk AND d.grp <> {r.randint(0, 3)})']))
                sql = f'SELECT f.dup, f.q FROM {F} f{wh()} ORDER BY f.dup, f.q LIMIT {L} OFFSET {O}'
            elif form == 'expr':
                sql = f'SELECT f.dup + 1 AS a, d.grp * 2 AS b, upper(d.name) AS nm FROM {frm}{wh()} ORDER BY a, b DESC, nm LIMIT {L} OFFSET {O}'
            elif form == 'unordered':   # no ORDER BY: the whole multiset
                sql = f'SELECT f.dup, d.grp, f.q FROM {frm}{wh()}'
            elif form == 'star':
                w += ['f.fk = d.pk', 'f.fk2 = s.pk3']
                sql = f'SELECT f.dup, d.grp, s.g3 FROM {F} f, {D} d, {S} s{wh()} ORDER BY 1, 2, 3 LIMIT {L} OFFSET {O}'
            elif form == 'alias':   # an alias named as a column of the probe, the build or the key: bare it is the item, qualified the column
                name = r.choice(['dup', 'grp', 'pk', 'fk'])
                item = r.choice(['f.q', 'f.id % 5'])
                alias = r.choice([name, f'"{name}"', f'"{name.capitalize()}"'])
                by = r.choice([name, f'"{name}"', name.upper(), ('f.' if name in ('dup', 'fk') else 'd.') + name])
                sql = (f"SELECT {item} AS {alias}, d.w FROM {frm}{wh()} ORDER BY {by} {r.choice(['', 'DESC'])}, f.id LIMIT {L} OFFSET {O}")
            elif form == 'strings':   # a build's strings under a LIMIT, which are fetched after the cut
                sql = f'SELECT f.dup, d.name FROM {frm}{wh()} ORDER BY f.dup, d.name LIMIT {L} OFFSET {O}'
            else:
                sql = f'SELECT d.grp, f.dup, count(*) AS n FROM {frm}{wh()} GROUP BY d.grp, f.dup ORDER BY n, d.grp, f.dup LIMIT {L} OFFSET {O}'
        else:  # exists
            form = r.choice(['exists', 'not-exists', 'not-in', 'in', 'exists-cond', 'not-exists-str', 'both', 'bare', 'in-join',
                             'named', 'implicit'])
            sql = f'SELECT count(*) AS n, sum(f.v) AS sv FROM {F} f'
            if form == 'named':   # no name a query can spell changes its meaning: the frontend's own, as aliases
                al = lambda: r.choice(['__zpq_not_exists', '__zpq_using_left', '__zpq_using_right', '__zpq_group_all', '__zpq_order_0'])
                x, y = al(), al()
                sub = r.choice([f'f.fk2 NOT IN (SELECT nk AS {al()} FROM {D} {y})', f'f.fk2 IN (SELECT nk AS {al()} FROM {D} {y})',
                                f'NOT EXISTS (SELECT nk AS {al()} FROM {D} {y} WHERE {y}.nk = f.fk2)'])
                w = [c.replace('f.', x + '.') for c in w + [sub]]
                sql = f'SELECT count(*) AS n, sum({x}.v) AS sv FROM {F} {x}'
            elif form == 'implicit':   # a file without an alias goes by its own name, outside and inside the subquery
                w.append(r.choice([f'f.fk2 IN (SELECT d.nk FROM {D} WHERE d.grp < 3)', f'f.fk2 NOT IN (SELECT nk FROM {D})',
                                   f'F.fk2 NOT IN (SELECT d.nk FROM {D} WHERE d.nk IS NOT NULL)',
                                   f'EXISTS (SELECT 1 FROM {D} WHERE d.nk = f.fk2)', f'NOT EXISTS (SELECT 1 FROM {D} WHERE d.nk = f.fk2)']))
                sql = f'SELECT count(*) AS n, sum(f.v) AS sv FROM {F}'
            if form in ('named', 'implicit'): pass
            elif form == 'exists': w.append(f'EXISTS (SELECT 1 FROM {D} d WHERE d.nk = f.fk2)')
            elif form == 'both':   # two of them on one table, beside the NOT IN whose NULLs differ
                w.append(f'EXISTS (SELECT 1 FROM {DU} u WHERE u.dpk = f.fk)')
                w.append(f"NOT EXISTS (SELECT 1 FROM {D} d WHERE f.fk2 = d.nk AND d.grp <> {r.randint(0, 3)})")
                if r.random() < 0.5: w.append(f'f.ek NOT IN (SELECT pk FROM {D} WHERE w > 3)')
            elif form == 'bare':   # no alias on the subquery's table, SELECT *, the key either way round
                w.append(f"{r.choice(['', 'NOT '])}EXISTS (SELECT * FROM {D} WHERE {r.choice(['nk = f.fk2', 'f.fk2 = nk'])} AND w < {r.randint(0, 30)})")
            elif form == 'in-join':   # on the probe and on a build of a join
                w.append(f"{r.choice(['', 'NOT '])}EXISTS (SELECT 1 FROM {DU} u WHERE u.dpk = {r.choice(['f.fk2', 'd.pk'])})")
                sql = f'SELECT d.grp, count(*) AS n, sum(f.v) AS sv FROM {F} f JOIN {D} d ON f.fk = d.pk'
            elif form == 'not-exists': w.append(f'NOT EXISTS (SELECT 1 FROM {D} d WHERE d.nk = f.fk2)')
            elif form == 'not-in': w.append(f'f.fk2 NOT IN (SELECT nk FROM {D})')
            elif form == 'in': w.append(f'f.fk2 IN (SELECT nk FROM {D})')
            elif form == 'exists-cond': w.append(f"{r.choice(['', 'NOT '])}EXISTS (SELECT 1 FROM {D} d WHERE d.nk = f.fk2 AND d.grp = {r.randint(0, 3)})")
            else: w.append(f'NOT EXISTS (SELECT 1 FROM {DS} ds WHERE ds.spk = f.sk)')
            sql += wh() + (' GROUP BY d.grp ORDER BY d.grp' if form == 'in-join' else '')
        out.append({'id': f's{seed}-{i}-{shape}', 'shape': shape, 'sql': sql,
                    'j': r.choice([1, 4, 16] if shape == 'globfk' else [1, 2, 8])})
    return out


def parse_seeds(s):
    out = []
    for part in s.split(','):
        a, _, b = part.partition('-')
        out += list(range(int(a), int(b or a) + 1))
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    ap.add_argument('--zpq', default='zig-out/bin/zpq')
    ap.add_argument('--duckdb', default=os.environ.get('ZPQ_DUCKDB', common.DEFAULT_DUCKDB))
    ap.add_argument('--seeds', default='1-12')
    ap.add_argument('--queries', type=int, default=40, help='per seed')
    ap.add_argument('--shapes', default=','.join(SHAPES))
    ap.add_argument('--dir', default=None)
    ap.add_argument('--json', default=None)
    ap.add_argument('--keep', action='store_true')
    ap.add_argument('--timeout', type=int, default=120)
    ap.add_argument('-v', '--verbose', action='store_true')
    args = ap.parse_args()
    shapes = args.shapes.split(',')
    bad_shapes = set(shapes) - set(SHAPES)
    if bad_shapes:
        raise SystemExit('unknown shapes: ' + ','.join(sorted(bad_shapes)))
    if args.dir is None:
        if not os.environ.get('ZPQ_TEST_PARQUET_DIR'):
            raise SystemExit('set --dir or ZPQ_TEST_PARQUET_DIR for the generated tables')
        args.dir = os.path.join(os.environ['ZPQ_TEST_PARQUET_DIR'], 'joins-differential')
    args.dir = os.path.abspath(args.dir)
    eng = common.zpq_version(args.zpq)
    total, by_shape, duck_err, fails, results = Counter(), defaultdict(Counter), 0, [], []
    for seed in parse_seeds(args.seeds):
        p = params(seed)
        T = os.path.join(args.dir, f's{seed}')
        os.makedirs(T, exist_ok=True)
        g = common.run_cmd([args.duckdb, '-no-agent', '-c', gen_sql(seed, p, T)], timeout=600)
        if g['rc'] != 0:
            raise SystemExit(f'seed {seed}: DuckDB could not build the tables: {common.first_line(g["err"])}')
        for q in queries(seed, p, T, args.queries, shapes):
            d = common.run_duckdb(args.duckdb, q['sql'], timeout=args.timeout)
            if d['rc'] != 0:
                duck_err += 1
                if args.verbose:
                    print(f"{q['id']:24} duckdb-error {common.first_line(d['err'])[:120]}")
                continue
            z = common.run_zpq(args.zpq, q['sql'], threads=q['j'], timeout=args.timeout)
            cls, detail = common.classify(q['sql'], d['out'], z)
            total[cls] += 1
            by_shape[q['shape']][cls] += 1
            rec = dict(q, seed=seed, params={k: v for k, v in p.items() if k != 'strs'}, cls=cls, detail=detail)
            results.append(rec)
            if cls in ('wrong', 'crash'):
                fails.append(rec)
                print(f"{cls.upper():6} {q['id']} -j{q['j']} {detail}\n  {q['sql']}", flush=True)
            elif args.verbose:
                print(f"{q['id']:24} {cls:15} {detail[:120]}")
        if not args.keep:
            shutil.rmtree(T, ignore_errors=True)
    if not args.keep:
        try:
            os.rmdir(args.dir)
        except OSError:
            pass
    print('shape        ' + ' '.join(f'{c:>15}' for c in common.CLASSES))
    for s in shapes:
        print(f'{s:12} ' + ' '.join(f'{by_shape[s][c]:15}' for c in common.CLASSES))
    n = sum(total.values())
    ref = total['refused-named'] + total['refused-unnamed']
    print(f"joins-differential seeds {args.seeds} x {args.queries} ({n} run, {duck_err} DuckDB errors skipped) {eng}: "
          f"{total['correct']} correct, {ref} refused ({total['refused-named']} named, {total['refused-unnamed']} unnamed), "
          f"{total['wrong']} wrong, {total['crash']} crash")
    if args.json:
        json.dump({'engine': eng, 'seeds': args.seeds, 'queries': args.queries, 'totals': total, 'results': results},
                  open(args.json, 'w'), indent=1, ensure_ascii=False)
    return 1 if fails else 0


if __name__ == '__main__':
    sys.exit(main())
