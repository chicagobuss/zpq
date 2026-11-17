"""Shared by suite.py and differential.py: run zpq or DuckDB on one query, compare an answer with DuckDB's, classify the
outcome, and a small S3 client (SigV4, standard library only) for pulling the suite's data. Credentials come from the
environment only and never appear in output, argv or files."""
import csv, datetime, hashlib, hmac, io, os, re, subprocess, time, urllib.error, urllib.parse, urllib.request
import xml.etree.ElementTree as ET
from decimal import Decimal, InvalidOperation

DEFAULT_DUCKDB = os.path.expanduser('~/.cache/zpq-duckdb2/bin-2.0.0/duckdb')

# ---------------------------------------------------------------- answers
_TS = re.compile(r'^(\d{4}-\d\d-\d\d)[T ](\d\d:\d\d:\d\d)(\.\d+)?(Z|\+00(:00)?)?$')


def _cell(v):
    if v in ('', 'NULL'):
        return None
    m = _TS.match(v)
    if m:
        frac = (m.group(3) or '').rstrip('0')
        return ('t', m.group(1) + ' ' + m.group(2) + ('' if frac in ('', '.') else frac))
    lv = v.lower()
    if lv in ('true', 'false'):
        return ('b', lv)
    try:
        d = Decimal(v)
        return ('n', d) if d.is_finite() else ('s', lv)
    except InvalidOperation:
        return ('s', v)


def rows(text):
    """CSV with a header row (both engines' --format csv / -csv) to tuples of normalized cells.
    NULL and the empty string both read as None: CSV can't tell them apart without quoting rules."""
    r = list(csv.reader(io.StringIO(text)))
    return [tuple(_cell(v) for v in row) for row in r[1:]] if r else []


def _ceq(a, b, notes):
    if a == b:
        return True
    if a is None or b is None or a[0] != b[0]:
        return False
    if a[0] != 'n':
        return False
    fa, fb = float(a[1]), float(b[1])
    if abs(fa - fb) <= 1e-9 * max(abs(fa), abs(fb), 1e-300):
        return True  # summation order
    # zpq's avg over DECIMAL(p,s) is DECIMAL(q, s+q-p) with q = min(38, p+4), and DECIMAL(38,s) stays DECIMAL(38,s):
    # it prints at that fixed scale, while DuckDB's avg is a DOUBLE. Accept zpq's value (b) when it has 1-6 fractional
    # digits, DuckDB's (a) is a full-precision double (12+ digits), and they agree to half a unit of zpq's last place.
    sb = -b[1].as_tuple().exponent
    if 1 <= sb <= 6 and len(a[1].as_tuple().digits) >= 12 and abs(fa - fb) <= 0.5 * 10.0 ** -sb * (1 + 1e-9):
        notes.add('DECIMAL avg at its own scale')
        return True
    return False


def _req(x, y, notes):
    return len(x) == len(y) and all(_ceq(a, b, notes) for a, b in zip(x, y))


def _skey(row):
    return tuple((0, '', '') if c is None else (1, c[0], '%.6g' % float(c[1]) if c[0] == 'n' else c[1]) for c in row)


def _multiset_eq(a, b, notes):
    if len(a) != len(b):
        return False
    sa, sb = sorted(a, key=_skey), sorted(b, key=_skey)
    if all(_req(x, y, notes) for x, y in zip(sa, sb)):
        return True
    left = list(sb)  # a rounding boundary can misalign the sort: match greedily
    for x in sa:
        for i, y in enumerate(left):
            if _req(x, y, notes):
                del left[i]
                break
        else:
            return False
    return True


def outer_order_by(sql):
    """True when the outermost query has ORDER BY (not one inside a subquery or CTE)."""
    depth, found = 0, False
    for m in re.finditer(r"'(?:[^']|'')*'|\(|\)|\bORDER\s+BY\b", sql, re.I):
        t = m.group(0)
        if t == '(':
            depth += 1
        elif t == ')':
            depth -= 1
        elif t[0] != "'" and depth == 0:
            found = True
    return found


def same_answer(sql, expected, got):
    """(equal, note). In order when the outer query orders; else as multisets. Floats to 1e-9 relative."""
    notes = set()
    e, g = rows(expected), rows(got)
    if outer_order_by(sql):
        if _req_all(e, g, notes):
            return True, '; '.join(sorted(notes))
        if _multiset_eq(e, g, notes):
            # same rows, another order: only a tie under ORDER BY can explain it; report it, don't pass it silently
            return False, f'same {len(e)} rows in another order (ORDER BY ties?)'
        return False, f'expected {len(e)} rows, got {len(g)}'
    if _multiset_eq(e, g, notes):
        return True, '; '.join(sorted(notes))
    return False, f'expected {len(e)} rows, got {len(g)}'


def _req_all(a, b, notes):
    return len(a) == len(b) and all(_req(x, y, notes) for x, y in zip(a, b))


# ---------------------------------------------------------------- classification
CRASH = re.compile(r'panic|reached unreachable|Segmentation fault|index out of bounds|integer overflow|'
                   r'attempt to use null value|\bthread \d+ panic|\berror\.[A-Z]\w+', re.I)
# A refusal is "unnamed" when its text names no SQL feature: a bare error name, or one of the messages an analyst
# can't act on. Everything else that exits 1 or 2 with a message is a refusal by name.
UNNAMED = [re.compile(p) for p in (
    r'^zpq query: [A-Z][A-Za-z0-9]+$',               # FileNotFound, UnsupportedFromClause, BadColumn, ...
    r'^[a-z]+: [A-Z][A-Za-z0-9]+$',
    r'unknown column referenced in expression',
    r'`` is an expression',
    r'is ambiguous: several top-level columns have this name',
)]
CLASSES = ('correct', 'refused-named', 'refused-unnamed', 'wrong', 'crash')


def first_line(err):
    for line in err.strip().splitlines():
        if line.strip():
            return line.strip()
    return ''


def refusal_text(err, strip_paths=True):
    line = first_line(err)
    return re.sub(r"'(?:/|s3://)[^']*'", "'<file>'", line) if strip_paths else line


def classify(sql, expected, run):
    """run: {'rc', 'out', 'err'}. Returns (class, detail)."""
    rc, err = run['rc'], run['err']
    if rc == 'timeout':
        return 'crash', 'timeout'
    if rc not in (0, 1, 2) or CRASH.search(err):
        return 'crash', f'rc {rc}: ' + (CRASH.search(err).group(0) if CRASH.search(err) else first_line(err))[:200]
    if rc != 0:
        text = refusal_text(err)
        return ('refused-unnamed' if any(p.search(text) for p in UNNAMED) else 'refused-named'), text[:240]
    ok, note = same_answer(sql, expected, run['out'])
    return ('correct', note) if ok else ('wrong', note)


# ---------------------------------------------------------------- engines
def run_cmd(cmd, stdin=None, env=None, timeout=600):
    t0 = time.perf_counter()
    try:
        p = subprocess.run(cmd, input=stdin, capture_output=True, text=True, timeout=timeout, env=env)
        return {'rc': p.returncode, 'out': p.stdout, 'err': p.stderr[-4000:], 't': time.perf_counter() - t0}
    except subprocess.TimeoutExpired:
        return {'rc': 'timeout', 'out': '', 'err': f'timeout after {timeout}s', 't': time.perf_counter() - t0}


def run_zpq(zpq, sql, threads=None, timeout=600, extra=()):
    cmd = [zpq, 'query', '--query', sql, '--format', 'csv'] + (['-j', str(threads)] if threads else []) + list(extra)
    return run_cmd(cmd, timeout=timeout)


def duckdb_s3_secret():
    """CREATE SECRET text from the environment (S3_* first, then AWS_*), or None. Goes to DuckDB on stdin only."""
    c = s3_credentials()
    if c is None:
        return None
    ep = urllib.parse.urlparse(c['endpoint']) if c['endpoint'] else None
    parts = [f"TYPE s3", f"KEY_ID '{c['key']}'", f"SECRET '{c['secret']}'", f"REGION '{c['region']}'"]
    if c['token']:
        parts.append(f"SESSION_TOKEN '{c['token']}'")
    if ep:
        parts += [f"ENDPOINT '{ep.netloc}'", "URL_STYLE 'path'", f"USE_SSL {'true' if ep.scheme == 'https' else 'false'}"]
    return 'CREATE SECRET joins_suite (' + ', '.join(parts) + ');\n'


def run_duckdb(duckdb, sql, s3=False, threads=None, memory='8GB', timeout=600, extension_dir=None):
    pre = f"SET memory_limit='{memory}';\n" + (f"SET threads={threads};\n" if threads else '')
    if s3:
        if extension_dir:
            pre += f"SET extension_directory='{extension_dir}';\n"
        secret = duckdb_s3_secret()
        if secret is None:
            raise SystemExit('DuckDB over S3 needs S3_ACCESS_KEY_ID/S3_SECRET_ACCESS_KEY (or AWS_*) in the environment')
        pre += 'INSTALL httpfs; LOAD httpfs;\n' + secret
    r = run_cmd([duckdb, '-no-agent', '-csv'], stdin=pre + sql + ';\n', timeout=timeout)
    out = r['out']
    if '\x1b[' in out:  # a deprecation warning printed to stdout
        out = out[out.rindex('\x1b[00m') + 5:].lstrip('\n')
    while out.startswith('Success\n') or out.startswith('true\n'):  # CREATE SECRET's result row
        out = out.split('\n', 1)[1]
    r['out'] = out
    return r


def zpq_version(zpq):
    return run_cmd([zpq, '--version'], timeout=30)['out'].strip() or '?'


# ---------------------------------------------------------------- S3 (SigV4, path-style)
def s3_credentials():
    g = os.environ.get
    key, secret = g('S3_ACCESS_KEY_ID') or g('AWS_ACCESS_KEY_ID'), g('S3_SECRET_ACCESS_KEY') or g('AWS_SECRET_ACCESS_KEY')
    if not key or not secret:
        return None
    return {'key': key, 'secret': secret, 'token': g('S3_SESSION_TOKEN') or g('AWS_SESSION_TOKEN'),
            'region': g('S3_REGION') or g('AWS_REGION') or g('AWS_DEFAULT_REGION') or 'us-east-1',
            'endpoint': (g('S3_ENDPOINT_URL') or g('AWS_ENDPOINT_URL') or '').rstrip('/')}


class S3:
    def __init__(self):
        c = s3_credentials()
        if c is None:
            raise SystemExit('S3 needs S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY (or AWS_*) in the environment')
        self.c = c
        self.base = c['endpoint'] or f"https://s3.{c['region']}.amazonaws.com"

    def _request(self, bucket, key='', query=None):
        q = sorted((query or {}).items())
        u = urllib.parse.urlparse(self.base)
        path = '/' + urllib.parse.quote(bucket, safe='') + ('/' + urllib.parse.quote(key, safe='/~') if key else '/')
        cq = '&'.join(urllib.parse.quote(k, safe='-_.~') + '=' + urllib.parse.quote(v, safe='-_.~') for k, v in q)
        now = datetime.datetime.now(datetime.timezone.utc)
        amz, day = now.strftime('%Y%m%dT%H%M%SZ'), now.strftime('%Y%m%d')
        h = {'host': u.netloc, 'x-amz-content-sha256': 'UNSIGNED-PAYLOAD', 'x-amz-date': amz}
        if self.c['token']:
            h['x-amz-security-token'] = self.c['token']
        names = sorted(h)
        creq = '\n'.join(['GET', path, cq, ''.join(f'{n}:{h[n]}\n' for n in names), ';'.join(names), 'UNSIGNED-PAYLOAD'])
        scope = f"{day}/{self.c['region']}/s3/aws4_request"
        sts = '\n'.join(['AWS4-HMAC-SHA256', amz, scope, hashlib.sha256(creq.encode()).hexdigest()])
        k = ('AWS4' + self.c['secret']).encode()
        for part in (day, self.c['region'], 's3', 'aws4_request'):
            k = hmac.new(k, part.encode(), hashlib.sha256).digest()
        sig = hmac.new(k, sts.encode(), hashlib.sha256).hexdigest()
        h['Authorization'] = (f"AWS4-HMAC-SHA256 Credential={self.c['key']}/{scope}, "
                              f"SignedHeaders={';'.join(names)}, Signature={sig}")
        del h['host']
        return urllib.request.Request(f'{u.scheme}://{u.netloc}{path}' + ('?' + cq if cq else ''), headers=h)

    def _open(self, req, what):
        try:
            return urllib.request.urlopen(req, timeout=120)
        except urllib.error.HTTPError as e:
            raise SystemExit(f'S3 {what}: HTTP {e.code} {e.reason}') from None
        except urllib.error.URLError as e:
            raise SystemExit(f'S3 {what}: {e.reason}') from None

    def list(self, bucket, prefix):
        """[(key, size)] under prefix."""
        out, token = [], None
        while True:
            q = {'list-type': '2', 'prefix': prefix}
            if token:
                q['continuation-token'] = token
            body = self._open(self._request(bucket, query=q), f'list s3://{bucket}/{prefix}').read()
            root = ET.fromstring(body)
            ns = root.tag[:root.tag.index('}') + 1] if root.tag.startswith('{') else ''
            for c in root.iter(ns + 'Contents'):
                out.append((c.find(ns + 'Key').text, int(c.find(ns + 'Size').text)))
            if (root.findtext(ns + 'IsTruncated') or 'false') != 'true':
                return out
            token = root.findtext(ns + 'NextContinuationToken')

    def get(self, bucket, key, dest):
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        part = dest + '.part'
        with self._open(self._request(bucket, key), f'get s3://{bucket}/{key}') as r, open(part, 'wb') as f:
            while True:
                b = r.read(1 << 20)
                if not b:
                    break
                f.write(b)
        os.replace(part, dest)
