"""Case registry and run batteries shared by the torture generators."""

import io

CASES = []


class Case:
    def __init__(self, name, bullet, fn):
        self.name = name
        self.bullet = bullet
        self.fn = fn
        self.known = None

    def build(self, path):
        spec = self.fn()
        data = spec.pop("file")
        if callable(data):
            data(path)
        else:
            with open(path, "wb") as f:
                f.write(data)
        return spec


def case(name, bullet, big=False, known=None):
    """`known`: the FINDINGS.md id of a filed bug this case reproduces; its failures are reported as KNOWN."""
    def deco(fn):
        c = Case(name, bullet, fn)
        c.big = big
        c.known = known
        CASES.append(c)
        return fn

    return deco


def E(kind, value=None):
    return {"kind": kind, "value": value}


def run(label, argv, expect, **kw):
    d = {"label": label, "argv": argv, "expect": expect}
    d.update(kw)
    return d


def battery(kind, rows=None, agg=None, agg_value=None, count=None, count_kind=None, filt=None, filt_rows=None,
            write=True, inspect=True, write_kind=None, select=None):
    """The standard reader battery. `kind` is the expectation for every data-decoding path ("err", "err|eq", "eq");
    `rows` the true rows, `agg`/`agg_value` an aggregate and its true answer, `count` the true count(*) (answered
    from metadata, so it gets its own expectation, `count_kind`, defaulting to `kind`)."""
    runs = []
    runs.append(run("count", ["query", "{F}", "--aggregate", "count(*) AS n"],
                    E(count_kind or kind, [{"n": count}] if count is not None else None)))
    if rows is not None or "eq" not in kind:
        runs.append(run("rows", ["query", "{F}"], E(kind, rows)))
    if agg is not None:
        runs.append(run("agg", ["query", "{F}", "--aggregate", agg], E(kind, [agg_value])))
        runs.append(run("agg-scanall", ["query", "{F}", "--scan-all", "--aggregate", agg], E(kind, [agg_value])))
        runs.append(run("agg-j1", ["query", "{F}", "-j", "1", "--aggregate", agg], E(kind, [agg_value])))
    if filt is not None:
        runs.append(run("filter", ["query", "{F}", "--filter", filt], E(kind, filt_rows)))
        runs.append(run("filter-write", ["query", "{F}", "--filter", filt, "-o", "{O}"], E(kind, filt_rows)))
    if write:
        # A plain `-o` copies pages without decoding them, so a page-level lie in a sound footer is copied as is.
        wk = write_kind or ("safe" if count_kind == "eq" and kind != "eq" else kind)
        runs.append(run("write", ["query", "{F}", "-o", "{O}"], E(wk, rows)))
    if select is not None:
        runs.append(run("select-write", ["query", "{F}", "--select", select, "-o", "{O}"], E(kind, rows)))
    if inspect:
        runs.append(run("inspect", ["inspect", "{F}", "--detail", "--format", "json"], E("safe")))
        runs.append(run("schema", ["schema", "{F}"], E("safe")))
    return runs
