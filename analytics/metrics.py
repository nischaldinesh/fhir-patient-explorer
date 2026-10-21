#!/usr/bin/env python3
"""The semantic layer's front door: query dbt MetricFlow metrics with small cell suppression applied.

The metrics are defined once in analytics/dbt/models/semantic/ (see analytics/SEMANTIC_LAYER.md). MetricFlow itself
is internal: it returns raw counts. Everything that leaves the warehouse goes through this command, which hides
every count from 1 to 10 (the CMS cell size rule of is_small_cell in analytics/dbt/macros/marts.sql):

  count  a count of 1 to 10 is hidden
  rate   hidden when the denominator, the numerator or the denominator minus the numerator is 1 to 10
  mean   hidden when the denominator (a count of stays) is 1 to 10
  none   shown (master data such as beds, not people)

A hidden value is empty (None) and the row's is_suppressed is true. A zero is shown. The rule of each metric is in its
config.meta in models/semantic/_metrics.yml. When a rate's numerator and denominator are both asked for, they are
checked together, so their difference cannot give a small count back.

Commands:
  query      mf query with suppression: --metrics a,b --group-by metric_time__month,encounter__source, or
             --saved-query ops_daily; prints a table, or writes --csv / --json
  validate   dbt parse, then mf validate-configs (semantic checks and checks against the warehouse)
  list       the metrics catalog: name, type, suppression rule, owner, label

Usage:
  python3 analytics/metrics.py query --metrics admissions,readmission_rate_30d --group-by encounter__source
  python3 analytics/metrics.py query --saved-query quality_all_time --json out.json
  python3 analytics/metrics.py validate
"""
import argparse
import csv
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import build  # noqa: E402

DBT_DIR = build.DBT_DIR
SEMANTIC = DBT_DIR / "models" / "semantic"
RULES = ("count", "rate", "mean", "none")


# ---------- the catalog (pure) ----------

def load_yaml(path):
    import yaml
    return yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}


def catalog(folder=SEMANTIC):
    """{metric name: {label, description, type, owner, suppression, numerator, denominator}} from _metrics.yml."""
    out = {}
    for m in load_yaml(Path(folder) / "_metrics.yml").get("metrics", []):
        meta = (m.get("config") or {}).get("meta") or {}
        out[m["name"]] = {"label": m.get("label"), "description": m.get("description"), "type": m.get("type"),
                          "owner": meta.get("owner"), "suppression": meta.get("suppression"),
                          "numerator": meta.get("numerator"), "denominator": meta.get("denominator")}
    return out


def saved_queries(folder=SEMANTIC):
    return {q["name"]: q["query_params"] for q in load_yaml(Path(folder) / "_saved_queries.yml").get("saved_queries", [])}


def group_by_name(spec):
    """A saved query's group by item as mf query takes it: TimeDimension('metric_time', 'day') -> metric_time__day,
    Dimension('encounter__source') -> encounter__source."""
    s = spec.strip()
    m = re.fullmatch(r"TimeDimension\(\s*'([\w]+)'\s*(?:,\s*'(\w+)'\s*)?\)", s)
    if m:
        return m.group(1) + (f"__{m.group(2)}" if m.group(2) else "")
    m = re.fullmatch(r"(?:Dimension|Entity)\(\s*'([\w]+)'\s*\)", s)
    if m:
        return m.group(1)
    raise ValueError(f"unsupported group by: {spec}")


def components(metrics, cat):
    """The metrics to ask MetricFlow for: the ones requested plus the numerator and denominator their rule needs."""
    want = []
    for name in metrics:
        if name not in cat:
            raise ValueError(f"unknown metric: {name} (see: python3 analytics/metrics.py list)")
        for n in (name, cat[name].get("numerator"), cat[name].get("denominator")):
            if n and n not in want:
                want.append(n)
    return want


# ---------- suppression (pure) ----------

def is_small(v):
    """1 to 10: a small cell. 0 and None are not."""
    return v is not None and 1 <= v <= 10


def is_small_cell(numerator, denominator, numerator_is_count):
    """is_small_cell of macros/marts.sql: the denominator is 1 to 10, or, for a count numerator, the numerator or the
    denominator minus the numerator is 1 to 10."""
    if is_small(denominator):
        return True
    if not numerator_is_count:
        return False
    if is_small(numerator):
        return True
    return numerator is not None and denominator is not None and is_small(denominator - numerator)


def hidden(name, row, cat):
    """True when metric name must be hidden in row (a dict holding it and its components)."""
    info = cat[name]
    rule = info.get("suppression")
    if rule not in RULES:
        raise ValueError(f"metric {name} has no suppression rule in config.meta (one of {', '.join(RULES)})")
    if rule == "none":
        return False
    if rule == "count":
        return is_small(row.get(name))
    num, den = row.get(info.get("numerator")), row.get(info.get("denominator"))
    if rule == "rate":
        return is_small_cell(num, den, True)
    return is_small_cell(num, den, False)   # mean: the denominator only


def drop_empty_time_rows(rows, cat):
    """Visits still open have no discharge day, so the metrics dated by discharge get a row with no metric_time and
    nothing in it. Drop rows whose metric_time is empty and whose metrics are all empty or zero."""
    def empty(r):
        times = [k for k in r if k.startswith("metric_time")]
        return times and all(r[k] is None for k in times) and all(not r[k] for k in r if k in cat)
    return [r for r in rows if not empty(r)]


def suppress(rows, metrics, cat):
    """Rows as they may leave the warehouse: the requested metrics (hidden ones None), the group by columns, and
    is_suppressed. Component metrics that were not requested are dropped."""
    out = []
    metrics = list(metrics)
    for row in rows:
        hide = {m for m in metrics if hidden(m, row, cat)}
        # a rate's numerator and denominator shown side by side would give the complement back
        for name, info in cat.items():
            if info.get("suppression") == "rate" and info["numerator"] in metrics and info["denominator"] in metrics:
                if is_small_cell(row.get(info["numerator"]), row.get(info["denominator"]), True):
                    hide |= {info["numerator"], info["denominator"]}
        keep = {k: v for k, v in row.items() if k not in cat}
        for m in metrics:
            keep[m] = None if m in hide else row.get(m)
        keep["is_suppressed"] = bool(hide)
        out.append(keep)
    return out


def complement(raw, out, metrics, cat, group_cols):
    """Complementary suppression of the count metrics in out (the suppressed rows, in the order of raw, the rows before
    suppression). A caller can ask for the same metric without one of the group by columns and subtract the shown cells
    from that total. So for each count metric and each group by column, within every slice of rows that share the other
    columns: while the hidden cells add up to 1 to 10, hide the smallest shown non zero cell too. Rates and means are
    left as they are: their counts are not shown. Returns out (changed in place)."""
    counts = [m for m in metrics if cat[m].get("suppression") == "count"]
    changed = True
    while changed:
        changed = False
        for m in counts:
            for col in group_cols:
                others = [c for c in group_cols if c != col]
                slices = {}
                for i, r in enumerate(raw):
                    slices.setdefault(tuple(r.get(c) for c in others), []).append(i)
                for idx in slices.values():
                    while True:
                        hidden_sum = sum(raw[i].get(m) or 0 for i in idx if out[i][m] is None)
                        shown = [i for i in idx if out[i][m] is not None and raw[i].get(m)]
                        if not is_small(hidden_sum) or not shown:
                            break
                        j = min(shown, key=lambda i: raw[i][m])
                        out[j][m] = None
                        out[j]["is_suppressed"] = True
                        changed = True
    return out


def parse_value(text):
    if text is None or text == "":
        return None
    for cast in (int, float):
        try:
            return cast(text)
        except ValueError:
            pass
    return text


def read_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return [{k: parse_value(v) for k, v in r.items()} for r in csv.DictReader(f)]


# ---------- running MetricFlow ----------

def mf_executable():
    local = Path(sys.executable).parent / "mf"
    found = str(local) if local.exists() else shutil.which("mf")
    if not found:
        sys.exit("MetricFlow is not installed. Run: python3 -m pip install -r analytics/requirements.txt")
    return found


def project_dir(target):
    """mf reads <project>/target/semantic_manifest.json. When dbt writes its target elsewhere (local Airflow mounts the
    repo read only and sets DBT_TARGET_PATH), point mf at a folder next to it that links to the project's files and
    names that target folder target."""
    target = Path(target).resolve()
    if target == (DBT_DIR / "target").resolve():
        return DBT_DIR
    shim = target.parent / "dbt-semantic-project"
    shim.mkdir(parents=True, exist_ok=True)
    for child in DBT_DIR.iterdir():
        if child.name in ("target", "logs"):
            continue
        link = shim / child.name
        if not link.is_symlink():
            link.symlink_to(child)
    link = shim / "target"
    if not link.is_symlink():
        link.symlink_to(target)
    return shim


def parse_project(target, db):
    """dbt parse: writes semantic_manifest.json, which mf reads. Against the warehouse file itself, because the
    manifest names its database (the file name without .duckdb) in every relation."""
    res = build.dbt(["parse"], db, target_path=target, quiet=True)
    if not res.success:
        raise SystemExit("dbt parse failed")


def run_mf(args, db, target):
    """Run one mf command against the DuckDB file db. Returns (exit code, output)."""
    target = Path(target)
    env = dict(os.environ)
    env["DBT_DUCKDB_PATH"] = str(Path(db).resolve())
    env["DBT_PROFILES_DIR"] = str(DBT_DIR)
    env["DBT_LOG_PATH"] = str(target / "logs")
    proj = project_dir(target)
    env["DBT_PROJECT_DIR"] = str(proj)
    proc = subprocess.run([mf_executable(), *args], cwd=str(proj), env=env, capture_output=True, text=True)
    return proc.returncode, proc.stdout + proc.stderr


def raw_query(metrics, group_by=(), where=(), order=(), db=build.DB, target=None, parse=True):
    """Unsuppressed rows from mf query, for use inside the warehouse only (semantic_parity.py)."""
    target = Path(target) if target else build.default_target()
    if parse:
        parse_project(target, db)
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "result.csv"
        args = ["query", "--metrics", ",".join(metrics), "--csv", str(out)]
        if group_by:
            args += ["--group-by", ",".join(group_by)]
        for w in where:
            args += ["--where", w]
        if order:
            args += ["--order", ",".join(order)]
        code, text = run_mf(args, db, target)
        if code != 0 or not out.exists():
            raise RuntimeError(f"mf query failed:\n{text}")
        return read_csv(out)


def query(metrics, group_by=(), where=(), order=(), db=build.DB, target=None, parse=True, cat=None,
          complementary=False):
    """Suppressed rows of the metrics: what may leave the warehouse. complementary: also hide cells a caller could
    work out from a total of another query (the metrics API turns this on)."""
    cat = cat or catalog()
    rows = drop_empty_time_rows(raw_query(components(metrics, cat), group_by, where, order, db, target, parse), cat)
    out = suppress(rows, metrics, cat)
    return complement(rows, out, metrics, cat, list(group_by)) if complementary else out


def resolve_saved_query(name, saved=None):
    """(metrics, group_by, where) of a saved query, in the form mf query takes."""
    saved = saved if saved is not None else saved_queries()
    if name not in saved:
        raise ValueError(f"unknown saved query: {name}")
    q = saved[name]
    return list(q["metrics"]), [group_by_name(g) for g in q.get("group_by", [])], list(q.get("where", []))


# ---------- commands ----------

def split(s):
    return [x.strip() for x in (s or "").split(",") if x.strip()]


def print_table(rows):
    if not rows:
        print("(no rows)")
        return
    cols = list(rows[0])
    cell = [[("" if r[c] is None else str(r[c])) for c in cols] for r in rows]
    widths = [max(len(c), *(len(x[i]) for x in cell)) for i, c in enumerate(cols)]
    print("  ".join(c.ljust(w) for c, w in zip(cols, widths)))
    for x in cell:
        print("  ".join(v.ljust(w) for v, w in zip(x, widths)))


def cmd_query(a):
    if a.saved_query:
        metrics, group_by, where = resolve_saved_query(a.saved_query)
    else:
        metrics, group_by, where = split(a.metrics), split(a.group_by), list(a.where or [])
    if not metrics:
        raise SystemExit("give --metrics or --saved-query")
    rows = query(metrics, group_by, where, split(a.order) or group_by, a.db, a.target)
    if a.csv:
        with open(a.csv, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0]) if rows else [*group_by, *metrics, "is_suppressed"])
            w.writeheader()
            w.writerows(rows)
        print(f"{len(rows)} rows to {a.csv}")
    if a.json:
        Path(a.json).write_text(json.dumps(rows, indent=1, default=str), encoding="utf-8")
        print(f"{len(rows)} rows to {a.json}")
    if not a.csv and not a.json:
        print_table(rows)
    hidden_rows = sum(1 for r in rows if r["is_suppressed"])
    print(f"{len(rows)} rows, {hidden_rows} with a suppressed value (1 to 10 hidden)")
    return 0


def cmd_validate(a):
    target = Path(a.target) if a.target else build.default_target()
    parse_project(target, a.db)
    code, text = run_mf(["validate-configs"] + (["--skip-dw"] if a.skip_dw else []), a.db, target)
    # mf draws spinners with carriage returns; keep the final state of each line
    print("\n".join(line.split("\r")[-1] for line in text.splitlines()))
    if code != 0 or re.search(r"\bERRORS: [1-9]", text):
        print("Semantic layer validation FAILED")
        return 1
    print("Semantic layer validation passed")
    return 0


def cmd_list(a):
    cat = catalog()
    print_table([{"metric": k, "type": v["type"], "suppression": v["suppression"], "owner": v["owner"],
                  "label": v["label"]} for k, v in cat.items()])
    return 0


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=str(build.DB))
    ap.add_argument("--target", default="", help="dbt target folder (default DBT_TARGET_PATH or analytics/dbt/target)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    q = sub.add_parser("query")
    q.add_argument("--metrics", default="")
    q.add_argument("--group-by", default="")
    q.add_argument("--where", action="append")
    q.add_argument("--order", default="")
    q.add_argument("--saved-query", default="")
    q.add_argument("--csv", default="")
    q.add_argument("--json", default="")
    q.set_defaults(func=cmd_query)
    v = sub.add_parser("validate")
    v.add_argument("--skip-dw", action="store_true", help="semantic checks only, no queries against the warehouse")
    v.set_defaults(func=cmd_validate)
    sub.add_parser("list").set_defaults(func=cmd_list)
    a = ap.parse_args(argv)
    return a.func(a)


if __name__ == "__main__":
    sys.exit(main())
