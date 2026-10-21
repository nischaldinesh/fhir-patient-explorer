"""Save and compare a fingerprint of the warehouse, to prove a refactor changes no result.

  python3 analytics/baseline.py save      # data/analytics/baseline/ from the current warehouse
  python3 analytics/baseline.py compare   # the current warehouse against that baseline, exits 1 on a difference
  python3 analytics/baseline.py compare --additive
                                          # the same, but new tables, new columns, new dbt tests and new snapshot
                                          # keys are allowed: only what the baseline holds must be unchanged

The fingerprint holds the row count of every table, the full contents of every gold_* table and analyst_* view
(sorted), a checksum of every other table, and the published snapshot JSON without generated_at.
"""
import argparse
import hashlib
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

BASELINE = Path("data/analytics/baseline")
DB = Path("data/analytics/warehouse.duckdb")
# bronze_id comes from ROW_NUMBER() over the file name only, so its order inside one file is not fixed
CHECKSUM_SKIP = {"bronze_resources": ["bronze_id"]}
# One row per dbt test: adding a test adds a row, so an additive comparison leaves it out
GROWS_WITH_TESTS = {"dq_test_results"}


def _value(v):
    if hasattr(v, "isoformat"):
        return v.isoformat()
    if isinstance(v, (bytes, bytearray)):
        return v.hex()
    if v is None or isinstance(v, (bool, int, float, str)):
        return v
    return str(v)


def relations(con):
    """Every table and view in the main schema."""
    return sorted(r[0] for r in con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' ORDER BY 1").fetchall())


def columns(con, name):
    return [(r[0], r[1]) for r in con.execute(
        "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'main' AND table_name = ? "
        "ORDER BY ordinal_position", [name]).fetchall()]


def sorted_rows(con, name, skip=(), only=None):
    cols = [c for c, _ in columns(con, name) if c not in skip and (only is None or c in only)]
    sel = ", ".join(f'"{c}"' for c in cols)
    return [[_value(v) for v in r] for r in con.execute(f"SELECT {sel} FROM {name} ORDER BY ALL").fetchall()]


def checksum(con, name, only=None):
    h = hashlib.sha256()
    for r in sorted_rows(con, name, CHECKSUM_SKIP.get(name, ()), only):
        h.update(json.dumps(r).encode())
        h.update(b"\n")
    return h.hexdigest()


def fingerprint(con, names, like=None):
    """names: the tables and views to cover (the warehouse's own, not dbt bookkeeping). With like (a baseline),
    contents and checksums cover only the columns that baseline has, so added columns do not count as changes."""
    from publish import snapshot

    out = {"counts": {}, "columns": {}, "contents": {}, "checksums": {}}
    for n in names:
        only = {c for c, _ in like["columns"][n]} if like and n in like.get("columns", {}) else None
        out["counts"][n] = con.execute(f"SELECT COUNT(*) FROM {n}").fetchone()[0]
        out["columns"][n] = columns(con, n)
        if n.startswith(("gold_", "analyst_")):
            out["contents"][n] = sorted_rows(con, n, only=only)
        else:
            out["checksums"][n] = checksum(con, n, only)
    snap = snapshot(con)
    snap.pop("generated_at", None)
    out["snapshot"] = json.loads(json.dumps(snap))
    return out


def save(fp, folder):
    folder = Path(folder)
    (folder / "contents").mkdir(parents=True, exist_ok=True)
    for n, rows in fp["contents"].items():
        (folder / "contents" / f"{n}.json").write_text(json.dumps(rows))
    head = {k: v for k, v in fp.items() if k != "contents"}
    (folder / "baseline.json").write_text(json.dumps(head, indent=1, sort_keys=True))


def load(folder):
    folder = Path(folder)
    fp = json.loads((folder / "baseline.json").read_text())
    fp["contents"] = {p.stem: json.loads(p.read_text()) for p in sorted((folder / "contents").glob("*.json"))}
    return fp


def shaped_like(now, like):
    """now without the keys like does not have, at any depth (a new field in every cohort row, a new snapshot key).
    Lists are followed item by item when they have the same length; anything else is returned as it is, so a changed,
    missing or reordered value still differs from like."""
    if isinstance(now, dict) and isinstance(like, dict):
        return {k: shaped_like(v, like[k]) for k, v in now.items() if k in like}
    if isinstance(now, list) and isinstance(like, list) and len(now) == len(like):
        return [shaped_like(n, x) for n, x in zip(now, like)]
    return now


def compare(base, now, additive=False):
    """A list of (item, ok, detail) lines, one per table, view and the snapshot. With additive, only what the baseline
    holds is compared: its tables (new ones are listed as new), its columns (added columns are fine) and its snapshot
    keys at any depth (shaped_like); dq_test_results, one row per dbt test, is left out because new tests add rows."""
    lines = []
    for n in sorted(set(base["counts"]) | set(now["counts"])):
        if additive and n not in base["counts"]:
            lines.append((n, True, "new"))
            continue
        if additive and n in GROWS_WITH_TESTS:
            lines.append((n, True, "left out: one row per dbt test"))
            continue
        b, c = base["counts"].get(n), now["counts"].get(n)
        now_cols = [list(x) for x in now["columns"].get(n, [])]
        base_cols = base["columns"].get(n)
        cols_ok = (all(list(x) in now_cols for x in base_cols or []) and n in now["counts"]) if additive else base_cols == now_cols
        ok = b == c and cols_ok
        detail = f"rows {b} -> {c}" if b != c else f"{c:,} rows"
        if b == c and not ok:
            detail += ", columns differ"
        if n in base["contents"] or n in now["contents"]:
            same = base["contents"].get(n) == json.loads(json.dumps(now["contents"].get(n)))
            ok, detail = ok and same, detail + (", contents equal" if same else ", CONTENTS DIFFER")
        elif n in base["checksums"] or n in now["checksums"]:
            same = base["checksums"].get(n) == now["checksums"].get(n)
            ok, detail = ok and same, detail + (", checksum equal" if same else ", CHECKSUM DIFFERS")
        lines.append((n, ok, detail))
    snap_now = now["snapshot"]
    if additive:
        snap_now = shaped_like(snap_now, base["snapshot"])
    same = base["snapshot"] == snap_now
    lines.append(("snapshot (without generated_at)", same, "equal" if same else "DIFFERS"))
    return lines


def main(argv=None):
    import duckdb

    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("cmd", choices=["save", "compare"])
    ap.add_argument("--db", default=str(DB))
    ap.add_argument("--out", default=str(BASELINE))
    ap.add_argument("--additive", action="store_true", help="allow new tables, columns, dbt tests and snapshot keys")
    a = ap.parse_args(argv)
    con = duckdb.connect(a.db, read_only=True)
    if a.cmd == "save":
        fp = fingerprint(con, relations(con))
        save(fp, a.out)
        print(f"Saved a baseline of {len(fp['counts'])} tables and views to {a.out}")
        return 0
    base = load(a.out)
    names = relations(con) if a.additive else [n for n in relations(con) if n in base["counts"]]
    lines = compare(base, fingerprint(con, names, like=base if a.additive else None), additive=a.additive)
    for name, ok, detail in lines:
        print(f"  [{'SAME' if ok else 'DIFF'}] {name:<32} {detail}")
    bad = [n for n, ok, _ in lines if not ok]
    print("\nIdentical to the baseline" if not bad else f"\n{len(bad)} differ: {', '.join(bad)}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
