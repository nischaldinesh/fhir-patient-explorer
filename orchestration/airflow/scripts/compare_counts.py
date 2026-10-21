#!/usr/bin/env python3
"""Row counts per mart of the warehouse in a data directory, to check that a rerun changes nothing.

  python3 orchestration/airflow/scripts/compare_counts.py --data-dir data/airflow
  python3 orchestration/airflow/scripts/compare_counts.py --data-dir data/airflow --save /tmp/first.json
  python3 orchestration/airflow/scripts/compare_counts.py --data-dir data/airflow --against /tmp/first.json

The warehouse is <data dir>/analytics/warehouse.duckdb, opened read only. Tables are the star schema of
analytics/build.py (MART_TABLES). --against exits 1 and lists every table whose count differs.
"""
import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "analytics"))
from build import MART_TABLES  # noqa: E402


def warehouse_path(data_dir):
    return Path(data_dir) / "analytics" / "warehouse.duckdb"


def mart_counts(con, tables=MART_TABLES):
    """{table: rows} for every table; None for a table the warehouse does not have."""
    have = {r[0] for r in con.execute("SELECT table_name FROM information_schema.tables").fetchall()}
    return {t: (con.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0] if t in have else None) for t in tables}


def diff_counts(before, after):
    """[(table, before, after)] for every table whose count is not the same in both, sorted by table."""
    return [(t, before.get(t), after.get(t)) for t in sorted(set(before) | set(after)) if before.get(t) != after.get(t)]


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data-dir", default="data/airflow")
    ap.add_argument("--save", help="write the counts to this JSON file")
    ap.add_argument("--against", help="compare with counts saved earlier; exit 1 on any difference")
    a = ap.parse_args(argv)
    import duckdb
    db = warehouse_path(a.data_dir)
    if not db.exists():
        sys.exit(f"No warehouse at {db}")
    con = duckdb.connect(str(db), read_only=True)
    try:
        counts = mart_counts(con)
    finally:
        con.close()
    for t, n in counts.items():
        print(f"  {t:<28} {'missing' if n is None else n}")
    if a.save:
        Path(a.save).write_text(json.dumps(counts, indent=1), encoding="utf-8")
        print(f"Saved to {a.save}")
    if a.against:
        diff = diff_counts(json.loads(Path(a.against).read_text(encoding="utf-8")), counts)
        for t, b, n in diff:
            print(f"  DIFFERENT {t}: {b} then {n}")
        print("Row counts identical" if not diff else f"{len(diff)} table(s) differ")
        return 1 if diff else 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
