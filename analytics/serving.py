"""The serving snapshot: a slim, read only copy of the warehouse for the metrics API (api/).

The API must never hold the warehouse file while the nightly build writes it, and must never see more than the
semantic layer needs. So `build.py snapshot`, run after a build:

  1. dbt parse into a new folder serving/<stamp>/target: the semantic manifest of the current definitions
  2. reads from it the tables the semantic models and the time spine use (fact_encounter, dim_patient, ...), plus
     the two suppressed quality marts and three small Gold tables of pipeline metadata the BI export reads
     (BI_TABLES, analytics/export_bi.py)
  3. creates serving/<stamp>/warehouse.duckdb with only those tables (ATTACH the warehouse READ_ONLY, then
     CREATE TABLE AS). Same file name as the warehouse, because the manifest names its database after the file.
     No Bronze, Silver, Gold, governance or internal tables, and no unsuppressed counts beyond the semantic layer's.
  4. writes serving/current.json (atomic rename): the version the API reads. Older versions are removed, keeping
     the newest `keep`, so a query still running on the previous one finishes.

The API only reads current.json and that copy, so the build and the API never share a file.
"""
import json
import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

POINTER = "current.json"
DB_NAME = "warehouse.duckdb"   # the manifest's database name is the file name without .duckdb
# Read by the BI export (analytics/export_bi.py); the metrics API does not read them. The two quality marts are
# aggregates with small cell and complementary suppression already applied in dbt. The three Gold tables are pipeline
# metadata for bi_data_quality: coding coverage per source, the EMPI's quality on its synthetic test set (one row) and
# the live feed's freshness (one row); no patient rows.
BI_TABLES = ("mart_quality_period", "mart_suppression_report", "gold_terminology_quality", "gold_empi_quality",
             "gold_feed_freshness")


def semantic_tables(manifest):
    """The table names the semantic layer reads: every semantic model's relation and the time spines."""
    names = [m["node_relation"]["alias"] for m in manifest.get("semantic_models", [])]
    conf = manifest.get("project_configuration") or {}
    names += [t["node_relation"]["alias"] for t in conf.get("time_spines") or []]
    names += [t.get("location", "").split(".")[-1] for t in conf.get("time_spine_table_configurations") or []
              if t.get("location")]
    out = []
    for n in names:
        if n and n not in out:
            out.append(n)
    return out


def snapshot_tables(manifest):
    """Everything the serving snapshot holds: the semantic layer's tables, then BI_TABLES."""
    return semantic_tables(manifest) + [t for t in BI_TABLES if t not in semantic_tables(manifest)]


def stamp(now=None):
    return (now or datetime.now(timezone.utc)).strftime("%Y%m%dT%H%M%SZ")


def read_pointer(serving):
    """current.json of the serving folder, or None when there is no snapshot yet (or it is unreadable)."""
    try:
        info = json.loads((Path(serving) / POINTER).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return info if isinstance(info, dict) and info.get("version") else None


def write_pointer(serving, info):
    tmp = Path(serving) / (POINTER + ".tmp")
    tmp.write_text(json.dumps(info, indent=1), encoding="utf-8")
    os.replace(tmp, Path(serving) / POINTER)


def versions_to_remove(names, current, keep):
    """Snapshot folders to delete: all but the newest keep (stamps sort by time), never the current one."""
    stamps = sorted(n for n in names if not n.startswith("."))
    old = stamps[:-keep] if keep > 0 else stamps
    return [n for n in old if n != current]


def copy_tables(source_db, dest_db, tables):
    """dest_db holds exactly these tables of source_db, copied with CREATE TABLE AS. Returns {table: rows}."""
    import duckdb
    con = duckdb.connect(str(dest_db))
    try:
        src = str(Path(source_db).resolve()).replace("'", "''")
        con.execute(f"ATTACH '{src}' AS src (READ_ONLY)")
        have = {r[0] for r in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_catalog = 'src'").fetchall()}
        missing = [t for t in tables if t not in have]
        if missing:
            raise SystemExit(f"The warehouse has no {', '.join(missing)}: it predates the semantic layer. "
                             "Run: python3 analytics/build.py build")
        rows = {}
        for t in tables:
            con.execute(f'CREATE TABLE "{t}" AS SELECT * FROM src.main."{t}"')
            rows[t] = con.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
        data_date = None
        try:
            data_date = con.execute("SELECT CAST(data_date AS VARCHAR) FROM src.main.gold_feed_freshness").fetchone()[0]
        except Exception:
            pass
        con.execute("DETACH src")
        con.execute("CHECKPOINT")
    finally:
        con.close()
    return rows, data_date


def create_snapshot(db, serving, keep=2, now=None):
    """Build a new serving version from the warehouse db and make it current. Returns the pointer written."""
    import build
    import metrics
    serving = Path(serving).resolve()   # dbt reads a relative target path against the project folder
    serving.mkdir(parents=True, exist_ok=True)
    version = stamp(now)
    for stale in serving.glob(".*.tmp"):     # left by a run that crashed
        shutil.rmtree(stale, ignore_errors=True)
    work = serving / f".{version}.tmp"
    work.mkdir(parents=True)
    dest_db = work / DB_NAME
    target = work / "target"
    metrics.parse_project(target, dest_db)     # names the database "warehouse", like the copy
    manifest = json.loads((target / "semantic_manifest.json").read_text(encoding="utf-8"))
    tables = snapshot_tables(manifest)
    if not tables:
        raise SystemExit("the semantic manifest names no tables")
    try:
        rows, data_date = copy_tables(db, dest_db, tables)
    except BaseException:
        shutil.rmtree(work, ignore_errors=True)
        raise
    build.release_duckdb()
    final = serving / version
    os.replace(work, final)
    info = {"version": version, "db": str((final / DB_NAME).resolve()), "target": str((final / "target").resolve()),
            "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "data_date": data_date,
            "tables": rows, "size_bytes": (final / DB_NAME).stat().st_size}
    write_pointer(serving, info)
    for name in versions_to_remove([p.name for p in serving.iterdir() if p.is_dir()], version, keep):
        shutil.rmtree(serving / name, ignore_errors=True)
    return info
