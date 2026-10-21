#!/usr/bin/env python3
"""Analytics warehouse (Option C): FHIR server -> landing files -> Bronze -> Silver -> Gold, in DuckDB,
built by the dbt project in analytics/dbt/, with the same Silver and Gold SQL rendered for Snowflake.

Commands:
  extract   pull Patient, Encounter, Condition, Observation, Location, Practitioner and CareTeam from the FHIR
            server into data/analytics/landing/*.ndjson (paged, a few minutes for ~200,000 resources), plus the
            live feed's run history (feed_runs.json) and the EMPI's two Binaries (Binary.ndjson).
            --mode bulk uses FHIR Bulk Data $export instead (analytics/bulk.py): same files, incremental with _since
  build     dbt seed and dbt build: Bronze, seeds, Silver, Gold, governed views and every data test,
            into data/analytics/warehouse.duckdb; appends one record per build (also a failed one) to
            data/analytics/run_history/builds.ndjson (analytics/run_log.py)
  render    write the Snowflake version of the Silver, Gold and mart models to analytics/sql/snowflake/
            (compiled offline by dbt, no Snowflake account needed)
  status    print table sizes and data quality checks
  freshness how current the landing files are (dbt source freshness on FHIR meta.lastUpdated): warn after 36 hours,
            error after 72
  publish   save a Gold snapshot to the FHIR server (Binary/analytics-snapshot) for the app's Analytics page,
            the care team worklists (Binary/worklist-bedboard, Binary/worklist-readmission) and a record in
            Binary/analytics-history
  snapshot  the slim serving copy of the warehouse for the metrics API (analytics/serving.py): only the tables the
            semantic layer reads, in data/analytics/serving/<stamp>/warehouse.duckdb
  all       extract, build, then publish

Pseudonyms for the analyst views use the PSEUDONYM_SALT environment variable (a local demo value if unset).

Usage:
  python3 -m pip install -r analytics/requirements.txt
  python3 analytics/build.py all
"""
import argparse
import json
import os
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(ROOT))

DBT_DIR = HERE / "dbt"
SQL = HERE / "sql"
LANDING = Path("data/analytics/landing")
DB = Path("data/analytics/warehouse.duckdb")
SERVING = Path("data/analytics/serving")
TABLES = ["bronze_resources", "seed_lab_loinc", "seed_icd9_icd10", "seed_key_labs", "seed_cohort_codes",
          "seed_charlson_codes", "seed_unmapped_labs",
          "silver_patient", "silver_encounter", "silver_condition", "silver_observation", "silver_care_team_member",
          "gold_encounter_summary", "gold_lab_latest", "gold_bmi", "gold_aki_rise", "gold_aki_signal",
          "gold_patient_summary", "gold_cohort_members", "gold_cohorts", "gold_terminology_quality",
          "gold_live_encounter", "gold_feed_freshness", "gold_daily_census", "gold_bed_board", "gold_unit_occupancy",
          "gold_weekly_metrics", "gold_readmission_risk",
          "silver_patient_link", "silver_empi_pair", "silver_empi_truth", "gold_patient_crosswalk", "gold_empi_quality"]
# The star schema (models/marts/ and snapshots/), listed with their row counts by status
MART_TABLES = ["dim_date", "dim_patient", "dim_patient_history", "dim_location", "dim_practitioner", "dim_diagnosis",
               "dim_lab", "fact_encounter", "fact_diagnosis", "fact_lab_result", "fact_bed_day", "fact_bed_capacity_day",
               "mart_quality_monthly", "mart_quality_period", "mart_suppression_report", "snap_patient_demographics"]
# The rendered Snowflake files, in run order: file -> the dbt models it creates, or the checks table.
SNOWFLAKE_FILES = {
    "silver/10_patient.sql": ["silver_patient"],
    "silver/15_encounter_location.sql": ["silver_encounter_location"],
    "silver/20_encounter.sql": ["silver_encounter"],
    "silver/30_condition.sql": ["silver_condition"],
    "silver/40_observation.sql": ["silver_observation"],
    "silver/50_location.sql": ["silver_location"],
    "silver/60_practitioner.sql": ["silver_practitioner"],
    "silver/70_care_team_member.sql": ["silver_care_team_member"],
    "silver/80_patient_link.sql": ["silver_patient_link"],
    "silver/85_empi.sql": ["silver_empi_pair", "silver_empi_truth"],
    "silver/90_checks.sql": "dq_checks",
    "gold/10_encounter_summary.sql": ["gold_encounter_summary"],
    "gold/20_lab_latest.sql": ["gold_lab_latest"],
    "gold/30_bmi.sql": ["gold_bmi"],
    "gold/40_aki_signal.sql": ["gold_aki_rise", "gold_aki_signal"],
    "gold/50_patient_summary.sql": ["gold_patient_summary"],
    "gold/55_patient_crosswalk.sql": ["gold_patient_crosswalk"],
    "gold/60_cohorts.sql": ["gold_cohort_members", "gold_cohorts"],
    "gold/70_terminology_quality.sql": ["gold_terminology_quality"],
    "gold/80_live_encounter.sql": ["gold_live_encounter", "gold_feed_freshness"],
    "gold/81_daily_census.sql": ["gold_daily_census"],
    "gold/82_bed_board.sql": ["gold_bed_board", "gold_unit_occupancy"],
    "gold/83_weekly_metrics.sql": ["gold_weekly_metrics"],
    "gold/84_readmission_risk.sql": ["gold_readmission_risk"],
    "gold/91_empi_quality.sql": ["gold_empi_quality"],
    "marts/10_dim_date.sql": ["dim_date"],
    "marts/11_patient_keys.sql": ["int_patient_key", "int_patient_demographics"],
    "marts/12_snapshot_patient_demographics.sql": "snapshot",
    "marts/13_dim_patient.sql": ["dim_patient", "dim_patient_history"],
    "marts/14_dim_location.sql": ["dim_location"],
    "marts/15_dim_practitioner.sql": ["dim_practitioner"],
    "marts/16_dim_diagnosis.sql": ["dim_diagnosis"],
    "marts/17_dim_lab.sql": ["dim_lab"],
    "marts/20_fact_encounter.sql": ["fact_encounter"],
    "marts/21_fact_diagnosis.sql": ["fact_diagnosis"],
    "marts/22_fact_lab_result.sql": ["fact_lab_result"],
    "marts/23_fact_bed_day.sql": ["fact_bed_day"],
    "marts/24_fact_bed_capacity_day.sql": ["fact_bed_capacity_day"],
    "marts/25_quality_events.sql": ["int_quality_events", "int_quality_cells"],
    "marts/30_quality_monthly.sql": ["mart_quality_monthly"],
    "marts/31_quality_period.sql": ["mart_quality_period", "mart_suppression_report"],
}

DEMO_SALT = "local-demo-salt"


def pseudonym_salt():
    return os.environ.get("PSEUDONYM_SALT") or DEMO_SALT


def connect(db, read_only=False):
    try:
        import duckdb
    except ImportError:
        sys.exit("DuckDB is not installed. Run: python3 -m pip install -r analytics/requirements.txt")
    Path(db).parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db), read_only=read_only)


def dbt_vars(landing, salt=None):
    """The vars the dbt project reads: where the landing files are, when they were extracted, the salt."""
    landing = Path(landing)
    manifest = landing / "manifest.json"
    loaded_at = json.loads(manifest.read_text())["extracted_at"] if manifest.exists() else "1970-01-01 00:00:00"
    return {"landing_path": str(landing.resolve()).replace("\\", "/"), "loaded_at": loaded_at,
            "pseudonym_salt": salt or pseudonym_salt()}


def default_target(env=None):
    """Where dbt writes its target folder and logs: DBT_TARGET_PATH when set (local Airflow mounts the repo read
    only), else analytics/dbt/target as before."""
    v = (os.environ if env is None else env).get("DBT_TARGET_PATH")
    return Path(v) if v else DBT_DIR / "target"


def dbt(args, db, variables=None, target_path=None, quiet=False, check=True):
    """Run one dbt command in this process (dbtRunner) against the DuckDB file db. Returns dbt's result.
    With check, a dbt that could not run at all (bad project, bad profile) stops the program."""
    try:
        from dbt.cli.main import dbtRunner
    except ImportError:
        sys.exit("dbt is not installed. Run: python3 -m pip install -r analytics/requirements.txt")
    os.environ["DBT_DUCKDB_PATH"] = str(Path(db).resolve()) if str(db) != ":memory:" else ":memory:"
    target = Path(target_path) if target_path else default_target()
    cli = [*args, "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR), "--target", "duckdb",
           "--target-path", str(target), "--log-path", str(target / "logs")]
    if variables:
        cli += ["--vars", json.dumps(variables)]
    if quiet:
        cli = ["--quiet", *cli]
    try:
        res = dbtRunner().invoke(cli)
    finally:
        release_duckdb()
    if check and res.exception is not None:
        raise SystemExit(f"dbt {args[0]} failed: {res.exception}")
    return res


def release_duckdb():
    """dbt-duckdb keeps the database open for the life of the process; close it so the file can be read."""
    try:
        from dbt.adapters.duckdb.connections import DuckDBConnectionManager as M
    except ImportError:
        return
    with M._LOCK:
        if M._ENV is not None:
            try:
                M._ENV.close()
            except Exception:
                pass
            M._ENV = None


def problems(res):
    """Names of the nodes that did not succeed (errors, failed or warned tests, skipped models)."""
    # no-op: an exposure (a planned dashboard), which has nothing to run
    return [f"{r.node.name}: {r.status}" for r in (res.result or []) if str(r.status) not in ("success", "pass", "no-op")]


def build(db, landing, salt=None, target_path=None, quiet=False, summary=None):
    """dbt seed, then dbt build. Returns the list of nodes that did not pass (empty when all is well). summary, when
    given, is filled with the test counts by status and the models built (run_log.summarize_results)."""
    landing = Path(landing)
    if not sorted(landing.glob("*.ndjson")):
        raise SystemExit(f"No landing files in {landing}. Run: python3 analytics/build.py extract")
    variables = dbt_vars(landing, salt)
    bad = problems(dbt(["seed"], db, variables, target_path, quiet))
    res = dbt(["build"], db, variables, target_path, quiet)
    bad += problems(res)
    if summary is not None:
        from run_log import summarize_results
        summary.update(summarize_results((r.node.resource_type, r.status) for r in (res.result or [])))
    return bad


def counts(con):
    return {t: con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in TABLES}


def print_status(con):
    print("Tables")
    for t, n in counts(con).items():
        print(f"  {t:<22} {n:>9,}")
    print("\nSilver patients by source")
    for src, n in con.execute("SELECT source, COUNT(*) FROM silver_patient GROUP BY source ORDER BY 2 DESC").fetchall():
        print(f"  {src:<10} {n:>5}")
    print("\nCoding coverage")
    lab = con.execute("""SELECT COUNT(*), COUNT(loinc) FROM silver_observation WHERE category = 'laboratory'
                         AND value_num IS NOT NULL""").fetchone()
    dx = con.execute("SELECT COUNT(*), COUNT(icd10_code), SUM(CASE WHEN needs_review THEN 1 ELSE 0 END) FROM silver_condition").fetchone()
    print(f"  numeric lab results with a LOINC code   {lab[1]:>7,} of {lab[0]:,} ({100 * lab[1] / max(lab[0], 1):.1f}%)")
    print(f"  diagnoses with an ICD 10 code            {dx[1]:>7,} of {dx[0]:,} ({100 * dx[1] / max(dx[0], 1):.1f}%), "
          f"{dx[2] or 0:,} need review")
    print("\nGold: cohorts")
    for row in con.execute("SELECT cohort, patients, mimic, synthea, hl7v2, pct_of_patients FROM gold_cohorts ORDER BY patients DESC").fetchall():
        print(f"  {row[0]:<16} {row[1]:>4} patients ({row[5]:>4}%)   mimic {row[2]:>3}  synthea {row[3]:>3}  hl7v2 {row[4]:>3}")
    adm = con.execute("""SELECT COUNT(*), SUM(CASE WHEN readmitted_30d THEN 1 ELSE 0 END), ROUND(MEDIAN(length_of_stay_days), 1)
                         FROM gold_encounter_summary WHERE is_admission""").fetchone()
    print(f"\nGold: {adm[0]:,} admissions, {adm[1] or 0:,} followed by a readmission within 30 days, median stay {adm[2]} days")
    kinds = con.execute("SELECT source, encounter_kind, COUNT(*) FROM gold_encounter_summary GROUP BY 1, 2 ORDER BY 1, 3 DESC").fetchall()
    print("  encounter kinds: " + ", ".join(f"{s} {k}={n}" for s, k, n in kinds))
    bmi = con.execute("""SELECT COUNT(*), SUM(CASE WHEN bmi_reported IS NOT NULL THEN 1 ELSE 0 END),
                                SUM(CASE WHEN ABS(bmi_computed - bmi_reported) <= 0.5 THEN 1 ELSE 0 END) FROM gold_bmi""").fetchone()
    print(f"Gold: BMI for {bmi[0]} patients; {bmi[2] or 0} of {bmi[1] or 0} match the BMI the source reported (within 0.5)")
    data_date = con.execute("SELECT data_date FROM gold_feed_freshness").fetchone()[0]
    if data_date:
        weeks = con.execute("SELECT COUNT(*) FROM gold_weekly_metrics").fetchone()[0]
        census = con.execute("SELECT in_house_at_midnight FROM gold_daily_census ORDER BY census_date DESC LIMIT 1").fetchone()
        occ = con.execute("SELECT SUM(occupied_beds), SUM(capacity) FROM gold_unit_occupancy").fetchone()
        bands = dict(con.execute("SELECT risk_band, COUNT(*) FROM gold_readmission_risk GROUP BY 1").fetchall())
        print(f"\nGold: live Riverside General feed (demo), data as of {data_date}, {weeks} weeks")
        print(f"  in house at midnight {census[0] if census else 0}, beds occupied {occ[0] or 0} of {occ[1] or 0}")
        print(f"  follow up list (LACE, last 30 days): {sum(bands.values())} discharges, "
              + ", ".join(f"{bands.get(b, 0)} {b}" for b in ("high", "moderate", "low")))
    else:
        print("\nGold: no live Riverside General feed data yet (python3 hl7v2/pipeline.py backfill --days 56)")
    people = con.execute("""SELECT COUNT(*), COUNT(DISTINCT master_id), SUM(CASE WHEN records_in_group > 1 THEN 1 ELSE 0 END)
                            FROM gold_patient_crosswalk""").fetchone()
    print(f"\nGold: patient crosswalk (EMPI), {people[0]:,} records of {people[1]:,} people, {people[2] or 0} records linked")
    q = con.execute("SELECT precision, recall, auto_linked, hard_negatives_auto_linked, pairs_in_review, true_match_pairs "
                    "FROM gold_empi_quality").fetchone()
    if q and q[5]:
        print(f"  EMPI test set (demo): precision {q[0]}, recall {q[1]}, {q[2]} auto linked, "
              f"{q[3]} hard negatives auto linked, {q[4]} pairs in review")
    print("\nMarts: star schema (models/marts/)")
    for t in MART_TABLES:
        print(f"  {t:<26} {con.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0]:>9,}")
    print("\nMarts: suppression of the quality measures (small cell and complementary)")
    for mart, grain, cells, hidden, comp, pct in con.execute(
            "SELECT mart, grain, cells, suppressed, suppressed_complementary, pct_suppressed "
            "FROM mart_suppression_report ORDER BY grain_order").fetchall():
        print(f"  {mart:<22} {grain:<22} {hidden:>6,} of {cells:>6,} suppressed ({pct:>5}%), {comp:,} complementary")
    print("\nGovernance: analyst views")
    cols = [r[0] for r in con.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'analyst_patient'").fetchall()]
    print(f"  analyst_patient columns: {', '.join(cols)}")
    if pseudonym_salt() == DEMO_SALT:
        print("  note: PSEUDONYM_SALT is not set, so pseudonyms use the local demo salt")
    print("\nData quality checks")
    failed = 0
    for name, n in con.execute("SELECT check_name, failures FROM dq_checks").fetchall():
        failed += n != 0
        print(f"  [{'PASS' if n == 0 else 'FAIL'}] {name}" + (f" ({n:,})" if n else ""))
    tests = con.execute("SELECT status, COUNT(*) FROM dq_test_results GROUP BY status ORDER BY 1").fetchall()
    print("  dbt tests: " + ", ".join(f"{n} {s}" for s, n in tests))
    for name, status, n in con.execute(
            "SELECT test_name, status, failures FROM dq_test_results WHERE status <> 'pass' ORDER BY 1").fetchall():
        failed += 1
        print(f"  [{status.upper()}] {name}" + (f" ({n:,})" if n else ""))
    return failed


def compile_models(dialect="duckdb", target_path=None):
    """Compile every model and test offline (an in memory DuckDB, nothing is run). With dialect snowflake the
    JSON macros render their Snowflake form. Returns {node name: compiled node} with table names unqualified."""
    res = dbt(["compile"], ":memory:", {"render_dialect": dialect}, target_path, quiet=True)
    errors = [f"{r.node.name}: {r.message}" for r in res.result if str(r.status) != "success"]
    if errors:
        raise SystemExit("dbt compile failed: " + "; ".join(errors))
    nodes = {}
    for r in res.result:
        n = r.node
        n.compiled_code = re.sub(r'"memory"\."main"\."(\w+)"', r"\1", n.compiled_code)
        nodes[n.name] = n
    return nodes


def split_header(sql):
    """Leading comment lines, then the rest."""
    lines = sql.strip("\n").split("\n")
    i = 0
    while i < len(lines) and lines[i].startswith("--"):
        i += 1
    return "\n".join(lines[:i]), "\n".join(lines[i:]).strip()


def checks_sql(nodes):
    """dq_checks for Snowflake: one row per named data quality check, from the compiled dbt tests."""
    tests = sorted((n.config.meta["check_order"], n) for n in nodes.values()
                   if n.resource_type == "test" and "check_name" in (n.config.meta or {}))
    parts = []
    for _, n in tests:
        name = n.config.meta["check_name"].replace("'", "''")
        body = "\n".join("        " + line for line in n.compiled_code.strip().split("\n"))
        parts.append(f"SELECT '{name}' AS check_name,\n       (SELECT {n.config.fail_calc} FROM (\n{body}\n       ) t) AS failures")
    return ("-- Data quality checks: the dbt tests with meta check_name, in page order. Each row is one rule; failures should be 0.\n"
            "CREATE OR REPLACE TABLE dq_checks AS\n" + "\nUNION ALL\n".join(parts) + ";\n")


def snapshot_sql(nodes):
    """The first load of snap_patient_demographics for Snowflake: every record as its first version, with the columns
    dbt snapshot adds. Tracking changes after that is dbt's job (dbt snapshot --target snowflake writes the MERGE)."""
    snap = nodes["snap_patient_demographics"]
    keys = ", ".join(snap.config.check_cols)
    return ("-- First load only: every record becomes its first version. Later changes are tracked by dbt snapshot\n"
            "-- (dbt snapshot --target snowflake), which closes a version and adds a new one when one of these changes:\n"
            f"-- {keys}.\n"
            "CREATE TABLE IF NOT EXISTS snap_patient_demographics AS\n"
            "SELECT\n    d.*,\n"
            "    MD5(CAST(d.patient_id AS VARCHAR) || '|' || CAST(d.loaded_at AS VARCHAR)) AS dbt_scd_id,\n"
            "    d.loaded_at AS dbt_updated_at,\n    d.loaded_at AS dbt_valid_from,\n"
            "    CAST(NULL AS TIMESTAMP_NTZ) AS dbt_valid_to\n"
            "FROM int_patient_demographics d;\n")


def render_snowflake(target_path=None):
    """{relative path: SQL} for every file in analytics/sql/snowflake/silver, gold and marts."""
    nodes = compile_models("snowflake", target_path)
    out = {}
    for path, models in SNOWFLAKE_FILES.items():
        layer = path.split("/")[0]
        if models == "dq_checks":
            src = "the tests in analytics/dbt/models/silver/_silver.yml and analytics/dbt/tests/"
            body = checks_sql(nodes)
        elif models == "snapshot":
            src = "analytics/dbt/snapshots/snap_patient_demographics.yml"
            body = snapshot_sql(nodes)
        else:
            src = ", ".join(f"analytics/dbt/models/{layer}/{m}.sql" for m in models)
            stmts = []
            for m in models:
                comments, sql = split_header(nodes[m].compiled_code)
                kind = "VIEW" if nodes[m].config.materialized == "view" else "TABLE"
                stmts.append((comments + "\n" if comments else "") + f"CREATE OR REPLACE {kind} {m} AS\n{sql};\n")
            body = "\n".join(stmts)
        out[path] = f"-- GENERATED by `python3 analytics/build.py render` from {src}. Do not edit.\n" + body
    return out


def cmd_extract(a):
    from tools import DEFAULT_BASE
    if getattr(a, "mode", "search") == "bulk":
        from bulk import BulkError, bulk_extract, split
        try:
            bulk_extract(a.base or DEFAULT_BASE, a.landing, split(a.types), a.since, a.group or None, a.max_wait)
        except BulkError as e:
            print(f"bulk export failed: {e}", file=sys.stderr)
            return 1
        return 0
    from extract import extract_all
    extract_all(a.base or DEFAULT_BASE, a.landing)
    return 0


def cmd_build(a):
    """Build, print the status, and append one record to the build log (run_log.py), also when the build fails."""
    import run_log
    started, summary, status = run_log.utc_now(), {}, "error"
    try:
        bad = build(a.db, a.landing, summary=summary)
        con = connect(a.db, read_only=True)
        try:
            failed = print_status(con)
        finally:
            con.close()
        for p in bad:
            print(f"  dbt: {p}")
        print(f"\nWarehouse: {a.db}")
        status = "failed" if failed or bad else "success"
        return 1 if failed or bad else 0
    finally:
        log_build(a, started, status, summary)


def log_build(a, started, status, summary):
    """One record in <warehouse folder>/run_history/builds.ndjson. A problem here is reported, never raised, so it
    can not hide the build's own result."""
    import run_log
    try:
        finished = run_log.utc_now()
        rows, newest = {}, {}
        if Path(a.db).exists():
            con = connect(a.db, read_only=True)
            try:
                rows, newest = run_log.warehouse_facts(con)
            finally:
                con.close()
        record = run_log.make_record(started, finished, status, summary, run_log.read_manifest(a.landing), rows,
                                     newest, run_log.git_commit(ROOT))
        path = run_log.log_path(a.db)
        run_log.append(path, record)
        print(f"Build log: {record['build_id']} {status} appended to {path}")
    except Exception as e:
        print(f"Build log: not written ({type(e).__name__}: {e})", file=sys.stderr)


def cmd_render(a):
    out = Path(a.out)
    for rel, sql in render_snowflake().items():
        f = out / rel
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text(sql, encoding="utf-8")
        print(f"wrote {f}")
    return 0


def feed_runs(landing):
    """The live feed's run history saved by extract (landing/feed_runs.json), [] when there is none."""
    f = Path(landing) / "feed_runs.json"
    try:
        runs = json.loads(f.read_text(encoding="utf-8")) if f.exists() else []
    except ValueError:
        return []
    return runs if isinstance(runs, list) else []


def cmd_publish(a):
    from publish import bedboard, publish_all, readmission, snapshot, test_summary
    from tools import DEFAULT_BASE
    if not Path(a.db).exists():
        sys.exit(f"No warehouse at {a.db}. Run: python3 analytics/build.py build")
    base = (a.base or DEFAULT_BASE).rstrip("/")
    con, runs = connect(a.db, read_only=True), feed_runs(a.landing)
    snap, beds, worklist = snapshot(con, runs), bedboard(con, runs), readmission(con, runs)
    n = publish_all(base, snap, beds, worklist, test_summary(con))
    print(f"Published to {base}:")
    print("  Binary/analytics-snapshot    aggregates for the Analytics page")
    print(f"  Binary/worklist-bedboard     {len(beds['beds'])} beds, {sum(1 for b in beds['beds'] if b['encounter_id'])} with a patient (PHI)")
    print(f"  Binary/worklist-readmission  {len(worklist['rows'])} discharges with a LACE score (PHI)")
    print(f"  Binary/analytics-history     {n} record{'' if n == 1 else 's'}")
    print(f"  data as of {snap['freshness']['data_date'] or 'no live feed data'}")
    return 0


def freshness(landing, target_path=None, quiet=True):
    """dbt source freshness on the landing files: [(table, status, newest meta.lastUpdated, age in hours)]."""
    res = dbt(["source", "freshness"], ":memory:", dbt_vars(landing), target_path, quiet=quiet, check=False)
    if res.result is None:
        raise SystemExit(f"dbt source freshness failed: {res.exception}")
    rows = []
    for r in res.result:
        if r.node.resource_type != "source":   # the on-run-end hook is in the results too
            continue
        age = getattr(r, "age", None)
        rows.append((r.node.name, str(r.status), getattr(r, "max_loaded_at", None),
                     round(age / 3600, 1) if age is not None else None))
    return sorted(rows)


def cmd_freshness(a):
    rows = freshness(a.landing)
    print("Landing freshness (newest meta.lastUpdated; warn after 36 hours, error after 72)")
    for name, status, newest, hours in rows:
        print(f"  [{status.upper()}] {name:<12} newest {newest or 'none'}" + (f", {hours} hours ago" if hours is not None else ""))
    return 1 if any(s not in ("pass", "warn") for _, s, _, _ in rows) else 0


def cmd_snapshot(a):
    from serving import create_snapshot
    if not Path(a.db).exists():
        sys.exit(f"No warehouse at {a.db}. Run: python3 analytics/build.py build")
    info = create_snapshot(a.db, a.serving, keep=a.keep)
    print(f"Serving snapshot {info['version']}: {len(info['tables'])} tables, {info['size_bytes'] / 1e6:.1f} MB "
          f"(the warehouse is {Path(a.db).stat().st_size / 1e6:.1f} MB)")
    for t, n in info["tables"].items():
        print(f"  {t:<24} {n:>9,} rows")
    print(f"  data as of {info['data_date'] or 'no live feed data'}; pointer {Path(a.serving) / 'current.json'}")
    return 0


def cmd_status(a):
    if not Path(a.db).exists():
        sys.exit(f"No warehouse at {a.db}. Run: python3 analytics/build.py build")
    return 1 if print_status(connect(a.db, read_only=True)) else 0


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--landing", default=str(LANDING))
    ap.add_argument("--db", default=str(DB))
    ap.add_argument("--base", default="", help="FHIR base URL (default http://localhost:8080/fhir)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    e = sub.add_parser("extract")
    e.add_argument("--mode", choices=["search", "bulk"], default="search",
                   help="search: paged searches (default); bulk: FHIR Bulk Data $export (analytics/bulk.py)")
    from bulk import add_export_args
    add_export_args(e)
    e.set_defaults(func=cmd_extract)
    sub.add_parser("build").set_defaults(func=cmd_build)
    r = sub.add_parser("render")
    r.add_argument("--out", default=str(SQL / "snowflake"))
    r.set_defaults(func=cmd_render)
    sub.add_parser("status").set_defaults(func=cmd_status)
    sub.add_parser("freshness").set_defaults(func=cmd_freshness)
    sub.add_parser("publish").set_defaults(func=cmd_publish)
    s = sub.add_parser("snapshot")
    s.add_argument("--serving", default=str(SERVING), help="serving folder (default data/analytics/serving)")
    s.add_argument("--keep", type=int, default=2, help="snapshot versions to keep (default 2)")
    s.set_defaults(func=cmd_snapshot)
    sub.add_parser("all").set_defaults(func=lambda a: cmd_extract(a) or cmd_build(a) or cmd_publish(a))
    a = ap.parse_args(argv)
    return a.func(a)


if __name__ == "__main__":
    sys.exit(main())
