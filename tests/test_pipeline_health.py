"""Pipeline health: the build log (analytics/run_log.py and build.py build) and the three BI export tabs built on it
(bi_pipeline_runs, bi_feed_runs, bi_data_quality), with the guard's operational counts. No network, no credentials,
never the local warehouse: every test works in a temporary folder."""
import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "analytics"))
sys.path.insert(0, str(ROOT / "tests"))
duckdb = pytest.importorskip("duckdb")
pytest.importorskip("yaml")
import build as wh  # noqa: E402
import export_bi as eb  # noqa: E402
import run_log  # noqa: E402
import test_export_bi as tx  # noqa: E402

T0 = datetime(2026, 10, 21, 6, 15, tzinfo=timezone.utc)


# ---------- the build log ----------

def test_two_appends_give_two_lines_and_never_rewrite(tmp_path):
    log = tmp_path / "run_history" / "builds.ndjson"
    run_log.append(log, tx.build_record(0))
    first = log.read_bytes()
    run_log.append(log, tx.build_record(1, "failed"))
    assert log.read_bytes().startswith(first)
    records, bad = run_log.read(log)
    assert bad == 0 and [r["status"] for r in records] == ["success", "failed"]
    assert list(records[0]) == list(run_log.FIELDS)
    assert len(log.read_text().splitlines()) == 2


def test_concurrent_appends_from_several_processes_all_land_whole(tmp_path):
    log = tmp_path / "builds.ndjson"
    script = ("import sys; sys.path.insert(0, sys.argv[1]); import run_log\n"
              "for i in range(50):\n"
              "    run_log.append(sys.argv[2], {'build_id': f'b{sys.argv[3]}-{i}', 'pad': 'x' * 3000})\n")
    procs = [subprocess.Popen([sys.executable, "-c", script, str(ROOT / "analytics"), str(log), str(k)])
             for k in range(4)]
    assert all(p.wait(timeout=60) == 0 for p in procs)
    records, bad = run_log.read(log)
    assert bad == 0 and len(records) == 200 and len({r["build_id"] for r in records}) == 200


def test_a_torn_line_is_skipped_and_the_next_record_starts_on_its_own_line(tmp_path):
    log = tmp_path / "builds.ndjson"
    run_log.append(log, tx.build_record(0))
    with open(log, "ab") as f:
        f.write(b'{"build_id": "b2026')                  # a crash in the middle of a write
    run_log.append(log, tx.build_record(1))
    records, bad = run_log.read(log)
    assert bad == 1 and [r["build_id"] for r in records] == ["b20261007T071000Z", "b20261007T071100Z"]
    assert run_log.read(tmp_path / "missing.ndjson") == ([], 0)


def test_a_record_is_always_one_line(tmp_path):
    run_log.append(tmp_path / "b.ndjson", {"build_id": "b", "note": "a\nb"})      # json escapes the newline
    assert len((tmp_path / "b.ndjson").read_text().splitlines()) == 1


def test_summarize_results():
    results = [("test", "pass"), ("test", "pass"), ("test", "warn"), ("test", "fail"), ("test", "error"),
               ("test", "skipped"), ("test", "runtime error"), ("model", "success"), ("model", "error"),
               ("seed", "success"), ("snapshot", "success")]
    assert run_log.summarize_results(results) == {"tests_pass": 2, "tests_warn": 1, "tests_fail": 1,
                                                  "tests_error": 2, "tests_skipped": 1, "models_built": 1}


def test_extract_complete():
    full = {"counts": {"Patient": {"extracted": 5, "server_total": 5}, "Encounter": {"extracted": 9, "server_total": 9}}}
    assert run_log.extract_complete(full)
    full["counts"]["Encounter"]["extracted"] = 8
    assert not run_log.extract_complete(full)
    assert not run_log.extract_complete(None) and not run_log.extract_complete({"counts": {}})


def test_landing_age_is_the_stalest_watched_type():
    newest = {"Patient": datetime(2026, 10, 20, 6, 0), "Encounter": datetime(2026, 10, 20, 0, 0),
              "Observation": datetime(2026, 10, 20, 5, 0), "Practitioner": datetime(2020, 1, 1)}
    assert run_log.landing_age_hours(newest, T0) == 30.2      # Encounter; Practitioner is not watched
    assert run_log.landing_age_hours({"Patient": datetime(2026, 10, 20)}, T0) is None


def write_git(tmp_path, head, refs=None, packed=None):
    git = tmp_path / ".git"
    git.mkdir(parents=True)
    (git / "HEAD").write_text(head + "\n")
    for ref, sha in (refs or {}).items():
        (git / ref).parent.mkdir(parents=True, exist_ok=True)
        (git / ref).write_text(sha + "\n")
    if packed:
        (git / "packed-refs").write_text("# pack-refs with: peeled\n" + packed + "\n")
    return tmp_path


def test_git_commit_from_the_git_files(tmp_path):
    sha = "7900e9e1234567890abcdef1234567890abcdef0"
    assert run_log.git_commit(write_git(tmp_path / "a", "ref: refs/heads/x", {"refs/heads/x": sha})) == sha[:12]
    assert run_log.git_commit(write_git(tmp_path / "b", "ref: refs/heads/x", packed=f"{sha} refs/heads/x")) == sha[:12]
    assert run_log.git_commit(write_git(tmp_path / "c", sha)) == sha[:12]           # detached
    assert run_log.git_commit(tmp_path / "none") == "unknown"
    wt = tmp_path / "wt"
    wt.mkdir()
    (wt / ".git").write_text(f"gitdir: {tmp_path / 'a' / '.git'}\n")
    assert run_log.git_commit(wt) == sha[:12]
    assert len(run_log.git_commit(ROOT)) == 12


def test_make_record():
    rec = tx.build_record(0)
    assert rec["build_id"] == "b20261007T071000Z" and rec["duration_s"] == 95.0
    assert rec["extract_complete"] is True and rec["landing_age_hours"] == 7.6    # Encounter 23:35 to 07:11:35
    assert rec["git_commit"] == "0123456789ab" and rec["tests_pass"] == 405


def tiny_warehouse(db):
    con = duckdb.connect(str(db))
    con.execute("CREATE TABLE bronze_resources (resource_type VARCHAR, raw JSON)")
    con.executemany("INSERT INTO bronze_resources VALUES (?, ?)", [
        (t, json.dumps({"meta": {"lastUpdated": f"2026-10-20T0{h}:00:00.123+00:00"}}))
        for t, h in (("Patient", 5), ("Encounter", 1), ("Observation", 6), ("Observation", 2))])
    for t in ("silver_observation", "fact_encounter", "fact_lab_result"):
        con.execute(f"CREATE TABLE {t} AS SELECT 1 AS x UNION ALL SELECT 2")
    con.close()


@pytest.fixture
def workdir(tmp_path, monkeypatch):
    db = tmp_path / "wh" / "warehouse.duckdb"
    db.parent.mkdir()
    tiny_warehouse(db)
    landing = tmp_path / "landing"
    landing.mkdir()
    (landing / "Patient.ndjson").write_text("{}\n")
    (landing / "manifest.json").write_text(json.dumps({"counts": {"Patient": {"extracted": 1, "server_total": 1}}}))
    monkeypatch.setattr(wh, "print_status", lambda con: 0)
    return db, landing


def test_cmd_build_appends_one_record_per_build(workdir, monkeypatch):
    db, landing = workdir

    def fake_build(db_, landing_, summary=None, **kw):
        summary.update({"tests_pass": 406, "tests_warn": 0, "tests_fail": 0, "tests_error": 0, "tests_skipped": 0,
                        "models_built": 90})
        return []
    monkeypatch.setattr(wh, "build", fake_build)
    assert wh.main(["--db", str(db), "--landing", str(landing), "build"]) == 0
    assert wh.main(["--db", str(db), "--landing", str(landing), "build"]) == 0
    records, bad = run_log.read(db.parent / "run_history" / "builds.ndjson")
    assert bad == 0 and len(records) == 2 and {r["status"] for r in records} == {"success"}
    r = records[0]
    assert r["rows_bronze_resources"] == 4 and r["rows_fact_encounter"] == 2 and r["tests_pass"] == 406
    assert r["extract_complete"] is True and r["landing_age_hours"] is not None


def test_cmd_build_logs_a_failed_and_a_broken_build(workdir, monkeypatch):
    db, landing = workdir
    monkeypatch.setattr(wh, "build", lambda *a, summary=None, **k: summary.update({"tests_fail": 1}) or ["x: fail"])
    assert wh.main(["--db", str(db), "--landing", str(landing), "build"]) == 1

    def broken(*a, **k):
        raise SystemExit("dbt build failed: bad profile")
    monkeypatch.setattr(wh, "build", broken)
    with pytest.raises(SystemExit, match="bad profile"):
        wh.main(["--db", str(db), "--landing", str(landing), "build"])
    records, _ = run_log.read(db.parent / "run_history" / "builds.ndjson")
    assert [r["status"] for r in records] == ["failed", "error"]
    assert records[0]["tests_fail"] == 1 and records[1]["tests_pass"] is None


def test_a_build_log_problem_never_hides_the_build_result(workdir, monkeypatch, capsys):
    db, landing = workdir
    monkeypatch.setattr(wh, "build", lambda *a, summary=None, **k: [])

    def no_disk(*a, **k):
        raise OSError("disk full")
    monkeypatch.setattr(run_log, "append", no_disk)
    assert wh.main(["--db", str(db), "--landing", str(landing), "build"]) == 0
    assert "Build log: not written (OSError: disk full)" in capsys.readouterr().err


# ---------- the guard: operational counts ----------

def test_op_count_allows_1_to_10_and_count_does_not():
    row = {c: None for c, _, _ in eb.SCHEMAS["bi_feed_runs"]}
    row.update({"day": "2026-10-06", "status": "success", "errors": 2, "load_failed": 7, "messages": 3})
    assert eb.check_tab("bi_feed_runs", [row]) == []
    assert eb.check_tab("bi_feed_runs", [{**row, "errors": -1}]) == ["bi_feed_runs row 0: errors is negative"]
    pop = {"sex": "female", "age_band": "40s", "source": "all", "people": 2, "is_suppressed": False}
    assert eb.check_tab("bi_population", [pop]) == ["bi_population row 0: people is a count of 2"]


@pytest.mark.parametrize("col,kind,why", [("sex", "dim", "demographic"), ("unit_name", "dim", "demographic"),
                                          ("age_band", "dim", "demographic"), ("people", "count", "people count"),
                                          ("ed_visits", "int", "patient event"),
                                          ("discharges", "int", "patient event"),
                                          ("open_admissions", "int", "patient event")])
def test_an_operational_tab_can_not_be_split_by_a_person(col, kind, why):
    schema = eb.SCHEMAS["bi_feed_runs"] + ((col, "int" if kind != "dim" else "str", kind if kind != "int" else "op_count"),)
    problems = eb.operational_problems(schema)
    assert len(problems) == 1 and col in problems[0] and why in problems[0]
    assert any(col in p for p in eb.check_tab("bi_feed_runs", [], schema))
    # the same column next to people counts only (no op_count) is not this rule's business
    assert eb.operational_problems(eb.SCHEMAS["bi_ops_daily"]) == []


def test_build_id_and_rows_of_warehouse_tables_are_allowed():
    assert eb.column_problem("build_id") is None
    assert eb.column_problem("rows_fact_encounter") is None and eb.column_problem("rows_bronze_resources") is None
    assert eb.column_problem("rows_patient_names") is not None        # not a warehouse table
    assert eb.column_problem("encounter_rows") is not None and eb.column_problem("run_id") is not None


# ---------- the tabs ----------

def test_feed_runs_keep_the_allow_list_and_the_newest_run_per_day(built):
    tabs, _ = built
    rows = tabs["bi_feed_runs"]
    assert [r["day"] for r in rows] == ["2026-10-05", "2026-10-06"]
    assert rows[1]["status"] == "success" and rows[1]["messages"] == 71        # the rerun replaced the failed run
    assert rows[0] == {"day": "2026-10-05", "status": "success", "duration_s": 0.6, "messages": 69, "processed": 41,
                       "skipped": 22, "duplicates": 5, "errors": 1, "resources_loaded": 233, "load_failed": 0,
                       "pct_loinc": 100.0}
    assert set(eb.FEED_RUN_FIELDS) == {c for c, _, _ in eb.SCHEMAS["bi_feed_runs"]}


def test_no_patient_event_field_leaves_in_a_pipeline_tab(built):
    """new_admissions, discharges, ed_visits, transfers and open_admissions are in every run record; none may reach
    the pipeline tabs, as a column or as a value."""
    tabs, _ = built
    fields = {"new_admissions", "discharges", "ed_visits", "transfers", "open_admissions", "resources_created",
              "run_id"}
    for t in ("bi_pipeline_runs", "bi_feed_runs", "bi_data_quality"):
        cols = {c for c, _, _ in eb.SCHEMAS[t]}
        assert not cols & fields, t
        assert all(not set(r) & fields for r in tabs[t]), t
        text = json.dumps(tabs[t])
        assert not any(f in text for f in fields), t
    assert eb.FEED_PATIENT_FIELDS >= {"new_admissions", "discharges", "ed_visits", "transfers", "open_admissions"}


def test_pipeline_runs_follow_the_log(built):
    tabs, stats = built
    rows = tabs["bi_pipeline_runs"]
    assert [r["status"] for r in rows] == ["success", "failed"] and rows[0]["build_date"] == "2026-10-07"
    assert [c for c in rows[0]] == [c for c, _, _ in eb.SCHEMAS["bi_pipeline_runs"]]
    assert rows[0]["rows_fact_encounter"] == 6349 and rows[0]["git_commit"] == "0123456789ab"
    assert stats["build_log_records"] == 2 and stats["build_log_bad_lines"] == 0


def test_confidence_bands():
    b = eb.confidence_band
    assert b({"suggested_loinc": "1-1", "confidence": "0.9"}) == "high"
    assert b({"suggested_loinc": "1-1", "confidence": "0.89"}) == "medium"
    assert b({"suggested_loinc": "1-1", "confidence": "0.6"}) == "medium"
    assert b({"suggested_loinc": "1-1", "confidence": "0.59"}) == "low"
    assert b({"suggested_loinc": "", "confidence": "0.95"}) == "no suggestion"
    assert b({"suggested_loinc": "1-1", "confidence": ""}) == "low"


def test_data_quality_rows(built):
    tabs, _ = built
    rows = tabs["bi_data_quality"]
    by = {(r["check_group"], r["measure"], r["scope"], r["band"]): r for r in rows}
    loinc = by[("coding", "loinc_coverage", "mimic", "all")]
    assert (loinc["value"], loinc["numerator"], loinc["denominator"]) == (85.2, 81134, 95258)
    assert by[("coding", "diagnoses_need_review", "hl7v2", "all")]["numerator"] == 194
    for m, v in (("precision", 1.0), ("recall", 0.95)):
        e = by[("empi", m, "synthetic test set", "all")]
        assert e["value"] == v and e["numerator"] is None and e["denominator"] is None   # rates only, no counts
        assert "synthetic test set" in e["label"]
    queue = {r["band"]: r["numerator"] for r in rows if r["measure"] == "review_pending"}
    assert queue == {"high": 2, "medium": 1, "low": 1, "no suggestion": 1}
    assert {r["band"]: r["numerator"] for r in rows if r["measure"] == "review_accepted"}["medium"] == 1
    assert by[("freshness", "data_date", "live feed", "all")]["label"] == "2026-10-06"
    assert by[("freshness", "feed_lag_hours", "live feed", "all")]["value"] == 7.5
    assert by[("freshness", "landing_age_hours", "landing", "all")]["value"] == 7.6       # the last build's
    obs = by[("completeness", "extracted_of_server_total", "Observation", "all")]
    assert (obs["numerator"], obs["denominator"]) == (201978, 201980)
    assert by[("completeness", "extract_complete", "all", "all")]["value"] == 1.0
    assert {r["as_of"] for r in rows} == {"2026-10-07"}


def test_no_build_log_yet(snap, tmp_path):
    health = {**snap["health"], "build_log": str(tmp_path / "none.ndjson")}
    tabs, stats = eb.build_datasets(snap, run=tx.FakeMF(), cat=tx.CAT, **health)
    assert tabs["bi_pipeline_runs"] == [] and stats["build_log_records"] == 0
    by = {(r["measure"], r["scope"]): r for r in tabs["bi_data_quality"]}
    assert by[("landing_age_hours", "landing")]["value"] is None
    assert {r["as_of"] for r in tabs["bi_data_quality"]} == {"2026-10-20"}             # the snapshot's date
    tabs["bi_meta"] = eb.meta_rows(tabs, snap, stats)
    eb.check_all(tabs)


@pytest.fixture
def built(snap):
    tabs, stats = tx.datasets(snap)
    tabs["bi_meta"] = eb.meta_rows(tabs, snap, stats)
    return tabs, stats


snap = tx.snap


# ---------- csv and sheets ----------

def test_csv_rerun_is_identical_and_a_new_build_changes_only_its_tabs(snap, tmp_path, monkeypatch):
    srv = tmp_path / "serving"
    srv.mkdir()
    eb.serving.write_pointer(srv, snap)
    real = eb.build_datasets
    monkeypatch.setattr(eb, "build_datasets", lambda s, **kw: real(s, run=tx.FakeMF(), cat=tx.CAT, **snap["health"]))
    out = tmp_path / "bi"

    def export():
        assert eb.main(["--serving", str(srv), "--out", str(out), "--no-env-file", "export", "--target", "csv"]) == 0
        return {p.name: p.read_bytes() for p in sorted(out.glob("*.csv"))}
    first = export()
    assert export() == first
    assert {f"{t}.csv" for t in ("bi_pipeline_runs", "bi_feed_runs", "bi_data_quality")} <= set(first)
    run_log.append(snap["health"]["build_log"], tx.build_record(5))
    third = export()
    changed = {n for n in first if first[n] != third[n]}
    assert changed == {"bi_pipeline_runs.csv", "bi_data_quality.csv", "bi_meta.csv"}


def test_sheets_writer_adds_the_three_tabs(built, snap):
    tabs, stats = built
    sh = tx.FakeSpreadsheet()
    target = eb.SheetsTarget(sh)
    eb.export(tabs, target)
    titles = [w.title for w in sh.worksheets()]
    assert {"bi_pipeline_runs", "bi_feed_runs", "bi_data_quality"} <= set(titles) and titles[0] == "Notes"
    for t in ("bi_pipeline_runs", "bi_feed_runs", "bi_data_quality"):
        assert eb.typed_rows(t, target.read(t)) == tabs[t], t
    first = {t: eb.values_hash(target.read(t)) for t in eb.TABS}
    tabs["bi_meta"] = eb.meta_rows(tabs, snap, stats, previous=target.read_meta())
    eb.export(tabs, target)
    assert {t: eb.values_hash(target.read(t)) for t in eb.TABS} == first
    assert sh.sheets[0].grid[0][0] == "my own notes"
