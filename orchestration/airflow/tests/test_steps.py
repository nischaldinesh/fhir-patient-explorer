import json
from datetime import datetime, timedelta, timezone

import pytest

from nightly import steps


def test_feed_day_is_the_utc_day_before():
    assert steps.feed_day(datetime(2026, 10, 18, 6, 15, tzinfo=timezone.utc)) == "2026-10-17"
    # 01:00 on the 18th in UTC+2 is still the 17th in UTC
    assert steps.feed_day(datetime(2026, 10, 18, 1, 0, tzinfo=timezone(timedelta(hours=2)))) == "2026-10-16"
    assert steps.feed_day(datetime(2026, 3, 1, 6, 15, tzinfo=timezone.utc)) == "2026-02-28"
    with pytest.raises(ValueError):
        steps.feed_day(datetime(2026, 10, 18))


def test_fhir_base():
    assert steps.fhir_base({}) == "http://host.docker.internal:8090/fhir"
    assert steps.fhir_base({"FHIR_BASE": "http://x:1/fhir/"}) == "http://x:1/fhir"


def test_commands_in_order():
    c = steps.commands("http://h/fhir", day="2026-10-17")
    assert list(c) == ["load_feed", "prune_reports", "match_patients", "extract", "dbt_build_and_test",
                       "validate_semantic_layer", "source_freshness", "publish", "serving_snapshot", "export_bi"]
    assert c["serving_snapshot"] == ["analytics/build.py", "snapshot"]
    assert c["export_bi"] == ["analytics/export_bi.py", "--no-env-file", "export", "--target", "csv"]
    assert steps.commands("b", bi_target="sheets")["export_bi"][-1] == "sheets"
    assert c["validate_semantic_layer"] == ["analytics/metrics.py", "validate"]
    assert c["load_feed"] == ["hl7v2/pipeline.py", "daily", "--day", "2026-10-17", "--base", "http://h/fhir"]
    assert c["prune_reports"] == ["hl7v2/pipeline.py", "prune-reports", "--keep", "30", "--base", "http://h/fhir"]
    assert c["match_patients"] == ["empi/match.py", "run", "--base", "http://h/fhir"]
    assert c["extract"] == ["analytics/build.py", "--base", "http://h/fhir", "extract"]
    assert c["publish"] == ["analytics/build.py", "--base", "http://h/fhir", "publish"]
    assert steps.commands("b")["load_feed"][3] == "yesterday"


def test_build_log_lands_in_the_data_mount():
    """build.py build appends to <warehouse folder>/run_history/builds.ndjson. The task runs it from PROJECT_DIR with
    no --db, so the log goes to data/analytics/run_history, inside the data directory mount (DATA_DIR)."""
    import sys
    from pathlib import Path
    c = steps.commands("http://h/fhir")
    assert c["dbt_build_and_test"] == ["analytics/build.py", "build"]
    assert "--db" not in c["dbt_build_and_test"] and "--build-log" not in c["export_bi"]
    sys.path.insert(0, str(steps.PROJECT_DIR / "analytics"))
    import run_log
    assert run_log.log_path("data/analytics/warehouse.duckdb") == Path("data/analytics/run_history/builds.ndjson")
    compose = (Path(__file__).resolve().parents[1] / "compose.yaml").read_text()
    assert "${DATA_DIR:-../../data/airflow}:/opt/project/data" in compose


def test_should_force_failure():
    assert not steps.should_force_failure(None, 1)
    assert not steps.should_force_failure("", 1)
    assert steps.should_force_failure("once", 1) and not steps.should_force_failure("once", 2)
    assert steps.should_force_failure(" Always ", 3)
    assert not steps.should_force_failure("sometimes", 1)


def test_child_env_drops_airflow_secrets():
    env = steps.child_env({"AIRFLOW__API_AUTH__JWT_SECRET": "s", "_AIRFLOW_X": "y", "PSEUDONYM_SALT": "p"})
    assert env == {"PSEUDONYM_SALT": "p", "PYTHONDONTWRITEBYTECODE": "1", "PYTHONUNBUFFERED": "1"}


def test_run_cli(tmp_path):
    script = tmp_path / "s.py"
    script.write_text("import sys; print('hello'); sys.exit(int(sys.argv[1]))")
    assert steps.run_cli([str(script), "0"], cwd=tmp_path, python="python3") == "hello\n"
    with pytest.raises(RuntimeError, match="exited with code 3"):
        steps.run_cli([str(script), "3"], cwd=tmp_path, python="python3")


def test_summarize_feed(tmp_path):
    folder = tmp_path / "2026-10-17"
    folder.mkdir()
    (folder / "load_report.json").write_text(json.dumps({"messages": [{}, {}, {}], "loaded": 40, "load_failed": 0}))
    (folder / "0001_ADT_A01.hl7").write_text("MSH")
    assert steps.summarize_feed("2026-10-17", "Riverside ...", tmp_path) == {
        "day": "2026-10-17", "already_loaded": False, "messages": 3, "resources_loaded": 40, "load_failed": 0,
        "files_written": 2}
    out = "2026-10-17 was already generated and loaded (last generated day 2026-10-17)."
    assert steps.summarize_feed("2026-10-17", out, tmp_path)["already_loaded"] is True
    assert steps.summarize_feed("2026-10-01", "", tmp_path)["files_written"] == 0


def test_summarize_prune_and_match():
    assert steps.summarize_prune("Day reports: kept the newest 30 days, deleted 2 older, failed 0") == {
        "deleted": 2, "failed": 0}
    out = ("Candidate pairs 71: auto linked 19, review 3, accepted 0, rejected 0, no match 49\n"
           "Links: 19 pairs, 0 Patients updated now\n"
           "Test set: precision 1.0, recall 0.95 (19 of 20 true matches auto linked, 0 false), "
           "0 of 10 hard negatives auto linked, 3 in review\n")
    assert steps.summarize_match(out) == {"candidate_pairs": 71, "auto_linked": 19, "review": 3, "patients_updated": 0}


def test_summarize_extract(tmp_path):
    (tmp_path / "manifest.json").write_text(json.dumps(
        {"extracted_at": "2026-10-18 06:20:00", "counts": {"Patient": {"extracted": 5, "server_total": 5}}}))
    (tmp_path / "Patient.ndjson").write_text("{}\n")
    assert steps.summarize_extract(tmp_path) == {"extracted_at": "2026-10-18 06:20:00", "resources": {"Patient": 5},
                                                 "files_written": ["Patient.ndjson", "manifest.json"]}


def test_summarize_semantic():
    out = ("Successfully validated the semantics of built manifest (ERRORS: 0, FUTURE_ERRORS: 0, WARNINGS: 1)\n"
           "Successfully validated measures against data warehouse (ERRORS: 0, FUTURE_ERRORS: 0, WARNINGS: 0)\n"
           "Semantic layer validation passed\n")
    assert steps.summarize_semantic(out) == {"errors": 0, "future_errors": 0, "warnings": 1, "passed": True}
    bad = "Validating metrics (ERRORS: 2, FUTURE_ERRORS: 0, WARNINGS: 0)\nSemantic layer validation FAILED\n"
    assert steps.summarize_semantic(bad) == {"errors": 2, "future_errors": 0, "warnings": 0, "passed": False}


def test_summarize_freshness_and_publish():
    out = ("Landing freshness (newest meta.lastUpdated; warn after 36 hours, error after 72)\n"
           "  [PASS] Encounter    newest 2026-10-18, 1.0 hours ago\n  [WARN] Patient      newest x\n")
    assert steps.summarize_freshness(out) == {"Encounter": "pass", "Patient": "warn"}
    out = ("Published to http://h/fhir:\n  Binary/analytics-snapshot    aggregates\n  Binary/worklist-bedboard     3 beds\n"
           "  data as of 2026-10-17\n")
    assert steps.summarize_publish(out) == {"binaries": 2, "data_date": "2026-10-17"}


def test_summarize_snapshot():
    out = "Serving snapshot 20261019T221723Z: 6 tables, 3.4 MB (the warehouse is 669.8 MB)\n  fact_encounter 6,349 rows\n"
    assert steps.summarize_snapshot(out) == {"version": "20261019T221723Z", "tables": 6, "size_mb": 3.4}
    assert steps.summarize_snapshot("nothing") == {}


def test_bi_switches():
    assert steps.is_true("true") and steps.is_true(" TRUE ")
    assert not steps.is_true("false") and not steps.is_true("") and not steps.is_true(None) and not steps.is_true("1")
    assert steps.bi_target(None) == "csv" and steps.bi_target("") == "csv" and steps.bi_target("Sheets") == "sheets"
    with pytest.raises(ValueError):
        steps.bi_target("excel")


def test_summarize_export_bi():
    out = """Serving snapshot 20261020T155726Z, data as of 2026-10-14
Export to csv: guard passed
  tab                      rows  suppressed
  bi_ops_daily               56          56
  bi_ops_weekly               9           4
  bi_meta                    25           0
  hidden by the check across grains: 0 day cells, 1 week cells, 1 month cells; by the unit check on occupancy: 0
Wrote 8 tabs to data/bi/ (full replace)
"""
    s = steps.summarize_export_bi(out, "csv")
    assert s == {"target": "csv", "tabs": {"bi_ops_daily": 56, "bi_ops_weekly": 9, "bi_meta": 25},
                 "hidden_cross_grain": {"day": 0, "week": 1, "month": 1}, "written": True}
    assert steps.summarize_export_bi("export_bi: guard refused the export", "sheets")["written"] is False
