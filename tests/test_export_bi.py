"""The BI export (analytics/export_bi.py): the guard, the check across grains, the dataset builders on a tiny DuckDB
with MetricFlow replaced by made up rows, idempotent csv output and the Sheets writer with a fake client. No network,
no credentials. One integration test runs on the local serving snapshot when it has the BI tables."""
import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "analytics"))
yaml = pytest.importorskip("yaml")
duckdb = pytest.importorskip("duckdb")
import export_bi as eb  # noqa: E402
import metrics as mx  # noqa: E402
import run_log  # noqa: E402
import serving  # noqa: E402

CAT = mx.catalog()


# ---------- a tiny warehouse and a fake MetricFlow ----------

START, END = date(2026, 8, 27), date(2026, 10, 6)       # partial weeks and months at both ends
DATA_DATE = date(2026, 10, 6)


def day_values(d):
    """admissions and ED visits are small every day (as in the demo); bed days mostly above 10."""
    i = (d - START).days
    return {"admissions": 2 + i % 4, "ed_visits": 1 + i % 3, "bed_days": 4 if i < 2 else 12 + i % 9}


DAYS = {START + timedelta(days=i): day_values(START + timedelta(days=i)) for i in range((END - START).days + 1)}
UNITS = {"Medical Unit": 24, "Intensive Care Unit": 10, "Emergency Department": 12, "Cardiac Unit": 16}


def grain_start(d, grain):
    if grain == "week":
        return d - timedelta(days=d.weekday())
    if grain == "month":
        return d.replace(day=1)
    return d


class FakeMF:
    """Stands in for metrics.raw_query: ops metrics from DAYS at any grain, occupancy per unit and month, quality by
    source. The quality rows match the mart fixture below."""

    def __init__(self, quality=None):
        self.calls = []
        self.quality = quality or {
            "hl7v2": {"readmissions_30d": 45, "index_stays": 192, "length_of_stay_days_total": 1027.78},
            "mimic": {"readmissions_30d": 53, "index_stays": 275, "length_of_stay_days_total": 1890.79},
            "synthea": {"readmissions_30d": 4, "index_stays": 76, "length_of_stay_days_total": 516.46},
        }

    def __call__(self, names, group_by=(), where=(), order=()):
        self.calls.append({"metrics": list(names), "group_by": list(group_by), "where": list(where)})
        if "bed_occupancy_rate" in names:
            out = []
            for month in sorted({grain_start(d, "month") for d in DAYS}):
                days = [d for d in DAYS if grain_start(d, "month") == month]
                for k, (unit, beds) in enumerate(UNITS.items()):
                    occupied = None if unit == "Emergency Department" else (
                        5 if unit == "Intensive Care Unit" and month.month == 8 else 3 * len(days) + k)
                    cap = beds * len(days)
                    out.append({"metric_time__month": f"{month} 00:00:00", "location__unit_name": unit,
                                "bed_occupancy_rate": None if occupied is None else occupied / cap,
                                "bed_days": occupied, "capacity_bed_days": cap})
            return out
        if "readmission_rate_30d" in names:
            out = []
            for src, q in self.quality.items():
                r = {"encounter__source": src, **q}
                r["readmission_rate_30d"] = q["readmissions_30d"] / q["index_stays"]
                r["average_length_of_stay_days"] = q["length_of_stay_days_total"] / q["index_stays"]
                out.append({k: r[k] for k in ["encounter__source", *names]})
            return out
        time = next(g for g in group_by if g.startswith("metric_time__"))
        grain = time.split("__")[1]
        acc = {}
        for d, v in DAYS.items():
            k = grain_start(d, grain)
            acc.setdefault(k, {m: 0 for m in names})
            for m in names:
                acc[k][m] += v[m]
        return [{time: f"{k} 00:00:00", "encounter__source": "hl7v2", **acc[k]} for k in sorted(acc)]


MART = [  # period_type, period_start, measure_id, name, source, unit, numerator, denominator, rate, value, supp, reason
    ("all_time", "1943-01-01", "readmission_30d", "30 day all cause readmission rate", "all", "proportion", 102.0, 543, 0.1878, 0.1878, False, None),
    ("all_time", "1943-01-01", "readmission_30d", "30 day all cause readmission rate", "hl7v2", "proportion", 45.0, 192, 0.2344, 0.2344, False, None),
    ("all_time", "1943-01-01", "readmission_30d", "30 day all cause readmission rate", "mimic", "proportion", None, None, None, None, True, "complementary"),
    ("all_time", "1943-01-01", "readmission_30d", "30 day all cause readmission rate", "synthea", "proportion", None, None, None, None, True, "small cell"),
    ("all_time", "1943-01-01", "los_mean_days", "Average length of stay", "hl7v2", "days", 1027.78, 192, None, 5.35, False, None),
    ("all_time", "1943-01-01", "los_mean_days", "Average length of stay", "mimic", "days", 1890.79, 275, None, 6.88, False, None),
    ("all_time", "1943-01-01", "los_mean_days", "Average length of stay", "synthea", "days", 516.46, 76, None, 6.8, False, None),
    ("year", "2026-01-01", "readmission_30d", "30 day all cause readmission rate", "hl7v2", "proportion", 45.0, 192, 0.2344, 0.2344, False, None),
    ("year", "2025-01-01", "readmission_30d", "30 day all cause readmission rate", "synthea", "proportion", None, None, None, None, True, "small cell"),
    ("quarter", "2026-07-01", "readmission_30d", "30 day all cause readmission rate", "hl7v2", "proportion", 20.0, 90, 0.2222, 0.2222, False, None),
]


def make_warehouse(path):
    con = duckdb.connect(str(path))
    con.execute("""CREATE TABLE mart_quality_period (period_type VARCHAR, period_start DATE, measure_id VARCHAR,
                   measure_name VARCHAR, source VARCHAR, unit VARCHAR, numerator DOUBLE, denominator BIGINT,
                   rate DOUBLE, value DOUBLE, is_suppressed BOOLEAN, suppression_reason VARCHAR)""")
    con.executemany("INSERT INTO mart_quality_period VALUES (?, CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", MART)
    con.execute("""CREATE TABLE mart_suppression_report AS SELECT * FROM (VALUES
                   ('mart_quality_monthly', 'month by source', 1, 1429, 1412, 0, 98.8),
                   ('mart_quality_period', 'all time all sources', 7, 6, 1, 0, 16.7))
                   t(mart, grain, grain_order, cells, suppressed, suppressed_complementary, pct_suppressed)""")
    people = []
    for sex in ("female", "male"):
        for band, n_mimic, n_hl7, n_both in (("40s", 14, 20, 0), ("50s", 3, 25, 2), ("60s", 12, 4, 0)):
            people += [(sex, band, "mimic")] * n_mimic + [(sex, band, "hl7v2")] * n_hl7
            people += [(sex, band, "hl7v2,synthea")] * n_both
    con.execute("CREATE TABLE dim_patient (patient_sk VARCHAR, sex VARCHAR, age_band VARCHAR, source_list VARCHAR)")
    con.executemany("INSERT INTO dim_patient VALUES (?, ?, ?, ?)",
                    [(f"sk{i:04d}", *p) for i, p in enumerate(people)])
    con.execute("""CREATE TABLE gold_terminology_quality AS SELECT * FROM (VALUES
                   ('hl7v2', 11333, 11333, 100.0, 680, 437, 0, 455, 66.9, 194),
                   ('mimic', 95258, 81134, 85.2, 5051, 2442, 0, 4447, 88.0, 604))
                   t(source, numeric_lab_results, with_loinc, pct_loinc, diagnoses, arrived_icd9, arrived_snomed,
                     with_icd10, pct_icd10, need_review)""")
    con.execute("""CREATE TABLE gold_empi_quality AS SELECT 20 AS true_match_pairs, 10 AS hard_negative_pairs,
                   19 AS auto_linked, 1.0 AS precision, 0.95 AS recall""")
    con.execute("""CREATE TABLE gold_feed_freshness AS SELECT DATE '2026-10-06' AS data_date,
                   TIMESTAMP '2026-10-06 23:35:00' AS last_event_at, TIMESTAMP '2026-10-07 07:05:00' AS extracted_at""")
    con.close()


FEED_RUNS = [  # as saved by extract, with the patient event fields the export must leave out
    {"run_id": "2026-10-05-a", "day": "2026-10-05", "started_at": "x", "finished_at": "2026-10-06T06:16:00+00:00",
     "duration_s": 0.6, "status": "success", "messages": 69, "process": 41, "skip": 22, "duplicate": 5, "error": 1,
     "resources_loaded": 233, "resources_created": 175, "load_failed": 0, "pct_loinc": 100.0, "new_admissions": 2,
     "discharges": 1, "ed_visits": 2, "transfers": 2, "open_admissions": 18},
    {"run_id": "2026-10-06-a", "day": "2026-10-06", "finished_at": "2026-10-07T06:16:00+00:00", "duration_s": 1,
     "status": "failed", "messages": 0, "process": 0, "skip": 0, "duplicate": 0, "error": 0, "resources_loaded": 0,
     "load_failed": 3, "pct_loinc": None, "new_admissions": 3, "discharges": 4},
    {"run_id": "2026-10-06-b", "day": "2026-10-06", "finished_at": "2026-10-07T08:00:00+00:00", "duration_s": 0.9,
     "status": "success", "messages": 71, "process": 44, "skip": 21, "duplicate": 4, "error": 2,
     "resources_loaded": 250, "load_failed": 0, "pct_loinc": 98.5, "new_admissions": 3, "discharges": 4,
     "ed_visits": 1, "transfers": 0, "open_admissions": 17},
]
QUEUE = [("pending", "1959-6", "0.95"), ("pending", "2345-7", "0.9"), ("pending", "1234-5", "0.6"),
         ("pending", "1234-5", "0.4"), ("pending", "", "0.9"), ("accepted", "2951-2", "0.8")]


def build_record(i, status="success"):
    started = datetime(2026, 10, 7, 7, 10 + i, tzinfo=timezone.utc)
    finished = started + timedelta(seconds=95)
    return run_log.make_record(started, finished, status,
                               {"tests_pass": 405, "tests_warn": 1, "tests_fail": 0, "tests_error": 0,
                                "tests_skipped": 0, "models_built": 88},
                               {"counts": {"Patient": {"extracted": 478, "server_total": 478}}},
                               {"rows_bronze_resources": 220000, "rows_silver_observation": 201978,
                                "rows_fact_encounter": 6349, "rows_fact_lab_result": 129000},
                               {"Patient": datetime(2026, 10, 6, 23, 40),
                                "Encounter": datetime(2026, 10, 6, 23, 35),
                                "Observation": datetime(2026, 10, 6, 23, 36)},
                               "0123456789ab")


def write_health(folder):
    """The pipeline health inputs: two builds in the log, the feed's run history, a manifest, a review queue."""
    landing = folder / "landing"
    landing.mkdir(parents=True)
    (landing / "feed_runs.json").write_text(json.dumps(FEED_RUNS))
    (landing / "manifest.json").write_text(json.dumps({"extracted_at": "2026-10-07 07:05:00", "counts": {
        "Patient": {"extracted": 478, "server_total": 478}, "Observation": {"extracted": 201978,
                                                                            "server_total": 201980}}}))
    log = folder / "run_history" / "builds.ndjson"
    run_log.append(log, build_record(0))
    run_log.append(log, build_record(1, "failed"))
    queue = folder / "queue.csv"
    queue.write_text("local_id,suggested_loinc,confidence,status\n"
                     + "".join(f"mimic:{i},{c},{conf},{st}\n" for i, (st, c, conf) in enumerate(QUEUE)))
    return {"build_log": str(log), "landing": str(landing), "review_queue": str(queue)}


@pytest.fixture
def snap(tmp_path):
    make_warehouse(tmp_path / "warehouse.duckdb")
    return {"version": "20261020T000000Z", "db": str(tmp_path / "warehouse.duckdb"), "target": str(tmp_path / "t"),
            "created_at": "2026-10-20T00:00:00+00:00", "data_date": DATA_DATE.isoformat(),
            "tables": {t: 1 for t in ("fact_encounter", *serving.BI_TABLES)},
            "health": write_health(tmp_path / "health")}


def datasets(snap, run=None):
    return eb.build_datasets(snap, run=run or FakeMF(), cat=CAT, **snap["health"])


@pytest.fixture
def built(snap):
    tabs, stats = datasets(snap)
    tabs["bi_meta"] = eb.meta_rows(tabs, snap, stats, now=None)
    return tabs, stats


def counts_in(tabs):
    return [(t, c, r[c]) for t in eb.TABS for c, _, k in eb.SCHEMAS[t] if k == "count" for r in tabs[t]]


# ---------- the guard ----------

def test_every_tab_is_declared_and_no_declared_column_is_refused():
    assert eb.TABS == ("bi_ops_daily", "bi_ops_weekly", "bi_ops_monthly", "bi_occupancy_monthly", "bi_quality",
                       "bi_population", "bi_suppression", "bi_pipeline_runs", "bi_feed_runs", "bi_data_quality",
                       "bi_meta")
    for tab, schema in eb.SCHEMAS.items():
        assert all(eb.column_problem(c) is None for c, _, _ in schema), tab
        assert eb.operational_problems(schema) == [], tab
        assert all(t in ("int", "float", "bool", "date", "str") for _, t, _ in schema), tab
        assert all(k in ("dim", "count", "op_count", "rate", "value", "flag", "text") for _, _, k in schema), tab


@pytest.mark.parametrize("col", ["patient_sk", "encounter_id", "location_key", "key", "mrn", "patient_ref",
                                 "pseudonym", "birth_year", "name", "postal_code", "fhir_url"])
def test_guard_rejects_a_planted_id_or_patient_column(col):
    rows = [{"sex": "female", "age_band": "40s", "source": "all", "people": 20, "is_suppressed": False, col: "x"}]
    problems = eb.check_tab("bi_population", rows)
    assert any(col in p for p in problems)
    schema = eb.SCHEMAS["bi_population"] + ((col, "str", "dim"),)
    assert any(f"column {col}" in p for p in eb.check_tab("bi_population", [], schema))


def test_measure_id_is_allowed():
    assert eb.column_problem("measure_id") is None and eb.column_problem("unit_id") is not None


@pytest.mark.parametrize("value,ok", [(7, False), (1, False), (10, False), (0, True), (11, True), (None, True),
                                      (-1, False)])
def test_guard_rejects_a_planted_count_of_1_to_10(value, ok):
    rows = [{"sex": "female", "age_band": "40s", "source": "all", "people": value, "is_suppressed": value is None}]
    assert (eb.check_tab("bi_population", rows) == []) == ok
    if value == 7:
        assert eb.check_tab("bi_population", rows) == ["bi_population row 0: people is a count of 7"]


def test_guard_checks_every_count_column():
    row = {"period": "2026-10-05", "source": "hl7v2", "admissions": 25, "ed_visits": 7, "bed_days": 120,
           "is_suppressed": False}
    assert eb.check_tab("bi_ops_daily", [row]) == ["bi_ops_daily row 0: ed_visits is a count of 7"]
    q = {"origin": "quality_mart", "period_type": "year", "period_start": "2026-01-01", "measure_id": "readmission_30d",
         "measure_name": "x", "source": "all", "unit": "proportion", "numerator": 3, "denominator": 40, "rate": 0.075,
         "value": 0.075, "is_suppressed": False, "suppression_reason": None}
    assert eb.check_tab("bi_quality", [q]) == ["bi_quality row 0: numerator is a count of 3"]


def test_guard_rejects_schema_drift():
    good = {"mart": "m", "grain": "g", "grain_order": 1, "pct_suppressed": 50.0}
    assert eb.check_tab("bi_suppression", [good]) == []
    extra = {**good, "cells": 6}
    missing = {k: v for k, v in good.items() if k != "grain"}
    reordered = {"grain": "g", "mart": "m", "grain_order": 1, "pct_suppressed": 50.0}
    wrong_type = {**good, "grain_order": "1"}
    assert "extra ['cells']" in eb.check_tab("bi_suppression", [extra])[0]
    assert "missing ['grain']" in eb.check_tab("bi_suppression", [missing])[0]
    assert "out of order" in eb.check_tab("bi_suppression", [reordered])[0]
    assert "declared int" in eb.check_tab("bi_suppression", [wrong_type])[0]
    assert "is empty" in eb.check_tab("bi_suppression", [{**good, "mart": None}])[0]
    bad_date = {"period": "2026/10/05", "source": "hl7v2", "admissions": None, "ed_visits": None, "bed_days": None,
                "is_suppressed": True}
    assert "declared date" in eb.check_tab("bi_ops_daily", [bad_date])[0]


@pytest.mark.parametrize("value", ["RV200003", "P-0A1B2C3D4E", "riverside-rv200003", "Patient/123",
                                   "0123456789abcdef0123456789abcdef"])
def test_guard_rejects_identifier_looking_values(value):
    row = {"item": "note", "value": value}
    assert eb.check_tab("bi_meta", [row]) == ["bi_meta row 0: value holds an identifier looking value"]


def test_check_all_needs_every_tab_and_only_those(built):
    tabs, _ = built
    eb.check_all(tabs)
    with pytest.raises(eb.GuardError, match="missing tab bi_meta"):
        eb.check_all({k: v for k, v in tabs.items() if k != "bi_meta"})
    with pytest.raises(eb.GuardError, match="unexpected tab bi_secret"):
        eb.check_all({**tabs, "bi_secret": []})


# ---------- the check across grains ----------

D = date(2026, 8, 24)   # a Monday


def week(start, shown=True, value=0):
    return {"grain": "week", "start": start, "end": start + timedelta(days=6), "shown": shown, "value": value}


def test_week_minus_its_shown_days():
    days = {D + timedelta(days=i): (v, ok) for i, (v, ok) in enumerate(
        [(11, True), (3, False), (4, False), (2, False), (0, True), (0, True), (0, True)])}
    leaks = eb.interval_leaks(days, [week(D, value=20)])
    assert leaks == [[(D, D + timedelta(days=6))]]                # 20 - 11 = 9 hidden: a leak
    hid_days, hid_aggs = eb.protect_grains(days, [week(D, value=20)])
    assert hid_days == [D] and hid_aggs == [] and not days[D][1]  # the 11 is hidden too: 20 hidden, no leak
    assert eb.interval_leaks(days, [week(D, value=20)]) == []


def test_month_minus_whole_weeks_gives_the_partial_days():
    """August = the week of the 17th + the week of the 24th + 31 August: hidden days everywhere, but the month and
    both weeks shown would give back 31 August (3)."""
    days = {}
    for i in range(14, 31 + 9):
        d = date(2026, 8, 1) + timedelta(days=i)
        days[d] = (3 if d.month == 8 and d.day >= 17 else 0, not (d.month == 8 and d.day >= 17))
    w1, w2, w3 = week(date(2026, 8, 17), value=21), week(date(2026, 8, 24), value=21), week(date(2026, 8, 31), False, 3)
    month = {"grain": "month", "start": date(2026, 8, 1), "end": date(2026, 8, 31), "shown": True, "value": 45}
    aggs = [w1, w2, w3, month]
    assert eb.interval_leaks(days, aggs)
    hid_days, hid_aggs = eb.protect_grains(days, aggs)
    assert hid_days == [] and len(hid_aggs) == 1 and hid_aggs[0]["grain"] == "week"   # a week goes, the month stays
    assert month["shown"] and eb.interval_leaks(days, aggs) == []


def test_differences_of_overlapping_intervals_are_found():
    """Two shown weeks and the month that holds the second: month minus week 2 is the first few days."""
    days = {D + timedelta(days=i): (2 if i < 3 else 20, i >= 3) for i in range(14)}
    month = {"grain": "month", "start": D + timedelta(days=3), "end": D + timedelta(days=13), "shown": True,
             "value": 220}
    aggs = [week(D, value=6 + 80), week(D + timedelta(days=7), value=140), month]
    leaks = eb.interval_leaks(days, aggs)
    assert leaks and all(sum(days[d][0] for s, e in u for d in days if s <= d <= e and not days[d][1]) <= 10
                         for u in leaks)
    eb.protect_grains(days, aggs)
    assert eb.interval_leaks(days, aggs) == []


def test_nothing_changes_when_nothing_leaks():
    days = {D + timedelta(days=i): (3, False) for i in range(7)}
    aggs = [week(D, value=21)]
    assert eb.protect_grains(days, aggs) == ([], []) and aggs[0]["shown"]


def test_complement_against_a_known_total():
    shown = [False, True, True, True]
    assert eb.complement_against_total([5, 0, 40, 30], shown) == [3]   # 5 hidden: the smallest non zero (30) too
    assert shown == [False, True, True, False]
    assert eb.complement_against_total([0, 40], [True, True]) == []


# ---------- the datasets ----------

def test_ops_tabs_follow_the_saved_query_and_hide_every_small_count(built):
    tabs, stats = built
    daily, weekly, monthly = tabs["bi_ops_daily"], tabs["bi_ops_weekly"], tabs["bi_ops_monthly"]
    assert len(daily) == len(DAYS) and {r["source"] for r in daily} == {"hl7v2"}
    assert [r["period"] for r in monthly] == ["2026-08-01", "2026-09-01", "2026-10-01"]
    assert all(r["admissions"] is None and r["ed_visits"] is None for r in daily)   # 1 to 5 a day: all hidden
    assert any(r["bed_days"] for r in daily)
    assert [r["is_complete_period"] for r in monthly] == [True, True, False]
    assert weekly[-1]["is_complete_period"] is False and weekly[0]["is_complete_period"] is True
    assert not [x for x in counts_in(tabs) if mx.is_small(x[2])]
    assert stats["hidden_cross_grain_week"] + stats["hidden_cross_grain_month"] + stats["hidden_cross_grain_day"] > 0


def test_no_sum_of_hidden_days_can_be_worked_out(built):
    """An independent look at the published tabs: for every metric, the shown weeks and months against the days."""
    tabs, _ = built
    for m in ("admissions", "ed_visits", "bed_days"):
        days = {date.fromisoformat(r["period"]): (DAYS[date.fromisoformat(r["period"])][m], r[m] is not None)
                for r in tabs["bi_ops_daily"]}
        aggs = []
        for t, g in (("bi_ops_weekly", "week"), ("bi_ops_monthly", "month")):
            for r in tabs[t]:
                s = date.fromisoformat(r["period"])
                aggs.append({"grain": g, "start": s, "end": eb.period_end(s, g), "shown": r[m] is not None,
                             "value": r[m] or 0})
        for d in eb.day_range(min(a["start"] for a in aggs), max(a["end"] for a in aggs)):
            days.setdefault(d, (0, True))
        assert eb.interval_leaks(days, aggs) == [], m


def test_ops_refuses_weeks_that_do_not_add_up(snap):
    class Broken(FakeMF):
        def __call__(self, names, group_by=(), where=(), order=()):
            rows = super().__call__(names, group_by, where, order)
            if "metric_time__week" in group_by:
                rows[0]["admissions"] += 1
            return rows
    with pytest.raises(eb.ExportError, match="its days add up to"):
        datasets(snap, run=Broken())


def test_occupancy_rates_with_the_unit_check(built):
    tabs, stats = built
    occ = tabs["bi_occupancy_monthly"]
    aug = {r["unit_name"]: r for r in occ if r["period"] == "2026-08-01"}
    assert aug["Emergency Department"]["bed_occupancy_rate"] == 0.0      # beds, nobody at midnight: 0, not empty
    assert aug["Intensive Care Unit"]["bed_occupancy_rate"] is None       # 5 occupied bed days: hidden
    # the hidden 5 would come back from the month's bed days minus the shown units: one more is hidden
    assert sum(r["is_suppressed"] for r in aug.values()) == 2 and stats["hidden_occupancy_complement"] == 1
    assert all(r["bed_occupancy_rate"] is None or 0 <= r["bed_occupancy_rate"] <= 1 for r in occ)


def test_quality_semantic_cells_follow_the_mart(built):
    tabs, _ = built
    q = tabs["bi_quality"]
    assert {r["period_type"] for r in q} == {"all_time", "year"}                      # no quarters
    sem = {(r["measure_id"], r["source"]): r for r in q if r["origin"] == "semantic_layer"}
    assert sem[("readmission_30d", "hl7v2")]["value"] == 0.2344 and not sem[("readmission_30d", "hl7v2")]["is_suppressed"]
    assert sem[("readmission_30d", "mimic")]["value"] is None                         # shown by mf, hidden in the mart
    assert sem[("readmission_30d", "mimic")]["suppression_reason"] == "hidden in the quality mart"
    assert sem[("readmission_30d", "synthea")]["suppression_reason"] == "small cell"  # 4 of 76
    assert sem[("los_mean_days", "mimic")]["value"] == 6.88
    mart_mean = next(r for r in q if r["origin"] == "quality_mart" and r["measure_id"] == "los_mean_days")
    assert mart_mean["numerator"] is None and mart_mean["denominator"] == 192         # a total of days is not a count


def test_quality_refuses_when_the_semantic_layer_and_the_mart_disagree(snap):
    fake = FakeMF()
    fake.quality["hl7v2"]["readmissions_30d"] = 50
    with pytest.raises(eb.ExportError, match="disagree on readmission_30d for hl7v2"):
        datasets(snap, run=fake)


def test_population_is_complementary_along_source(built):
    tabs, _ = built
    pop = tabs["bi_population"]
    assert {r["source"] for r in pop} == {"all", "mimic", "hl7v2", "hl7v2 and synthea"}
    by = {(r["sex"], r["age_band"], r["source"]): r["people"] for r in pop}
    assert by[("female", "50s", "all")] == 30
    # 50s: mimic 3 and both 2 are small, 5 hidden: hl7v2 25 is hidden too, so all minus shown gives nothing back
    assert by[("female", "50s", "mimic")] is None and by[("female", "50s", "hl7v2 and synthea")] is None
    assert by[("female", "50s", "hl7v2")] is None and by[("female", "40s", "mimic")] == 14
    for (sex, band, src), n in by.items():
        if src == "all" and n is not None:
            shown = sum(v or 0 for (s, b, x), v in by.items() if (s, b) == (sex, band) and x != "all")
            assert not mx.is_small(n - shown), (sex, band)


def test_suppression_tab_has_percentages_only(built):
    tabs, _ = built
    assert tabs["bi_suppression"] == [
        {"mart": "mart_quality_monthly", "grain": "month by source", "grain_order": 1, "pct_suppressed": 98.8},
        {"mart": "mart_quality_period", "grain": "all time all sources", "grain_order": 7, "pct_suppressed": 16.7}]


def test_meta_keeps_the_export_time_when_nothing_changed(built, snap):
    tabs, stats = built
    t1 = datetime(2026, 10, 20, 7, 0, tzinfo=timezone.utc)
    t2 = datetime(2026, 10, 21, 7, 0, tzinfo=timezone.utc)
    first = {r["item"]: r["value"] for r in eb.meta_rows(tabs, snap, stats, now=t1)}
    again = {r["item"]: r["value"] for r in eb.meta_rows(tabs, snap, stats, previous=first, now=t2)}
    assert again == first and first["exported_at"] == "2026-10-20T07:00:00Z"
    tabs["bi_suppression"][0]["pct_suppressed"] = 99.0
    changed = {r["item"]: r["value"] for r in eb.meta_rows(tabs, snap, stats, previous=first, now=t2)}
    assert changed["exported_at"] == "2026-10-21T07:00:00Z" and changed["content_hash"] != first["content_hash"]
    assert first["data_as_of"] == "2026-10-06" and first["rows_bi_ops_daily"] == str(len(DAYS))
    assert "Not a real hospital" in first["demo_notice"]


# ---------- targets ----------

@pytest.fixture
def cli(snap, tmp_path, monkeypatch):
    """main() on the tiny warehouse: a serving pointer next to it and the fake MetricFlow."""
    srv = tmp_path / "serving"
    srv.mkdir()
    serving.write_pointer(srv, snap)
    real = eb.build_datasets
    monkeypatch.setattr(eb, "build_datasets", lambda s, **kw: real(s, run=FakeMF(), cat=CAT, **snap["health"]))
    out = tmp_path / "bi"
    return lambda *args: eb.main(["--serving", str(srv), "--out", str(out), "--no-env-file", *args]), out


def test_dry_run_counts_match_the_csv(cli, capsys):
    run, out = cli
    assert run("export", "--target", "csv", "--dry-run") == 0
    printed = capsys.readouterr().out
    assert not out.exists()
    counts = {line.split()[0]: int(line.split()[1]) for line in printed.splitlines() if line.strip().startswith("bi_")}
    assert set(counts) == set(eb.TABS)
    assert run("export", "--target", "csv") == 0
    for t in eb.TABS:
        lines = (out / f"{t}.csv").read_text(encoding="utf-8").splitlines()
        assert lines[0] == ",".join(c for c, _, _ in eb.SCHEMAS[t]) and len(lines) - 1 == counts[t], t


def test_csv_export_is_idempotent(cli, capsys):
    run, out = cli
    assert run("export", "--target", "csv") == 0
    first = {p.name: p.read_bytes() for p in sorted(out.glob("*.csv"))}
    assert run("export", "--target", "csv") == 0
    assert {p.name: p.read_bytes() for p in sorted(out.glob("*.csv"))} == first
    assert len(first) == len(eb.TABS) and not list(out.glob(".*"))
    assert run("verify", "--target", "csv") == 0
    assert "0 values from 1 to 10" in capsys.readouterr().out


def test_verify_catches_a_count_planted_in_the_csv(cli, capsys):
    run, out = cli
    assert run("export", "--target", "csv") == 0
    path = out / "bi_ops_weekly.csv"
    lines = path.read_text().splitlines()
    cells = lines[1].split(",")
    cells[4] = "7"                                  # bed_days of the first week
    path.write_text("\n".join([lines[0], ",".join(cells), *lines[2:]]) + "\n")
    assert run("verify", "--target", "csv") == 1
    assert "bed_days is a count of 7" in capsys.readouterr().err


def test_no_snapshot_or_an_old_one(tmp_path, snap, capsys):
    assert eb.main(["--serving", str(tmp_path / "none"), "export", "--target", "csv", "--dry-run"]) == 1
    assert "no serving snapshot" in capsys.readouterr().err
    srv = tmp_path / "old"
    srv.mkdir()
    serving.write_pointer(srv, {**snap, "tables": {"fact_encounter": 1}})
    assert eb.main(["--serving", str(srv), "export", "--target", "csv", "--dry-run"]) == 1
    assert ("has no mart_quality_period, mart_suppression_report, gold_terminology_quality, gold_empi_quality, "
            "gold_feed_freshness") in capsys.readouterr().err


class FakeWorksheet:
    """Like the Sheets API: a grid that resize, clear and update change; get drops trailing empty cells and rows."""

    def __init__(self, title, rows, cols, log):
        self.title, self.grid, self.log, self.frozen = title, [[""] * cols for _ in range(rows)], log, 0

    def resize(self, rows, cols):
        self.log.append(("resize", self.title))
        self.grid = [(r + [""] * cols)[:cols] for r in self.grid[:rows]] + [[""] * cols] * max(0, rows - len(self.grid))

    def clear(self):
        self.log.append(("clear", self.title))
        self.grid = [[""] * len(r) for r in self.grid]

    def update(self, values, range_name, value_input_option):
        assert range_name == "A1" and value_input_option == "RAW"
        self.log.append(("update", self.title))
        for i, row in enumerate(values):
            if i >= len(self.grid):
                raise AssertionError("update past the grid")
            for j, v in enumerate(row):
                assert v is not None and not isinstance(v, (dict, list))
                self.grid[i][j] = v

    def freeze(self, rows):
        self.frozen = rows

    def get(self, value_render_option):
        assert value_render_option == "UNFORMATTED_VALUE"
        out = []
        for r in self.grid:
            r = list(r)
            while r and r[-1] == "":
                r.pop()
            out.append(r)
        while out and not out[-1]:
            out.pop()
        return json.loads(json.dumps(out))        # numbers come back as JSON numbers, as from the API


class FakeSpreadsheet:
    def __init__(self):
        self.log = []
        self.sheets = [FakeWorksheet("Notes", 3, 2, self.log)]
        self.sheets[0].grid[0][0] = "my own notes"

    def worksheets(self):
        return list(self.sheets)

    def add_worksheet(self, title, rows, cols):
        self.log.append(("add", title))
        ws = FakeWorksheet(title, rows, cols, self.log)
        self.sheets.append(ws)
        return ws


def test_sheets_writer_full_replace_and_identical_rerun(built, snap):
    tabs, stats = built
    sh = FakeSpreadsheet()
    target = eb.SheetsTarget(sh)
    eb.export(tabs, target)
    assert [w.title for w in sh.worksheets()] == ["Notes", *eb.TABS]
    assert [e for e in sh.log if e[0] == "update"][-1] == ("update", "bi_meta")       # bi_meta last
    first = {t: eb.values_hash(target.read(t)) for t in eb.TABS}
    for t in eb.TABS:
        assert target.read(t) == eb.expected_sheet_values(t, tabs[t]), t
        assert eb.typed_rows(t, target.read(t)) == tabs[t], t
    assert target.read_meta()["content_hash"] == eb.content_hash(tabs)
    # the rerun reuses the export time, so every tab is the same
    tabs["bi_meta"] = eb.meta_rows(tabs, snap, stats, previous=target.read_meta())
    eb.export(tabs, target)
    assert {t: eb.values_hash(target.read(t)) for t in eb.TABS} == first
    assert sh.sheets[0].grid[0][0] == "my own notes" and len(sh.worksheets()) == 1 + len(eb.TABS)
    # fewer rows next time: the old ones are gone, not left below
    tabs["bi_population"] = tabs["bi_population"][:3]
    eb.export(tabs, target)
    assert len(target.read("bi_population")) == 4


def test_sheets_writer_writes_nothing_when_the_guard_fails(built):
    tabs, _ = built
    tabs["bi_population"][0]["people"] = 7
    sh = FakeSpreadsheet()
    with pytest.raises(eb.GuardError, match="people is a count of 7"):
        eb.export(tabs, eb.SheetsTarget(sh))
    assert sh.log == []


def test_settings_never_show_their_values(tmp_path, capsys):
    env_file = tmp_path / ".env"
    env_file.write_text("BI_SHEET_ID=sheet-abc-123\nBI_CREDENTIALS=data/secrets/key.json\n")
    sheet_id, path = eb.settings(env_file, env={})
    assert sheet_id == "sheet-abc-123" and path == ROOT / "data/secrets/key.json"
    assert eb.settings(env_file, env={"BI_SHEET_ID": "from-env"})[0] == "from-env"
    with pytest.raises(eb.ExportError) as e:
        eb.settings(None, env={})
    assert "must be set" in str(e.value)
    with pytest.raises(eb.ExportError) as e:
        eb.settings(None, env={"BI_SHEET_ID": "sheet-abc-123"})
    assert "sheet-abc-123" not in str(e.value)
    assert eb.redact("open sheet-abc-123 with /k/key.json", ["sheet-abc-123", "/k/key.json"]) == \
        "open <redacted> with <redacted>"


def test_a_missing_key_file_is_reported_without_its_path(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("BI_SHEET_ID", "sheet-abc-123")
    monkeypatch.setenv("BI_CREDENTIALS", str(tmp_path / "nowhere" / "secret-key.json"))
    assert eb.main(["--serving", str(tmp_path), "--no-env-file", "verify", "--target", "sheets"]) == 1
    err = capsys.readouterr().err
    assert "does not exist" in err and "secret-key" not in err and "sheet-abc-123" not in err


def test_requirements_bi_are_pinned():
    lines = [x.split("#")[0].strip() for x in (ROOT / "analytics" / "requirements-bi.txt").read_text().splitlines()]
    assert sorted(x for x in lines if x) == ["google-auth==2.62.0", "gspread==6.2.1"]


# ---------- integration: the local serving snapshot, if it has the BI tables ----------

def test_integration_on_the_local_snapshot():
    pytest.importorskip("dbt_metricflow")
    snap = serving.read_pointer(ROOT / "data" / "analytics" / "serving")
    if not snap or any(t not in (snap.get("tables") or {}) for t in serving.BI_TABLES):
        pytest.skip("no local serving snapshot with the BI tables (python3 analytics/build.py snapshot)")
    tabs, stats = eb.build_datasets(snap)
    tabs["bi_meta"] = eb.meta_rows(tabs, snap, stats)
    eb.check_all(tabs)
    assert all(tabs[t] for t in eb.TABS if t != "bi_pipeline_runs")   # the build log may not exist yet
    assert not [x for x in counts_in(tabs) if mx.is_small(x[2])]
