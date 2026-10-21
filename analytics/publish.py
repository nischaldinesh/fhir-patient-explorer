"""Publish Gold results back to the FHIR server, for apps that read it (worklists, snapshots).

The app talks only to the FHIR server, so it needs no database access and still works when deployed.
Everything is saved as a FHIR Binary holding JSON, the same way the HL7 v2 pipeline publishes its run report:

  Binary/analytics-snapshot    aggregates only (no Bronze or Silver data, analyst sample rows by pseudonym), for the
                               analyst's Analytics page: the archive, plus the live feed's weekly metrics and daily
                               census for the last 12 weeks and how fresh the data is
  Binary/worklist-bedboard     PHI: the bed board, patient by patient (name, MRN, bed, attending, lab flags)
  Binary/worklist-readmission  PHI: the LACE follow up list, patient by patient (name, MRN, unit, scores, due dates)
  Binary/analytics-history     a compact record of every publish, newest first, the last 400, with the dbt test
                               results (counts and the tests that did not pass) for the data engineer's pipeline page

The two worklists are what is called reverse ETL in the real world: scores computed in the warehouse pushed back to
the care team's own tools. They hold protected health information, so only clinical roles should read them; the snapshot never holds a name, MRN or patient id.
"""
import base64
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from tools import http  # noqa: E402

SNAPSHOT_ID = "analytics-snapshot"
BEDBOARD_ID = "worklist-bedboard"
READMISSION_ID = "worklist-readmission"
HISTORY_ID = "analytics-history"
HISTORY_CAP = 400
TREND_WEEKS = 12
LOS_BUCKETS = [("under 1 day", 0, 1), ("1 to 2 days", 1, 2), ("2 to 4 days", 2, 4), ("4 to 7 days", 4, 7),
               ("7 to 14 days", 7, 14), ("14 days or more", 14, 100000)]
COHORT_LABELS = {
    "hypertension": "Hypertension",
    "low_sodium": "Low sodium (below 135)",
    "diabetes_type2": "Type 2 diabetes",
    "aki_signal": "Kidney injury signal",
    "readmitted_30d": "Readmitted within 30 days",
    "heart_failure": "Heart failure",
    "sepsis": "Sepsis",
}


def rows(con, sql, params=None):
    cur = con.execute(sql, params or [])
    names = [d[0] for d in cur.description]
    return [dict(zip(names, (v.isoformat() if hasattr(v, "isoformat") else v for v in r))) for r in cur.fetchall()]


def latest_runs_by_day(runs):
    """The feed's run history can hold several runs of the same day (a retry, a rerun); keep the newest of each
    day, by finish time. Newest day first."""
    newest = {}
    for r in runs or []:
        if not isinstance(r, dict) or not r.get("day"):
            continue
        kept = newest.get(r["day"])
        if kept is None or (r.get("finished_at") or "") > (kept.get("finished_at") or ""):
            newest[r["day"]] = r
    return [newest[d] for d in sorted(newest, reverse=True)]


def last_loaded_run(runs):
    """The newest run of the newest day that loaded data (success or partial), as a short record, or None."""
    for r in latest_runs_by_day(runs):
        if r.get("status") in ("success", "partial"):
            return {k: r.get(k) for k in ("day", "status", "finished_at")}
    return None


def freshness(con, runs=None):
    """How current the live feed data is: the data date (the day the data runs to), the last feed event and the
    extract time from the warehouse, and the last loaded day from the run history."""
    f = rows(con, "SELECT data_date, last_event_at, extracted_at FROM gold_feed_freshness")
    f = f[0] if f else {"data_date": None, "last_event_at": None, "extracted_at": None}
    return {**f, "last_run": last_loaded_run(runs)}


def live_aggregates(con):
    """The live feed's weekly metrics and daily census for the last TREND_WEEKS weeks. Aggregates only."""
    weeks = rows(con, f"SELECT * FROM gold_weekly_metrics ORDER BY week_start DESC LIMIT {TREND_WEEKS}")[::-1]
    first = weeks[0]["week_start"] if weeks else None
    census = rows(con, "SELECT * FROM gold_daily_census WHERE census_date >= ? ORDER BY census_date", [first]) if first else []
    checks = rows(con, """SELECT test_name, status, failures FROM dq_test_results
                          WHERE SUBSTR(test_name, 1, 5) = 'live_' ORDER BY test_name""")
    return {"weekly_metrics": weeks, "daily_census": census, "data_quality": checks}


def snapshot(con, runs=None):
    as_of = con.execute("SELECT CAST(MAX(loaded_at) AS DATE) FROM bronze_resources").fetchone()[0]
    patients = rows(con, "SELECT source, COUNT(*) AS patients FROM silver_patient GROUP BY source ORDER BY patients DESC")
    readmit = rows(con, """
        SELECT source,
               COUNT(*)                                          AS admissions,
               SUM(CASE WHEN readmitted_30d THEN 1 ELSE 0 END)   AS readmissions,
               ROUND(100.0 * SUM(CASE WHEN readmitted_30d THEN 1 ELSE 0 END) / COUNT(*), 1) AS rate_pct,
               ROUND(MEDIAN(length_of_stay_days), 1)             AS median_los_days
        FROM gold_encounter_summary WHERE is_admission GROUP BY source ORDER BY admissions DESC""")
    total = con.execute("""
        SELECT COUNT(*), SUM(CASE WHEN readmitted_30d THEN 1 ELSE 0 END), ROUND(MEDIAN(length_of_stay_days), 1),
               ROUND(QUANTILE_CONT(length_of_stay_days, 0.9), 1)
        FROM gold_encounter_summary WHERE is_admission""").fetchone()
    los = []
    for label, lo, hi in LOS_BUCKETS:
        n = con.execute("""SELECT COUNT(*) FROM gold_encounter_summary
                           WHERE is_admission AND length_of_stay_days >= ? AND length_of_stay_days < ?""", [lo, hi]).fetchone()[0]
        los.append({"bucket": label, "admissions": n})
    open_stays = con.execute("SELECT COUNT(*) FROM gold_encounter_summary WHERE is_admission AND length_of_stay_days IS NULL").fetchone()[0]
    if open_stays:
        los.append({"bucket": "no discharge time", "admissions": open_stays})
    cohorts = rows(con, """SELECT cohort, patients, mimic, synthea, hl7v2, pct_of_patients, people, records, pct_of_people
                           FROM gold_cohorts ORDER BY patients DESC""")
    for c in cohorts:
        c["label"] = COHORT_LABELS.get(c["cohort"], c["cohort"])
    bmi = rows(con, """SELECT bmi_category AS category, COUNT(*) AS patients FROM gold_bmi
                       GROUP BY bmi_category ORDER BY patients DESC""")
    bmi_check = con.execute("""SELECT SUM(CASE WHEN bmi_reported IS NOT NULL THEN 1 ELSE 0 END),
                                      SUM(CASE WHEN ABS(bmi_computed - bmi_reported) <= 0.5 THEN 1 ELSE 0 END) FROM gold_bmi""").fetchone()
    # Read from the governed view: the page is for analysts, so pseudonyms only, no names or ids.
    aki = rows(con, """
        SELECT patient_key, source, first_signal_at, max_rise_mg_dl, n_signal_results
        FROM analyst_aki_signal ORDER BY max_rise_mg_dl DESC, patient_key LIMIT 15""")
    analyst_sample = rows(con, """SELECT * FROM analyst_patient_summary
                                  ORDER BY n_admissions DESC, patient_key LIMIT 5""")
    analyst_columns = [r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'analyst_patient' ORDER BY ordinal_position").fetchall()]
    return {
        "generated_at": now_iso(),
        "as_of": as_of.isoformat() if as_of else None,
        "patients": patients,
        "people": people_count(con),
        "admissions": {"total": total[0], "readmissions": total[1] or 0,
                       "rate_pct": round(100.0 * (total[1] or 0) / total[0], 1) if total[0] else 0.0,
                       "median_los_days": total[2], "p90_los_days": total[3], "by_source": readmit, "los_buckets": los},
        "cohorts": cohorts,
        "terminology_quality": rows(con, "SELECT * FROM gold_terminology_quality ORDER BY source"),
        "bmi": {"categories": bmi, "reported": bmi_check[0] or 0, "matching": bmi_check[1] or 0},
        "aki_signal": aki,
        "governance": {"analyst_patient_columns": analyst_columns, "analyst_sample": analyst_sample},
        "data_quality": rows(con, "SELECT check_name, failures FROM dq_checks"),
        "freshness": freshness(con, runs),
        "live": live_aggregates(con),
    }


def people_count(con):
    """Persons and patient records in the warehouse: records the EMPI linked are one person (one master id in
    gold_patient_crosswalk). Aggregates only."""
    people, records = con.execute(
        "SELECT COUNT(DISTINCT master_id), COUNT(*) FROM gold_patient_crosswalk").fetchone()
    return {"people": people, "records": records}


# PHI: patient level rows with name and MRN. Only the clinical roles may read these worklists in the app.
def bedboard(con, runs=None):
    units = rows(con, "SELECT * FROM gold_unit_occupancy ORDER BY unit_name")
    beds = rows(con, """
        SELECT unit_location_id, bed_location_id, bed_name, bed_status, is_occupied, encounter_id, visit_number,
               patient_id, patient_name, mrn, encounter_kind, admit_at, in_bed_since, day_of_stay,
               attending_practitioner_id, attending_name, latest_sodium, latest_potassium, latest_creatinine,
               kidney_signal, low_sodium, abnormal_potassium
        FROM gold_bed_board ORDER BY unit_name, bed_name""")
    return {"contains_phi": True, "generated_at": now_iso(), "freshness": freshness(con, runs),
            "units": units, "beds": beds}


# PHI: patient level rows with name and MRN. Only the clinical roles may read these worklists in the app.
def readmission(con, runs=None):
    worklist = rows(con, """
        SELECT * FROM gold_readmission_risk
        ORDER BY CASE risk_band WHEN 'high' THEN 0 WHEN 'moderate' THEN 1 ELSE 2 END, discharge_date, lace_score DESC, encounter_id""")
    return {"contains_phi": True, "generated_at": now_iso(), "freshness": freshness(con, runs), "rows": worklist}


def test_summary(con):
    """The dbt test results of the last build, for the data engineer's pipeline page: counts, and every test that did
    not pass (a warning counts as not passing: tests have severity warn so the build never stops halfway)."""
    tests = rows(con, "SELECT test_name, model, status, failures FROM dq_test_results ORDER BY test_name")
    failing = [t for t in tests if t["status"] != "pass"]
    return {"total": len(tests), "passed": len(tests) - len(failing), "failed": len(failing), "failing": failing}


def history_record(snap, beds, worklist, tests=None):
    """A compact, aggregate only record of one publish for Binary/analytics-history."""
    units = beds.get("units", [])
    week = next((w for w in reversed(snap["live"]["weekly_metrics"]) if w.get("is_complete_week")), None)
    risk = [r["risk_band"] for r in worklist.get("rows", [])]
    extra = {"tests": tests} if tests is not None else {}
    return {
        "generated_at": snap["generated_at"],
        "data_date": snap["freshness"]["data_date"],
        "extracted_at": snap["freshness"]["extracted_at"],
        "patients": sum(p["patients"] for p in snap["patients"]),
        "occupied_beds": sum(u["occupied_beds"] or 0 for u in units),
        "capacity": sum(u["capacity"] or 0 for u in units),
        "last_full_week": week and {k: week[k] for k in ("week_start", "admissions", "readmission_rate_pct",
                                                         "median_los_days", "ed_visits")},
        "followup": {"discharges": len(risk), "high": risk.count("high"), "moderate": risk.count("moderate"),
                     "low": risk.count("low")},
        "checks_failing": sum(1 for d in snap["data_quality"] if d["failures"]) +
                          sum(1 for d in snap["live"]["data_quality"] if d["status"] != "pass"),
        "checks": [{"name": d["check_name"], "failures": d["failures"]} for d in snap["data_quality"]],
        **extra,
    }


def append_history(history, record, cap=HISTORY_CAP):
    """Newest first, at most cap records."""
    return [record, *(history if isinstance(history, list) else [])][:cap]


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def json_binary(rid, value):
    data = base64.b64encode(json.dumps(value).encode("utf-8")).decode("ascii")
    return {"resourceType": "Binary", "id": rid, "contentType": "application/json", "data": data}


def read_history(base):
    """Binary/analytics-history, [] when it does not exist yet. Any other error is raised, so a server hiccup never
    replaces 400 records with one."""
    try:
        binary = http("GET", f"{base}/Binary/{HISTORY_ID}", retries=2)
    except RuntimeError as e:
        if "HTTP 404" in str(e) or "HTTP 410" in str(e):
            return []
        raise
    value = json.loads(base64.b64decode(binary.get("data", "")) or "null")
    return value if isinstance(value, list) else []


def publish(base, snap):
    http("PUT", f"{base}/Binary/{SNAPSHOT_ID}", json_binary(SNAPSHOT_ID, snap))


def publish_all(base, snap, beds, worklist, tests=None):
    """The snapshot, both worklists, then one more record in the history. Returns the history length."""
    publish(base, snap)
    http("PUT", f"{base}/Binary/{BEDBOARD_ID}", json_binary(BEDBOARD_ID, beds))
    http("PUT", f"{base}/Binary/{READMISSION_ID}", json_binary(READMISSION_ID, worklist))
    history = append_history(read_history(base), history_record(snap, beds, worklist, tests))
    http("PUT", f"{base}/Binary/{HISTORY_ID}", json_binary(HISTORY_ID, history))
    return len(history)
