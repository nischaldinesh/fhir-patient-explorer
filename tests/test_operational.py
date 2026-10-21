"""Operational analytics on the live Riverside General feed: census, bed board, weekly metrics, LACE follow up list,
the live feed data quality tests, and publishing the worklists. Built with dbt like tests/test_analytics.py."""
import base64
import json
import sys
import threading
from datetime import date
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "tests"), str(ROOT / "analytics"), str(ROOT / "hl7v2")]
duckdb = pytest.importorskip("duckdb")
pytest.importorskip("dbt.cli.main")
import build as wh  # noqa: E402
import publish as pub  # noqa: E402
from test_analytics import IDENTIFYING, build_at, live_feed_resources, one, write_landing  # noqa: E402

LIVE_DAYS = 56
FIRST_DAY = date(2026, 8, 20)   # 56 days ending 2026-10-14, extracted the next morning like the nightly job


@pytest.fixture(scope="module")
def live56(tmp_path_factory):
    """The backfilled 56 days of the live feed, as the acceptance criteria describe, plus each day's message counts."""
    tmp_path = tmp_path_factory.mktemp("live56")
    summaries = {}
    write_landing(tmp_path / "landing", live_feed_resources(LIVE_DAYS, summaries, start=FIRST_DAY),
                  extracted_at="2026-10-15 06:30:00")
    con = build_at(tmp_path, tmp_path / "landing")
    yield con, summaries
    con.close()


def test_backfill_gives_eight_weeks_and_a_follow_up_list(live56):
    con, _ = live56
    assert one(con, "SELECT COUNT(*) FROM gold_weekly_metrics")[0] >= 8
    assert one(con, "SELECT COUNT(*) FROM gold_readmission_risk")[0] > 0
    f = one(con, "SELECT first_day, data_date, extract_date FROM gold_feed_freshness")
    assert f == (FIRST_DAY, date(2026, 10, 14), date(2026, 10, 15))
    statuses = dict(con.execute("SELECT status, COUNT(*) FROM dq_test_results GROUP BY 1").fetchall())
    assert set(statuses) == {"pass"}   # every test, the live feed checks included
    live = {r[0] for r in con.execute("SELECT test_name FROM dq_test_results WHERE test_name LIKE 'live%'").fetchall()}
    assert live == {"live_discharge_not_before_admit", "live_no_open_admission_older_than_30_days",
                    "live_no_bed_with_two_current_patients", "live_every_lab_has_loinc_or_is_listed_unmapped"}


def test_daily_census_matches_the_messages_of_each_day(live56):
    con, summaries = live56
    rows = {str(r[0]): r[1:] for r in con.execute(
        "SELECT census_date, admissions, discharges, ed_visits, transfers FROM gold_daily_census").fetchall()}
    assert set(rows) == set(summaries)
    for day, s in summaries.items():
        assert rows[day] == (s["new_admissions"], s["discharges"], s["ed_visits"], s["transfers"]), day
    # the last midnight census is everyone still in hospital
    last = one(con, "SELECT in_house_at_midnight FROM gold_daily_census ORDER BY census_date DESC LIMIT 1")[0]
    assert last == one(con, "SELECT COUNT(*) FROM gold_live_encounter WHERE is_admission AND end_at IS NULL")[0] > 0


def test_weekly_metrics(live56):
    con, _ = live56
    weeks = con.execute("""SELECT week_start, days_with_data, is_complete_week, admissions, discharges, readmissions_30d,
        readmission_rate_pct, median_los_days, p90_los_days, loinc_coverage_pct FROM gold_weekly_metrics ORDER BY 1""").fetchall()
    assert all(w[0].weekday() == 0 for w in weeks)                    # ISO weeks start on Monday
    assert weeks[0][2] is False and weeks[-1][2] is False             # 2026-08-20 is a Thursday, 2026-10-14 a Wednesday
    assert all(w[2] for w in weeks[1:-1]) and sum(w[1] for w in weeks) == LIVE_DAYS
    full = [w for w in weeks if w[2]]
    assert all(w[3] > 0 and w[4] > 0 and w[7] <= w[8] and w[9] == 100.0 for w in full)
    assert all(abs(w[6] - 100.0 * w[5] / w[4]) <= 0.05 for w in full)
    total = one(con, "SELECT SUM(admissions), SUM(ed_visits) FROM gold_weekly_metrics")
    assert total == one(con, "SELECT SUM(admissions), SUM(ed_visits) FROM gold_daily_census")


def test_bed_board_and_my_patients(live56):
    con, _ = live56
    units = {r[0]: r[1:] for r in con.execute(
        "SELECT unit_name, capacity, occupied_beds, beds_with_patient FROM gold_unit_occupancy").fetchall()}
    assert units["Medical Unit"][0] == 24 and units["Intensive Care Unit"][0] == 10 and sum(u[0] for u in units.values()) == 62
    assert all(occupied == with_patient for _, occupied, with_patient in units.values())   # bed status agrees with the visits
    beds = con.execute("""SELECT patient_name, mrn, day_of_stay, attending_name, kidney_signal, low_sodium, abnormal_potassium,
        latest_sodium, attending_practitioner_id FROM gold_bed_board WHERE encounter_id IS NOT NULL""").fetchall()
    assert beds and all(b[0] and b[1].startswith("RV2") and b[2] >= 1 and b[3] for b in beds)
    assert all(b[5] == (b[7] is not None and b[7] < 135) for b in beds)
    assert any(b[4] for b in beds) or one(con, "SELECT COUNT(*) FROM gold_aki_rise")[0] > 0
    mine = [b for b in beds if b[8] == "riverside-pract-1234"]   # Dr. Dana Morgan, the clinician persona
    assert mine and all(b[3] == "Dr Dana Morgan" for b in mine)
    empty = one(con, "SELECT COUNT(*) FROM gold_bed_board WHERE encounter_id IS NULL AND patient_name IS NULL AND kidney_signal IS NULL")[0]
    assert empty == 62 - len(beds)


def test_follow_up_list_is_the_last_30_days(live56):
    con, _ = live56
    r = one(con, """SELECT MIN(discharge_date), MAX(discharge_date), COUNT(*), COUNT(care_manager_name),
        SUM(CASE WHEN risk_band = 'high' THEN 1 ELSE 0 END) FROM gold_readmission_risk""")
    assert r[0] >= date(2026, 9, 15) and r[1] <= date(2026, 10, 14)
    assert r[3] == r[2]   # a care manager joins every inpatient discharge
    expected = one(con, """SELECT COUNT(*) FROM gold_live_encounter
        WHERE is_admission AND end_date BETWEEN DATE '2026-09-15' AND DATE '2026-10-15'""")[0]
    assert r[2] == expected


# ---------- LACE, hand computed ----------

TAG = {"system": "https://github.com/nischaldinesh/fhir-patient-explorer/CodeSystem/source", "code": "hl7v2"}
ICD9 = "http://hl7.org/fhir/sid/icd-9-cm"
ICD10 = "http://hl7.org/fhir/sid/icd-10-cm"


def patient(n, given, family):
    return {"resourceType": "Patient", "id": f"riverside-rv20000{n}", "meta": {"tag": [TAG]},
            "identifier": [{"value": f"RV20000{n}"}], "name": [{"family": family, "given": [given]}],
            "gender": "female", "birthDate": "1950-01-01"}


def visit(visit_no, n, cls, start, end=None, priority=None):
    e = {"resourceType": "Encounter", "id": f"rv-{visit_no.lower()}", "meta": {"tag": [TAG]},
         "identifier": [{"system": "https://riverside.example.org/fhir/sid/visit", "value": visit_no}],
         "status": "finished" if end else "in-progress", "class": {"code": cls},
         "subject": {"reference": f"Patient/riverside-rv20000{n}"}, "period": {"start": start, **({"end": end} if end else {})}}
    if priority:
        e["priority"] = {"coding": [{"system": "http://terminology.hl7.org/CodeSystem/v3-ActPriority", "code": priority}]}
    return e


def dx(visit_no, n, system, code):
    return {"resourceType": "Condition", "id": f"rv-{visit_no.lower()}-dx-{code.replace('.', '').lower()}", "meta": {"tag": [TAG]},
            "code": {"coding": [{"system": system, "code": code}]}, "subject": {"reference": f"Patient/riverside-rv20000{n}"},
            "encounter": {"reference": f"Encounter/rv-{visit_no.lower()}"}, "recordedDate": "2026-10-01T08:00:00"}


def lace_landing():
    """Hand computed examples, extracted Tuesday 2026-10-20 06:30; the last event is Monday 2026-10-19 (the data date).

      Low       Ada:   elective, 1.5 days (L1), no Charlson condition, no ED visit                     LACE 1
                       discharged Tue 10/13: contact due Thu 10/15 (overdue on 10/19); readmitted on 10/19
      Moderate  Ben:   emergency (A3), 3.1 days (L3), heart failure ICD 9 428.0 (C1), 1 ED visit (E1)    LACE 8
                       discharged Wed 10/14: contact due Fri 10/16; heart failure, so the Cardiology Clinic
      High      Cleo:  emergency (A3), 15 days (L7), Charlson 10 (C5): diabetes with complication 2 (the
                       uncomplicated code does not count too), metastatic 6 (the primary tumor does not count
                       too), renal 2; 5 ED visits in 6 months (E4), one more 7 months ago not counted   LACE 19
                       discharged Fri 10/16: contact due Tue 10/20, visit due 7 days later, Fri 10/23
      Low       Dev:   elective, 8 hours (L0), discharged Sat 10/17: contact due Tue 10/20             LACE 0
      Out       Eve:   discharged 2026-09-10, more than 30 days before the extract
      Out       Fay:   the original 25 patient feed (visit V70013), an archive, not the live feed
    """
    ed_cleo = ["2026-03-01", "2026-05-01", "2026-06-01", "2026-07-01", "2026-08-01", "2026-09-20"]
    return [
        patient(1, "Ada", "Low"), patient(2, "Ben", "Moderate"), patient(3, "Cleo", "High"), patient(4, "Dev", "Weekend"),
        patient(5, "Eve", "Old"), patient(6, "Fay", "Archive"),
        visit("V2026101201", 1, "IMP", "2026-10-12T10:00:00", "2026-10-13T22:00:00", "EL"),
        visit("V2026101901", 1, "IMP", "2026-10-19T08:00:00", None, "EL"),
        visit("V2026101102", 2, "IMP", "2026-10-11T09:00:00", "2026-10-14T11:00:00", "EM"),
        visit("V2026081501", 2, "EMER", "2026-08-15T09:00:00", "2026-08-15T13:00:00", "EM"),
        dx("V2026101102", 2, ICD9, "428.0"),
        visit("V2026100101", 3, "IMP", "2026-10-01T08:00:00", "2026-10-16T09:00:00", "EM"),
        *[visit(f"V{d.replace('-', '')}0{k}", 3, "EMER", f"{d}T10:00:00", f"{d}T14:00:00", "EM") for k, d in enumerate(ed_cleo)],
        *[dx("V2026100101", 3, ICD10, c) for c in ("E11.9", "E11.22", "C34.1", "C78.0", "N18.3", "I10")],
        visit("V2026101701", 4, "IMP", "2026-10-17T07:00:00", "2026-10-17T15:00:00", "EL"),
        visit("V2026090101", 5, "IMP", "2026-09-01T07:00:00", "2026-09-10T15:00:00", "EM"),
        visit("V70013", 6, "IMP", "2026-10-10T07:00:00", "2026-10-15T15:00:00", "EM"),
        {"resourceType": "CareTeam", "id": "rv-v2026101102-careteam", "meta": {"tag": [TAG]},
         "subject": {"reference": "Patient/riverside-rv200002"}, "encounter": {"reference": "Encounter/rv-v2026101102"},
         "participant": [
             {"role": [{"coding": [{"code": "attending"}]}], "member": {"reference": "PractitionerRole/riverside-role-1234", "display": "Dr Dana Morgan"},
              "period": {"start": "2026-10-11T09:00:00", "end": "2026-10-14T11:00:00"}},
             {"role": [{"coding": [{"code": "care-manager"}]}], "member": {"reference": "PractitionerRole/riverside-role-8001", "display": "Jordan Lee"},
              "period": {"start": "2026-10-14T11:00:00"}}]},
    ]


@pytest.fixture(scope="module")
def lace(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("lace")
    write_landing(tmp_path / "landing", lace_landing(), extracted_at="2026-10-20 06:30:00")
    con = build_at(tmp_path, tmp_path / "landing")
    yield con
    con.close()


def test_lace_hand_computed_examples(lace):
    rows = {r[0]: r[1:] for r in lace.execute("""SELECT patient_name, lace_l, lace_a, lace_c, lace_e, lace_score, risk_band,
        charlson_score, ed_visits_6m FROM gold_readmission_risk""").fetchall()}
    assert rows == {
        "Ada Low": (1, 0, 0, 0, 1, "low", 0, 0),
        "Ben Moderate": (3, 3, 1, 1, 8, "moderate", 1, 1),
        "Cleo High": (7, 3, 5, 4, 19, "high", 10, 5),
        "Dev Weekend": (0, 0, 0, 0, 0, "low", 0, 0),
    }
    assert one(lace, "SELECT data_date FROM gold_feed_freshness")[0] == date(2026, 10, 19)


def test_lace_follow_up_timing_and_service(lace):
    rows = {r[0]: r[1:] for r in lace.execute("""SELECT patient_name, discharge_date, contact_due_date, contact_overdue,
        visit_due_date, visit_overdue, days_since_discharge, readmitted, care_manager_name, suggested_service_name
        FROM gold_readmission_risk""").fetchall()}
    d = date
    assert rows["Ada Low"] == (d(2026, 10, 13), d(2026, 10, 15), True, d(2026, 10, 27), False, 6, True, None, "Discharge Follow Up Clinic")
    assert rows["Ben Moderate"] == (d(2026, 10, 14), d(2026, 10, 16), True, d(2026, 10, 28), False, 5, False, "Jordan Lee", "Cardiology Clinic")
    # a Friday discharge: 2 business days lands on Tuesday; high risk, so the visit is due within 7 days
    assert rows["Cleo High"] == (d(2026, 10, 16), d(2026, 10, 20), False, d(2026, 10, 23), False, 3, False, None, "Discharge Follow Up Clinic")
    assert rows["Dev Weekend"][:2] == (d(2026, 10, 17), d(2026, 10, 20))   # a Saturday discharge: Tuesday too


def test_live_encounters_leave_the_archive_out(lace):
    visits = {r[0] for r in lace.execute("SELECT visit_number FROM gold_live_encounter").fetchall()}
    assert "V70013" not in visits and "V2026101201" in visits and len(visits) == 13
    census = {str(r[0]): r[1:] for r in lace.execute(
        "SELECT census_date, in_house_at_midnight, admissions, discharges, ed_visits FROM gold_daily_census").fetchall()}
    assert census["2026-10-14"] == (1, 0, 1, 0)   # Ben discharged, Ada left the day before: only Cleo is in
    assert census["2026-10-19"] == (1, 1, 0, 0)   # Ada back in
    assert max(census) == "2026-10-19"


@pytest.mark.parametrize("day,expected", [
    ("2026-10-12", "2026-10-14"),   # Monday -> Wednesday
    ("2026-10-15", "2026-10-19"),   # Thursday -> Monday
    ("2026-10-16", "2026-10-20"),   # Friday -> Tuesday
    ("2026-10-17", "2026-10-20"),   # Saturday -> Tuesday
    ("2026-10-18", "2026-10-20"),   # Sunday -> Tuesday
])
def test_add_business_days_macro(tmp_path, day, expected):
    res = wh.dbt(["compile", "--inline",
                  f"SELECT {{{{ add_business_days(\"DATE '{day}'\", 2) }}}} AS due, {{{{ week_start(\"DATE '{day}'\") }}}} AS monday"],
                 ":memory:", target_path=tmp_path / "t", quiet=True)
    sql = res.result.results[0].node.compiled_code
    due, monday = duckdb.connect().execute(sql).fetchone()
    assert str(due) == expected and monday.weekday() == 0 and monday <= date.fromisoformat(day)


def test_open_admission_check_catches_a_lost_discharge(tmp_path):
    """An admission still open 31 days after it started is a lost discharge message: the live feed test warns."""
    landing = tmp_path / "landing"
    write_landing(landing, [patient(1, "Ada", "Lost"),
                            visit("V2026090101", 1, "IMP", "2026-09-01T08:00:00", None, "EM"),
                            visit("V2026100501", 1, "EMER", "2026-10-05T08:00:00", "2026-10-05T12:00:00", "EM")],
                  extracted_at="2026-10-06 06:30:00")
    bad = wh.build(tmp_path / "w.duckdb", landing, target_path=tmp_path / "target", quiet=True)
    assert bad == ["live_no_open_admission_older_than_30_days: warn"]


def test_empty_live_feed_still_builds(tmp_path):
    """Before the live feed has run, every operational table is empty and freshness is one row of NULLs."""
    landing = tmp_path / "landing"
    write_landing(landing, [patient(1, "Ada", "Archive"), visit("V70013", 1, "IMP", "2026-03-01T08:00:00", "2026-03-04T08:00:00")])
    con = build_at(tmp_path, landing)
    assert one(con, "SELECT data_date, last_event_at FROM gold_feed_freshness") == (None, None)
    for t in ("gold_live_encounter", "gold_daily_census", "gold_weekly_metrics", "gold_readmission_risk", "gold_bed_board"):
        assert one(con, f"SELECT COUNT(*) FROM {t}")[0] == 0, t
    snap = pub.snapshot(con)
    assert snap["freshness"]["data_date"] is None and snap["live"]["weekly_metrics"] == []


# ---------- publishing ----------

def test_latest_runs_by_day_keeps_the_newest_run_of_each_day():
    runs = [
        {"day": "2026-10-14", "status": "success", "finished_at": "2026-10-15T07:10:00+00:00"},   # the rerun
        {"day": "2026-10-14", "status": "failed", "finished_at": "2026-10-15T06:16:00+00:00"},
        {"day": "2026-10-13", "status": "success", "finished_at": "2026-10-14T06:16:00+00:00"},
        {"status": "failed"}, "not a run",
    ]
    assert [r["finished_at"] for r in pub.latest_runs_by_day(runs)] == ["2026-10-15T07:10:00+00:00", "2026-10-14T06:16:00+00:00"]
    assert pub.last_loaded_run(runs) == {"day": "2026-10-14", "status": "success", "finished_at": "2026-10-15T07:10:00+00:00"}
    # the newest run of the newest day failed: the last loaded day is the one before
    failed = [{"day": "2026-10-15", "status": "failed", "finished_at": "2026-10-16T06:16:00+00:00"}, *runs]
    assert pub.last_loaded_run(failed)["day"] == "2026-10-14"
    assert pub.latest_runs_by_day(None) == [] and pub.last_loaded_run([]) is None


def test_append_history_is_newest_first_and_capped():
    assert pub.append_history([], {"n": 1}) == [{"n": 1}]
    assert pub.append_history(None, {"n": 1}) == [{"n": 1}]
    old = [{"n": i} for i in range(400)]
    new = pub.append_history(old, {"n": "new"})
    assert len(new) == 400 and new[0] == {"n": "new"} and new[-1] == {"n": 398}


def test_feed_runs_reads_the_landing_file(tmp_path):
    assert wh.feed_runs(tmp_path) == []
    (tmp_path / "feed_runs.json").write_text('[{"day": "2026-10-14"}]')
    assert wh.feed_runs(tmp_path) == [{"day": "2026-10-14"}]
    (tmp_path / "feed_runs.json").write_text("not json")
    assert wh.feed_runs(tmp_path) == []


def keys_anywhere(value):
    if isinstance(value, dict):
        return set(value) | set().union(*(keys_anywhere(v) for v in value.values()))
    if isinstance(value, list):
        return set().union(*(keys_anywhere(v) for v in value)) if value else set()
    return set()


class FakeFhir:
    """A tiny FHIR server: GET and PUT on Binary, with an optional failing GET."""

    def __init__(self, binaries=None, get_status=None):
        self.binaries, self.puts, self.get_status = dict(binaries or {}), [], get_status

    def __enter__(self):
        fake = self

        class H(BaseHTTPRequestHandler):
            def reply(self, code, body):
                d = json.dumps(body).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/fhir+json")
                self.send_header("Content-Length", str(len(d)))
                self.end_headers()
                self.wfile.write(d)

            def do_GET(self):
                rid = self.path.rsplit("/", 1)[-1]
                if fake.get_status:
                    return self.reply(fake.get_status, {"resourceType": "OperationOutcome"})
                if rid not in fake.binaries:
                    return self.reply(404, {"resourceType": "OperationOutcome"})
                self.reply(200, fake.binaries[rid])

            def do_PUT(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                fake.puts.append(self.path)
                fake.binaries[self.path.rsplit("/", 1)[-1]] = body
                self.reply(200, body)

            def log_message(self, *a):
                pass

        self.srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()
        self.base = f"http://127.0.0.1:{self.srv.server_port}/fhir"
        return self

    def __exit__(self, *a):
        self.srv.shutdown()

    def json(self, rid):
        return json.loads(base64.b64decode(self.binaries[rid]["data"]))


def test_publish_snapshot_worklists_and_history(live56):
    con, _ = live56
    runs = [{"day": "2026-10-14", "status": "success", "finished_at": "2026-10-15T06:16:02+00:00"}]
    snap, beds, worklist = pub.snapshot(con, runs), pub.bedboard(con, runs), pub.readmission(con, runs)
    json.dumps([snap, beds, worklist])   # plain JSON

    # the analyst's snapshot: aggregates only, the last 12 weeks, oldest first, and how fresh the data is
    assert not keys_anywhere(snap) & (IDENTIFYING | {"patient_name", "encounter_id"})
    weeks = snap["live"]["weekly_metrics"]
    assert len(weeks) == min(12, one(con, "SELECT COUNT(*) FROM gold_weekly_metrics")[0])
    assert [w["week_start"] for w in weeks] == sorted(w["week_start"] for w in weeks)
    assert snap["live"]["daily_census"][0]["census_date"] >= weeks[0]["week_start"]
    assert snap["freshness"] == {"data_date": "2026-10-14", "last_event_at": snap["freshness"]["last_event_at"],
                                 "extracted_at": "2026-10-15T06:30:00", "last_run": runs[0] | {"finished_at": runs[0]["finished_at"]}}
    assert {c["test_name"] for c in snap["live"]["data_quality"]} >= {"live_no_bed_with_two_current_patients"}

    # the worklists: patient level, for the care team
    assert beds["contains_phi"] and worklist["contains_phi"]
    assert len(beds["beds"]) == 62 and {u["unit_name"] for u in beds["units"]} >= {"Medical Unit", "Emergency Department"}
    assert any(b["patient_name"] and b["mrn"] for b in beds["beds"])
    bands = [r["risk_band"] for r in worklist["rows"]]
    assert bands == sorted(bands, key=["high", "moderate", "low"].index)
    assert all(r["patient_name"] and r["contact_due_date"] for r in worklist["rows"])

    existing = [{"generated_at": f"old {i}"} for i in range(400)]
    with FakeFhir({"analytics-history": pub.json_binary("analytics-history", existing)}) as fhir:
        n = pub.publish_all(fhir.base, snap, beds, worklist, pub.test_summary(con))
        assert fhir.puts == ["/fhir/Binary/analytics-snapshot", "/fhir/Binary/worklist-bedboard",
                             "/fhir/Binary/worklist-readmission", "/fhir/Binary/analytics-history"]
        history = fhir.json("analytics-history")
        assert n == len(history) == 400 and history[1] == existing[0]
        rec = history[0]
        assert rec["data_date"] == "2026-10-14" and rec["capacity"] == 62 and rec["followup"]["discharges"] == len(bands)
        # the dbt test results for the data engineer's pipeline page
        total = one(con, "SELECT COUNT(*) FROM dq_test_results")[0]
        assert rec["tests"]["total"] == total > 100 and rec["tests"]["passed"] + rec["tests"]["failed"] == total
        assert rec["tests"]["failed"] == len(rec["tests"]["failing"])
        assert [c["name"] for c in rec["checks"]] == [d["check_name"] for d in snap["data_quality"]]
        assert not keys_anywhere(rec) & IDENTIFYING
        assert fhir.json("worklist-readmission")["rows"][0]["patient_name"]


def test_publish_never_overwrites_the_history_after_a_failed_read(live56):
    con, _ = live56
    snap, beds, worklist = pub.snapshot(con), pub.bedboard(con), pub.readmission(con)
    with FakeFhir(get_status=400) as fhir:
        with pytest.raises(RuntimeError, match="HTTP 400"):
            pub.publish_all(fhir.base, snap, beds, worklist)
        assert "/fhir/Binary/analytics-history" not in fhir.puts
    with FakeFhir() as fhir:   # no history yet: it starts with this record
        assert pub.publish_all(fhir.base, snap, beds, worklist) == 1


def test_test_summary_lists_what_did_not_pass():
    con = duckdb.connect()
    con.execute("""CREATE TABLE dq_test_results AS SELECT * FROM (VALUES
        ('unique_a', 'm', 'pass', 0), ('b_check', 'm', 'warn', 3), ('c_check', 'n', 'error', NULL))
        AS t(test_name, model, status, failures)""")
    s = pub.test_summary(con)
    assert (s["total"], s["passed"], s["failed"]) == (3, 1, 2)
    assert [t["test_name"] for t in s["failing"]] == ["b_check", "c_check"] and s["failing"][0]["failures"] == 3
    rec = pub.history_record({"generated_at": "x", "freshness": {"data_date": None, "extracted_at": None},
                              "patients": [], "live": {"weekly_metrics": [], "data_quality": []},
                              "data_quality": [{"check_name": "Unique ids", "failures": 0}]}, {}, {}, s)
    assert rec["tests"] == s and rec["checks"] == [{"name": "Unique ids", "failures": 0}]
    assert "tests" not in pub.history_record({**{"generated_at": "x", "freshness": {"data_date": None, "extracted_at": None},
                                                 "patients": [], "live": {"weekly_metrics": [], "data_quality": []},
                                                 "data_quality": []}}, {}, {})
