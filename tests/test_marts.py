"""The star schema marts (analytics/dbt/models/marts/), the SCD type 2 snapshot and the quality measures."""
import csv
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "analytics"))
sys.path.insert(0, str(ROOT / "hl7v2"))
duckdb = pytest.importorskip("duckdb")
pytest.importorskip("dbt.cli.main")
yaml = pytest.importorskip("yaml")
import build as wh  # noqa: E402
from test_analytics import (build_at, hl7v2_resources, live_feed_resources, mimic_and_synthea,  # noqa: E402
                            write_landing)

DBT = ROOT / "analytics" / "dbt"
MARTS = DBT / "models" / "marts"
SYNTHEA = "https://github.com/synthetichealth/synthea"
HL7V2_TAG = {"system": "https://github.com/nischaldinesh/fhir-patient-explorer/CodeSystem/source", "code": "hl7v2"}
MIMIC_PROFILE = "http://mimic.mit.edu/fhir/mimic/StructureDefinition/mimic-patient"
# The models BI tools read; the int_* models and the snapshot are internal
BI_MODELS = ["dim_date", "dim_patient", "dim_patient_history", "dim_location", "dim_practitioner", "dim_diagnosis",
             "dim_lab", "fact_encounter", "fact_diagnosis", "fact_lab_result", "fact_bed_day", "fact_bed_capacity_day",
             "mart_quality_monthly", "mart_quality_period", "mart_suppression_report"]


def one(con, sql):
    return con.execute(sql).fetchone()


def patient(pid, source, **extra):
    p = {"resourceType": "Patient", "id": pid, "gender": "female", "birthDate": "1970-05-06"}
    if source == "synthea":
        p["identifier"] = [{"system": SYNTHEA, "value": pid}, {"system": "http://hospital.smarthealthit.org", "value": f"MRN-{pid}"}]
    elif source == "hl7v2":
        p["meta"] = {"tag": [HL7V2_TAG]}
        p["identifier"] = [{"system": "https://riverside.example.org/fhir/sid/mrn", "value": f"RV-{pid}"}]
    else:
        p["meta"] = {"profile": [MIMIC_PROFILE]}
    p.update(extra)
    return p


def lab(oid, pid, when, value, loinc="2823-3", unit="mmol/L"):
    return {"resourceType": "Observation", "id": oid, "subject": {"reference": f"Patient/{pid}"}, "status": "final",
            "category": [{"coding": [{"code": "laboratory"}]}],
            "code": {"coding": [{"system": "http://loinc.org", "code": loinc}]},
            "valueQuantity": {"value": value, "unit": unit}, "effectiveDateTime": when}


def admission(eid, pid, start, end, mimic=False):
    e = {"resourceType": "Encounter", "id": eid, "subject": {"reference": f"Patient/{pid}"}, "status": "finished",
         "class": {"code": "IMP"}, "period": {"start": start, "end": end}}
    if mimic:
        e["identifier"] = [{"system": "http://mimic.mit.edu/fhir/mimic/identifier/encounter-hosp", "value": eid}]
    return e


def link(pid, other):
    return [{"other": {"reference": f"Patient/{other}"}, "type": "seealso"}]


@pytest.fixture(scope="module")
def warehouse(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("marts")
    landing = tmp_path / "landing"
    write_landing(landing, mimic_and_synthea() + hl7v2_resources())
    con = build_at(tmp_path, landing)
    yield con
    con.close()


# ---------- seeds and pure helpers ----------

def test_staff_roles_seed_matches_the_reference_data():
    import reference

    roles = {"doctor": "doctor", "nurse": "nurse", "care_manager": "care manager"}
    want = [{"practitioner_id": reference.practitioner_id(s["number"]), "role": roles[s["kind"]], "specialty": s["title"],
             "service_name": reference.SERVICE_NAMES[s["service"]]} for s in reference.STAFF]
    with open(DBT / "seeds" / "staff_roles.csv", newline="") as f:
        assert list(csv.DictReader(f)) == want


def test_icd10_chapters_are_disjoint_and_in_order():
    with open(DBT / "seeds" / "icd10_chapters.csv", newline="") as f:
        rows = list(csv.DictReader(f))
    assert sorted(int(r["chapter"]) for r in rows) == list(range(1, 23))
    ranges = sorted((r["first_category"], r["last_category"]) for r in rows)
    assert all(len(a) == 3 and len(b) == 3 and a <= b for a, b in ranges)
    assert all(prev[1] < nxt[0] for prev, nxt in zip(ranges, ranges[1:]))   # no category in two chapters
    chapter = lambda cat: [r["chapter"] for r in rows if r["first_category"] <= cat <= r["last_category"]]  # noqa: E731
    assert chapter("E11") == ["4"] and chapter("I50") == ["9"] and chapter("O9A") == ["15"] and chapter("Z99") == ["21"]


def test_problems_ignores_exposures():
    class R:
        def __init__(self, name, status):
            self.node, self.status = type("N", (), {"name": name})(), status

    res = type("Res", (), {"result": [R("dim_date", "success"), R("a_test", "pass"), R("hospital_quality_scorecard", "no-op"),
                                      R("bad_test", "warn"), R("fact_x", "error")]})()
    assert wh.problems(res) == ["bad_test: warn", "fact_x: error"]


def test_every_mart_is_documented_with_an_enforced_contract():
    models = yaml.safe_load((MARTS / "_marts.yml").read_text())["models"]
    by_name = {m["name"]: m for m in models}
    assert set(by_name) == {p.stem for p in MARTS.glob("*.sql")}
    for name, m in by_name.items():
        assert m.get("description"), name
        assert m["config"]["contract"] == {"enforced": True}, name
        for c in m["columns"]:
            assert c.get("description") and c.get("data_type"), f"{name}.{c['name']}"
    exposures = yaml.safe_load((MARTS / "exposures.yml").read_text())["exposures"]
    assert exposures and all(e["owner"]["name"] and e["depends_on"] for e in exposures)
    assert all("demo" in e["description"] for e in exposures)


# ---------- the star schema on the test feed ----------

def test_bi_models_carry_no_patient_identifiers(warehouse):
    w = warehouse
    forbidden_cols = {"patient_id", "mrn", "family_name", "given_name", "patient_name", "birth_date", "address_line",
                      "city", "postal_code", "encounter_id", "observation_id", "condition_id", "visit_number"}
    secrets = {r[0] for r in w.execute("""SELECT patient_id FROM silver_patient UNION SELECT mrn FROM silver_patient
                                          WHERE mrn IS NOT NULL UNION SELECT encounter_id FROM silver_encounter""").fetchall()}
    for t in BI_MODELS:
        cols = w.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{t}'").fetchall()
        assert not forbidden_cols & {c for c, _ in cols}, t
        for c, typ in cols:
            if typ == "VARCHAR":
                hits = w.execute(f'SELECT DISTINCT "{c}" FROM {t}').fetchall()
                assert not secrets & {h[0] for h in hits}, f"{t}.{c}"


def test_dimensions_and_facts_cover_the_warehouse(warehouse):
    w = warehouse
    assert one(w, "SELECT COUNT(*) FROM dim_patient")[0] == one(w, "SELECT COUNT(DISTINCT master_id) FROM gold_patient_crosswalk")[0]
    assert one(w, "SELECT COUNT(*) FROM fact_encounter")[0] == one(w, "SELECT COUNT(*) FROM silver_encounter")[0]
    n_lab = one(w, """SELECT COUNT(*) FROM silver_observation WHERE category = 'laboratory' AND value_num IS NOT NULL
                      AND effective_at IS NOT NULL""")[0]
    assert one(w, "SELECT COUNT(*) FROM fact_lab_result")[0] == n_lab
    # every dimension but date and patient has one Unknown member
    for dim, key in [("dim_location", "location_sk"), ("dim_practitioner", "practitioner_sk"),
                     ("dim_diagnosis", "diagnosis_sk"), ("dim_lab", "lab_sk")]:
        assert one(w, f"SELECT COUNT(*) FROM {dim} WHERE {key} = 'unknown'")[0] == 1
    # MIMIC ICD 9 upgraded to A419 through the GEM seed: chapter 1, the sepsis cohort
    a419 = one(w, "SELECT category_3char, chapter, is_sepsis, is_heart_failure FROM dim_diagnosis WHERE icd10_code = 'A419'")
    assert a419 == ("A41", 1, True, False)
    # the unmapped MIMIC lab and the ICD 9 heart failure waiting for a coder point at the Unknown members
    assert one(w, "SELECT COUNT(*) FROM fact_lab_result WHERE lab_sk = 'unknown'")[0] >= 1
    assert one(w, """SELECT diagnosis_sk, source_code, needs_review FROM fact_diagnosis
                     WHERE code_system = 'icd9' AND source = 'mimic'""") == ("unknown", "4280", True)
    # a diagnosis without a visit is not an encounter diagnosis
    assert one(w, "SELECT COUNT(*) FROM fact_diagnosis")[0] == one(w, "SELECT COUNT(encounter_id) FROM silver_condition")[0]
    staff = one(w, "SELECT role, specialty, practitioner_source FROM dim_practitioner WHERE practitioner_id = 'riverside-pract-1234'")
    assert staff == ("doctor", "Hospitalist", "riverside reference")


def test_dim_date_is_a_full_iso_calendar(warehouse):
    rows = warehouse.execute("SELECT date_key, calendar_date, iso_year, iso_week, iso_weekday, is_weekend, month_start_date, "
                             "quarter_start_date, year_start_date FROM dim_date ORDER BY calendar_date").fetchall()
    first, last = rows[0][1], rows[-1][1]
    assert (first.month, first.day, last.month, last.day) == (1, 1, 12, 31)  # whole years
    assert len(rows) == (last - first).days + 1                              # no gaps
    for key, d, iy, iw, iwd, weekend, ms, qs, ys in rows:
        assert key == d.year * 10000 + d.month * 100 + d.day
        assert (iy, iw, iwd) == d.isocalendar()[:3]
        assert weekend == (iwd >= 6) and ms == d.replace(day=1)
        assert qs == d.replace(month=(d.month - 1) // 3 * 3 + 1, day=1) and ys == d.replace(month=1, day=1)


def test_linked_records_are_one_person(tmp_path):
    landing = tmp_path / "landing"
    write_landing(landing, [
        patient("s1", "synthea", link=link("s1", "h1"), address=[{"state": "MA"}]),
        patient("h1", "hl7v2", link=link("h1", "s1"), deceasedBoolean=True),
        patient("s2", "synthea", gender="male"),
        admission("e1", "s1", "2024-01-01T08:00:00", "2024-01-03T08:00:00"),
        admission("e2", "h1", "2026-09-01T08:00:00", "2026-09-02T20:00:00"),
    ])
    con = build_at(tmp_path, landing)
    people = con.execute("SELECT source_list, source_count, record_count, sex, is_deceased, state, age_band "
                         "FROM dim_patient ORDER BY source_list").fetchall()
    assert people == [("hl7v2,synthea", 2, 2, "female", True, "MA", "50s"), ("synthea", 1, 1, "male", False, None, "50s")]
    assert one(con, "SELECT COUNT(DISTINCT patient_sk), COUNT(DISTINCT patient_record_sk) FROM fact_encounter") == (1, 2)
    assert one(con, "SELECT length_of_stay_hours FROM fact_encounter WHERE source = 'hl7v2'")[0] == 36.0


def test_bed_days_add_up_to_the_midnight_census(tmp_path):
    landing = tmp_path / "landing"
    write_landing(landing, live_feed_resources(days=8))
    con = build_at(tmp_path, landing)
    census = dict(con.execute("SELECT census_date, in_house_at_midnight FROM gold_daily_census").fetchall())
    beds = dict(con.execute("SELECT census_date, COUNT(*) FROM fact_bed_day WHERE is_live_feed AND is_admission "
                            "GROUP BY 1").fetchall())
    assert census and sum(census.values()) > 0
    assert {d: beds.get(d, 0) for d in census} == census
    # every bed day is a bed of the hospital
    assert one(con, """SELECT COUNT(*) FROM fact_bed_day f JOIN dim_location l ON l.location_sk = f.location_sk
                       WHERE NOT l.is_bed""")[0] == 0


# ---------- quality measures and small cell suppression ----------

def test_quality_measures_and_suppression(tmp_path):
    rows = []
    # March 2024: 25 admissions, none readmitted -> shown (0 is not a small cell)
    for i in range(25):
        rows += [patient(f"m{i:02}", "synthea"), admission(f"em{i:02}", f"m{i:02}", "2024-03-02T08:00:00", "2024-03-04T08:00:00")]
    # May 2024: 13 discharges, one of them readmitted -> the numerator 1 is a small cell
    for i in range(11):
        rows += [patient(f"y{i:02}", "synthea"), admission(f"ey{i:02}", f"y{i:02}", "2024-05-02T08:00:00", "2024-05-05T08:00:00")]
    rows += [patient("r1", "synthea"), admission("er1", "r1", "2024-05-01T08:00:00", "2024-05-03T08:00:00"),
             admission("er2", "r1", "2024-05-10T08:00:00", "2024-05-12T08:00:00")]
    # potassium on admission: 12 of the March stays have a result within 24 hours, 3 abnormal -> small cell
    rows += [lab(f"k{i:02}", f"m{i:02}", "2024-03-02T12:00:00", 6.1 if i < 3 else 4.2) for i in range(12)]
    landing = tmp_path / "landing"
    write_landing(landing, rows)
    con = build_at(tmp_path, landing)

    def q(measure, month):
        return one(con, f"""SELECT numerator, denominator, rate, value, is_suppressed FROM mart_quality_monthly
                            WHERE measure_id = '{measure}' AND month_key = {month} AND source = 'synthea'""")

    assert q("readmission_30d", 202403) == (0.0, 25, 0.0, 0.0, False)
    assert q("readmission_30d", 202405) == (None, None, None, None, True)
    assert q("los_mean_days", 202403) == (50.0, 25, None, 2.0, False)
    assert q("los_median_days", 202405) == (None, 13, None, 3.0, False)   # a median has no count numerator
    assert q("potassium_abnormal_on_admission", 202403) == (None, None, None, None, True)
    name = one(con, "SELECT DISTINCT measure_name FROM mart_quality_monthly WHERE measure_id = 'readmission_30d'")[0]
    assert name == "30 day all cause readmission rate"


def test_diabetes_measure_is_labelled_hedis_style(warehouse):
    names = dict(warehouse.execute("SELECT DISTINCT measure_id, measure_name FROM mart_quality_monthly").fetchall())
    for m, n in names.items():
        assert "-" not in n and "–" not in n and "—" not in n, n
    seed = (DBT / "seeds" / "quality_measures.csv").read_text()
    assert '"Diabetes HbA1c under 8 percent (HEDIS style, not certified)"' in seed


# ---------- history and incremental loading ----------

def test_incremental_lab_facts_and_scd2_history(tmp_path):
    """Two nightly builds into one warehouse: the incremental fact must equal a full build of the second night, and the
    snapshot must keep the old address as a closed version."""
    night1 = [
        patient("sA", "synthea", address=[{"line": ["1 Elm St"], "city": "Salem", "state": "MA"}]),
        patient("hA", "hl7v2"),
        patient("mA", "mimic"),
        lab("o1", "sA", "2024-01-01T08:00:00", 3.9),
        lab("o2", "sA", "2024-06-01T08:00:00", 4.0),
        lab("o3", "hA", "2026-09-01T08:00:00", 4.1),
        lab("o4", "mA", "2150-01-01T08:00:00", 4.2),   # a shifted MIMIC date far beyond the live feed
    ]
    night2 = [
        # the EMPI linked sA and hA: sA's person changes (the group's smallest id is hA); sA moved
        patient("sA", "synthea", address=[{"line": ["9 Oak Ave"], "city": "Lynn", "state": "MA"}], link=link("sA", "hA")),
        patient("hA", "hl7v2", link=link("hA", "sA")),
        patient("mA", "mimic"),
        lab("o1", "sA", "2024-01-01T08:00:00", 3.9),
        lab("o2", "sA", "2024-06-01T08:00:00", 4.5),   # corrected within the lookback
        lab("o3", "hA", "2026-09-01T08:00:00", 4.1),
        lab("o4", "mA", "2150-01-01T08:00:00", 4.2),
        lab("o5", "hA", "2026-09-02T08:00:00", 5.6),   # the next day of the live feed
    ]
    nightly = tmp_path / "nightly"
    write_landing(tmp_path / "l1", night1, extracted_at="2026-10-14 06:30:00")
    build_at(nightly, tmp_path / "l1").close()
    write_landing(tmp_path / "l2", night2, extracted_at="2026-10-15 06:30:00")
    inc = build_at(nightly, tmp_path / "l2")
    full = build_at(tmp_path / "full", tmp_path / "l2")
    cols = "lab_result_sk, patient_sk, patient_record_sk, lab_sk, effective_date_key, source, value_num"
    got = inc.execute(f"SELECT {cols} FROM fact_lab_result ORDER BY ALL").fetchall()
    assert len(got) == 5 and got == full.execute(f"SELECT {cols} FROM fact_lab_result ORDER BY ALL").fetchall()
    assert one(inc, "SELECT COUNT(DISTINCT patient_sk) FROM fact_lab_result WHERE source <> 'mimic'")[0] == 1

    hist = inc.execute("""SELECT h.version_number, h.address_version, h.address_changed, h.is_current, h.valid_from, h.valid_to
                          FROM dim_patient_history h JOIN fact_lab_result f ON f.patient_record_sk = h.patient_record_sk
                          WHERE f.source = 'synthea' GROUP BY ALL ORDER BY 1""").fetchall()
    t1, t2 = datetime(2026, 10, 14, 6, 30), datetime(2026, 10, 15, 6, 30)
    assert hist == [(1, 1, False, False, t1, t2), (2, 2, True, True, t2, None)]
    assert one(inc, "SELECT COUNT(*) FROM dim_patient_history WHERE NOT is_current")[0] == 1
    # the snapshot keeps the address; the BI view does not show it
    assert one(inc, "SELECT COUNT(*) FROM snap_patient_demographics WHERE city IN ('Salem', 'Lynn')")[0] == 2
    cols = {r[0] for r in inc.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'dim_patient_history'").fetchall()}
    assert not cols & {"address_line", "city", "postal_code", "patient_id"}


# ---------- freshness and Snowflake ----------

def test_source_freshness_reads_meta_last_updated(tmp_path):
    now = datetime.now(timezone.utc)
    stamp = lambda hours: (now - timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%S.000+00:00")  # noqa: E731
    landing = tmp_path / "landing"
    write_landing(landing, [
        dict(patient("p1", "synthea"), meta={"lastUpdated": stamp(2)}),
        dict(admission("e1", "p1", "2026-10-01T08:00:00", "2026-10-02T08:00:00"), meta={"lastUpdated": stamp(48)}),
        dict(lab("o1", "p1", "2026-10-01T09:00:00", 4.0), meta={"lastUpdated": stamp(100)}),
    ])
    rows = {name: (status, hours) for name, status, _, hours in wh.freshness(landing, target_path=tmp_path / "target")}
    assert {n: s for n, (s, _) in rows.items()} == {"Encounter": "warn", "Observation": "error", "Patient": "pass"}
    assert 1.5 < rows["Patient"][1] < 2.5


def test_rendered_marts_never_carry_the_salt(tmp_path, monkeypatch):
    monkeypatch.setenv("PSEUDONYM_SALT", "do-not-commit-this-salt")
    rendered = wh.render_snowflake(target_path=tmp_path / "target")
    marts = {k: v for k, v in rendered.items() if k.startswith("marts/")}
    assert len(marts) == 16
    assert not any("do-not-commit-this-salt" in sql or "local-demo-salt" in sql for sql in rendered.values())
    assert "(SELECT pseudonym_salt FROM governance_settings)" in marts["marts/20_fact_encounter.sql"]
    assert "CREATE OR REPLACE VIEW dim_patient_history AS" in marts["marts/13_dim_patient.sql"]
    assert "CREATE TABLE IF NOT EXISTS snap_patient_demographics" in marts["marts/12_snapshot_patient_demographics.sql"]
    assert json.dumps(sorted(marts)) == json.dumps(sorted(k for k in wh.SNOWFLAKE_FILES if k.startswith("marts/")))


# ---------- longer periods, the pooled source and complementary suppression ----------

def readmission_landing():
    """Readmissions arranged so that subtraction across the pooled and per source cells would reveal small counts:
    15 of 30 HL7 v2 stays readmitted (2026), 15 of 30 MIMIC stays (2150), and Synthea with 2 of 4 in 2026 and 2 of 4 in
    2020 (small cells)."""
    rows = []
    for src, n, year in (("hl7v2", 15, 2026), ("mimic", 15, 2150), ("synthea", 2, 2026), ("synthea", 2, 2020)):
        for i in range(n):
            pid = f"{src[0]}{year}{i:02}"
            rows += [patient(pid, src),
                     admission(f"{pid}a", pid, f"{year}-03-01T08:00:00", f"{year}-03-03T08:00:00", mimic=src == "mimic"),
                     admission(f"{pid}b", pid, f"{year}-03-10T08:00:00", f"{year}-03-12T08:00:00", mimic=src == "mimic")]
    return rows


def shown(con, period_type, source, year=None):
    sql = f"""SELECT numerator, denominator FROM mart_quality_period WHERE measure_id = 'readmission_30d'
              AND period_type = '{period_type}' AND source = '{source}' AND NOT is_suppressed"""
    if year:
        sql += f" AND period_start = DATE '{year}-01-01'"
    return one(con, sql)


def test_period_mart_hides_what_subtraction_would_reveal(tmp_path):
    landing = tmp_path / "landing"
    write_landing(landing, readmission_landing())
    con = build_at(tmp_path, landing)
    # by source: the big cells show at every grain, Synthea's 2 of 4 never does
    assert shown(con, "year", "hl7v2", 2026) == (15.0, 30) and shown(con, "all_time", "mimic") == (15.0, 30)
    assert shown(con, "year", "synthea", 2026) is None and shown(con, "all_time", "synthea") is None
    # "all" for 2026 would be HL7 v2 plus Synthea's hidden 2 of 4: hidden, since a source of that year is hidden
    assert shown(con, "year", "all", 2026) is None
    # "all" for all time minus the shown sources is Synthea's 4 of 8; no source can be hidden instead (each one's single
    # year is shown and would give it back), so the total is hidden
    assert shown(con, "all_time", "all") is None
    reasons = dict(con.execute("""SELECT source, suppression_reason FROM mart_quality_period WHERE period_type = 'all_time'
                                  AND measure_id = 'readmission_30d'""").fetchall())
    assert reasons == {"all": "complementary", "hl7v2": None, "mimic": None, "synthea": "small cell"}
    # the only "all" cells shown are the ones with a single source (2150: MIMIC alone), equal to it and so redundant;
    # all time minus 2026 "all" minus MIMIC (Synthea's 2020 cell) can never be formed
    pooled = con.execute("""SELECT period_type, period_start, numerator, denominator FROM mart_quality_period
                            WHERE measure_id = 'readmission_30d' AND source = 'all' AND NOT is_suppressed""").fetchall()
    assert sorted((t, d.year, n, den) for t, d, n, den in pooled) == [("quarter", 2150, 15.0, 30), ("year", 2150, 15.0, 30)]
    report = {g: (c, h) for g, c, h in con.execute("SELECT grain, cells, suppressed FROM mart_suppression_report").fetchall()}
    # 3 years x 3 stay measures (readmission, mean and median stay); only 2150's three are shown
    assert report["year all sources"] == (9, 6)


def test_secondary_suppression_keeps_the_pooled_total(tmp_path):
    """With MIMIC's stays spread over many small years, hiding MIMIC's all time cell protects Synthea, so the pooled
    all time total can be shown."""
    rows = [r for r in readmission_landing() if not r["id"].startswith(("m2150",))]
    for i in range(15):   # MIMIC: 15 patients, each in their own year (small cells), 15 of 30 readmitted in all
        pid, year = f"m{2150 + i}{i:02}", 2150 + i
        rows += [patient(pid, "mimic"),
                 admission(f"{pid}a", pid, f"{year}-03-01T08:00:00", f"{year}-03-03T08:00:00", mimic=True),
                 admission(f"{pid}b", pid, f"{year}-03-10T08:00:00", f"{year}-03-12T08:00:00", mimic=True)]
    landing = tmp_path / "landing"
    write_landing(landing, rows)
    con = build_at(tmp_path, landing)
    assert shown(con, "all_time", "all") == (34.0, 68)
    assert shown(con, "all_time", "hl7v2") == (15.0, 30)
    reasons = dict(con.execute("""SELECT source, suppression_reason FROM mart_quality_period WHERE period_type = 'all_time'
                                  AND measure_id = 'readmission_30d'""").fetchall())
    assert reasons["mimic"] == "complementary" and reasons["synthea"] == "small cell"
    # what a reader can still subtract: all minus HL7 v2 = MIMIC and Synthea together, 19 of 38 (not small)
    assert 34 - 15 > 10 and 68 - 30 > 10
