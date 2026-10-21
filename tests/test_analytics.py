import json
import sys
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import re

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "analytics"))
sys.path.insert(0, str(ROOT / "hl7v2"))
duckdb = pytest.importorskip("duckdb")
pytest.importorskip("dbt.cli.main")
import baseline as bl  # noqa: E402
import build as wh  # noqa: E402

MIMIC_PROFILE = "http://mimic.mit.edu/fhir/mimic/StructureDefinition/mimic-patient"
MIMIC_LAB = "http://mimic.mit.edu/fhir/mimic/CodeSystem/mimic-d-labitems"
MIMIC_ICD9 = "http://mimic.mit.edu/fhir/mimic/CodeSystem/mimic-diagnosis-icd9"
MIMIC_ICD10 = "http://mimic.mit.edu/fhir/mimic/CodeSystem/mimic-diagnosis-icd10"


def mimic_and_synthea():
    return [
        {"resourceType": "Patient", "id": "m1", "meta": {"profile": [MIMIC_PROFILE]},
         "identifier": [{"system": "http://mimic.mit.edu/fhir/mimic/identifier/patient", "value": "10014729"}],
         "name": [{"family": "Patient_10014729"}], "gender": "female", "birthDate": "2079-03-01",
         "deceasedDateTime": "2150-02-28T10:00:00-05:00"},
        {"resourceType": "Encounter", "id": "me1", "subject": {"reference": "Patient/m1"}, "status": "finished",
         "identifier": [{"system": "http://mimic.mit.edu/fhir/mimic/identifier/encounter-hosp", "value": "20001"}], "class": {"code": "EMER"}, "period": {"start": "2150-02-20T08:00:00-05:00", "end": "2150-02-28T10:00:00-05:00"}},
        {"resourceType": "Condition", "id": "mc1", "subject": {"reference": "Patient/m1"}, "encounter": {"reference": "Encounter/me1"},
         "code": {"coding": [{"system": MIMIC_ICD9, "code": "4280", "display": "Congestive heart failure, unspecified"}]}},
        {"resourceType": "Condition", "id": "mc2", "subject": {"reference": "Patient/m1"},
         "code": {"coding": [{"system": MIMIC_ICD9, "code": "0389", "display": "Septicemia"}]}},
        {"resourceType": "Condition", "id": "mc3", "subject": {"reference": "Patient/m1"},
         "code": {"coding": [{"system": MIMIC_ICD10, "code": "I5023", "display": "Acute on chronic systolic heart failure"}]}},
        {"resourceType": "Observation", "id": "mo1", "subject": {"reference": "Patient/m1"}, "status": "final",
         "category": [{"coding": [{"code": "laboratory"}]}],
         "code": {"coding": [{"system": MIMIC_LAB, "code": "50983", "display": "Sodium"}]},
         "valueQuantity": {"value": 128, "unit": "mEq/L"}, "referenceRange": [{"low": {"value": 133}, "high": {"value": 145}}],
         "effectiveDateTime": "2150-02-21T06:00:00-05:00"},
        {"resourceType": "Observation", "id": "mo2", "subject": {"reference": "Patient/m1"}, "status": "final",
         "category": [{"coding": [{"code": "laboratory"}]}],
         "code": {"coding": [{"system": MIMIC_LAB, "code": "99999", "display": "Unmapped thing"}]},
         "valueQuantity": {"value": 1.2, "unit": "x"}, "effectiveDateTime": "2150-02-21T06:00:00-05:00"},
        {"resourceType": "Patient", "id": "s1", "identifier": [{"system": "https://github.com/synthetichealth/synthea", "value": "uuid"},
                                                               {"system": "http://hospital.smarthealthit.org", "value": "MRN-S1"}],
         "name": [{"family": "Hickle134", "given": ["Jody426"]}], "gender": "male", "birthDate": "2017-06-17",
         "address": [{"city": "Newburyport", "state": "MA"}]},
        {"resourceType": "Condition", "id": "sc1", "subject": {"reference": "Patient/s1"},
         "code": {"coding": [{"system": "http://snomed.info/sct", "code": "10509002", "display": "Acute bronchitis"}], "text": "Acute bronchitis"}},
        # second admission 10 days after discharge (readmission) and an ICU stay inside the first one (not a readmission)
        # MIMIC: the class is the admission type, so a hospital stay can be EMER or AMB; an ED only visit is EMER too
        {"resourceType": "Encounter", "id": "me2", "subject": {"reference": "Patient/m1"}, "status": "finished",
         "identifier": [{"system": "http://mimic.mit.edu/fhir/mimic/identifier/encounter-hosp", "value": "20002"}], "class": {"code": "AMB"},
         "period": {"start": "2150-03-10T08:00:00", "end": "2150-03-12T08:00:00"}},
        {"resourceType": "Encounter", "id": "med", "subject": {"reference": "Patient/m1"}, "status": "finished",
         "identifier": [{"system": "http://mimic.mit.edu/fhir/mimic/identifier/encounter-ed", "value": "30001"}],
         "class": {"code": "EMER"}, "period": {"start": "2150-03-05T08:00:00", "end": "2150-03-05T12:00:00"}},
        {"resourceType": "Encounter", "id": "micu", "subject": {"reference": "Patient/m1"}, "status": "finished",
         "identifier": [{"system": "http://mimic.mit.edu/fhir/mimic/identifier/encounter-icu", "value": "40001"}],
         "class": {"code": "ACUTE"}, "partOf": {"reference": "Encounter/me1"},
         "period": {"start": "2150-02-22T08:00:00", "end": "2150-02-24T08:00:00"}},
        # creatinine 1.0 -> 1.4 within 24 hours: kidney injury signal; 2.0 four days later is outside 48 hours
        *[{"resourceType": "Observation", "id": f"cr{i}", "subject": {"reference": "Patient/m1"}, "status": "final",
           "category": [{"coding": [{"code": "laboratory"}]}],
           "code": {"coding": [{"system": MIMIC_LAB, "code": "50912", "display": "Creatinine"}]},
           "valueQuantity": {"value": v, "unit": "mg/dL"}, "effectiveDateTime": t}
          for i, (v, t) in enumerate([(1.0, "2150-02-21T06:00:00"), (1.4, "2150-02-22T06:00:00"), (2.0, "2150-02-26T06:00:00")])],
        # Synthea vitals: height, weight and the BMI Synthea itself reported
        *[{"resourceType": "Observation", "id": f"sv{i}", "subject": {"reference": "Patient/s2"}, "status": "final",
           "category": [{"coding": [{"code": "vital-signs"}]}],
           "code": {"coding": [{"system": "http://loinc.org", "code": c}]},
           "valueQuantity": {"value": v, "unit": u}, "effectiveDateTime": t}
          for i, (c, v, u, t) in enumerate([("8302-2", 180.0, "cm", "2020-01-01T10:00:00"), ("8302-2", 175.0, "cm", "2024-05-01T10:00:00"),
                                            ("29463-7", 90.0, "kg", "2024-05-01T10:00:00"), ("39156-5", 29.4, "kg/m2", "2024-05-01T10:00:00")])],
        {"resourceType": "Patient", "id": "s2", "identifier": [{"system": "https://github.com/synthetichealth/synthea", "value": "u2"}],
         "name": [{"family": "Okafor12", "given": ["Ada3"]}], "gender": "female", "birthDate": "1980-01-01"},
        {"resourceType": "Condition", "id": "sc2", "subject": {"reference": "Patient/s2"},
         "code": {"coding": [{"system": "http://snomed.info/sct", "code": "44054006", "display": "Diabetes mellitus type 2"}]}},
        {"resourceType": "Observation", "id": "so1", "subject": {"reference": "Patient/s1"}, "status": "final",
         "category": [{"coding": [{"code": "laboratory"}]}],
         "code": {"coding": [{"system": "http://loinc.org", "code": "718-7", "display": "Hemoglobin"}]},
         "valueQuantity": {"value": 13.3, "unit": "g/dL"}, "effectiveDateTime": "2023-06-03T10:00:00+00:00"},
    ]


def hl7v2_resources():
    import generate
    import reference
    import to_fhir
    from route import Router

    translations = {"4280": [{"code": "I50.814", "equivalence": "inexact"}, {"code": "I50.9", "equivalence": "inexact"}],
                    "42731": [{"code": "I48.91", "display": "Unspecified atrial fibrillation", "equivalence": "inexact"}]}
    r, m = Router(), to_fhir.FeedMapper(translate=lambda c: translations.get(c, []))
    for _, _, text in generate.build():
        o = r.route(text)
        if o.status == "process":
            m.apply(o.message)
    # as extract.py would land them, with the staff the attending points at
    return [x for x in m.resources() + reference.practitioners() if x["resourceType"] in EXTRACTED]


EXTRACTED = {"Patient", "Encounter", "Condition", "Observation", "Location", "Practitioner", "CareTeam"}


def write_landing(folder, resources, extracted_at="2026-10-14 12:00:00"):
    folder.mkdir(parents=True, exist_ok=True)
    by_type = {}
    for res in resources:
        by_type.setdefault(res["resourceType"], []).append(res)
    for t, rows in by_type.items():
        (folder / f"{t}.ndjson").write_text("".join(json.dumps(x) + "\n" for x in rows))
    (folder / "manifest.json").write_text(json.dumps({"extracted_at": extracted_at}))


def build_at(folder, landing, salt=None):
    """Build the warehouse with dbt into folder/w.duckdb and open it. Asserts every dbt node passed."""
    db = folder / "w.duckdb"
    bad = wh.build(db, landing, salt=salt, target_path=folder / "target", quiet=True)
    assert bad == [], bad
    return duckdb.connect(str(db))


@pytest.fixture(scope="module")
def warehouse(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("warehouse")
    landing = tmp_path / "landing"
    resources = mimic_and_synthea() + hl7v2_resources()
    write_landing(landing, resources + [dict(resources[0], gender="other")])  # duplicate copy of m1
    con = build_at(tmp_path, landing)
    yield con
    con.close()


def one(con, sql):
    return con.execute(sql).fetchone()


@pytest.fixture(scope="module")
def compiled(tmp_path_factory):
    """Every model and test compiled offline, once per dialect."""
    t = tmp_path_factory.mktemp("compile")
    return {d: wh.compile_models(d, target_path=t / d) for d in ("duckdb", "snowflake")}


def test_json_macros_render_both_dialects(compiled):
    d = compiled["duckdb"]["silver_observation"].compiled_code
    s = compiled["snowflake"]["silver_observation"].compiled_code
    assert "json_extract_string(raw, '$.code.coding[0].code')" in d
    assert "TRY_CAST(json_extract_string(raw, '$.valueQuantity.value') AS DOUBLE)" in d
    assert "TRY_CAST(SUBSTR(json_extract_string(raw, '$.effectiveDateTime'), 1, 19) AS TIMESTAMP)" in d
    assert "raw:code.coding[0].code::string" in s
    assert "TRY_CAST(raw:valueQuantity.value::string AS DOUBLE)" in s
    assert "TRY_CAST(SUBSTR(raw:effectiveDateTime::string, 1, 19) AS TIMESTAMP_NTZ)" in s
    assert "json_extract_string" not in s and "{{" not in s


def test_json_macros_reject_bad_input(tmp_path, capsys):
    with pytest.raises(SystemExit, match="unknown render_dialect"):
        wh.compile_models("oracle", target_path=tmp_path / "a")
    res = wh.dbt(["compile", "--inline", "SELECT {{ json_str('raw', 'path with spaces') }}"], ":memory:",
                 target_path=tmp_path / "b", quiet=True, check=False)
    assert not res.success and "bad JSON path path with spaces" in capsys.readouterr().out


def test_pseudonym_macro_escapes_the_salt(compiled):
    sql = compiled["duckdb"]["analyst_patient"].compiled_code
    assert f"'P-' || UPPER(SUBSTR(MD5('{wh.DEMO_SALT}' || p.patient_id), 1, 10))" in sql


def statements(sql):
    """Split a SQL file into statements, ignoring comments (which may contain semicolons)."""
    no_comments = re.sub(r"--[^\n]*", "", sql)
    return [s for s in no_comments.split(";") if s.strip()]


def test_snowflake_sql_parses_and_is_up_to_date(tmp_path):
    sqlglot = pytest.importorskip("sqlglot")
    rendered = wh.render_snowflake(target_path=tmp_path / "target")
    assert set(rendered) == set(wh.SNOWFLAKE_FILES)
    for rel, sf in rendered.items():
        for stmt in statements(sf):
            sqlglot.parse_one(stmt, read="snowflake")
        committed = ROOT / "analytics" / "sql" / "snowflake" / rel
        assert committed.exists(), f"run: python3 analytics/build.py render ({rel} missing)"
        assert committed.read_text() == sf, f"{rel} is stale: run python3 analytics/build.py render"
    checks = rendered["silver/90_checks.sql"]
    assert checks.count("UNION ALL") == 6 and "'bronze has no duplicate copies left in silver'" in checks
    setup = (ROOT / "analytics" / "sql" / "snowflake" / "00_setup_and_load.sql").read_text()
    parsed = 0
    for stmt in statements(setup):
        sqlglot.parse_one(stmt, read="snowflake")
        parsed += 1
    assert parsed >= 10


def test_bronze_keeps_everything_with_lineage(warehouse):
    n, files, raw_ok = one(warehouse, """SELECT COUNT(*), COUNT(DISTINCT source_file),
        SUM(CASE WHEN json_extract_string(raw, '$.id') = resource_id THEN 1 ELSE 0 END) FROM bronze_resources""")
    assert files == 6 and raw_ok == n   # the five types above, plus the original feed's CareTeams
    assert one(warehouse, "SELECT COUNT(*) FROM bronze_resources WHERE resource_id = 'm1'")[0] == 2  # both copies kept in Bronze
    assert one(warehouse, "SELECT MIN(loaded_at) FROM bronze_resources")[0].year == 2026


def test_silver_patient(warehouse):
    rows = dict(warehouse.execute("SELECT source, COUNT(*) FROM silver_patient GROUP BY source").fetchall())
    assert rows == {"mimic": 1, "synthea": 2, "hl7v2": 25}
    assert one(warehouse, "SELECT COUNT(*) FROM silver_patient WHERE patient_id = 'm1'")[0] == 1  # deduplicated
    m = one(warehouse, "SELECT family_name, mrn, is_deceased, deceased_at FROM silver_patient WHERE patient_id = 'm1'")
    assert m[0] == "Patient 10014729" and m[1] == "10014729" and m[2] is True and m[3].hour == 10
    s = one(warehouse, "SELECT family_name, given_name, mrn, city FROM silver_patient WHERE patient_id = 's1'")
    assert s == ("Hickle", "Jody", "MRN-S1", "Newburyport")
    h = one(warehouse, "SELECT mrn, family_name FROM silver_patient WHERE patient_id = 'riverside-rv100354'")
    assert h == ("RV100354", "Lindgren")


def test_silver_condition_icd10(warehouse):
    q = "SELECT code_system, icd10_code, icd10_origin, needs_review FROM silver_condition WHERE condition_id = '{}'"
    assert one(warehouse, q.format("mc3")) == ("icd10", "I5023", "source", False)
    sepsis = one(warehouse, q.format("mc2"))  # MIMIC ICD 9 with a single CMS target, from the seed
    assert sepsis[0] == "icd9" and sepsis[1] == "A419" and sepsis[2].startswith("cms_gem_")
    chf = one(warehouse, q.format("mc1"))     # MIMIC ICD 9 with two CMS options: no guess
    assert chf == ("icd9", None, None, True)
    assert one(warehouse, q.format("sc1")) == ("snomed", None, None, False)
    hl7 = dict(((r[0], r[1]), r[2]) for r in warehouse.execute(
        "SELECT icd10_origin, needs_review, COUNT(*) FROM silver_condition WHERE condition_id LIKE 'rv-%' GROUP BY 1, 2").fetchall())
    assert hl7[("hl7v2_pipeline", False)] > 0 and hl7[(None, True)] > 0 and hl7[("source", False)] > 0


def test_silver_observation(warehouse):
    q = "SELECT loinc, value_num, range_flag, comparator, effective_at FROM silver_observation WHERE observation_id = '{}'"
    na = one(warehouse, q.format("mo1"))
    assert na[0] == "2951-2" and na[1] == 128 and na[2] == "low" and na[4].hour == 6  # time zone offset dropped
    assert one(warehouse, q.format("mo2"))[0] is None
    assert one(warehouse, q.format("so1"))[:3] == ("718-7", 13.3, None)
    trop = one(warehouse, "SELECT COUNT(*) FROM silver_observation WHERE comparator = '<' AND range_flag IS NULL")[0]
    assert trop > 0  # "<0.01" is never flagged as normal or abnormal
    assert one(warehouse, "SELECT COUNT(*) FROM silver_observation WHERE value_text = 'Moderate' OR value_text = 'Mild' OR value_text = 'None'")[0] > 0


def test_tag_order_does_not_matter(tmp_path):
    """HAPI returns meta.tag sorted, so the review tag can come before the source tag."""
    src = {"system": "https://github.com/nischaldinesh/fhir-patient-explorer/CodeSystem/source", "code": "hl7v2"}
    rev = {"system": "https://github.com/nischaldinesh/fhir-patient-explorer/CodeSystem/review", "code": "needs-review"}
    landing = tmp_path / "landing"
    write_landing(landing, [
        {"resourceType": "Patient", "id": "h1", "meta": {"tag": [{"system": "x", "code": "other"}, src]}},
        {"resourceType": "Condition", "id": "c1", "meta": {"tag": [rev, src]}, "subject": {"reference": "Patient/h1"}},
        {"resourceType": "Condition", "id": "c2", "meta": {"tag": [src, rev]}, "subject": {"reference": "Patient/h1"}},
        {"resourceType": "Condition", "id": "c3", "meta": {"tag": [src]}, "subject": {"reference": "Patient/h1"}},
    ])
    con = build_at(tmp_path, landing)
    assert one(con, "SELECT source FROM silver_patient")[0] == "hl7v2"
    assert dict(con.execute("SELECT condition_id, needs_review FROM silver_condition").fetchall()) == {"c1": True, "c2": True, "c3": False}


CHECKS = ["patient ids are unique", "every patient has a known source", "encounters point to a known patient",
          "conditions point to a known patient", "observations point to a known patient",
          "encounters do not end before they start", "bronze has no duplicate copies left in silver"]


def test_data_quality_checks_pass(warehouse):
    failures = warehouse.execute("SELECT check_name, failures FROM dq_checks WHERE failures > 0").fetchall()
    assert failures == []
    assert one(warehouse, "SELECT COUNT(*) FROM dq_checks")[0] == 7
    assert [r[0] for r in warehouse.execute("SELECT check_name FROM dq_checks").fetchall()] == CHECKS  # page order
    statuses = dict(warehouse.execute("SELECT status, COUNT(*) FROM dq_test_results GROUP BY 1").fetchall())
    assert set(statuses) == {"pass"} and statuses["pass"] >= 70


def test_failed_checks_are_counted_and_the_warehouse_still_builds(tmp_path):
    landing = tmp_path / "landing"
    write_landing(landing, [
        {"resourceType": "Patient", "id": "p1", "identifier": [{"system": "https://github.com/synthetichealth/synthea", "value": "1"}]},
        {"resourceType": "Patient", "id": "nobody"},                                  # no known source
        {"resourceType": "Encounter", "id": "e1", "subject": {"reference": "Patient/p1"},
         "period": {"start": "2024-01-02T00:00:00", "end": "2024-01-01T00:00:00"}},    # ends before it starts
        {"resourceType": "Encounter", "id": "e2", "subject": {"reference": "Patient/ghost"}},
        {"resourceType": "Observation", "id": "o1", "subject": {"reference": "Patient/ghost"}},
        {"resourceType": "Observation", "id": "o2", "subject": {"reference": "Patient/ghost"}},
    ])
    bad = wh.build(tmp_path / "w.duckdb", landing, target_path=tmp_path / "target", quiet=True)
    assert bad and all(b.endswith(": warn") for b in bad)   # reported, never stops the build
    con = duckdb.connect(str(tmp_path / "w.duckdb"))
    assert dict(con.execute("SELECT check_name, failures FROM dq_checks").fetchall()) == {
        CHECKS[0]: 0, CHECKS[1]: 1, CHECKS[2]: 1, CHECKS[3]: 0, CHECKS[4]: 2, CHECKS[5]: 1, CHECKS[6]: 0}
    assert one(con, "SELECT COUNT(*) FROM gold_patient_summary")[0] == 2
    assert wh.print_status(con) > 0


def test_rebuild_is_idempotent(tmp_path):
    landing = tmp_path / "landing"
    write_landing(landing, mimic_and_synthea())
    con = build_at(tmp_path, landing)
    first = wh.counts(con)
    con.close()
    con = build_at(tmp_path, landing)
    assert wh.counts(con) == first


def test_extract_follows_paging(tmp_path):
    from extract import extract_all

    data = {"Patient": [{"resourceType": "Patient", "id": f"p{i}"} for i in range(7)],
            "Encounter": [], "Condition": [], "Observation": [{"resourceType": "Observation", "id": "o1"}],
            "Location": [], "Practitioner": [], "CareTeam": []}
    import base64
    runs = [{"day": "2026-10-14", "status": "success", "finished_at": "2026-10-15T06:16:02+00:00"}]

    class H(BaseHTTPRequestHandler):
        def do_GET(self):
            u = urllib.parse.urlparse(self.path)
            if u.path.endswith("/Binary/pipeline-run-history"):
                d = json.dumps({"resourceType": "Binary", "data": base64.b64encode(json.dumps(runs).encode()).decode()}).encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(d)))
                self.end_headers()
                self.wfile.write(d)
                return
            if "/Binary/" in u.path:  # no EMPI Binaries on this server: HAPI answers 404
                self.send_response(404)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            q = {k: v[0] for k, v in urllib.parse.parse_qs(u.query).items()}
            rtype = q.get("t") or u.path.rstrip("/").split("/")[-1]
            rows = data[rtype]
            if q.get("_summary") == "count":
                body = {"resourceType": "Bundle", "total": len(rows)}
            else:
                off, size = int(q.get("off", 0)), 3
                body = {"resourceType": "Bundle", "entry": [{"resource": r} for r in rows[off:off + size]], "link": []}
                if off + size < len(rows):  # next link names a different host on purpose
                    body["link"].append({"relation": "next", "url": f"http://elsewhere:9/fhir?t={rtype}&off={off + size}"})
            d = json.dumps(body).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(d)))
            self.end_headers()
            self.wfile.write(d)

        def log_message(self, *a):
            pass

    srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        m = extract_all(f"http://127.0.0.1:{srv.server_port}/fhir", tmp_path / "landing", quiet=True)
    finally:
        srv.shutdown()
    assert m["counts"]["Patient"] == {"extracted": 7, "server_total": 7}
    assert len((tmp_path / "landing" / "Patient.ndjson").read_text().splitlines()) == 7
    assert (tmp_path / "landing" / "Encounter.ndjson").read_text() == ""
    assert json.loads((tmp_path / "landing" / "feed_runs.json").read_text()) == runs and m["feed_runs"] == 1


def test_gold_readmission_ignores_icu_stays(warehouse):
    rows = {r[0]: r[1:] for r in warehouse.execute(
        "SELECT encounter_id, is_admission, readmitted_30d, days_to_next_admission, length_of_stay_days, encounter_kind FROM gold_encounter_summary "
        "WHERE patient_id = 'm1'").fetchall()}
    assert rows["micu"][0] is False                     # ICU stay inside the hospital stay
    assert rows["med"][0] is False                      # ED only visit: EMER class, but not an admission
    assert rows["me1"][:3] == (True, True, 9.9)         # next admission 9.9 days after discharge
    assert rows["me1"][3] == 8.08
    assert rows["me2"][:2] == (True, False)
    assert rows["me1"][4] == rows["me2"][4] == "hospital admission"   # EMER and AMB classes, both hospital stays
    assert (rows["med"][4], rows["micu"][4]) == ("emergency visit", "part of another stay")


def test_gold_aki_signal_within_48_hours(warehouse):
    n, rise = one(warehouse, "SELECT n_signal_results, max_rise_mg_dl FROM gold_aki_signal WHERE patient_id = 'm1'")
    assert (n, rise) == (1, 0.4)  # 1.0 -> 1.4 in 24 h counts; 1.4 -> 2.0 four days later does not


def test_gold_bmi_pairs_closest_height_and_checks_reported(warehouse):
    r = one(warehouse, "SELECT height_cm, weight_kg, bmi_computed, bmi_reported, bmi_category, days_between_height_and_weight "
                       "FROM gold_bmi WHERE patient_id = 's2'")
    assert r == (175.0, 90.0, 29.4, 29.4, "overweight", 0)


def test_gold_latest_labs_and_patient_summary(warehouse):
    na = one(warehouse, "SELECT latest_value, latest_flag, n_results FROM gold_lab_latest WHERE patient_id = 'm1' AND lab = 'sodium'")
    assert na == (128.0, "low", 1)
    assert one(warehouse, "SELECT COUNT(*) FROM gold_lab_latest WHERE lab = 'creatinine' AND patient_id = 'm1'")[0] == 1
    m = one(warehouse, "SELECT age_years, age_basis, n_admissions, readmissions_30d, has_aki_signal, n_diagnoses_need_review "
                       "FROM gold_patient_summary WHERE patient_id = 'm1'")
    assert m == (70, "at death", 2, 1, True, 1)
    s2 = one(warehouse, "SELECT age_years, age_basis, latest_bmi FROM gold_patient_summary WHERE patient_id = 's2'")
    assert s2 == (46, "as of extract", 29.4)   # extract date 2026-10-14
    assert one(warehouse, "SELECT COUNT(*) FROM gold_patient_summary")[0] == one(warehouse, "SELECT COUNT(*) FROM silver_patient")[0]


def test_gold_cohorts_cross_code_systems(warehouse):
    members = set(warehouse.execute("SELECT cohort, patient_id FROM gold_cohort_members").fetchall())
    assert ("diabetes_type2", "s2") in members        # SNOMED 44054006
    assert ("heart_failure", "m1") in members         # ICD 9 4280 (needs review) and ICD 10 I5023
    assert ("sepsis", "m1") in members                # ICD 9 0389, upgraded to A41.9 by the CMS seed
    assert ("low_sodium", "m1") in members and ("aki_signal", "m1") in members and ("readmitted_30d", "m1") in members
    hl7_hf = one(warehouse, "SELECT COUNT(*) FROM gold_cohort_members WHERE cohort = 'heart_failure' AND patient_id LIKE 'riverside-%'")[0]
    assert hl7_hf > 0                                  # HL7 v2 ICD 9 428.0 arrives dotted and still matches
    c = one(warehouse, "SELECT patients, mimic, synthea, hl7v2 FROM gold_cohorts WHERE cohort = 'diabetes_type2'")
    assert c[0] == c[1] + c[2] + c[3] and c[2] == 1


def test_gold_terminology_quality(warehouse):
    rows = {r[0]: r[1:] for r in warehouse.execute(
        "SELECT source, pct_loinc, arrived_snomed, need_review FROM gold_terminology_quality").fetchall()}
    assert set(rows) == {"mimic", "synthea", "hl7v2"}
    assert rows["synthea"][0] == 100.0 and rows["synthea"][1] == 2
    assert rows["mimic"][2] == 1 and rows["hl7v2"][2] > 0


IDENTIFYING = {"patient_id", "mrn", "family_name", "given_name", "city", "postal_code", "birth_date", "deceased_at"}


def view_columns(con, view):
    return {r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = ?", [view]).fetchall()}


def test_analyst_views_hide_identifiers(warehouse):
    for v in ("analyst_patient", "analyst_patient_summary", "analyst_cohort_members", "analyst_aki_signal"):
        cols = view_columns(warehouse, v)
        assert "patient_key" in cols and not cols & IDENTIFYING, v
    roles = dict(warehouse.execute("SELECT role, sees_identifiers FROM governance_roles").fetchall())
    assert roles == {"clinician": True, "analyst": False}


def test_pseudonyms_are_stable_unique_and_salted(tmp_path, warehouse):
    keys = [r[0] for r in warehouse.execute("SELECT patient_key FROM analyst_patient").fetchall()]
    assert len(keys) == len(set(keys)) == one(warehouse, "SELECT COUNT(*) FROM silver_patient")[0]
    assert all(re.fullmatch(r"P-[0-9A-F]{10}", k) for k in keys)
    m1 = one(warehouse, f"SELECT 'P-' || UPPER(SUBSTR(MD5('{wh.DEMO_SALT}' || 'm1'), 1, 10))")[0]
    assert m1 in keys
    # the cohort view uses the same key, so analysts can join without seeing ids
    assert ("heart_failure", m1) in set(warehouse.execute("SELECT cohort, patient_key FROM analyst_cohort_members").fetchall())
    landing = tmp_path / "landing2"
    write_landing(landing, mimic_and_synthea())
    other = build_at(tmp_path, landing, salt="a different secret's salt")   # quote in the salt is escaped
    assert m1 not in {r[0] for r in other.execute("SELECT patient_key FROM analyst_patient").fetchall()}


def test_analyst_aki_signal_is_pseudonymous(warehouse):
    # Same rows as gold_aki_signal, keyed by the same pseudonym as the other analyst views.
    m1 = one(warehouse, f"SELECT 'P-' || UPPER(SUBSTR(MD5('{wh.DEMO_SALT}' || 'm1'), 1, 10))")[0]
    aki = {r[0]: r[1:] for r in warehouse.execute(
        "SELECT patient_key, source, max_rise_mg_dl, n_signal_results FROM analyst_aki_signal").fetchall()}
    assert len(aki) == one(warehouse, "SELECT COUNT(*) FROM gold_aki_signal")[0]
    assert aki[m1] == one(warehouse, "SELECT 'mimic', max_rise_mg_dl, n_signal_results FROM gold_aki_signal WHERE patient_id = 'm1'")
    assert m1 in {r[0] for r in warehouse.execute("SELECT patient_key FROM analyst_patient").fetchall()}


def test_ages_90_and_over_are_grouped(tmp_path):
    landing = tmp_path / "landing"
    write_landing(landing, [
        {"resourceType": "Patient", "id": "old", "identifier": [{"system": "https://github.com/synthetichealth/synthea", "value": "o"}],
         "gender": "female", "birthDate": "1930-05-01"},
        {"resourceType": "Patient", "id": "young", "identifier": [{"system": "https://github.com/synthetichealth/synthea", "value": "y"}],
         "gender": "male", "birthDate": "1990-05-01"},
    ])
    con = build_at(tmp_path, landing)
    rows = {r[0]: r[1:] for r in con.execute(
        "SELECT a.age_band, a.age_90_or_over, a.birth_year, s.age_years FROM analyst_patient a "
        "JOIN analyst_patient_summary s USING (patient_key)").fetchall()}
    assert rows["90+"] == (True, None, None)
    assert rows["30s"] == (False, 1990, 36)


def test_snowflake_governance_sql():
    sql = (ROOT / "analytics" / "sql" / "snowflake" / "30_governance.sql").read_text()
    for col in ("mrn", "family_name", "given_name", "city", "postal_code", "birth_date"):
        assert re.search(rf"MODIFY COLUMN {col}\s+SET MASKING POLICY", sql), col
    assert "IS_ROLE_IN_SESSION('CLINICIAN')" in sql
    for v in ("analyst_patient", "analyst_patient_summary", "analyst_cohort_members", "analyst_aki_signal"):
        assert f"CREATE OR REPLACE SECURE VIEW {v}" in sql
        assert re.search(rf"GRANT SELECT ON VIEW {v}\s+TO ROLE analyst", sql), v
    # the analyst role is never granted the Silver tables, the identified Gold tables or the internal mart models
    grants = [g for g in re.findall(r"GRANT SELECT ON (?:VIEW|TABLE) (\w+)\s+TO ROLE analyst", sql)]
    aggregates = ("gold_cohorts", "gold_terminology_quality", "gold_weekly_metrics", "gold_daily_census")
    marts = ("dim_date", "dim_patient", "dim_patient_history", "dim_location", "dim_practitioner", "dim_diagnosis",
             "dim_lab", "fact_encounter", "fact_diagnosis", "fact_lab_result", "fact_bed_day", "fact_bed_capacity_day",
             "mart_quality_monthly", "mart_quality_period", "mart_suppression_report")
    assert grants and all(g.startswith("analyst_") or g in aggregates or g in marts for g in grants)
    assert set(marts) <= set(grants)   # every BI mart, and nothing internal
    assert not [g for g in grants if g.startswith(("silver_", "bronze_", "int_", "snap_", "seed_")) or g == "governance_settings"]
    assert "gold_weekly_metrics" in grants and "gold_bed_board" not in grants and "gold_readmission_risk" not in grants
    assert re.search(r"GRANT SELECT ON VIEW dim_patient_history\s+TO ROLE analyst", sql)   # rendered as a view
    # the salt table comes from the setup file, so the marts (which read it) can run before this file grants on them
    setup = (ROOT / "analytics" / "sql" / "snowflake" / "00_setup_and_load.sql").read_text()
    assert "CREATE OR REPLACE TABLE governance_settings" in setup and "CREATE OR REPLACE TABLE governance_settings" not in sql
    duck = (ROOT / "analytics" / "dbt" / "macros" / "pseudonym.sql").read_text()
    formula = "'P-' || UPPER(SUBSTR(MD5("
    assert formula in sql and formula in duck   # same pseudonym in both engines


def test_snapshot_and_publish(warehouse):
    import publish as pub

    snap = pub.snapshot(warehouse)
    json.dumps(snap)  # must be plain JSON
    assert sum(p["patients"] for p in snap["patients"]) == 28
    a = snap["admissions"]
    assert a["total"] == 2 + one(warehouse, "SELECT COUNT(*) FROM gold_encounter_summary WHERE is_admission AND patient_id <> 'm1'")[0]
    assert sum(b["admissions"] for b in a["los_buckets"]) == a["total"]
    assert {c["cohort"] for c in snap["cohorts"]} >= {"heart_failure", "sepsis"} and all(c["label"] for c in snap["cohorts"])
    rises = [k["max_rise_mg_dl"] for k in snap["aki_signal"]]
    assert rises == sorted(rises, reverse=True)
    # the kidney signal list is for analysts: pseudonym only, no names or ids
    m1 = one(warehouse, f"SELECT 'P-' || UPPER(SUBSTR(MD5('{wh.DEMO_SALT}' || 'm1'), 1, 10))")[0]
    assert m1 in {k["patient_key"] for k in snap["aki_signal"]}
    assert all(not set(k) & IDENTIFYING for k in snap["aki_signal"])
    assert not set(snap["governance"]["analyst_patient_columns"]) & IDENTIFYING
    assert all(not set(r) & IDENTIFYING for r in snap["governance"]["analyst_sample"])
    assert all(d["failures"] == 0 for d in snap["data_quality"])

    got = {}

    class H(BaseHTTPRequestHandler):
        def do_PUT(self):
            got["path"] = self.path
            got["body"] = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            d = json.dumps(got["body"]).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/fhir+json")
            self.send_header("Content-Length", str(len(d)))
            self.end_headers()
            self.wfile.write(d)

        def log_message(self, *a):
            pass

    srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        pub.publish(f"http://127.0.0.1:{srv.server_port}/fhir", snap)
    finally:
        srv.shutdown()
    import base64
    assert got["path"] == "/fhir/Binary/analytics-snapshot"
    assert got["body"]["resourceType"] == "Binary" and got["body"]["contentType"] == "application/json"
    assert json.loads(base64.b64decode(got["body"]["data"]))["admissions"]["total"] == a["total"]


def test_dbt_vars(tmp_path):
    landing = tmp_path / "landing"
    write_landing(landing, [], extracted_at="2026-10-14 17:28:51")
    v = wh.dbt_vars(landing, salt="s")
    assert v == {"landing_path": str(landing.resolve()).replace("\\", "/"), "loaded_at": "2026-10-14 17:28:51",
                 "pseudonym_salt": "s"}
    (landing / "manifest.json").unlink()
    assert wh.dbt_vars(landing)["loaded_at"] == "1970-01-01 00:00:00"


def test_duckdb_setting_hook(tmp_path, monkeypatch):
    """The on-run-start hooks set DuckDB settings only when their variable is set (local Airflow), never otherwise."""
    landing = tmp_path / "landing"
    write_landing(landing, [])

    def rendered():
        res = wh.dbt(["compile", "--inline", "{{ duckdb_setting('memory_limit', 'DUCKDB_MEMORY_LIMIT') }}"], ":memory:",
                     wh.dbt_vars(landing), target_path=tmp_path / "target", quiet=True)
        return res.result.results[0].node.compiled_code.strip()
    monkeypatch.delenv("DUCKDB_MEMORY_LIMIT", raising=False)
    assert rendered() == ""
    monkeypatch.setenv("DUCKDB_MEMORY_LIMIT", " 1500MB ")
    assert rendered() == "SET memory_limit = '1500MB'"


def test_default_target():
    assert wh.default_target({}) == wh.DBT_DIR / "target"
    assert wh.default_target({"DBT_TARGET_PATH": ""}) == wh.DBT_DIR / "target"
    assert wh.default_target({"DBT_TARGET_PATH": "/data/dbt-target"}) == Path("/data/dbt-target")


def test_split_header():
    assert wh.split_header("-- a\n-- b\nSELECT 1\n-- c\n") == ("-- a\n-- b", "SELECT 1\n-- c")
    assert wh.split_header("\nSELECT 1") == ("", "SELECT 1")


def test_seeds_are_the_terminology_files():
    seeds = ROOT / "analytics" / "dbt" / "seeds"
    assert (seeds / "lab_loinc_map.csv").read_text() == (ROOT / "terminology" / "lab_loinc_map.csv").read_text()
    assert (seeds / "icd9_to_icd10.csv").read_text() == (ROOT / "terminology" / "out" / "icd9_to_icd10.csv").read_text()


def test_baseline_compare_reports_each_difference():
    base = {"counts": {"gold_a": 2, "silver_b": 1}, "columns": {"gold_a": [["x", "INTEGER"]], "silver_b": [["y", "VARCHAR"]]},
            "contents": {"gold_a": [[1], [2]]}, "checksums": {"silver_b": "abc"}, "snapshot": {"as_of": "2026-10-14"}}
    same = json.loads(json.dumps(base))
    assert all(ok for _, ok, _ in bl.compare(base, same))
    changed = json.loads(json.dumps(base))
    changed["contents"]["gold_a"] = [[1], [3]]
    changed["checksums"]["silver_b"] = "abd"
    changed["snapshot"]["as_of"] = "2026-10-15"
    assert [n for n, ok, _ in bl.compare(base, changed) if not ok] == ["gold_a", "silver_b", "snapshot (without generated_at)"]
    fewer = json.loads(json.dumps(base))
    fewer["counts"]["gold_a"] = 1
    assert [n for n, ok, _ in bl.compare(base, fewer) if not ok] == ["gold_a"]


def test_baseline_compare_additive_allows_new_things_only():
    base = {"counts": {"gold_a": 2, "dq_test_results": 3}, "columns": {"gold_a": [["x", "INTEGER"]], "dq_test_results": [["t", "VARCHAR"]]},
            "contents": {"gold_a": [[1], [2]]}, "checksums": {"dq_test_results": "abc"}, "snapshot": {"as_of": "2026-10-14"}}
    grown = {"counts": {"gold_a": 2, "gold_new": 5, "dq_test_results": 9},
             "columns": {"gold_a": [["x", "INTEGER"], ["y", "VARCHAR"]], "gold_new": [["z", "INTEGER"]], "dq_test_results": [["t", "VARCHAR"]]},
             "contents": {"gold_a": [[1], [2]], "gold_new": [[1]]}, "checksums": {"dq_test_results": "xyz"},
             "snapshot": {"as_of": "2026-10-14", "live": {"weeks": []}}}
    assert all(ok for _, ok, _ in bl.compare(base, grown, additive=True))
    assert not all(ok for _, ok, _ in bl.compare(base, grown))            # the strict comparison still sees every change
    changed = json.loads(json.dumps(grown))
    changed["contents"]["gold_a"] = [[1], [3]]
    changed["snapshot"]["as_of"] = "2026-10-15"
    assert [n for n, ok, _ in bl.compare(base, changed, additive=True) if not ok] == ["gold_a", "snapshot (without generated_at)"]
    dropped = json.loads(json.dumps(grown))
    dropped["columns"]["gold_a"] = [["y", "VARCHAR"]]
    assert [n for n, ok, _ in bl.compare(base, dropped, additive=True) if not ok] == ["gold_a"]


def test_baseline_additive_allows_new_fields_inside_the_snapshot_only():
    snap = {"as_of": "2026-10-14", "cohorts": [{"cohort": "sepsis", "patients": 18}, {"cohort": "hf", "patients": 9}]}
    base = {"counts": {}, "columns": {}, "contents": {}, "checksums": {}, "snapshot": snap}
    now = lambda s: dict(base, snapshot=s)  # noqa: E731
    bad = lambda s: [n for n, ok, _ in bl.compare(base, now(s), additive=True) if not ok]  # noqa: E731
    grown = {"as_of": "2026-10-14", "people": {"people": 26, "records": 27},
             "cohorts": [{"cohort": "sepsis", "patients": 18, "people": 17}, {"cohort": "hf", "patients": 9, "people": 9}]}
    assert bad(grown) == []
    changed = json.loads(json.dumps(grown))
    changed["cohorts"][0]["patients"] = 19
    assert bad(changed) == ["snapshot (without generated_at)"]
    assert bad(dict(grown, cohorts=grown["cohorts"][::-1])) == ["snapshot (without generated_at)"]   # reordered
    assert bad(dict(grown, cohorts=grown["cohorts"][:1])) == ["snapshot (without generated_at)"]     # a row less
    assert bad({k: v for k, v in grown.items() if k != "as_of"}) == ["snapshot (without generated_at)"]
    assert bl.shaped_like({"a": 1, "b": [{"c": 1, "d": 2}]}, {"a": 0, "b": [{"c": 0}]}) == {"a": 1, "b": [{"c": 1}]}


def test_baseline_fingerprint_round_trip(warehouse, tmp_path):
    fp = bl.fingerprint(warehouse, bl.relations(warehouse))
    assert "gold_cohorts" in fp["contents"] and "silver_patient" in fp["checksums"] and "generated_at" not in fp["snapshot"]
    bl.save(fp, tmp_path / "b")
    assert all(ok for _, ok, _ in bl.compare(bl.load(tmp_path / "b"), fp))


BASELINE = ROOT / "data" / "analytics" / "baseline"
LANDING = ROOT / "data" / "analytics" / "landing"


@pytest.mark.skipif(not (BASELINE / "baseline.json").exists() or not list(LANDING.glob("*.ndjson")),
                    reason="no baseline in data/analytics/baseline (python3 analytics/baseline.py save)")
def test_fresh_build_matches_the_baseline(tmp_path):
    """The real landing files, built fresh with dbt: every count, Gold table, analyst view and the snapshot
    must equal the saved baseline."""
    con = build_at(tmp_path, LANDING)
    base = bl.load(BASELINE)
    # additive: new tables, columns and dbt tests are allowed; everything the baseline holds must be unchanged
    lines = bl.compare(base, bl.fingerprint(con, bl.relations(con), like=base), additive=True)
    assert [(n, d) for n, ok, d in lines if not ok] == []


# ---------- who and where: locations, staff, beds and transfers (the live Riverside General feed) ----------

def live_feed_resources(days=6, summaries=None, start=None):
    """The reference data plus a few days of the live feed, mapped the way the daily pipeline does it: each day's
    mapper is seeded with the Encounters and CareTeams of the visits still open, and the beds carry tonight's
    status. summaries, when given, gets each day's generate.day_summary() by ISO day, as the run history records it
    (a resend of yesterday's message replayed after midnight is not a new admission)."""
    from datetime import date, timedelta

    import feed_state
    import generate
    import reference
    import to_fhir
    from route import Router

    state, server, seen = feed_state.FeedState(), {}, set()
    for k in range(days):
        open_visits = {to_fhir.fhir_id("rv", a["visit"]) for a in state.open_admissions}
        day = (start or date(2026, 9, 1)) + timedelta(days=k)
        msgs = generate.build_day(day, state)
        if summaries is not None:
            summaries[day.isoformat()] = generate.day_summary(msgs, day)
        r, m = Router(seen=seen), to_fhir.FeedMapper(translate=lambda c: [])
        m.seed([x for (t, i), x in server.items() if t in ("Encounter", "CareTeam") and
                i in open_visits | {to_fhir.fhir_id(v, "careteam") for v in open_visits}])
        for _, _, text in msgs:
            o = r.route(text)
            if o.status == "process":
                m.apply(o.message)
        seen = r.seen
        server.update({(x["resourceType"], x["id"]): x for x in m.resources()})
    beds = reference.resources(generate.occupied_beds(state))
    return [x for x in [*beds, *server.values()] if x["resourceType"] in EXTRACTED]


@pytest.fixture(scope="module")
def live_warehouse(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("live")
    landing = tmp_path / "landing"
    write_landing(landing, live_feed_resources())
    con = build_at(tmp_path, landing)
    yield con
    con.close()


def test_silver_location_tree(live_warehouse):
    w = live_warehouse
    assert dict(w.execute("SELECT physical_type, COUNT(*) FROM silver_location GROUP BY 1").fetchall()) == {
        "si": 1, "wa": 4, "wi": 3, "ro": 42, "bd": 62}
    bed = one(w, "SELECT name, part_of_location_id, unit_location_id FROM silver_location WHERE location_id = 'riverside-loc-med-214-a'")
    assert bed == ("Room 214, bed A", "riverside-loc-med-214", "riverside-loc-med")
    assert one(w, "SELECT unit_location_id FROM silver_location WHERE location_id = 'riverside-loc-icu-401'")[0] == "riverside-loc-icu"
    assert one(w, "SELECT unit_location_id FROM silver_location WHERE location_id = 'riverside-loc-hospital'")[0] is None


def test_silver_practitioner(live_warehouse):
    rows = dict(live_warehouse.execute("SELECT practitioner_id, display_name FROM silver_practitioner").fetchall())
    assert len(rows) == 11 and rows["riverside-pract-1234"] == "Dr Dana Morgan" and rows["riverside-pract-8001"] == "Jordan Lee"


def test_encounters_have_attending_and_location(live_warehouse):
    w = live_warehouse
    n, with_attending, with_place = one(w, """SELECT COUNT(*), COUNT(attending_practitioner_id), COUNT(last_location_id)
        FROM silver_encounter""")
    assert n > 20 and with_attending == n and with_place == n  # every live feed visit has both
    moved = one(w, "SELECT COUNT(*) FROM silver_encounter WHERE bed_moves >= 1")[0]
    assert moved > 0
    g = one(w, """SELECT attending_name, last_unit_name, last_location_name FROM gold_encounter_summary
        WHERE last_location_id LIKE 'riverside-loc-med-%' LIMIT 1""")
    assert g[0].startswith("Dr ") and g[1] == "Medical Unit" and g[2].startswith("Room 2")


def test_encounter_location_path(live_warehouse):
    w = live_warehouse
    # an ED to unit admission: the ED bed is completed when the unit bed starts
    path = w.execute("""SELECT e.encounter_id, e.location_id, e.status, e.start_at, e.end_at
        FROM silver_encounter_location e
        WHERE e.physical_type = 'bd' AND e.encounter_id IN (
            SELECT encounter_id FROM silver_encounter_location WHERE location_id LIKE 'riverside-loc-ed-%'
            INTERSECT SELECT encounter_id FROM silver_encounter_location WHERE location_id LIKE 'riverside-loc-med-%')
        ORDER BY e.encounter_id, e.entry_index""").fetchall()
    assert path
    first, second = path[0], path[1]
    assert first[0] == second[0] and first[1].startswith("riverside-loc-ed-") and first[2] == "completed"
    assert first[4] == second[3]
    statuses = dict(w.execute("SELECT status, COUNT(*) FROM dq_test_results GROUP BY 1").fetchall())
    assert set(statuses) == {"pass"}  # including: no bed holds two stays, a transfer closes the previous bed


def test_bed_overlap_test_catches_a_double_booking(tmp_path):
    bed = {"reference": "Location/b1"}
    kind = {"coding": [{"code": "bd"}]}
    landing = tmp_path / "landing"
    write_landing(landing, [
        {"resourceType": "Patient", "id": "p1", "meta": {"tag": [{"code": "hl7v2"}]}},
        {"resourceType": "Location", "id": "b1", "physicalType": kind},
        *[{"resourceType": "Encounter", "id": f"e{i}", "subject": {"reference": "Patient/p1"},
           "location": [{"location": bed, "physicalType": kind, "status": "active", "period": {"start": start}}]}
          for i, start in ((1, "2026-09-01T08:00:00"), (2, "2026-09-02T08:00:00"))],
    ])
    bad = wh.build(tmp_path / "w.duckdb", landing, target_path=tmp_path / "target", quiet=True)
    assert any(b.startswith("beds_never_hold_two_stays") for b in bad)


class StaleSearchFhir:
    """A HAPI stand in with its search cache: a search without Cache-Control: no-cache gets the result from before
    the last write (the first 4 patients of 7), on every page and in _summary=count. Reads by id are never stale."""

    FRESH, STALE = 7, 4

    def __init__(self):
        self.searches = []   # (path, sent no-cache)
        fake = self

        class H(BaseHTTPRequestHandler):
            def reply(self, body):
                d = json.dumps(body).encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(d)))
                self.end_headers()
                self.wfile.write(d)

            def do_GET(self):
                u = urllib.parse.urlparse(self.path)
                if "/Binary/" in u.path:
                    self.send_response(404)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                fresh = self.headers.get("Cache-Control") == "no-cache"
                fake.searches.append((self.path, fresh))
                q = {k: v[0] for k, v in urllib.parse.parse_qs(u.query).items()}
                rtype = q.get("t") or u.path.rstrip("/").split("/")[-1]
                n = (fake.FRESH if fresh else fake.STALE) if rtype == "Patient" else 0
                rows = [{"resourceType": "Patient", "id": f"p{i}", "name": [{"family": "Doe", "given": ["Jo"]}],
                         "birthDate": "1970-01-01"} for i in range(n)]
                if q.get("_summary") == "count":
                    return self.reply({"resourceType": "Bundle", "total": len(rows)})
                off = int(q.get("off", 0))
                body = {"resourceType": "Bundle", "entry": [{"resource": r} for r in rows[off:off + 3]], "link": []}
                if off + 3 < len(rows):
                    body["link"].append({"relation": "next", "url": f"http://x/fhir?t={rtype}&off={off + 3}"})
                self.reply(body)

            def log_message(self, *a):
                pass

        self.srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()
        self.base = f"http://127.0.0.1:{self.srv.server_port}/fhir"


def test_searches_right_after_a_write_are_fresh(tmp_path, capsys):
    """Right after empi-seed and match.py run, extract must see all 7 patients, not HAPI's cached 4."""
    sys.path.insert(0, str(ROOT / "empi"))
    sys.path.insert(0, str(ROOT))
    import match
    import tools
    from extract import extract_all

    fake = StaleSearchFhir()
    try:
        manifest = extract_all(fake.base, tmp_path / "landing", quiet=True)
        assert manifest["counts"]["Patient"] == {"extracted": 7, "server_total": 7}
        assert (tmp_path / "landing" / "Patient.ndjson").read_text().count("\n") == 7
        assert sum(1 for p, _ in fake.searches if "off=" in p) >= 2      # paging was followed ...
        assert fake.searches and all(fresh for _, fresh in fake.searches)  # ... and every request asked for a fresh result
        assert len(match.fetch_patients(fake.base)) == 7
        assert tools.main(["--base", fake.base, "verify", "--min-patients", "7", "--min-observations", "0"]) == 0
        assert all(fresh for _, fresh in fake.searches)
    finally:
        fake.srv.shutdown()
