"""The EMPI in the warehouse: Patient.link and the two EMPI Binaries in Silver, gold_patient_crosswalk (one master id
per person) and gold_empi_quality, which must agree with empi/match.py's own quality numbers."""
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "tests"), str(ROOT / "analytics"), str(ROOT / "hl7v2"), str(ROOT / "empi")]
duckdb = pytest.importorskip("duckdb")
pytest.importorskip("dbt.cli.main")
import extract  # noqa: E402
import match as m  # noqa: E402
import pipeline  # noqa: E402
from test_analytics import build_at, mimic_and_synthea, one, write_landing  # noqa: E402
from test_empi import CFG, NICK, FakeFhir, seeded  # noqa: E402


@pytest.fixture(scope="module")
def empi_warehouse(tmp_path_factory):
    """The seeded test set matched and linked the way empi/match.py run does, landed as extract would land it."""
    tmp = tmp_path_factory.mktemp("empi")
    population, messages, truth = seeded()
    patients = population + pipeline.empi_patients(messages)
    pairs = m.match_all([m.record(p) for p in patients], CFG, NICK)
    by_id = {p["id"]: p for p in patients}
    for pair in m.to_link(pairs):
        a, b = pair["a"]["id"], pair["b"]["id"]
        by_id[a], by_id[b] = m.with_link(by_id[a], b), m.with_link(by_id[b], a)
    doc = m.review_document(pairs, CFG, {"pairs_linked": 0, "patients_updated": 0}, truth["pairs"], "2026-10-18T06:00:00+00:00")
    binaries = [m.json_binary(m.REVIEW_ID, doc), m.json_binary(m.TRUTH_ID, truth)]
    resources = [r for r in mimic_and_synthea() if r["resourceType"] != "Patient" or r["id"] in ("m1",)]
    resources = [r for r in resources if r.get("subject", {}).get("reference", "Patient/m1") == "Patient/m1"]
    write_landing(tmp / "landing", resources + list(by_id.values()) + binaries)
    con = build_at(tmp, tmp / "landing")
    yield con, pairs, truth, doc
    con.close()


def test_links_land_in_silver(empi_warehouse):
    con, pairs, _, _ = empi_warehouse
    linked = len(m.to_link(pairs))
    assert one(con, "SELECT COUNT(*) FROM silver_patient_link")[0] == 2 * linked
    assert one(con, "SELECT COUNT(DISTINCT link_type) FROM silver_patient_link")[0] == 1


def test_review_queue_and_truth_land_in_silver(empi_warehouse):
    con, _, truth, doc = empi_warehouse
    assert one(con, "SELECT COUNT(*) FROM silver_empi_pair")[0] == len(doc["pairs"])
    statuses = dict(con.execute("SELECT status, COUNT(*) FROM silver_empi_pair GROUP BY 1").fetchall())
    assert statuses == {k: v for k, v in doc["counts"].items() if k in ("auto_link", "review", "accepted", "rejected") and v}
    top = one(con, "SELECT pair_id, score FROM silver_empi_pair ORDER BY score DESC, pair_id LIMIT 1")
    assert top == (doc["pairs"][0]["pair_id"], doc["pairs"][0]["score"])
    assert one(con, "SELECT COUNT(*), COUNT(DISTINCT kind) FROM silver_empi_truth") == (30, 2)


def test_crosswalk_gives_one_master_id_per_person(empi_warehouse):
    con, pairs, truth, _ = empi_warehouse
    records, people = one(con, "SELECT COUNT(*), COUNT(DISTINCT master_id) FROM gold_patient_crosswalk")
    assert records == one(con, "SELECT COUNT(*) FROM silver_patient")[0]
    assert people == records - len(m.to_link(pairs))   # each link joins two records into one person
    for t in truth["pairs"]:
        a, b = (one(con, f"SELECT master_id FROM gold_patient_crosswalk WHERE patient_id = '{x}'")[0]
                for x in (t["seed_id"], t["existing_id"]))
        auto = any({p["a"]["id"], p["b"]["id"]} == {t["seed_id"], t["existing_id"]} for p in m.to_link(pairs))
        assert (a == b) == auto
    two = one(con, "SELECT MIN(records_in_group), MAX(records_in_group), MAX(sources_in_group) FROM gold_patient_crosswalk "
                   "WHERE records_in_group > 1")
    assert two == (2, 2, 2)   # a Synthea record and a Riverside General record
    assert one(con, "SELECT master_id FROM gold_patient_crosswalk WHERE patient_id = 'm1'")[0].startswith("MPI-")


def test_quality_in_sql_agrees_with_the_matcher(empi_warehouse):
    con, pairs, truth, _ = empi_warehouse
    q = m.quality(pairs, truth["pairs"])
    cols = [c for c in q]
    row = one(con, f"SELECT {', '.join(cols)} FROM gold_empi_quality")
    assert dict(zip(cols, row)) == q
    assert q["precision"] >= 0.95 and q["recall"] >= 0.90 and q["hard_negatives_auto_linked"] == 0


def test_quality_without_a_test_set(tmp_path):
    write_landing(tmp_path / "landing", mimic_and_synthea())
    con = build_at(tmp_path, tmp_path / "landing")
    row = one(con, "SELECT true_match_pairs, auto_linked, precision, recall FROM gold_empi_quality")
    assert row == (0, 0, None, None)
    assert one(con, "SELECT COUNT(*) FROM gold_patient_crosswalk")[0] == one(con, "SELECT COUNT(*) FROM silver_patient")[0]
    # no links: every cohort counts as many people as records, and so does the snapshot
    assert con.execute("SELECT COUNT(*) FROM gold_cohorts WHERE people <> records OR records <> patients").fetchone()[0] == 0
    assert one(con, "SELECT COUNT(*) FROM gold_cohorts")[0] > 0
    import publish as pub
    assert pub.people_count(con) == {"people": 3, "records": 3}
    con.close()


def linked_copy_of_s2():
    """s2 and a second record of the same person (another Synthea id), linked both ways, each with type 2 diabetes."""
    base = mimic_and_synthea()
    s2 = next(r for r in base if r.get("id") == "s2")
    s2 = dict(s2, link=[{"other": {"reference": "Patient/s2b"}, "type": "seealso"}])
    copy = dict(s2, id="s2b", identifier=[{"system": "https://github.com/synthetichealth/synthea", "value": "u2b"}],
                link=[{"other": {"reference": "Patient/s2"}, "type": "seealso"}])
    dm = {"resourceType": "Condition", "id": "sc2b", "subject": {"reference": "Patient/s2b"},
          "code": {"coding": [{"system": "http://snomed.info/sct", "code": "44054006", "display": "Diabetes mellitus type 2"}]}}
    return [r for r in base if r.get("id") != "s2"] + [s2, copy, dm]


def test_cohorts_count_linked_records_as_one_person(tmp_path):
    write_landing(tmp_path / "landing", linked_copy_of_s2())
    con = build_at(tmp_path, tmp_path / "landing")   # also runs the dbt test cohort_people_not_more_than_records
    dm = one(con, "SELECT records, people, patients, synthea, pct_of_patients, pct_of_people FROM gold_cohorts "
                  "WHERE cohort = 'diabetes_type2'")
    assert dm == (2, 1, 2, 2, 50.0, 33.3)              # 4 records (m1, s1, s2, s2b), 3 people
    other = con.execute("SELECT cohort FROM gold_cohorts WHERE cohort <> 'diabetes_type2' AND people <> records").fetchall()
    assert other == []                                 # cohorts without the linked person are unchanged
    import publish as pub
    snap = pub.snapshot(con)
    assert snap["people"] == {"people": 3, "records": 4}
    c = next(c for c in snap["cohorts"] if c["cohort"] == "diabetes_type2")
    assert (c["people"], c["records"], c["pct_of_people"]) == (1, 2, 33.3)
    status = one(con, "SELECT status FROM dq_test_results WHERE test_name = 'cohort_people_not_more_than_records'")
    assert status == ("pass",)
    con.close()


def test_extract_lands_only_the_empi_binaries(tmp_path):
    review = m.json_binary(m.REVIEW_ID, {"pairs": []})
    with FakeFhir([review, m.json_binary("worklist-bedboard", {"phi": True})]) as fake:
        assert extract.extract_binaries(fake.base, tmp_path / "Binary.ndjson") == ["empi-review"]
        assert (tmp_path / "Binary.ndjson").read_text().count("\n") == 1
    with FakeFhir([]) as fake:
        assert extract.extract_binaries(fake.base, tmp_path / "none.ndjson") == []
        assert not (tmp_path / "none.ndjson").exists()
