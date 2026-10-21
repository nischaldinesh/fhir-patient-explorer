import gzip
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "terminology"))
import profile_codes as prof  # noqa: E402

LAB = "http://mimic.mit.edu/fhir/mimic/CodeSystem/mimic-d-labitems"
ICD9 = "http://mimic.mit.edu/fhir/mimic/CodeSystem/mimic-diagnosis-icd9"
ICD10 = "http://mimic.mit.edu/fhir/mimic/CodeSystem/mimic-diagnosis-icd10"


def ndjson(path, rows):
    with gzip.open(path, "wt", encoding="utf-8") as f:
        f.write("\n".join(json.dumps(r) for r in rows))


def lab(code, display, value, unit):
    return {"resourceType": "Observation", "code": {"coding": [{"system": LAB, "code": code, "display": display}]},
            "valueQuantity": {"value": value, "unit": unit}}


def test_profile_counts_labs_and_condition_systems(tmp_path):
    m = tmp_path / "mimic"
    m.mkdir()
    ndjson(m / "MimicObservationLabevents.ndjson.gz",
           [lab("50931", "Glucose", 100, "mg/dL"), lab("50931", "Glucose", 120, "mg/dL"), lab("50912", "Creatinine", 1.1, "mg/dL")])
    ndjson(m / "MimicCondition.ndjson.gz", [
        {"resourceType": "Condition", "code": {"coding": [{"system": ICD9, "code": "25000", "display": "Diabetes"}]}},
        {"resourceType": "Condition", "code": {"coding": [{"system": ICD10, "code": "E119", "display": "Type 2 diabetes"}]}},
        {"resourceType": "Condition", "code": {"coding": [{"system": ICD10, "code": "E119", "display": "Type 2 diabetes"}]}},
    ])
    s = tmp_path / "synthea"
    s.mkdir()
    (s / "a.json").write_text(json.dumps({"entry": [
        {"resource": {"resourceType": "Observation", "category": [{"coding": [{"code": "laboratory"}]}],
                      "code": {"coding": [{"system": "http://loinc.org", "code": "2339-0", "display": "Glucose"}]},
                      "valueQuantity": {"value": 90, "unit": "mg/dL"}}},
        {"resourceType": "Observation", "resource": {"resourceType": "Observation", "category": [{"coding": [{"code": "vital-signs"}]}],
                      "code": {"coding": [{"system": "http://loinc.org", "code": "8302-2"}]}}},
    ]}))

    rep = prof.run(m, s)
    mimic_labs = rep["labs"][f"MIMIC | {LAB}"]
    assert mimic_labs["distinct_codes"] == 2 and mimic_labs["results"] == 3
    assert mimic_labs["top"][0] == {"code": "50931", "display": "Glucose", "results": 2, "numeric": 2, "unit": "mg/dL"}
    assert rep["labs"]["Synthea | http://loinc.org"]["distinct_codes"] == 1  # vital sign excluded
    assert rep["conditions"][f"MIMIC | {ICD9}"]["rows"] == 1
    assert rep["conditions"][f"MIMIC | {ICD10}"]["distinct_codes"] == 1
    assert rep["conditions"][f"MIMIC | {ICD10}"]["rows"] == 2
