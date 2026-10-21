import gzip
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "terminology"))
import build_maps as bm  # noqa: E402

GEM = """\
25000     E119      00000
4019      I10       00000
2765      E870      10000
2765      E871      10000
V5861     Z7901     10000
E8889     W19XXXA   10000
99999     NoDx      11000
29620     F329      10000
0389      A419      00000
"""


def test_parse_gem_and_classify():
    g = bm.parse_gem(GEM.splitlines())
    assert bm.classify(g["25000"]) == ("exact", ["E119"])
    assert bm.classify(g["V5861"]) == ("approximate", ["Z7901"])
    assert bm.classify(g["2765"]) == ("one_to_many", ["E870", "E871"])
    assert bm.classify(g["99999"]) == ("no_map", [])
    assert bm.classify(g.get("12345", [])) == ("not_in_gem", [])


def test_combination_flag_wins():
    rows = bm.parse_gem(["99662     T8579XA   10111", "99662     A419      10112"])["99662"]
    assert bm.classify(rows) == ("combination", ["T8579XA", "A419"])


def test_normalize_and_icd10_names():
    assert bm.normalize_icd("250.00") == "25000"
    line = "00001 E119    1 Type 2 diabetes mellitus without complications".ljust(77) + "Type 2 diabetes mellitus without complications"
    assert bm.parse_icd10_order([line]) == {"E119": "Type 2 diabetes mellitus without complications"}


def test_lab_coverage_weights_by_results_and_flags_mismatches():
    lab_map = {"50931": {"mimic_display": "Glucose"}, "50912": {"mimic_display": "Creatinine"}, "99999": {"mimic_display": "Ghost"}}
    cov = bm.lab_coverage({"50931": 70, "50912": 20, "51000": 10}, {"50931": "Glucose", "50912": "CREATININE", "51000": "Other"}, lab_map)
    assert cov["pct_results_mapped"] == 90.0
    assert cov["codes_mapped"] == 2
    assert cov["unmapped"] == [("51000", "Other", 10)]
    assert cov["mismatches"] == [("99999", "Ghost", "(not in data)")]  # case differences are fine


def test_curated_lab_table_is_valid():
    m = bm.load_lab_map(ROOT / "terminology" / "lab_loinc_map.csv")
    assert len(m) >= 40
    for code, row in m.items():
        assert code.isdigit()
        assert row["loinc"].count("-") == 1 and row["loinc"].replace("-", "").isdigit()
        assert row["equivalence"] in {"equivalent", "wider", "narrower", "relatedto"}
        assert row["loinc_display"]


def test_main_end_to_end(tmp_path):
    mimic = tmp_path / "mimic"
    mimic.mkdir()
    lab_sys = bm.MIMIC_LAB_SYSTEM
    with gzip.open(mimic / "MimicObservationLabevents.ndjson.gz", "wt") as f:
        for code, disp in [("50931", "Glucose"), ("50931", "Glucose"), ("51000", "Other")]:
            f.write(json.dumps({"code": {"coding": [{"system": lab_sys, "code": code, "display": disp}]}}) + "\n")
    with gzip.open(mimic / "MimicCondition.ndjson.gz", "wt") as f:
        for code in ["25000", "25000", "2765", "12345"]:
            f.write(json.dumps({"code": {"coding": [{"system": "x/mimic-diagnosis-icd9", "code": code, "display": "d"}]}}) + "\n")
        f.write(json.dumps({"code": {"coding": [{"system": "x/mimic-diagnosis-icd10", "code": "E119"}]}}) + "\n")
    (tmp_path / "gem.txt").write_text(GEM)
    labmap = tmp_path / "map.csv"
    labmap.write_text("mimic_code,mimic_display,loinc,loinc_display,unit,equivalence,note\n50931,Glucose,2345-7,Glucose,mg/dL,equivalent,\n")
    out = tmp_path / "out"

    rc = bm.main(["--mimic", str(mimic), "--gem", str(tmp_path / "gem.txt"), "--lab-map", str(labmap), "--out", str(out)])
    assert rc == 0
    rows = (out / "icd9_to_icd10.csv").read_text().splitlines()
    assert rows[1].startswith("25000,d,2,exact,no,E119")
    assert len(rows) == 4  # header + 3 distinct ICD 9 codes, ICD 10 row ignored
    report = (out / "report.md").read_text()
    assert "66.7%" in report  # 2 of 3 lab results mapped
    assert "| exact | 1 | 2 |" in report


def test_excluded_codes_leave_the_denominator():
    lab_map = {"50931": {"mimic_display": "Glucose"}}
    cov = bm.lab_coverage({"50931": 80, "50934": 10, "51000": 10}, {"50931": "Glucose", "50934": "H", "51000": "X"},
                          lab_map, {"50934": {"reason": "hemolysis index"}})
    assert cov["results_total"] == 90
    assert cov["results_excluded"] == 10
    assert cov["pct_results_mapped"] == 88.9
    assert [u[0] for u in cov["unmapped"]] == ["51000"]


def test_mapped_and_excluded_overlap_is_an_error():
    import pytest
    with pytest.raises(ValueError):
        bm.lab_coverage({"1": 1}, {"1": "x"}, {"1": {"mimic_display": "x"}}, {"1": {}})


def test_curated_exclude_list_is_valid():
    ex = bm.load_exclude(ROOT / "terminology" / "lab_exclude.csv")
    assert set(ex) == {"50934", "50947", "51678", "52033"}
    assert not set(ex) & set(bm.load_lab_map(ROOT / "terminology" / "lab_loinc_map.csv"))
