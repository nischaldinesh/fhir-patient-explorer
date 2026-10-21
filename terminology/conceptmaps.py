#!/usr/bin/env python3
"""Option A, step 3: publish the mappings as FHIR ConceptMaps and use HAPI's $translate.

Commands:
  build      write terminology/out/ConceptMap-*.json from the two mapping tables
  load       build, then PUT both ConceptMaps to the FHIR server (idempotent)
  translate  ask the server to translate one code, e.g.
             python3 terminology/conceptmaps.py translate icd9 25000
             python3 terminology/conceptmaps.py translate lab 50931
  verify     run a fixed set of translations and check the answers

Two ConceptMaps:
  mimic-labs-to-loinc   MIMIC lab item codes -> LOINC        (from lab_loinc_map.csv)
  mimic-icd9-to-icd10   MIMIC ICD 9 diagnosis codes -> ICD 10 CM (from out/icd9_to_icd10.csv, CMS GEMs)

FHIR R4 equivalence used for ICD 9 -> ICD 10:
  exact -> equivalent, approximate -> inexact, one_to_many / combination -> inexact plus a
  "needs review" comment on every option. no_map codes are left out of the ConceptMap, so
  $translate honestly answers "no match"; they stay listed in out/icd9_to_icd10.csv for review.
"""
import argparse
import csv
import json
import sys
import urllib.parse
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from tools import DEFAULT_BASE, http  # noqa: E402

REPO = "https://github.com/nischaldinesh/fhir-patient-explorer"
LOINC = "http://loinc.org"
ICD10CM = "http://hl7.org/fhir/sid/icd-10-cm"
MIMIC_LAB = "http://mimic.mit.edu/fhir/mimic/CodeSystem/mimic-d-labitems"
MIMIC_ICD9 = "http://mimic.mit.edu/fhir/mimic/CodeSystem/mimic-diagnosis-icd9"

MAPS = {
    "lab": {"id": "mimic-labs-to-loinc", "source": MIMIC_LAB, "target": LOINC},
    "icd9": {"id": "mimic-icd9-to-icd10", "source": MIMIC_ICD9, "target": ICD10CM},
}

ICD_EQUIVALENCE = {
    "exact": "equivalent",
    "approximate": "inexact",
    "one_to_many": "inexact",
    "combination": "inexact",
}
REVIEW_COMMENT = {
    "approximate": "CMS GEM flags this as an approximate match",
    "one_to_many": "Needs review: CMS GEM lists several alternatives, pick the one the record supports",
    "combination": "Needs review: CMS GEM says to code these together as a combination",
}

# Answers we expect from the server in `verify` (taken from the curated tables and CMS GEMs).
EXPECTED = [
    ("lab", "50931", "2345-7"),   # serum glucose
    ("lab", "50809", "2339-0"),   # blood gas glucose (whole blood)
    ("lab", "50882", "2028-9"),   # 'bicarbonate' as total CO2
    ("icd9", "4019", "I10"),      # essential hypertension
    ("icd9", "25000", "E11.9"),   # type 2 diabetes without complications
]


# ---------- pure helpers (unit tested) ----------

def dot_icd10(code):
    """ICD 10 CM is written with a dot after the third character: E119 -> E11.9."""
    code = code.strip().upper()
    return code if len(code) <= 3 or "." in code else f"{code[:3]}.{code[3:]}"


def concept_map(key, elements, title, description):
    m = MAPS[key]
    return {
        "resourceType": "ConceptMap",
        "id": m["id"],
        "url": f"{REPO}/ConceptMap/{m['id']}",
        "version": "1",
        "name": m["id"].replace("-", "_"),
        "title": title,
        "status": "draft",
        "experimental": True,
        "description": description,
        "sourceUri": m["source"],
        "targetUri": m["target"],
        "group": [{"source": m["source"], "target": m["target"], "element": elements}],
    }


def lab_elements(rows):
    out = []
    for r in rows:
        target = {"code": r["loinc"], "display": r["loinc_display"], "equivalence": r["equivalence"] or "equivalent"}
        if r.get("note"):
            target["comment"] = r["note"]
        out.append({"code": r["mimic_code"], "display": r["mimic_display"], "target": [target]})
    return out


def icd9_elements(rows):
    out = []
    for r in rows:
        cat = r["category"]
        if cat not in ICD_EQUIVALENCE:  # no_map, not_in_gem: nothing to translate to
            continue
        el = {"code": r["icd9"], "display": r["icd9_display"]}
        codes = [c for c in r["icd10"].split(" | ") if c]
        names = r["icd10_display"].split(" | ") if r["icd10_display"] else [""] * len(codes)
        el["target"] = []
        for code, name in zip(codes, names + [""] * (len(codes) - len(names))):
            t = {"code": dot_icd10(code), "equivalence": ICD_EQUIVALENCE[cat]}
            if name:
                t["display"] = name
            if cat in REVIEW_COMMENT:
                t["comment"] = REVIEW_COMMENT[cat]
            el["target"].append(t)
        out.append(el)
    return out


def read_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def build(lab_csv, icd_csv):
    lab = concept_map(
        "lab", lab_elements(read_csv(lab_csv)),
        "MIMIC IV lab items to LOINC",
        "Curated map from MIMIC IV local lab item codes to LOINC. Judgment calls are recorded in each target comment.",
    )
    icd = concept_map(
        "icd9", icd9_elements(read_csv(icd_csv)),
        "MIMIC IV ICD 9 CM diagnoses to ICD 10 CM",
        "Every ICD 9 CM diagnosis code found in the MIMIC IV demo, mapped to ICD 10 CM with the 2018 CMS General "
        "Equivalence Mappings. Inexact targets with a 'Needs review' comment require a human decision.",
    )
    return lab, icd


def translate_path(key, code):
    m = MAPS[key]
    q = urllib.parse.urlencode({
        "url": f"{REPO}/ConceptMap/{m['id']}",
        "system": m["source"],
        "code": code,
        "targetsystem": m["target"],
    })
    return f"ConceptMap/$translate?{q}"


def parse_translate(params):
    """FHIR Parameters from $translate -> {'result': bool, 'message': str, 'matches': [...]}"""
    out = {"result": False, "message": "", "matches": []}
    for p in params.get("parameter", []):
        if p.get("name") == "result":
            out["result"] = bool(p.get("valueBoolean"))
        elif p.get("name") == "message":
            out["message"] = p.get("valueString", "")
        elif p.get("name") == "match":
            m = {}
            for part in p.get("part", []):
                if part.get("name") == "equivalence":
                    m["equivalence"] = part.get("valueCode", "")
                elif part.get("name") == "concept":
                    c = part.get("valueCoding", {})
                    m["code"], m["display"] = c.get("code", ""), c.get("display", "")
            out["matches"].append(m)
    return out


# ---------- commands ----------

def cmd_build(a):
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    cms = build(a.lab_map, a.icd_csv)
    for cm in cms:
        (out / f"ConceptMap-{cm['id']}.json").write_text(json.dumps(cm, indent=1), encoding="utf-8")
        print(f"built  ConceptMap/{cm['id']:<22} {len(cm['group'][0]['element']):>4} source codes")
    return cms


def cmd_load(a):
    for cm in cmd_build(a):
        http("PUT", f"{a.base}/ConceptMap/{cm['id']}", cm)
        print(f"loaded ConceptMap/{cm['id']} -> {a.base}")
    return 0


def show(key, code, res):
    head = f"{key} {code}: "
    if not res["matches"]:
        return head + (res["message"] or "no match")
    parts = [f"{m.get('code') or '(none)'} [{m.get('equivalence')}] {m.get('display', '')}".strip() for m in res["matches"]]
    return head + "  |  ".join(parts)


def cmd_translate(a):
    res = parse_translate(http("GET", f"{a.base}/{translate_path(a.map, a.code)}"))
    print(show(a.map, a.code, res))
    return 0 if res["result"] else 1


def cmd_verify(a):
    ok = True
    for key, code, want in EXPECTED:
        res = parse_translate(http("GET", f"{a.base}/{translate_path(key, code)}"))
        got = [m.get("code") for m in res["matches"]]
        passed = want in got
        ok &= passed
        print(f"[{'PASS' if passed else 'FAIL'}] {show(key, code, res)}")
    return 0 if ok else 1


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--base", default=DEFAULT_BASE)
    ap.add_argument("--lab-map", default=str(HERE / "lab_loinc_map.csv"))
    ap.add_argument("--icd-csv", default=str(HERE / "out" / "icd9_to_icd10.csv"))
    ap.add_argument("--out", default=str(HERE / "out"))
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("build").set_defaults(func=lambda a: (cmd_build(a), 0)[1])
    sub.add_parser("load").set_defaults(func=cmd_load)
    t = sub.add_parser("translate")
    t.add_argument("map", choices=sorted(MAPS))
    t.add_argument("code")
    t.set_defaults(func=cmd_translate)
    sub.add_parser("verify").set_defaults(func=cmd_verify)
    a = ap.parse_args(argv)
    a.base = a.base.rstrip("/")
    return a.func(a)


if __name__ == "__main__":
    sys.exit(main())
