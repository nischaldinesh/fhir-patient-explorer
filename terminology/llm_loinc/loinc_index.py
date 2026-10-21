#!/usr/bin/env python3
"""A small local LOINC index and candidate retrieval for one local lab. No LLM here.

The index holds the ACTIVE laboratory terms (CLASSTYPE 1) of data/loinc/Loinc.csv, searchable over LONG_COMMON_NAME,
COMPONENT, SHORTNAME and RELATEDNAMES2 with BM25F (field weights below), plus a table of every LOINC code and its
status for the validator, and MapTo.csv for deprecated codes. It is pickled to data/loinc/index.pkl and rebuilt when
Loinc.csv or MapTo.csv change.

Retrieval for one local lab: BM25F on the label, then the score is multiplied by boosts that say whether the LOINC
term fits what else the lab system knows:

  specimen   LOINC SYSTEM preferred for the fluid and category (Blood and Chemistry: Ser/Plas; Blood Gas: Bld) x1.6,
             acceptable x1.2, another body specimen x0.5
  property   LOINC PROPERTY expected for the unit (mmol/L: SCnc, mg/dL: MCnc, IU/L: CCnc, %: NFr ...) x1.4, other x0.75
  scale      Qn for numeric results, Ord or Nom for text results x1.2, the other way round x0.8
  common     LOINC's COMMON_TEST_RANK, up to x1.3 for the most often ordered tests

  python3 terminology/llm_loinc/loinc_index.py build
  python3 terminology/llm_loinc/loinc_index.py search "Glucose" --specimen Blood --category Chemistry --unit mg/dL
"""
import argparse
import csv
import math
import pickle
import re
import sys
from array import array
from collections import Counter, defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import config as cfg  # noqa: E402

FIELDS = {"component": 3.0, "name": 2.0, "short": 2.0, "related": 1.0}
K1, B = 1.2, 0.75
STOPWORDS = {"in", "of", "by", "or", "and", "the", "a", "an", "per", "to", "for", "at", "on", "with"}
INDEX_VERSION = 2

SPECIMENS = {  # (fluid, category) -> (preferred LOINC SYSTEMs, acceptable)
    ("blood", "blood gas"): ({"Bld", "BldA", "BldV", "BldMV", "BldC"}, {"Ser/Plas", "Plas", "Ser", "Ser/Plas/Bld"}),
    ("blood", "chemistry"): ({"Ser/Plas", "Ser", "Plas", "Ser/Plas/Bld"}, {"Bld"}),
    ("blood", "hematology"): ({"Bld", "RBC", "PPP", "WBC", "Plt"}, {"Plas", "Ser/Plas", "BldA", "BldV"}),
    ("blood", ""): ({"Bld", "Ser/Plas", "Ser", "Plas", "RBC", "PPP", "Ser/Plas/Bld"}, {"BldA", "BldV"}),
    ("serum or plasma", ""): ({"Ser/Plas", "Ser", "Plas"}, {"Bld", "Ser/Plas/Bld"}),
    ("urine", ""): ({"Urine", "Urine sed"}, set()),
    ("pleural", ""): ({"Plr fld"}, {"Body fld"}),
    ("ascites", ""): ({"Periton fld"}, {"Body fld"}),
    ("joint fluid", ""): ({"Synv fld"}, {"Body fld"}),
    ("cerebrospinal fluid", ""): ({"CSF"}, {"Body fld"}),
    ("bone marrow", ""): ({"Bone mar"}, set()),
    ("stool", ""): ({"Stool"}, set()),
    ("other body fluid", ""): ({"Body fld"}, set()),
}
NEUTRAL_SYSTEMS = {"XXX", "{Specimen}"}

UNIT_PROPERTIES = {
    "SCnc": {"meq/l", "mmol/l", "umol/l", "nmol/l", "pmol/l"},
    "MCnc": {"mg/dl", "g/dl", "ng/ml", "ug/dl", "ug/ml", "pg/ml", "mg/l", "g/l", "ng/dl", "ug/l"},
    "EntMCnc": {"g/dl"},
    "CCnc": {"iu/l", "u/l"},
    "ACnc": {"iu/l", "u/l", "iu/ml", "miu/ml", "uiu/ml", "u/ml"},
    "NFr": {"%"}, "VFr": {"%"}, "MFr": {"%"}, "SFr": {"%"}, "DistWidth": {"%"}, "Ratio": {"%", "ratio"},
    "NCnc": {"k/ul", "m/ul", "#/ul", "/ul", "10*3/ul", "10*6/ul", "#/mm3", "cells/ul", "#/cu mm"},
    "Naric": {"#/hpf", "#/lpf", "/hpf", "/lpf"},
    "EntVol": {"fl"}, "EntMeanVol": {"fl"},
    "EntMass": {"pg"},
    "Time": {"sec", "s", "seconds"},
    "PPres": {"mm hg", "mmhg"},
    "LsCnc": {"units", "ph"},
    "ArVRat": {"ml/min/1.73m2", "ml/min/1.73 m2"},
    "Osmol": {"mosm/kg"},
    "RelTime": {"ratio"},
    "MRto": {"ratio"}, "SRto": {"ratio"},
}
QUANT_SCALES = {"Qn"}
TEXT_SCALES = {"Ord", "Nom", "OrdQn", "Nar"}


def tokens(text):
    return [t for t in re.findall(r"[a-z0-9]+", (text or "").lower()) if t not in STOPWORDS]


def expected_properties(unit):
    u = (unit or "").strip().lower()
    return {p for p, units in UNIT_PROPERTIES.items() if u in units} if u else set()


def specimen_systems(specimen, category=""):
    s, c = (specimen or "").strip().lower(), (category or "").strip().lower()
    return SPECIMENS.get((s, c)) or SPECIMENS.get((s, "")) or (set(), set())


def boost(term, lab):
    """The multiplier for one LOINC term (dict with system, property, scale, rank) given the local lab's metadata."""
    m = 1.0
    preferred, acceptable = specimen_systems(lab.get("specimen"), lab.get("category"))
    system = term["system"]
    if preferred:
        if system in preferred:
            m *= 1.6
        elif system in acceptable:
            m *= 1.2
        elif system not in NEUTRAL_SYSTEMS:
            m *= 0.5
    props = expected_properties(lab.get("unit"))
    if props:
        m *= 1.4 if term["property"] in props else 0.75
    vt = lab.get("value_type")
    if vt == "number":
        m *= 1.2 if term["scale"] in QUANT_SCALES else 0.8
    elif vt == "text":
        m *= 1.2 if term["scale"] in TEXT_SCALES else 0.8
    rank = term.get("rank") or 0
    if rank > 0:
        m *= 1 + 0.3 * max(0.0, 1 - math.log10(rank) / 4.3)
    return m


class Index:
    """docs: list of term dicts; postings: token -> (doc ids, BM25F weighted tf); idf: token -> idf; codes: every LOINC code."""

    def __init__(self, docs, postings, idf, codes, mapto, stamp=None):
        self.docs, self.postings, self.idf, self.codes, self.mapto, self.stamp = docs, postings, idf, codes, mapto, stamp

    def lexical(self, query):
        scores = defaultdict(float)
        for t in set(tokens(query)):
            if t not in self.postings:
                continue
            ids, tfs, idf = *self.postings[t], self.idf[t]
            for d, tf in zip(ids, tfs):
                scores[d] += idf * tf / (K1 + tf)
        return scores

    def search(self, lab, k=cfg.TOP_K, exclude=()):
        """Top k candidates for a local lab dict (label, specimen, category, unit, value_type), best first."""
        scored = []
        for d, s in self.lexical(lab.get("label", "")).items():
            term = self.docs[d]
            if term["code"] in exclude:
                continue
            scored.append((round(s * boost(term, lab), 6), term["code"], d))
        scored.sort(key=lambda x: (-x[0], x[1]))
        return [dict(self.docs[d], score=s) for s, _code, d in scored[:k]]

    def status(self, code):
        return (self.codes.get(code) or {}).get("status")

    def name(self, code):
        return (self.codes.get(code) or {}).get("name", "")

    def equivalents(self, code):
        """The code and what MapTo.csv says replaces it (one step: MapTo has no chains to follow in practice)."""
        return {code, *self.mapto.get(code, ())}


def build(rows, mapto, stamp=None):
    """rows: dicts with the Loinc.csv columns used here (all statuses; only ACTIVE CLASSTYPE 1 is searchable)."""
    docs, codes = [], {}
    field_tf, field_len = [], defaultdict(list)
    for r in rows:
        codes[r["LOINC_NUM"]] = {"status": r["STATUS"], "name": r["LONG_COMMON_NAME"], "classtype": r["CLASSTYPE"]}
        if r["STATUS"] != "ACTIVE" or r["CLASSTYPE"] != "1":
            continue
        rank = int(r.get("COMMON_TEST_RANK") or 0)
        docs.append({"code": r["LOINC_NUM"], "name": r["LONG_COMMON_NAME"], "component": r["COMPONENT"],
                     "system": r["SYSTEM"], "property": r["PROPERTY"], "scale": r["SCALE_TYP"],
                     "method": r["METHOD_TYP"], "short": r["SHORTNAME"], "units": r.get("EXAMPLE_UCUM_UNITS", ""),
                     "rank": rank})
        text = {"component": r["COMPONENT"], "name": r["LONG_COMMON_NAME"], "short": r["SHORTNAME"],
                "related": r["RELATEDNAMES2"].replace(";", " ")}
        tf = {f: Counter(tokens(v)) for f, v in text.items()}
        field_tf.append(tf)
        for f in FIELDS:
            field_len[f].append(sum(tf[f].values()))
    avg = {f: (sum(v) / len(v) if v else 1.0) or 1.0 for f, v in field_len.items()}
    postings, df = defaultdict(lambda: (array("i"), array("f"))), Counter()
    for d, tf in enumerate(field_tf):
        weighted = defaultdict(float)
        for f, w in FIELDS.items():
            norm = 1 - B + B * field_len[f][d] / avg[f]
            for t, c in tf[f].items():
                weighted[t] += w * c / norm
        for t, v in weighted.items():
            postings[t][0].append(d)
            postings[t][1].append(v)
            df[t] += 1
    n = len(docs)
    idf = {t: math.log(1 + (n - c + 0.5) / (c + 0.5)) for t, c in df.items()}
    return Index(docs, dict(postings), idf, codes, mapto, stamp)


def read_loinc(path):
    with open(path, newline="", encoding="utf-8") as f:
        yield from csv.DictReader(f)


def read_mapto(path):
    mapto = defaultdict(list)
    if Path(path).exists():
        with open(path, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                mapto[r["LOINC"]].append(r["MAP_TO"])
    return dict(mapto)


def _stamp(*paths):
    return [INDEX_VERSION] + [(p.name, p.stat().st_size, int(p.stat().st_mtime)) for p in paths if p.exists()]


def load(loinc=cfg.LOINC_CSV, mapto=cfg.MAPTO_CSV, path=cfg.INDEX_PATH, rebuild=False):
    """The pickled index, rebuilt first when missing, outdated or asked for."""
    loinc, mapto, path = Path(loinc), Path(mapto), Path(path)
    if not loinc.exists():
        raise SystemExit(f"Missing {loinc}: put the LOINC table (Loinc.csv, MapTo.csv) in data/loinc/")
    stamp = _stamp(loinc, mapto)
    if path.exists() and not rebuild:
        with open(path, "rb") as f:
            state = pickle.load(f)
        if state.get("stamp") == stamp:
            return Index(**state)
    idx = build(read_loinc(loinc), read_mapto(mapto), stamp)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".part")
    with open(tmp, "wb") as f:
        pickle.dump(vars(idx), f, protocol=pickle.HIGHEST_PROTOCOL)  # plain data: loads from any module
    tmp.replace(path)
    return idx


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("build")
    s = sub.add_parser("search")
    s.add_argument("label")
    for opt in ("specimen", "category", "unit", "value_type"):
        s.add_argument(f"--{opt.replace('_', '-')}", dest=opt, default="")
    s.add_argument("-k", type=int, default=cfg.TOP_K)
    a = ap.parse_args(argv)
    idx = load(rebuild=a.cmd == "build")
    if a.cmd == "build":
        print(f"{len(idx.docs)} searchable LOINC terms, {len(idx.codes)} codes, {len(idx.mapto)} MapTo entries "
              f"-> {cfg.INDEX_PATH.relative_to(cfg.ROOT)}")
        return
    lab = {k: getattr(a, k) for k in ("label", "specimen", "category", "unit", "value_type")}
    for i, c in enumerate(idx.search(lab, k=a.k), 1):
        print(f"{i:2}  {c['score']:7.3f}  {c['code']:>8}  {c['system']:<10} {c['property']:<8} {c['scale']:<5} {c['name']}")


if __name__ == "__main__":
    main()
