#!/usr/bin/env python3
"""Option A, step 1: profile the codes in the raw data before mapping anything.

Reads the local MIMIC NDJSON files and Synthea bundles (no FHIR server needed) and writes
terminology/profile.json with:
  * lab codes per code system, with display, most common unit, and result count
  * condition codes per code system (ICD 9, ICD 10, SNOMED ...), with counts and examples

The output holds code catalogs and counts only, no patient level data.

Usage:  python3 terminology/profile_codes.py [--mimic data/mimic] [--synthea data/synthea/output/fhir]
"""
import argparse
import gzip
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

LAB_FILE_HINTS = ("Labevents",)
CONDITION_FILE_HINTS = ("Condition",)


def iter_ndjson(path):
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                yield json.loads(line)


def iter_synthea(folder):
    for path in sorted(Path(folder).glob("*.json")):
        bundle = json.loads(path.read_text(encoding="utf-8"))
        for e in bundle.get("entry", []):
            yield e.get("resource", {})


def is_lab(obs):
    for cat in obs.get("category", []):
        for c in cat.get("coding", []):
            if c.get("code") == "laboratory":
                return True
    return False


class Profiler:
    def __init__(self):
        # (source, system, code) -> stats
        self.labs = defaultdict(lambda: {"display": "", "count": 0, "numeric": 0, "units": Counter()})
        self.conditions = defaultdict(lambda: {"display": "", "count": 0})

    def add_lab(self, source, obs):
        for c in obs.get("code", {}).get("coding", [])[:1]:
            s = self.labs[(source, c.get("system", ""), c.get("code", ""))]
            s["display"] = s["display"] or c.get("display") or obs.get("code", {}).get("text", "")
            s["count"] += 1
            vq = obs.get("valueQuantity")
            if vq and isinstance(vq.get("value"), (int, float)):
                s["numeric"] += 1
                s["units"][vq.get("unit") or vq.get("code") or ""] += 1

    def add_condition(self, source, cond):
        for c in cond.get("code", {}).get("coding", [])[:1]:
            s = self.conditions[(source, c.get("system", ""), c.get("code", ""))]
            s["display"] = s["display"] or c.get("display") or cond.get("code", {}).get("text", "")
            s["count"] += 1

    def report(self, top=150):
        labs = defaultdict(list)
        for (src, system, code), s in self.labs.items():
            labs[f"{src} | {system}"].append({
                "code": code,
                "display": s["display"],
                "results": s["count"],
                "numeric": s["numeric"],
                "unit": s["units"].most_common(1)[0][0] if s["units"] else "",
            })
        cond_systems = defaultdict(lambda: {"distinct_codes": 0, "rows": 0, "examples": []})
        for (src, system, code), s in sorted(self.conditions.items(), key=lambda kv: -kv[1]["count"]):
            g = cond_systems[f"{src} | {system}"]
            g["distinct_codes"] += 1
            g["rows"] += s["count"]
            if len(g["examples"]) < 15:
                g["examples"].append({"code": code, "display": s["display"], "rows": s["count"]})
        return {
            "labs": {k: {"distinct_codes": len(v), "results": sum(x["results"] for x in v),
                         "top": sorted(v, key=lambda x: -x["results"])[:top]}
                     for k, v in sorted(labs.items())},
            "conditions": dict(sorted(cond_systems.items())),
        }


def run(mimic_dir, synthea_dir):
    p = Profiler()
    mimic = Path(mimic_dir)
    for path in sorted(mimic.glob("*.ndjson*")):
        name = path.name
        if any(h in name for h in LAB_FILE_HINTS):
            for r in iter_ndjson(path):
                p.add_lab("MIMIC", r)
        elif any(h in name for h in CONDITION_FILE_HINTS):
            for r in iter_ndjson(path):
                p.add_condition("MIMIC", r)
    if Path(synthea_dir).exists():
        for r in iter_synthea(synthea_dir):
            if r.get("resourceType") == "Observation" and is_lab(r):
                p.add_lab("Synthea", r)
            elif r.get("resourceType") == "Condition":
                p.add_condition("Synthea", r)
    return p.report()


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mimic", default="data/mimic")
    ap.add_argument("--synthea", default="data/synthea/output/fhir")
    ap.add_argument("--out", default="terminology/profile.json")
    a = ap.parse_args(argv)
    if not Path(a.mimic).exists():
        print(f"MIMIC folder not found: {a.mimic}. Run from the project root.", file=sys.stderr)
        return 1
    rep = run(a.mimic, a.synthea)
    Path(a.out).parent.mkdir(parents=True, exist_ok=True)
    Path(a.out).write_text(json.dumps(rep, indent=1), encoding="utf-8")

    print("LAB CODE SYSTEMS")
    for k, v in rep["labs"].items():
        print(f"  {k:<85} {v['distinct_codes']:>5} codes {v['results']:>9,} results")
    print("CONDITION CODE SYSTEMS")
    for k, v in rep["conditions"].items():
        print(f"  {k:<85} {v['distinct_codes']:>5} codes {v['rows']:>9,} rows")
    print(f"\nWrote {a.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
