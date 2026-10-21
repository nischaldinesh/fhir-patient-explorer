#!/usr/bin/env python3
"""Option A, step 2: build and measure the terminology mappings.

1. Lab codes: applies the curated table terminology/lab_loinc_map.csv (MIMIC lab item -> LOINC),
   checks every row against the real MIMIC data, and measures coverage weighted by result count.
2. Diagnoses: maps every ICD 9 code found in MIMIC to ICD 10 using the official CMS/CDC
   General Equivalence Mappings (GEMs) and classifies each mapping:
     exact        one ICD 10 code, not flagged approximate
     approximate  one ICD 10 code, flagged approximate by CMS
     one_to_many  several alternative ICD 10 codes, a human must choose   -> needs review
     combination  must be coded as several ICD 10 codes together         -> needs review
     no_map       CMS says there is no equivalent                         -> needs review
     not_in_gem   code missing from the GEM file (often a bad source code) -> needs review

Outputs (code lists and counts only, no patient data):
  terminology/out/icd9_to_icd10.csv   one row per ICD 9 code in the data
  terminology/out/lab_review.csv      unmapped MIMIC lab codes, most results first
  terminology/out/report.md           summary numbers

Inputs to download once (see README): data/terminology/2018_I9gem.txt and, optionally,
the ICD 10 description file icd10cm_order_2018.txt for display names.
"""
import argparse
import csv
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from profile_codes import iter_ndjson  # noqa: E402

MIMIC_LAB_SYSTEM = "http://mimic.mit.edu/fhir/mimic/CodeSystem/mimic-d-labitems"
NEEDS_REVIEW = {"one_to_many", "combination", "no_map", "not_in_gem"}


# ---------- pure helpers (unit tested) ----------

def load_lab_map(path):
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    seen = set()
    for r in rows:
        if r["mimic_code"] in seen:
            raise ValueError(f"duplicate mimic_code {r['mimic_code']} in {path}")
        seen.add(r["mimic_code"])
    return {r["mimic_code"]: r for r in rows}


def load_exclude(path):
    if not Path(path).exists():
        return {}
    with open(path, newline="", encoding="utf-8") as f:
        return {r["mimic_code"]: r for r in csv.DictReader(f)}


def lab_coverage(lab_counts, lab_displays, lab_map, exclude=None):
    """lab_counts: {mimic_code: results}. Codes in `exclude` (not patient measurements) leave the denominator.
    Returns coverage numbers, display mismatches, unmapped list."""
    exclude = exclude or {}
    overlap = set(exclude) & set(lab_map)
    if overlap:
        raise ValueError(f"codes both mapped and excluded: {sorted(overlap)}")
    excluded_results = sum(n for c, n in lab_counts.items() if c in exclude)
    lab_counts = {c: n for c, n in lab_counts.items() if c not in exclude}
    total = sum(lab_counts.values())
    mapped = sum(n for code, n in lab_counts.items() if code in lab_map)
    mismatches = [
        (code, row["mimic_display"], lab_displays.get(code, "(not in data)"))
        for code, row in lab_map.items()
        if lab_displays.get(code, "").strip().lower() != row["mimic_display"].strip().lower()
    ]
    unmapped = sorted(((c, lab_displays.get(c, ""), n) for c, n in lab_counts.items() if c not in lab_map),
                      key=lambda x: -x[2])
    return {
        "codes_excluded": sum(1 for c in exclude if c in lab_displays),
        "results_excluded": excluded_results,
        "codes_total": len(lab_counts),
        "codes_mapped": sum(1 for c in lab_counts if c in lab_map),
        "results_total": total,
        "results_mapped": mapped,
        "pct_results_mapped": round(100 * mapped / total, 1) if total else 0.0,
        "mismatches": mismatches,
        "unmapped": unmapped,
    }


def parse_gem(lines):
    """CMS GEM rows: 'source target flags' where flags = approximate,no_map,combination,scenario,choice_list."""
    gem = {}
    for line in lines:
        parts = line.split()
        if len(parts) != 3 or len(parts[2]) != 5:
            continue
        src, tgt, flags = parts
        gem.setdefault(src, []).append({
            "target": tgt,
            "approximate": flags[0] == "1",
            "no_map": flags[1] == "1",
            "combination": flags[2] == "1",
            "scenario": int(flags[3]),
            "choice_list": int(flags[4]),
        })
    return gem


def classify(rows):
    """Return (category, [icd10 targets]) for one ICD 9 code's GEM rows."""
    if not rows:
        return "not_in_gem", []
    if any(r["no_map"] for r in rows):
        return "no_map", []
    targets = list(dict.fromkeys(r["target"] for r in rows))
    if any(r["combination"] for r in rows):
        return "combination", targets
    if len(targets) > 1:
        return "one_to_many", targets
    return ("approximate" if rows[0]["approximate"] else "exact"), targets


def normalize_icd(code):
    return code.replace(".", "").strip().upper()


def parse_icd10_order(lines):
    """CDC fixed width order file: code in columns 7 to 13, long description from column 78."""
    out = {}
    for line in lines:
        if len(line) > 77:
            out[line[6:13].strip()] = line[77:].strip()
    return out


# ---------- IO ----------

def read_mimic(mimic_dir):
    lab_counts, lab_displays = Counter(), {}
    icd9_counts, icd9_displays = Counter(), {}
    for path in sorted(Path(mimic_dir).glob("*.ndjson*")):
        if "Labevents" in path.name:
            for r in iter_ndjson(path):
                for c in r.get("code", {}).get("coding", [])[:1]:
                    if c.get("system") == MIMIC_LAB_SYSTEM:
                        lab_counts[c["code"]] += 1
                        lab_displays.setdefault(c["code"], c.get("display", ""))
        elif "Condition" in path.name:
            for r in iter_ndjson(path):
                for c in r.get("code", {}).get("coding", [])[:1]:
                    if "icd9" in c.get("system", ""):
                        code = normalize_icd(c["code"])
                        icd9_counts[code] += 1
                        icd9_displays.setdefault(code, c.get("display", ""))
    return lab_counts, lab_displays, icd9_counts, icd9_displays


def write_csv(path, header, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mimic", default="data/mimic")
    ap.add_argument("--lab-map", default="terminology/lab_loinc_map.csv")
    ap.add_argument("--lab-exclude", default="terminology/lab_exclude.csv")
    ap.add_argument("--gem", default="data/terminology/2018_I9gem.txt")
    ap.add_argument("--icd10-names", default="", help="icd10cm_order_*.txt; found automatically in data/terminology")
    ap.add_argument("--out", default="terminology/out")
    a = ap.parse_args(argv)

    if not Path(a.gem).exists():
        print(f"GEM file not found: {a.gem}. See the download step in README.", file=sys.stderr)
        return 1
    names_path = a.icd10_names or next(iter(sorted(Path(a.gem).parent.glob("icd10cm_order_*.txt"))), "")
    icd10_names = parse_icd10_order(Path(names_path).read_text(encoding="latin-1").splitlines()) if names_path else {}

    lab_counts, lab_displays, icd9_counts, icd9_displays = read_mimic(a.mimic)
    lab = lab_coverage(lab_counts, lab_displays, load_lab_map(a.lab_map), load_exclude(a.lab_exclude))
    gem = parse_gem(Path(a.gem).read_text(encoding="latin-1").splitlines())

    out = Path(a.out)
    rows, cat_codes, cat_rows = [], Counter(), Counter()
    for code, n in icd9_counts.most_common():
        cat, targets = classify(gem.get(code, []))
        cat_codes[cat] += 1
        cat_rows[cat] += n
        rows.append([code, icd9_displays[code], n, cat, "yes" if cat in NEEDS_REVIEW else "no",
                     " | ".join(targets), " | ".join(icd10_names.get(t, "") for t in targets)])
    write_csv(out / "icd9_to_icd10.csv",
              ["icd9", "icd9_display", "rows", "category", "needs_review", "icd10", "icd10_display"], rows)
    write_csv(out / "lab_review.csv", ["mimic_code", "mimic_display", "results"], lab["unmapped"])

    total_rows = sum(icd9_counts.values())
    auto = cat_rows["exact"] + cat_rows["approximate"]
    lines = [
        "# Terminology mapping report",
        "",
        "## Labs: MIMIC local codes to LOINC",
        f"- Excluded {lab['codes_excluded']} codes ({lab['results_excluded']:,} results) that are not patient measurements",
        f"- Curated table maps {lab['codes_mapped']} of {lab['codes_total']} remaining MIMIC lab codes",
        f"- Those cover **{lab['results_mapped']:,} of {lab['results_total']:,} lab results ({lab['pct_results_mapped']}%)**",
        f"- Display mismatches to check: {len(lab['mismatches'])}",
        "",
        "## Diagnoses: ICD 9 to ICD 10 (CMS GEMs 2018)",
        f"- {len(icd9_counts)} distinct ICD 9 codes in {total_rows:,} diagnosis rows",
        f"- Mapped automatically (exact or approximate): **{auto:,} rows ({round(100 * auto / total_rows, 1) if total_rows else 0}%)**",
        "",
        "| Category | Codes | Rows |",
        "|---|---|---|",
        *[f"| {c} | {cat_codes[c]} | {cat_rows[c]:,} |" for c in
          ["exact", "approximate", "one_to_many", "combination", "no_map", "not_in_gem"]],
    ]
    (out / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")

    print("\n".join(lines))
    if lab["mismatches"]:
        print("\nLAB TABLE ROWS TO CHECK (code, expected display, display in data):")
        for m in lab["mismatches"]:
            print(f"  {m[0]:<8} {m[1]!r:<40} {m[2]!r}")
    print("\nTop unmapped MIMIC lab codes:")
    for code, disp, n in lab["unmapped"][:15]:
        print(f"  {code:<8} {disp:<40} {n:>7,}")
    print(f"\nICD 10 names loaded: {len(icd10_names):,}" + ("" if icd10_names else " (optional file not found)"))
    print(f"Wrote {out}/icd9_to_icd10.csv, {out}/lab_review.csv, {out}/report.md")
    return 0


if __name__ == "__main__":
    sys.exit(main())
