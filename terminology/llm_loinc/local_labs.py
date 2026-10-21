#!/usr/bin/env python3
"""The local side of every lab: what a lab system knows about its own tests, never patient data.

One row per local lab, written to terminology/llm_loinc/local_labs.csv (committed, so the eval runs without MIMIC):

  local_id     mimic:<itemid>, hl7v2:code:<code> or hl7v2:text:<normalized name>
  label        the test name as the lab system sends it
  specimen     fluid (MIMIC d_labitems.fluid; for the feed, what Riverside's lab system runs the test on)
  category     MIMIC d_labitems.category (Chemistry, Hematology, Blood Gas); for the feed, the order panel
  unit         the most common unit
  value_type   number or text, whichever most results are
  normal_low, normal_high   the most common reference range
  value_p5, value_p95       the 5th and 95th percentile of numeric results
  answers      the most common short coded text answers, separated by " | "

Small cells: a code with 10 or fewer results gets no value range and no answers, and an answer is listed only when
it occurs at least 11 times. Answers must be short codes: under 30 characters, at most 3 words, no digits mixed with
letters, no punctuation that marks a comment. No result counts, dates or identifiers are written.

  python3 terminology/llm_loinc/local_labs.py            # needs data/mimic and data/mimic_raw/d_labitems.csv.gz
"""
import collections
import csv
import gzip
import json
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import config as cfg  # noqa: E402

COLUMNS = ["local_id", "source", "local_code", "label", "specimen", "category", "unit", "value_type",
           "normal_low", "normal_high", "value_p5", "value_p95", "answers"]
MAX_ANSWERS = 8

# What the Riverside General lab system knows about its own tests (demo): the specimen it runs each on and the
# panel it is ordered in. The keys are hl7v2/generate.py LABS keys.
FEED_SPECIMEN = {
    "glucose": "Serum or plasma", "sodium": "Serum or plasma", "potassium": "Serum or plasma",
    "creatinine": "Serum or plasma", "bun": "Serum or plasma", "hemoglobin": "Blood", "wbc": "Blood",
    "lactate": "Serum or plasma", "troponin": "Serum or plasma",
}
# Rows of hl7v2/lab_synonyms.csv that are not laboratory tests (vital signs, LOINC CLASSTYPE 2): not in the gold set.
FEED_NOT_LABS = {"8302-2", "29463-7"}


def clean_answer(text):
    """A short coded answer such as NEG, TR, 1+, CLEAR, FEW; None for anything that reads like free text.

    MIMIC ends coded answers with a period and marks abnormal ones with an asterisk ('LG*.'): both are dropped and
    the case is folded, so 'Yellow.' and 'YELLOW' are one answer.
    """
    t = re.sub(r"\s+", " ", (text or "").replace("*", "")).strip().rstrip(". ").upper()
    if not t or len(t) >= 30 or len(t.split(" ")) > 3:
        return None
    if re.search(r"\d", t) and re.search(r"[A-Z]", t):
        return None
    if re.search(r"[:;/\\=_@#'\"]|\.\s|\.\.", t):
        return None
    return t


def percentile(sorted_values, p):
    """Nearest rank percentile of a sorted list."""
    if not sorted_values:
        return None
    k = max(0, min(len(sorted_values) - 1, round(p / 100 * (len(sorted_values) - 1))))
    return sorted_values[k]


def fmt(x):
    if x is None:
        return ""
    return f"{x:g}" if isinstance(x, (int, float)) else str(x)


def summarize(results, small_cell=cfg.SMALL_CELL):
    """results: list of dicts {value: float or None, text: str or None, unit, low, high}. Returns the summary columns."""
    n = len(results)
    nums = sorted(r["value"] for r in results if r.get("value") is not None)
    units = collections.Counter(r["unit"] for r in results if r.get("unit"))
    ranges = collections.Counter((r["low"], r["high"]) for r in results if r.get("low") is not None or r.get("high") is not None)
    value_type = "number" if len(nums) * 2 >= n and nums else "text"
    out = {"unit": units.most_common(1)[0][0] if units else "", "value_type": value_type,
           "normal_low": "", "normal_high": "", "value_p5": "", "value_p95": "", "answers": ""}
    if ranges:
        low, high = ranges.most_common(1)[0][0]
        out["normal_low"], out["normal_high"] = fmt(low), fmt(high)
    if n <= small_cell:
        return out
    if value_type == "number":
        out["value_p5"], out["value_p95"] = fmt(percentile(nums, 5)), fmt(percentile(nums, 95))
    else:
        answers = collections.Counter(a for a in (clean_answer(r.get("text")) for r in results) if a)
        kept = [a for a, c in sorted(answers.items(), key=lambda kv: (-kv[1], kv[0])) if c > small_cell]
        out["answers"] = " | ".join(kept[:MAX_ANSWERS])
    return out


def read_mimic_results(labs_path):
    """{itemid: [result dicts]} from the MIMIC FHIR lab Observations. Only value, unit and range are kept."""
    results = collections.defaultdict(list)
    with gzip.open(labs_path, "rt", encoding="utf-8") as f:
        for line in f:
            r = json.loads(line)
            code = r["code"]["coding"][0]["code"]
            vq = r.get("valueQuantity") or {}
            rr = (r.get("referenceRange") or [{}])[0]
            results[code].append({
                "value": vq.get("value") if isinstance(vq.get("value"), (int, float)) else None,
                "unit": vq.get("unit"),
                "text": r.get("valueString"),
                "low": (rr.get("low") or {}).get("value"),
                "high": (rr.get("high") or {}).get("value"),
            })
    return results


def read_d_labitems(path):
    with gzip.open(path, "rt", encoding="utf-8", newline="") as f:
        return {r["itemid"]: r for r in csv.DictReader(f)}


def mimic_rows(results, labitems):
    rows = []
    for code in sorted(results, key=int):
        item = labitems.get(code, {})
        row = {"local_id": f"mimic:{code}", "source": "mimic", "local_code": code,
               "label": item.get("label", ""), "specimen": item.get("fluid", ""), "category": item.get("category", "")}
        row.update(summarize(results[code]))
        rows.append(row)
    return rows


def feed_rows(labs, panels, synonyms, normalize):
    """Rows for the Riverside feed: every local code and text name of hl7v2/lab_synonyms.csv, plus the pain score."""
    loinc_to_lab, label_of, panel_of = {}, {}, {}
    for panel, members in panels.items():
        for lab in members:
            panel_of[lab] = panel.split("^")[1]
    for lab, (variants, *_rest) in labs.items():
        for v in variants:
            code, text, *system = (v.split("^") + ["", ""])[:3]
            if system and system[0] == "LN":
                loinc_to_lab.setdefault(code, lab)
                continue
            if code:
                label_of[("code", code.upper())] = text
            label_of[("text", normalize(text))] = text
    by_name = {normalize(t): lab for lab, (variants, *_r) in labs.items() for t in (v.split("^")[1] for v in variants)}
    rows = []
    for s in synonyms:
        if s["loinc"] in FEED_NOT_LABS:
            continue
        kind = s["match_type"]
        key = s["match"].upper() if kind == "code" else normalize(s["match"])
        lab = by_name.get(normalize(label_of.get((kind, key), ""))) or by_name.get(key) or _lab_for_loinc(s["loinc"], synonyms, by_name, normalize)
        variants, unit, (lo, hi), (vmin, vmax), _dec = labs[lab]
        rows.append({
            "local_id": f"hl7v2:{kind}:{key}", "source": "hl7v2", "local_code": key if kind == "code" else "",
            "label": label_of.get((kind, key)) or s["match"], "specimen": FEED_SPECIMEN[lab],
            "category": panel_of.get(lab, ""), "unit": unit, "value_type": "number",
            "normal_low": fmt(lo), "normal_high": fmt(hi), "value_p5": fmt(vmin), "value_p95": fmt(vmax), "answers": "",
        })
    rows.append({"local_id": "hl7v2:text:pain score", "source": "hl7v2", "local_code": "", "label": "Pain score",
                 "specimen": "", "category": "Vital signs", "unit": "", "value_type": "text", "normal_low": "",
                 "normal_high": "", "value_p5": "", "value_p95": "",
                 "answers": " | ".join(clean_answer(a) for a in ("Mild", "Moderate", "None"))})
    return rows


def _lab_for_loinc(loinc, synonyms, by_name, normalize):
    """The generator lab of a synonym row whose own name is not a generator variant (e.g. 'glucose serum')."""
    for s in synonyms:
        if s["loinc"] == loinc and normalize(s["match"]) in by_name:
            return by_name[normalize(s["match"])]
    raise KeyError(loinc)


def read_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_csv(path, rows, columns=COLUMNS):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=columns, lineterminator="\n")
        w.writeheader()
        w.writerows(rows)


def load(path=cfg.LOCAL_LABS):
    return {r["local_id"]: r for r in read_csv(path)}


def main():
    for p in (cfg.MIMIC_LABS, cfg.D_LABITEMS):
        if not p.exists():
            raise SystemExit(f"Missing {p.relative_to(cfg.ROOT)}: see terminology/README.md, LLM assisted mapping")
    sys.path.insert(0, str(cfg.ROOT / "hl7v2"))
    import generate  # noqa: E402
    from to_fhir import normalize_name  # noqa: E402
    rows = mimic_rows(read_mimic_results(cfg.MIMIC_LABS), read_d_labitems(cfg.D_LABITEMS))
    rows += feed_rows(generate.LABS, generate.PANELS, read_csv(cfg.LAB_SYNONYMS), normalize_name)
    write_csv(cfg.LOCAL_LABS, rows)
    print(f"{len(rows)} local labs -> {cfg.LOCAL_LABS.relative_to(cfg.ROOT)}")


if __name__ == "__main__":
    main()
