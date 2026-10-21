"""The holdout set and the confidence threshold.

Holdout: every lab item of the MIMIC III Clinical Database Demo D_LABITEMS with a LOINC_CODE, whose item id is one of
our MIMIC IV demo lab items and is not in the dev set (the 82 human mapped pairs). The local side is still what our
MIMIC IV lab system says (local_labs.csv); only the gold code comes from MIMIC III. MIMIC III's codes are known to be
imperfect, so they are checked, never fixed: `flags` lists what looks doubtful.

Threshold: chosen on the dev set only, before the holdout runs. Answers at or above it would be applied after a quick
look; the rest go to a full review.
"""
import csv
import random
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import config as cfg  # noqa: E402
import loinc_index  # noqa: E402

SPOT_CHECK_SIZE = 10
SPOT_SEED = 7
FLUID_SYSTEMS = {  # which LOINC systems fit a MIMIC fluid, for the doubtful check only
    "blood": {"Bld", "BldA", "BldV", "BldMV", "BldC", "Ser/Plas", "Ser", "Plas", "Ser/Plas/Bld", "RBC", "PPP", "WBC",
              "Plt", "Bld/Tiss", "Hgb", "Bld.dot", "XXX"},
    "urine": {"Urine", "Urine sed", "XXX"},
    "pleural": {"Plr fld", "Body fld", "XXX"},
    "ascites": {"Periton fld", "Body fld", "XXX"},
    "joint fluid": {"Synv fld", "Body fld", "XXX"},
    "cerebrospinal fluid": {"CSF", "XXX"},
    "bone marrow": {"Bone mar", "Bld", "XXX"},
    "stool": {"Stool", "XXX"},
    "other body fluid": {"Body fld", "XXX"},
}


def load_mimic3(path=cfg.MIMIC3_LABITEMS):
    """{local_id: {loinc, mimic3_label, mimic3_fluid, mimic3_category}} for every MIMIC III item with a LOINC code."""
    path = Path(path)
    if not path.exists():
        raise SystemExit(f"Missing {path}: curl -L -o {path} {cfg.MIMIC3_URL}")
    out = {}
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            r = {k.lower(): (v or "").strip() for k, v in r.items()}
            if r.get("loinc_code"):
                out[f"mimic:{r['itemid']}"] = {"loinc": r["loinc_code"], "mimic3_label": r["label"],
                                               "mimic3_fluid": r["fluid"], "mimic3_category": r["category"]}
    return out


def load_holdout(labs, dev, excluded, path=cfg.MIMIC3_LABITEMS):
    """The holdout pairs: MIMIC III items with a LOINC code that are our lab items and not in the dev set."""
    m3 = load_mimic3(path)
    out = {lid: v for lid, v in m3.items() if lid in labs and lid not in dev and lid not in excluded}
    return dict(sorted(out.items(), key=lambda kv: int(kv[0].split(":")[1])))


def dev_agreement(dev, path=cfg.MIMIC3_LABITEMS):
    """Items in both the dev set and MIMIC III: where MIMIC III's code differs from the human map."""
    m3 = load_mimic3(path)
    both = sorted((lid for lid in dev if lid.startswith("mimic:") and lid in m3), key=lambda i: int(i.split(":")[1]))
    differ = [{"local_id": lid, "dev": dev[lid], "mimic3": m3[lid]["loinc"], "label": m3[lid]["mimic3_label"]}
              for lid in both if m3[lid]["loinc"] != dev[lid]]
    return {"both": len(both), "agree": len(both) - len(differ), "differ": differ}


def flags(lab, code, index):
    """Reasons the MIMIC III code looks doubtful for this lab (empty when nothing stands out)."""
    out = []
    info = index.codes.get(code)
    if info is None:
        return ["not in Loinc.csv"]
    if info["status"] != "ACTIVE":
        out.append(f"{info['status'].lower()} in LOINC")
    if info["classtype"] != "1":
        out.append("not a laboratory term (CLASSTYPE 2)")
    term = next((d for d in index.docs if d["code"] == code), None)
    if term is None:
        return out
    fluid = (lab.get("specimen") or "").lower()
    if fluid in FLUID_SYSTEMS and term["system"] not in FLUID_SYSTEMS[fluid]:
        out.append(f"specimen {term['system']} for a {lab['specimen']} test")
    props = loinc_index.expected_properties(lab.get("unit"))
    if props and term["property"] not in props:
        out.append(f"property {term['property']} for unit {lab['unit']}")
    if lab.get("value_type") == "number" and term["scale"] in ("Nom", "Nar"):
        out.append(f"scale {term['scale']} for numeric results")
    if lab.get("value_type") == "text" and term["scale"] == "Qn" and not lab.get("unit"):
        out.append("quantitative code for text results")
    return out


def flag_kind(flag):
    """The kind of a doubt flag, for counting: 'specimen Urine for a Blood test' -> 'specimen does not fit the fluid'."""
    for start, kind in (("specimen ", "specimen does not fit the fluid"), ("property ", "property does not fit the unit"),
                        ("scale ", "nominal scale for numeric results"),
                        ("quantitative code", "quantitative code for text results")):
        if flag.startswith(start):
            return kind
    return flag


def spot_check_ids(ids, n=SPOT_CHECK_SIZE, seed=SPOT_SEED):
    ids = sorted(ids, key=lambda i: int(i.split(":")[1]))
    return sorted(random.Random(seed).sample(ids, min(n, len(ids))), key=lambda i: int(i.split(":")[1]))


# Read by a person on 2026-10-19 (the seeded spot check above): the verdict on each MIMIC III code. Kept as written;
# the holdout scores use MIMIC III's code either way.
SPOT_CHECK_NOTES = {
    "mimic:50867": ("ok", "serum amylase activity, matches"),
    "mimic:50876": ("ok", "antibody presence in serum; serum rather than serum or plasma is a fair choice"),
    "mimic:50890": ("ok", "complement C3 mass concentration, matches"),
    "mimic:50907": ("ok", "total cholesterol in serum or plasma, matches"),
    "mimic:50965": ("ok", "intact PTH in pg/mL; MIMIC does not name the assay, intact is the usual one"),
    "mimic:51144": ("wrong", "the result is a percent of white cells, but the code is a count per volume; the fraction "
                             "code (band neutrophils per 100 leukocytes by manual count) fits"),
    "mimic:51184": ("plausible", "CD5 cells as a fraction of cells; the results arrive as text, so the unit is unknown"),
    "mimic:51224": ("doubtful", "the code names a method (acid electrophoresis) the lab system never states"),
    "mimic:51436": ("doubtful", "deprecated code; correct when MIMIC III was coded, outdated now"),
    "mimic:51489": ("ok", "non squamous epithelial cells per high power field in urine sediment, matches"),
}


def sample_ids(ids, size, seed=cfg.TOP_K + 1):
    """A seeded subset of the holdout for the costly model, so all rankers can be compared on the same pairs."""
    ids = sorted(ids, key=lambda i: int(i.split(":")[1]))
    if size is None or size >= len(ids):
        return ids
    return sorted(random.Random(seed).sample(ids, size), key=lambda i: int(i.split(":")[1]))


def choose_threshold(pairs, target=cfg.THRESHOLD_TARGET, grid=cfg.THRESHOLD_GRID, min_auto=10):
    """pairs: (confidence, correct) on the dev set. The lowest grid value whose answers at or above it are at least
    target right (with at least min_auto of them); when none is, the value with the best accuracy. Returns
    {threshold, auto, correct, accuracy, met_target}."""
    rows = []
    for t in grid:
        auto = [ok for c, ok in pairs if c is not None and c >= t]
        if len(auto) >= min_auto:
            rows.append({"threshold": t, "auto": len(auto), "correct": sum(auto), "accuracy": sum(auto) / len(auto)})
    if not rows:
        return {"threshold": None, "auto": 0, "correct": 0, "accuracy": None, "met_target": False}
    met = [r for r in rows if r["accuracy"] >= target]
    best = met[0] if met else max(rows, key=lambda r: (r["accuracy"], -r["threshold"]))
    return dict(best, met_target=bool(met))


def route(decisions, threshold):
    """Split (confidence, correct) answers at the threshold: (auto answers, routed to review)."""
    auto = [(c, ok) for c, ok in decisions if threshold is not None and c is not None and c >= threshold]
    review = [(c, ok) for c, ok in decisions if not (threshold is not None and c is not None and c >= threshold)]
    return auto, review


SERUM_OR_PLASMA = {"Ser", "Plas", "Ser/Plas"}


def near_match(choice, accepted, terms):
    """A secondary lens, never the headline: the chosen code names the same component, property, scale and specimen
    as a gold code (method ignored; Ser, Plas and Ser/Plas count as one specimen). terms: {code: term dict}."""
    t = terms.get(choice) if choice else None
    if t is None:
        return False
    for code in accepted:
        g = terms.get(code)
        if g is None:
            continue
        same_system = t["system"] == g["system"] or {t["system"], g["system"]} <= SERUM_OR_PLASMA
        if (t["component"].lower() == g["component"].lower() and t["property"] == g["property"]
                and t["scale"] == g["scale"] and same_system):
            return True
    return False
