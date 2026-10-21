"""The gold set (the repo's human maps) and the review queue (local labs nobody mapped yet).

Gold: every MIMIC row of terminology/lab_loinc_map.csv and every lab row of hl7v2/lab_synonyms.csv (the two vital
sign rows are not laboratory tests). Queue: every local lab of local_labs.csv that is neither mapped nor listed in
terminology/lab_exclude.csv (sample quality indices and specimen type, not patient measurements).
"""
import csv
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import config as cfg  # noqa: E402
from local_labs import FEED_NOT_LABS  # noqa: E402

sys.path.insert(0, str(cfg.ROOT / "hl7v2"))
from to_fhir import normalize_name  # noqa: E402


def read_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def feed_local_id(match_type, match):
    return f"hl7v2:code:{match.upper()}" if match_type == "code" else f"hl7v2:text:{normalize_name(match)}"


def load_gold(lab_map=cfg.LAB_MAP, synonyms=cfg.LAB_SYNONYMS):
    """{local_id: gold LOINC code}."""
    gold = {f"mimic:{r['mimic_code']}": r["loinc"] for r in read_csv(lab_map)}
    for r in read_csv(synonyms):
        if r["loinc"] not in FEED_NOT_LABS:
            gold[feed_local_id(r["match_type"], r["match"])] = r["loinc"]
    return gold


def load_excluded(path=cfg.LAB_EXCLUDE):
    return {f"mimic:{r['mimic_code']}" for r in read_csv(path)}


def queue_ids(local_labs, gold, excluded):
    """Local labs to suggest a code for, in file order."""
    return [i for i in local_labs if i not in gold and i not in excluded]


def slice_of(local_id):
    return "mimic" if local_id.startswith("mimic:") else "feed"
