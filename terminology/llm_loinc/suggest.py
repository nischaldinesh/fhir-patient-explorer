#!/usr/bin/env python3
"""LOINC suggestions for the labs nobody has mapped yet, and the review commands that apply them.

  python3 terminology/llm_loinc/suggest.py run --dry-run            # calls needed and estimated cost
  python3 terminology/llm_loinc/suggest.py run --max-calls 500      # writes terminology/loinc_review_queue.csv
  python3 terminology/llm_loinc/suggest.py accept mimic:51007 --reviewer "Alex Kim"
  python3 terminology/llm_loinc/suggest.py accept mimic:51007 --reviewer "Alex Kim" --loinc 3084-1   # another code
  python3 terminology/llm_loinc/suggest.py reject mimic:51266 --reviewer "Alex Kim"

Nothing is applied automatically: `run` only writes the queue, every row status pending. `accept` is a person's
decision: it appends the row to terminology/lab_loinc_map.csv (MIMIC) or hl7v2/lab_synonyms.csv (the feed), records
who decided and when in terminology/lab_loinc_provenance.csv (source llm_reviewed), and marks the queue row accepted.
The ConceptMap build (terminology/conceptmaps.py) and the dbt seed read the map, so they pick the row up as is.
Rows already accepted or rejected keep their decision when `run` writes the queue again.
"""
import argparse
import csv
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import config as cfg  # noqa: E402
import gold as G  # noqa: E402
import loinc_index  # noqa: E402
import local_labs  # noqa: E402
from cache import Budget, Cache, CallCapReached  # noqa: E402
from rankers import ClaudeRanker, cost, default_model  # noqa: E402

QUEUE_COLUMNS = ["local_id", "source", "local_code", "label", "specimen", "category", "unit", "suggested_loinc",
                 "suggested_name", "top3", "confidence", "reason", "model", "prompt_version", "status",
                 "reviewed_by", "reviewed_on"]
PROVENANCE_COLUMNS = ["local_id", "loinc", "loinc_name", "source", "reviewer", "reviewed_on", "model",
                      "prompt_version", "suggested_loinc", "confidence"]
MAP_COLUMNS = ["mimic_code", "mimic_display", "loinc", "loinc_display", "unit", "equivalence", "note"]
SYNONYM_COLUMNS = ["match_type", "match", "loinc", "loinc_display", "note"]
WORKERS = 4


def read_csv(path):
    if not Path(path).exists():
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_csv(path, rows, columns):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=columns, lineterminator="\n")
        w.writeheader()
        w.writerows(rows)


def append_csv(path, row, columns):
    """Append one row; writes the header first when the file is new."""
    new = not Path(path).exists()
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=columns, lineterminator="\n")
        if new:
            w.writeheader()
        w.writerow(row)


def queue_row(lab, decision, index, model, version):
    return {
        "local_id": lab["local_id"], "source": lab["source"], "local_code": lab["local_code"], "label": lab["label"],
        "specimen": lab["specimen"], "category": lab["category"], "unit": lab["unit"],
        "suggested_loinc": decision.choice or "", "suggested_name": index.name(decision.choice) if decision.choice else "",
        "top3": " | ".join(decision.top3), "confidence": "" if decision.confidence is None else f"{decision.confidence:.2f}",
        "reason": decision.error or decision.reason, "model": model, "prompt_version": version, "status": "pending",
        "reviewed_by": "", "reviewed_on": "",
    }


def build_queue(labs, ids, index, ranker, existing):
    """Queue rows for ids, keeping rows a person already decided."""
    decided = {r["local_id"]: r for r in existing if r.get("status") in ("accepted", "rejected")}
    todo = [i for i in ids if i not in decided]
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        decisions = dict(zip(todo, pool.map(lambda i: ranker.rank(labs[i], index.search(labs[i])), todo)))
    rows = []
    for i in ids:
        if i in decided:
            rows.append(decided[i])
        elif not decisions[i].dry_run:
            rows.append(queue_row(labs[i], decisions[i], index, ranker.model, ranker.prompt_version))
    return rows


def cmd_run(a):
    labs, gold, index = local_labs.load(), G.load_gold(), loinc_index.load()
    ids = G.queue_ids(labs, gold, G.load_excluded())
    model = a.model or default_model()
    ranker = ClaudeRanker(model, index, Cache(cfg.CACHE_DIR), Budget(a.max_calls), dry_run=a.dry_run, purpose="queue")
    try:
        rows = build_queue(labs, ids, index, ranker, read_csv(cfg.REVIEW_QUEUE))
    except CallCapReached as e:
        raise SystemExit(f"Stopped: {e}")
    if a.dry_run:
        tin, tout = ranker.planned_tokens
        print(f"{len(ids)} unmapped labs, {ranker.planned_calls} calls not cached yet, about "
              f"${cost(model, tin, tout):.3f} with {model} (estimate: 3.5 characters per token)")
        return
    write_csv(cfg.REVIEW_QUEUE, rows, QUEUE_COLUMNS)
    suggested = sum(1 for r in rows if r["suggested_loinc"])
    print(f"{len(rows)} labs -> {cfg.REVIEW_QUEUE.relative_to(cfg.ROOT)}: {suggested} with a suggestion, "
          f"{len(rows) - suggested} NONE; status pending, nothing applied")


def mapped_ids():
    return set(G.load_gold())


def decide(local_id, reviewer, status, loinc=None, on=None, index=None, queue_path=cfg.REVIEW_QUEUE,
           lab_map=cfg.LAB_MAP, synonyms=cfg.LAB_SYNONYMS, provenance=cfg.PROVENANCE, already_mapped=None):
    """Accept or reject one queue row. Returns the queue row as written."""
    if not reviewer or not reviewer.strip():
        raise SystemExit("--reviewer is required: a person decides every row")
    rows = read_csv(queue_path)
    row = next((r for r in rows if r["local_id"] == local_id), None)
    if row is None:
        raise SystemExit(f"{local_id} is not in {Path(queue_path).name}")
    if row["status"] != "pending":
        raise SystemExit(f"{local_id} is already {row['status']}")
    on = on or str(date.today())
    if status == "accepted":
        code = (loinc or row["suggested_loinc"]).strip()
        if not code:
            raise SystemExit(f"{local_id} has no suggestion: pass --loinc with the code to map it to")
        index = index or loinc_index.load()
        st = index.status(code)
        if st is None:
            raise SystemExit(f"{code} is not in Loinc.csv")
        if st == "DEPRECATED":
            raise SystemExit(f"{code} is deprecated; MapTo.csv suggests {', '.join(index.mapto.get(code, [])) or 'nothing'}")
        if local_id in (already_mapped if already_mapped is not None else mapped_ids()):
            raise SystemExit(f"{local_id} is already mapped")
        name = index.name(code)
        how = "the suggestion" if code == row["suggested_loinc"] else "another code than the suggestion"
        note = (f"LLM suggested ({row['model']}, prompt {row['prompt_version']}), reviewed by {reviewer} on {on}: "
                f"{how}")
        if row["source"] == "mimic":
            append_csv(lab_map, {"mimic_code": row["local_code"], "mimic_display": row["label"], "loinc": code,
                                 "loinc_display": name, "unit": row["unit"], "equivalence": "equivalent", "note": note},
                       MAP_COLUMNS)
        else:
            _source, kind, key = local_id.split(":", 2)  # hl7v2:code:GLU or hl7v2:text:blood sugar
            append_csv(synonyms, {"match_type": kind, "match": key, "loinc": code, "loinc_display": name, "note": note},
                       SYNONYM_COLUMNS)
        append_csv(provenance, {"local_id": local_id, "loinc": code, "loinc_name": name, "source": "llm_reviewed",
                                "reviewer": reviewer, "reviewed_on": on, "model": row["model"],
                                "prompt_version": row["prompt_version"], "suggested_loinc": row["suggested_loinc"],
                                "confidence": row["confidence"]}, PROVENANCE_COLUMNS)
    row.update(status=status, reviewed_by=reviewer, reviewed_on=on)
    write_csv(queue_path, rows, QUEUE_COLUMNS)
    return row


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--model", help=f"default: LLM_MODEL or {cfg.DEFAULT_MODEL}")
    r.add_argument("--max-calls", type=int, default=cfg.DEFAULT_MAX_CALLS)
    r.add_argument("--dry-run", action="store_true")
    for name in ("accept", "reject"):
        p = sub.add_parser(name)
        p.add_argument("local_id")
        p.add_argument("--reviewer", required=True)
        if name == "accept":
            p.add_argument("--loinc", help="map to this code instead of the suggestion")
    a = ap.parse_args(argv)
    if a.cmd == "run":
        return cmd_run(a)
    row = decide(a.local_id, a.reviewer, "accepted" if a.cmd == "accept" else "rejected", getattr(a, "loinc", None))
    print(f"{row['local_id']}: {row['status']} by {row['reviewed_by']}")
    if a.cmd == "accept":
        print("Next: python3 terminology/build_maps.py && python3 terminology/conceptmaps.py build")


if __name__ == "__main__":
    main()
