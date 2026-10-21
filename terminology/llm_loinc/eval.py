#!/usr/bin/env python3
"""Accuracy eval of LOINC suggestions against the repo's human maps.

  python3 terminology/llm_loinc/eval.py run --dry-run                 # calls needed and estimated cost, no API call
  python3 terminology/llm_loinc/eval.py run --max-calls 400           # retrieval, both rankers, the doc
  python3 terminology/llm_loinc/eval.py run --stability 2 --max-calls 120   # also repeat 20 labs twice, no cache
  python3 terminology/llm_loinc/eval.py spend                         # every API call made so far (the ledger)

Every prompt version in config.EVAL_PROMPTS is evaluated with every model; v1 is the blind first prompt, v2 the one
written after reading v1's errors (see the doc).

What it measures, separately for MIMIC, the Riverside feed and both:
  * retrieval recall at 1, 5 and 20 (if the right code is not retrieved, no ranker can pick it)
  * per ranker: top 1 and top 3 accuracy, strict (the gold code) and lenient (the gold code or its MapTo.csv
    replacement), each with a 95% Wilson interval
  * abstention: for a seeded half of the gold set the gold code (and its replacements) is removed from the
    candidates; the right answer is then NONE
  * calibration: accuracy by confidence band; cost from the token counts the API reported; latency p50 and p95
  * the serum versus blood gas pairs with and without the lab category
Writes data/llm_eval/results.json and docs/LLM_LOINC_EVAL.md. Answers come from data/llm_cache, so a rerun makes no
API call. The stability check only runs when asked for and is kept in data/llm_eval/stability.json.
"""
import argparse
import json
import math
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import config as cfg  # noqa: E402
import gold as G  # noqa: E402
import holdout as H  # noqa: E402
import loinc_index  # noqa: E402
import local_labs  # noqa: E402
from cache import Budget, Cache, CallCapReached, Ledger  # noqa: E402
from rankers import ClaudeRanker, LexicalBaseline, cost  # noqa: E402

SEED = 42
STABILITY_SIZE = 20
WORKERS = 4
SLICES = ("mimic", "feed", "all")
BANDS = [(0.0, 0.5), (0.5, 0.8), (0.8, 0.95), (0.95, 1.0001)]
# MIMIC's serum chemistry and blood gas versions of the same analyte: the fluid is Blood for both, only the category
# (Chemistry or Blood Gas) tells them apart.
HARD_PAIRS = [("mimic:50931", "mimic:50809"), ("mimic:50971", "mimic:50822"),
              ("mimic:50983", "mimic:50824"), ("mimic:50902", "mimic:50806")]


# ---------------------------------------------------------------- metrics (pure)

def wilson(k, n, z=1.96):
    """(rate, low, high) of the 95% Wilson score interval; (None, None, None) when n is 0."""
    if n == 0:
        return None, None, None
    p = k / n
    d = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / d
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return p, max(0.0, centre - half), min(1.0, centre + half)


def position(codes, accepted):
    """0 based rank of the first code in accepted, or None."""
    return next((i for i, c in enumerate(codes) if c in accepted), None)


def recall_at(positions, k):
    hits = sum(1 for p in positions if p is not None and p < k)
    return hits, len(positions)


def percentile(values, p):
    v = sorted(x for x in values if x is not None)
    if not v:
        return None
    k = (len(v) - 1) * p / 100
    lo, hi = math.floor(k), math.ceil(k)
    return v[lo] + (v[hi] - v[lo]) * (k - lo)


def calibration(pairs, bands=BANDS):
    """pairs: (confidence, correct). Returns [{band, n, correct, accuracy, mean_confidence}] per band."""
    out = []
    for lo, hi in bands:
        sel = [(c, ok) for c, ok in pairs if c is not None and lo <= c < hi]
        k = sum(1 for _c, ok in sel if ok)
        out.append({"band": f"{lo:g} to {min(hi, 1):g}", "n": len(sel), "correct": k,
                    "accuracy": wilson(k, len(sel))[0],
                    "mean_confidence": sum(c for c, _ok in sel) / len(sel) if sel else None})
    return out


def selective(records, name, threshold=0.8):
    """Answers at confidence threshold or more, over the gold pairs and the abstention test: how many, how many right."""
    sel = [(r, r["rankers"][name]) for r in records if r["case"] in ("main", "abstention")]
    conf = [(r, d) for r, d in sel if d["confidence"] is not None and d["confidence"] >= threshold]
    ok = sum(1 for r, d in conf if (d["lenient"] if r["case"] == "main" else d["abstained"]))
    return {"threshold": threshold, "answers": len(sel), "confident": len(conf), "accuracy": rate(ok, len(conf))}


def mcnemar(b, c):
    """Exact two sided McNemar p value for paired right/wrong answers: b and c are the two kinds of disagreement."""
    n = b + c
    if n == 0:
        return 1.0
    tail = sum(math.comb(n, i) for i in range(0, min(b, c) + 1)) / 2 ** n
    return min(1.0, 2 * tail)


def rate(k, n):
    p, lo, hi = wilson(k, n)
    return {"k": k, "n": n, "rate": p, "low": lo, "high": hi}


def fmt_rate(r):
    if not r or r["n"] == 0:
        return "n/a"
    return f"{r['rate'] * 100:.0f}% ({r['low'] * 100:.0f} to {r['high'] * 100:.0f}), {r['k']}/{r['n']}"


def in_slice(local_id, name):
    return name == "all" or G.slice_of(local_id) == name


# ---------------------------------------------------------------- running

def abstention_ids(gold_ids, seed=SEED):
    ids = sorted(gold_ids)
    return sorted(random.Random(seed).sample(ids, len(ids) // 2))


def without_category(lab):
    return dict(lab, category="")


def run_rankers(rankers, jobs):
    """jobs: list of (case, lab, candidates). Returns {ranker name: [Decision per job]}, calling the API in parallel."""
    out = {}
    for r in rankers:
        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            out[r.name] = list(pool.map(lambda j, r=r: r.rank(j[1], j[2]), jobs))
    return out


def decision_record(d, accepted, gold_code, names):
    return {
        "choice": d.choice, "choice_name": names(d.choice) if d.choice else "", "top3": d.top3,
        "confidence": d.confidence, "reason": d.reason, "violations": d.violations, "error": d.error,
        "strict": d.choice == gold_code, "lenient": d.choice in accepted if d.choice else False,
        "top3_strict": gold_code in d.top3, "top3_lenient": any(c in accepted for c in d.top3),
        "abstained": d.choice is None and d.error is None,
        "input_tokens": d.input_tokens, "output_tokens": d.output_tokens, "latency_ms": d.latency_ms,
        "cached": d.cached,
    }


def evaluate(models, max_calls, dry_run=False, stability=0, client=None, cache_dir=cfg.CACHE_DIR,
             prompts=cfg.EVAL_PROMPTS, stability_path=None, budget=None):
    """The dev set: the 82 human mapped pairs, every prompt version (v2 was written on it, so it is tuned)."""
    labs, gold, idx = local_labs.load(), G.load_gold(), loinc_index.load()
    cache, budget = Cache(cache_dir), budget or Budget(max_calls)
    rankers = [LexicalBaseline()] + [ClaudeRanker(m, idx, cache, budget, client=client, dry_run=dry_run,
                                                  prompt_version=v) for v in prompts for m in models]
    gold_ids = sorted(gold)
    abst = set(abstention_ids(gold_ids))
    hard = [i for pair in HARD_PAIRS for i in pair]

    jobs, cases = [], []
    for lid in gold_ids:
        accepted = idx.equivalents(gold[lid])
        cands = idx.search(labs[lid])
        cases.append({"case": "main", "local_id": lid, "accepted": accepted, "candidates": cands})
        if lid in abst:
            cases.append({"case": "abstention", "local_id": lid, "accepted": accepted,
                          "candidates": idx.search(labs[lid], exclude=accepted)})
    for lid in hard:
        cases.append({"case": "no_category", "local_id": lid, "accepted": idx.equivalents(gold[lid]),
                      "candidates": idx.search(without_category(labs[lid]))})
    for c in cases:
        lab = labs[c["local_id"]] if c["case"] != "no_category" else without_category(labs[c["local_id"]])
        jobs.append((c, lab, c["candidates"]))

    try:
        decisions = run_rankers(rankers, jobs)
    except CallCapReached as e:
        raise SystemExit(f"Stopped: {e}")
    if dry_run:
        return plan(rankers, stability)

    records = []
    for i, c in enumerate(cases):
        lid, lab = c["local_id"], labs[c["local_id"]]
        codes = [x["code"] for x in c["candidates"]]
        rec = {"case": c["case"], "local_id": lid, "slice": G.slice_of(lid), "label": lab["label"],
               "specimen": lab["specimen"], "category": lab["category"] if c["case"] != "no_category" else "",
               "unit": lab["unit"], "gold": gold[lid], "gold_name": idx.name(gold[lid]),
               "accepted": sorted(c["accepted"]), "candidates": codes,
               "retrieval_strict": position(codes, {gold[lid]}), "retrieval_lenient": position(codes, c["accepted"]),
               "rankers": {}}
        for r in rankers:
            rec["rankers"][r.name] = decision_record(decisions[r.name][i], c["accepted"], gold[lid], idx.name)
        records.append(rec)

    result = summarize(records, rankers, models, idx)
    stability_path = Path(stability_path or cfg.EVAL_DIR / "stability.json")
    if stability:
        st = stability_check(records, models, idx, cache, budget, stability, client, labs)
        stability_path.parent.mkdir(parents=True, exist_ok=True)
        stability_path.write_text(json.dumps(st, indent=1), encoding="utf-8")
    if stability_path.exists():
        result["stability"] = json.loads(stability_path.read_text(encoding="utf-8"))
    result["spend"] = spend(cache_dir)
    return result


def evaluate_holdout(models, budget, thresholds, dry_run=False, client=None, cache_dir=cfg.CACHE_DIR,
                     sample_models=(), sample_size=None):
    """Baseline and the frozen prompt on the MIMIC III holdout, once. Models in sample_models only run on a seeded
    subset of sample_size pairs (cost); every ranker is also scored on that subset, so they compare on equal terms."""
    labs, dev, idx = local_labs.load(), G.load_gold(), loinc_index.load()
    ho = H.load_holdout(labs, dev, G.load_excluded())
    ids = list(ho)
    sample = H.sample_ids(ids, sample_size)
    cache = Cache(cache_dir)
    rankers = [LexicalBaseline()] + [ClaudeRanker(m, idx, cache, budget, client=client, dry_run=dry_run,
                                                  prompt_version=cfg.HOLDOUT_PROMPT, purpose="holdout")
                                     for m in models]
    cands = {i: idx.search(labs[i]) for i in ids}
    terms = {d["code"]: d for d in idx.docs}
    decisions = {}
    try:
        for r in rankers:
            mine = sample if getattr(r, "model", None) in sample_models else ids
            with ThreadPoolExecutor(max_workers=WORKERS) as pool:
                decisions[r.name] = dict(zip(mine, pool.map(lambda i, r=r: r.rank(labs[i], cands[i]), mine)))
    except CallCapReached as e:
        raise SystemExit(f"Stopped: {e}")
    if dry_run:
        return plan(rankers, 0)
    records = []
    for i in ids:
        lab, g = labs[i], ho[i]
        accepted = idx.equivalents(g["loinc"])
        codes = [c["code"] for c in cands[i]]
        rec = {"case": "holdout", "local_id": i, "slice": "mimic", "label": lab["label"], "specimen": lab["specimen"],
               "category": lab["category"], "unit": lab["unit"], "gold": g["loinc"], "gold_name": idx.name(g["loinc"]),
               "gold_status": idx.status(g["loinc"]), "accepted": sorted(accepted), "mimic3_label": g["mimic3_label"],
               "flags": H.flags(lab, g["loinc"], idx), "in_sample": i in sample, "candidates": codes,
               "retrieval_strict": position(codes, {g["loinc"]}), "retrieval_lenient": position(codes, accepted),
               "rankers": {}}
        for r in rankers:
            if i in decisions[r.name]:
                d = decision_record(decisions[r.name][i], accepted, g["loinc"], idx.name)
                d["near"] = d["lenient"] or H.near_match(d["choice"], accepted, terms)
                rec["rankers"][r.name] = d
        records.append(rec)
    llm = {r.name: r.model for r in rankers if isinstance(r, ClaudeRanker)}
    out = {"pairs": len(ids), "sample_size": len(sample), "sample_models": list(sample_models),
           "prompt_version": cfg.HOLDOUT_PROMPT, "frozen": cfg.FROZEN, "frozen_on": cfg.FROZEN_ON,
           "status": dict(sorted(Counter_(r["gold_status"] or "not in Loinc.csv" for r in records).items())),
           "not_lab_terms": sum(1 for r in records if (idx.codes.get(r["gold"]) or {}).get("classtype") not in ("1",)),
           "flag_counts": dict(Counter_(H.flag_kind(f) for r in records for f in r["flags"])),
           "flagged": sum(1 for r in records if r["flags"]),
           "spot_check": [dict(local_id=i, label=labs[i]["label"], specimen=labs[i]["specimen"], unit=labs[i]["unit"],
                               gold=ho[i]["loinc"], gold_name=idx.name(ho[i]["loinc"]), flags=H.flags(labs[i], ho[i]["loinc"], idx),
                               verdict=H.SPOT_CHECK_NOTES.get(i, ("unchecked", ""))[0],
                               note=H.SPOT_CHECK_NOTES.get(i, ("unchecked", ""))[1]) for i in H.spot_check_ids(ids)],
           "dev_agreement": H.dev_agreement(dev), "thresholds": thresholds, "records": records, "subsets": {}}
    subsets = {"all": ids}
    if len(sample) < len(ids):
        subsets["sample"] = sample
    for sname, sids in subsets.items():
        sel = [r for r in records if r["local_id"] in set(sids)]
        per = {"pairs": len(sel),
               "recall_20_lenient": rate(*recall_at([r["retrieval_lenient"] for r in sel], 20)),
               "recall_20_strict": rate(*recall_at([r["retrieval_strict"] for r in sel], 20)),
               "rankers": {}}
        for r in rankers:
            ds = [x["rankers"].get(r.name) for x in sel]
            if not all(ds):
                continue
            th = (thresholds.get(r.name) or {}).get("threshold") if r.name in llm else None
            auto, review = H.route([(d["confidence"], d["lenient"]) for d in ds], th)
            auto_strict, _ = H.route([(d["confidence"], d["strict"]) for d in ds], th)
            tin, tout = sum(d["input_tokens"] for d in ds), sum(d["output_tokens"] for d in ds)
            lat = [d["latency_ms"] for d in ds if d["latency_ms"] is not None]
            per["rankers"][r.name] = {
                "top1_strict": rate(sum(d["strict"] for d in ds), len(ds)),
                "top1_lenient": rate(sum(d["lenient"] for d in ds), len(ds)),
                "top3_lenient": rate(sum(d["top3_lenient"] for d in ds), len(ds)),
                "near": rate(sum(d["near"] for d in ds), len(ds)),
                "unflagged": rate(sum(d["lenient"] for x, d in zip(sel, ds) if not x["flags"]),
                                  sum(1 for x in sel if not x["flags"])),
                "flagged": rate(sum(d["lenient"] for x, d in zip(sel, ds) if x["flags"]),
                                sum(1 for x in sel if x["flags"])),
                "none": sum(d["abstained"] for d in ds),
                "threshold": th,
                "routed": rate(len(review), len(ds)) if th is not None else None,
                "auto_lenient": rate(sum(ok for _c, ok in auto), len(auto)) if th is not None else None,
                "auto_strict": rate(sum(ok for _c, ok in auto_strict), len(auto_strict)) if th is not None else None,
                "cost": cost(llm[r.name], tin, tout) if r.name in llm else 0.0,
                "calls": len(ds) if r.name in llm else 0,
                "new_calls": sum(1 for d in ds if not d["cached"]) if r.name in llm else 0,
                "latency_p50_ms": percentile(lat, 50), "latency_p95_ms": percentile(lat, 95),
                "violations": sum(len(d["violations"]) for d in ds), "errors": sum(1 for d in ds if d["error"]),
            }
        base = {x["local_id"]: x["rankers"]["lexical_baseline"]["lenient"] for x in sel}
        for name in llm:
            if name in per["rankers"]:
                mine = {x["local_id"]: x["rankers"][name]["lenient"] for x in sel}
                b = sum(1 for i in mine if mine[i] and not base[i])
                c = sum(1 for i in mine if base[i] and not mine[i])
                per["rankers"][name]["vs_baseline"] = {"only_this_right": b, "only_baseline_right": c,
                                                       "p_value": mcnemar(b, c)}
        out["subsets"][sname] = per
    return out


def Counter_(items):
    from collections import Counter
    return Counter(items)


def plan(rankers, stability):
    out = {"dry_run": True, "models": {}}
    for r in rankers:
        if isinstance(r, ClaudeRanker):
            extra = STABILITY_SIZE * stability if r.prompt_version == cfg.PROMPT_VERSION else 0
            tin, tout = r.planned_tokens
            per_in = tin / r.planned_calls if r.planned_calls else 2000
            per_out = tout / r.planned_calls if r.planned_calls else 120
            calls = r.planned_calls + extra
            out["models"][r.name] = {"calls": calls, "estimated_cost": cost(r.model, per_in * calls, per_out * calls)}
    out["calls"] = sum(m["calls"] for m in out["models"].values())
    out["estimated_cost"] = sum(m["estimated_cost"] for m in out["models"].values())
    return out


def stability_check(records, models, idx, cache, budget, runs, client, labs):
    """Repeat a seeded slice of gold labs `runs` times without the cache; how often does the answer change?"""
    main = [r for r in records if r["case"] == "main"]
    picked = sorted(random.Random(SEED + 1).sample(main, min(STABILITY_SIZE, len(main))), key=lambda r: r["local_id"])
    log = Cache(Path(cache.root) / "_stability")
    out = {"labs": len(picked), "runs": runs, "prompt_version": cfg.PROMPT_VERSION, "date": str(date.today()),
           "models": {}}
    for m in models:
        ranker = ClaudeRanker(m, idx, cache, budget, client=client, use_cache=False)
        name = ranker.name
        jobs = [(r, labs[r["local_id"]], [c for c in idx.search(labs[r["local_id"]])]) for r in picked]
        agree, changed, spent = 0, [], 0.0
        all_runs = []
        for k in range(runs):
            try:
                ds = run_rankers([ranker], jobs)[name]
            except CallCapReached as e:
                raise SystemExit(f"Stopped in the stability check: {e}")
            for j, d in zip(jobs, ds):
                log.put(m, f"{j[0]['local_id'].replace(':', '_')}-{time.time_ns()}",
                        {"choice": d.choice, "input_tokens": d.input_tokens, "output_tokens": d.output_tokens})
                spent += cost(m, d.input_tokens, d.output_tokens)
            all_runs.append([d.choice for d in ds])
        for i, r in enumerate(picked):
            first = r["rankers"][name]["choice"]
            answers = [first] + [run[i] for run in all_runs]
            if len(set(answers)) == 1:
                agree += 1
            else:
                changed.append({"local_id": r["local_id"], "answers": answers})
        out["models"][name] = {"agreement": rate(agree, len(picked)), "changed": changed, "cost": spent}
    return out


def summarize(records, rankers, models, idx):
    ranker_names = [r.name for r in rankers]
    llm = {r.name: r.model for r in rankers if isinstance(r, ClaudeRanker)}
    res = {"date": str(date.today()), "seed": SEED, "top_k": cfg.TOP_K, "prompt_version": cfg.PROMPT_VERSION,
           "llm_rankers": list(llm), "current_rankers": [n for n in llm if n.endswith(" " + cfg.PROMPT_VERSION)],
           "models": models, "prices": {m: cfg.price(m) for m in models}, "prices_checked": cfg.PRICES_CHECKED,
           "prices_source": cfg.PRICES_SOURCE, "loinc_terms": len(idx.docs), "records": records}
    main = [r for r in records if r["case"] == "main"]
    abst = [r for r in records if r["case"] == "abstention"]
    res["gold"] = {s: sum(1 for r in main if in_slice(r["local_id"], s)) for s in SLICES}
    res["retrieval"] = {}
    for s in SLICES:
        sel = [r for r in main if in_slice(r["local_id"], s)]
        res["retrieval"][s] = {mode: {f"at_{k}": rate(*recall_at([r[f"retrieval_{mode}"] for r in sel], k))
                                      for k in (1, 5, 20)} for mode in ("strict", "lenient")}
    res["retrieval_misses"] = [{"local_id": r["local_id"], "label": r["label"], "gold": r["gold"],
                                "gold_name": r["gold_name"], "position": r["retrieval_lenient"],
                                "top1": r["candidates"][0] if r["candidates"] else None,
                                "top1_name": idx.name(r["candidates"][0]) if r["candidates"] else "",
                                "label_words_in_gold": shares_words(r["label"], r["gold"], idx)}
                               for r in main if r["retrieval_lenient"] is None]
    res["rankers"] = {}
    for name in ranker_names:
        per = {}
        for s in SLICES:
            m = [r["rankers"][name] for r in main if in_slice(r["local_id"], s)]
            a = [r["rankers"][name] for r in abst if in_slice(r["local_id"], s)]
            per[s] = {
                "top1_strict": rate(sum(d["strict"] for d in m), len(m)),
                "top1_lenient": rate(sum(d["lenient"] for d in m), len(m)),
                "top3_strict": rate(sum(d["top3_strict"] for d in m), len(m)),
                "top3_lenient": rate(sum(d["top3_lenient"] for d in m), len(m)),
                "abstention": rate(sum(d["abstained"] for d in a), len(a)),
                "abstained_when_gold_shown": sum(d["abstained"] for d in m),
            }
        calls = [r["rankers"][name] for r in records]
        latencies = [d["latency_ms"] for d in calls if d["latency_ms"] is not None]
        tin, tout = sum(d["input_tokens"] for d in calls), sum(d["output_tokens"] for d in calls)
        is_llm = name in llm
        res["rankers"][name] = {
            "slices": per,
            "calls": len(calls) if is_llm else 0,
            "input_tokens": tin, "output_tokens": tout,
            "cost": cost(llm[name], tin, tout) if is_llm else 0.0,
            "latency_p50_ms": percentile(latencies, 50), "latency_p95_ms": percentile(latencies, 95),
            "violations": sum(len(d["violations"]) for d in calls),
            "hidden_gold_named": sum(1 for r in records if r["case"] == "abstention"
                                     for v in r["rankers"][name]["violations"] if v.split(":")[0] in r["accepted"]),
            "hidden_gold_examples": [
                {"local_id": r["local_id"], "label": r["label"], "gold": r["gold"], "gold_name": r["gold_name"],
                 "times": sum(1 for v in r["rankers"][name]["violations"] if v.split(":")[0] in r["accepted"]),
                 "reason": r["rankers"][name]["reason"]}
                for r in records if r["case"] == "abstention"
                and any(v.split(":")[0] in r["accepted"] for v in r["rankers"][name]["violations"])],
            "confident": selective(records, name),
            "errors": sum(1 for d in calls if d["error"]),
            "calibration": calibration([(d["confidence"], d["lenient"] if r["case"] == "main" else d["abstained"])
                                        for r, d in ((r, r["rankers"][name]) for r in records)
                                        if r["case"] in ("main", "abstention")]) if is_llm else [],
        }
    res["thresholds"] = {name: H.choose_threshold(
        [(d["confidence"], d["lenient"] if r["case"] == "main" else d["abstained"])
         for r, d in ((r, r["rankers"][name]) for r in records) if r["case"] in ("main", "abstention")])
        for name in res["current_rankers"]}
    res["hard_pairs"] = hard_pairs(records, ranker_names)
    res["suggested_outside_candidates"] = sum(
        1 for r in records for d in r["rankers"].values()
        for c in ([d["choice"]] if d["choice"] else []) + d["top3"] if c not in r["candidates"] or not idx.status(c))
    return res


def shares_words(label, code, idx):
    term = next((d for d in idx.docs if d["code"] == code), None)
    if term is None:
        return False
    words = set(loinc_index.tokens(" ".join([term["name"], term["component"], term["short"]])))
    return bool(set(loinc_index.tokens(label)) & words)


def hard_pairs(records, ranker_names):
    with_cat = {r["local_id"]: r for r in records if r["case"] == "main"}
    without = {r["local_id"]: r for r in records if r["case"] == "no_category"}
    rows = []
    for pair in HARD_PAIRS:
        for lid in pair:
            a, b = with_cat[lid], without[lid]
            rows.append({"local_id": lid, "label": a["label"], "category": a["category"], "gold": a["gold"],
                         "retrieval_with": a["retrieval_lenient"], "retrieval_without": b["retrieval_lenient"],
                         "rankers": {n: {"with": a["rankers"][n]["choice"], "without": b["rankers"][n]["choice"],
                                         "with_ok": a["rankers"][n]["lenient"], "without_ok": b["rankers"][n]["lenient"]}
                                     for n in ranker_names}})
    return rows


def spend(cache_dir=cfg.CACHE_DIR):
    """Every API call ever sent, from the ledger: calls, tokens and dollars per model and per purpose."""
    out, purposes = {}, {}
    for e in Ledger(Path(cache_dir) / "_ledger.jsonl").entries():
        m = out.setdefault(e["model"], {"calls": 0, "input_tokens": 0, "output_tokens": 0, "estimated_calls": 0})
        m["calls"] += 1
        m["input_tokens"] += e.get("input_tokens", 0)
        m["output_tokens"] += e.get("output_tokens", 0)
        m["estimated_calls"] += 1 if e.get("estimated") else 0
        p = purposes.setdefault(e.get("purpose", "eval"), {"calls": 0, "cost": 0.0})
        p["calls"] += 1
        p["cost"] += cost(e["model"], e.get("input_tokens", 0), e.get("output_tokens", 0)) if e["model"] in cfg.PRICES else 0
    for model, m in out.items():
        m["cost"] = cost(model, m["input_tokens"], m["output_tokens"]) if model in cfg.PRICES else None
    return {"models": out, "purposes": purposes, "total_cost": sum(m["cost"] or 0 for m in out.values()),
            "total_calls": sum(m["calls"] for m in out.values())}


# ---------------------------------------------------------------- the doc

def write_doc(res, path=cfg.EVAL_DOC):
    from report import render
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render(res), encoding="utf-8")


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--models", default=",".join(cfg.EVAL_MODELS))
    r.add_argument("--max-calls", type=int, default=cfg.DEFAULT_MAX_CALLS)
    r.add_argument("--dry-run", action="store_true")
    r.add_argument("--stability", type=int, default=0, metavar="N", help="repeat 20 labs N times without the cache")
    r.add_argument("--no-doc", action="store_true")
    r.add_argument("--sample-models", default="claude-sonnet-5-5",
                   help="models run on a seeded subset of the holdout only (cost)")
    r.add_argument("--sample-size", type=int, default=cfg.HOLDOUT_SAMPLE_SIZE)
    sub.add_parser("spend")
    a = ap.parse_args(argv)
    if a.cmd == "spend":
        s = spend()
        for model, m in sorted(s["models"].items()):
            print(f"{model:20} {m['calls']:5} calls  {m['input_tokens']:9} in  {m['output_tokens']:7} out  "
                  f"${m['cost'] or 0:.4f}")
        for purpose, p in sorted(s["purposes"].items()):
            print(f"  {purpose:18} {p['calls']:5} calls  ${p['cost']:.4f}")
        print(f"total {s['total_calls']} calls, ${s['total_cost']:.4f} (token counts from the API x prices checked "
              f"{cfg.PRICES_CHECKED})")
        return
    models = [m.strip() for m in a.models.split(",") if m.strip()]
    for m in models:
        cfg.price(m)
    budget = Budget(a.max_calls)
    res = evaluate(models, a.max_calls, dry_run=a.dry_run, stability=a.stability, budget=budget)
    sample_models = [m.strip() for m in a.sample_models.split(",") if m.strip()]
    ho = evaluate_holdout(models, budget, {} if a.dry_run else res["thresholds"], dry_run=a.dry_run,
                          sample_models=sample_models, sample_size=a.sample_size)
    if a.dry_run:
        for m, p in ho["models"].items():
            res["models"][m + " (holdout)"] = p
        res["calls"] += ho["calls"]
        res["estimated_cost"] += ho["estimated_cost"]
        for m, p in res["models"].items():
            print(f"{m:20} {p['calls']:4} calls not cached yet, about ${p['estimated_cost']:.3f}")
        print(f"total {res['calls']} calls, about ${res['estimated_cost']:.3f} (estimate: 3.5 characters per token)")
        if res["calls"] > a.max_calls:
            print(f"more than --max-calls {a.max_calls}: raise it to run everything")
        return
    res["holdout"] = ho
    res["spend"] = spend()
    cfg.EVAL_DIR.mkdir(parents=True, exist_ok=True)
    out = cfg.EVAL_DIR / "results.json"
    out.write_text(json.dumps(res, indent=1, default=list), encoding="utf-8")
    if not a.no_doc:
        write_doc(res)
    called = sum(1 for r in res["records"] + ho["records"] for d in r["rankers"].values()
                 if d["input_tokens"] and not d["cached"])
    print(f"results -> {out.relative_to(cfg.ROOT)}" + ("" if a.no_doc else f", {cfg.EVAL_DOC.relative_to(cfg.ROOT)}"))
    print(f"API calls this run: {called} (the rest from the cache); all time: {res['spend']['total_calls']} calls, "
          f"${res['spend']['total_cost']:.4f}")
    for name, rk in res["rankers"].items():
        s = rk["slices"]["all"]
        print(f"  dev  {name:22} top1 {fmt_rate(s['top1_lenient'])}  abstention {fmt_rate(s['abstention'])}  "
              f"${rk['cost']:.4f}")
    for sname, sub in ho["subsets"].items():
        for name, x in sub["rankers"].items():
            print(f"  holdout {sname:6} {name:20} top1 {fmt_rate(x['top1_lenient'])}  "
                  f"routed {fmt_rate(x['routed'])}  auto {fmt_rate(x['auto_lenient'])}  ${x['cost']:.4f}")


if __name__ == "__main__":
    main()
