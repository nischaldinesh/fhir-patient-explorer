#!/usr/bin/env python3
"""HL7 v2 ingestion pipeline (Option B).

Commands:
  generate   write the synthetic HL7 v2 feed to data/hl7v2/feed/ (one .hl7 file per message)
  route      parse and route every message, print a summary, write data/hl7v2/route_report.json
             and copy failed messages to data/hl7v2/quarantine/
  show FILE  print one message field by field, with HL7 field names
  load       route, map processed messages to US Core FHIR, translate ICD 9 with the server's
             $translate, and upsert everything into the FHIR server (safe to rerun).
             --dry-run maps without a server and writes the resources to a file instead.
  daily      the live feed: generate one day of traffic (default today, UTC; --day yesterday for a
             nightly batch where every event is in the past) from the saved feed state,
             write it to data/hl7v2/daily/<day>/, route, map and load it, publish the run report (as the
             latest, Binary/hl7v2-ingestion-report, and as the day's, Binary/hl7v2-ingestion-report-<yyyymmdd>)
             and append the run to Binary/pipeline-run-history. The state is saved only after a successful
             run, so a failed day can simply be run again. --dry-run writes the files, loads nothing,
             and leaves the state as it was.
  backfill   run daily for each of the last N days (ending yesterday) that has not been generated yet
  reference  load the hospital's reference data (organizations, locations and beds, staff, services), demo
             data from reference.py. PUT by id, so a second run creates nothing. daily, backfill and load
             run it first when the server does not have it yet.
  prune-reports  delete the day reports older than the newest 30 days of the run history (the nightly job)
  empi-seed  the EMPI test set (demo, empi_seed.py): 30 ADT^A04 registrations, 20 copies of Synthea patients with
             realistic variations and 10 hard negatives (same name, another person), loaded like the feed (PUT by id,
             the test set tag added, existing Patient.link kept). The ground truth goes to data/empi/truth.json and
             Binary/empi-truth. Then run: python3 empi/match.py run
             --seed N (not 42) writes a held out set instead: its own MRNs, none of the tuning set's Synthea
             patients, truth in data/empi/truth-seedN.json only (for: python3 empi/match.py evaluate --truth FILE)

Usage:
  python3 hl7v2/pipeline.py generate
  python3 hl7v2/pipeline.py route
  python3 hl7v2/pipeline.py show data/hl7v2/feed/0001_ADT_A01.hl7
  python3 hl7v2/pipeline.py load [--dry-run]
  python3 hl7v2/pipeline.py daily [--day YYYY-MM-DD|yesterday] [--base URL] [--dry-run]
  python3 hl7v2/pipeline.py backfill --days 56 [--base URL]
  python3 hl7v2/pipeline.py reference [--base URL]
  python3 hl7v2/pipeline.py prune-reports [--keep 30] [--base URL]
  python3 hl7v2/pipeline.py empi-seed [--base URL] [--dry-run] [--seed N]
"""
import argparse
import base64
import json
import shutil
import sys
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import feed_state  # noqa: E402
import reference  # noqa: E402
from generate import build, build_day, day_summary, occupied_beds  # noqa: E402
from hl7 import describe, parse  # noqa: E402
from route import Router  # noqa: E402
from to_fhir import FeedMapper, fhir_id  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "terminology"))
sys.path.insert(0, str(ROOT / "empi"))
import empi_seed  # noqa: E402
import match as empi  # noqa: E402
from tools import DEFAULT_BASE, batch_bundle, chunked, failed_entries, http  # noqa: E402

REPORT_ID = "hl7v2-ingestion-report"
HISTORY_ID = "pipeline-run-history"
HISTORY_CAP = 400
REPORTS_KEPT = 30


def json_binary(rid, value):
    """A JSON value as a FHIR Binary, so any app can read it from the FHIR server."""
    data = base64.b64encode(json.dumps(value).encode("utf-8")).decode("ascii")
    return {"resourceType": "Binary", "id": rid, "contentType": "application/json", "data": data}


def report_binary(report, rid=REPORT_ID):
    return json_binary(rid, report)


def day_report_id(day):
    """The id of one day's report, kept next to the latest: hl7v2-ingestion-report-20261014."""
    return f"{REPORT_ID}-{date.fromisoformat(str(day)[:10]):%Y%m%d}"


def reports_to_prune(history, keep=REPORTS_KEPT):
    """The day reports to delete: every day in the run history older than the newest `keep` days."""
    days = sorted({r["day"] for r in history or [] if isinstance(r, dict) and r.get("day")}, reverse=True)
    return [day_report_id(d) for d in days[keep:]]


def binary_json(resource):
    return json.loads(base64.b64decode(resource.get("data", "")) or "null")


def append_run(history, record, cap=HISTORY_CAP):
    """Run history: newest first, at most cap runs."""
    return [record, *(history or [])][:cap]


def run_status(loaded, failed):
    if not failed:
        return "success"
    return "partial" if loaded else "failed"

FEED = Path("data/hl7v2/feed")
OUT = Path("data/hl7v2")
DAILY = Path("data/hl7v2/daily")


def read_feed(folder):
    return [(p, p.read_text(encoding="utf-8")) for p in sorted(Path(folder).glob("*.hl7"))]


def write_feed(folder, messages):
    """One .hl7 file per message, replacing what the folder held."""
    folder = Path(folder)
    if folder.exists():
        shutil.rmtree(folder)
    folder.mkdir(parents=True)
    for n, (_, label, text) in enumerate(messages, start=1):
        # newline="" keeps the HL7 segment separator as a real carriage return
        with open(folder / f"{n:04d}_{label}.hl7", "w", encoding="utf-8", newline="") as f:
            f.write(text)


def cmd_generate(a):
    folder = Path(a.feed)
    messages = build(seed=a.seed, patients=a.patients)
    write_feed(folder, messages)
    kinds = Counter(label for _, label, _ in messages)
    print(f"Wrote {len(messages)} messages to {folder}/")
    for k, v in sorted(kinds.items()):
        print(f"  {k:<10} {v:>4}")
    return 0


def route_all(files):
    router = Router()
    results = []
    for path, text in files:
        o = router.route(text)
        results.append({"file": path.name, "status": o.status, "type": o.message_type,
                        "control_id": o.control_id, "reason": o.reason})
    return results


def cmd_route(a):
    files = read_feed(a.feed)
    if not files:
        print(f"No .hl7 files in {a.feed}. Run: python3 hl7v2/pipeline.py generate", file=sys.stderr)
        return 1
    results = route_all(files)
    by_status = Counter(r["status"] for r in results)
    by_type = Counter((r["status"], r["type"] or "(unreadable)") for r in results)

    out = Path(a.out)
    q = out / "quarantine"
    if q.exists():
        shutil.rmtree(q)
    q.mkdir(parents=True)
    for (path, _), r in zip(files, results):
        if r["status"] == "error":
            shutil.copy(path, q / path.name)
    report = {"total": len(results), "by_status": dict(by_status),
              "by_type": [{"status": s, "type": t, "count": c} for (s, t), c in sorted(by_type.items())],
              "messages": results}
    (out / "route_report.json").write_text(json.dumps(report, indent=1), encoding="utf-8")

    print(f"Routed {len(results)} messages")
    for s in ("process", "skip", "duplicate", "error"):
        print(f"  {s:<10} {by_status.get(s, 0):>4}")
    print("\nBy message type")
    for (s, t), c in sorted(by_type.items()):
        print(f"  {s:<10} {t:<12} {c:>4}")
    errors = [r for r in results if r["status"] == "error"]
    if errors:
        print(f"\nQuarantined ({len(errors)}), copied to {q}/")
        for r in errors:
            print(f"  {r['file']:<22} {r['reason']}")
    print(f"\nWrote {out / 'route_report.json'}")
    return 0


def make_translator(base):
    """ICD 9 -> ICD 10 through the FHIR server's ConceptMap/$translate. Cached per code.
    Returns None for a code when the server cannot answer, so the mapper records 'not translated'."""
    from conceptmaps import parse_translate, translate_path

    cache = {}

    def translate(code):
        if code not in cache:
            try:
                res = parse_translate(http("GET", f"{base}/{translate_path('icd9', code)}", retries=1))
                cache[code] = [m for m in res["matches"] if m.get("code")]
            except RuntimeError:
                cache[code] = None
        return cache[code]

    return translate


def now_utc():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def route_and_map(files, router, mapper):
    """Route every message, map the processed ones. Returns the run report (without load results)."""
    routed, by_type, log = Counter(), Counter(), []
    for path, text in files:
        o = router.route(text)
        routed[o.status] += 1
        by_type[(o.status, o.message_type or "(unreadable)")] += 1
        log.append({"file": path.name, "status": o.status, "type": o.message_type, "control_id": o.control_id, "reason": o.reason})
        if o.status == "process":
            mapper.apply(o.message)
    return {
        "generated_at": now_utc(),
        "routing": dict(routed),
        "routing_by_type": [{"status": s, "type": t, "count": c} for (s, t), c in sorted(by_type.items())],
        **mapper.report(),
        "messages": log,
    }


def write_resources(out, resources):
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    with open(out / "fhir_resources.ndjson", "w", encoding="utf-8") as f:
        for r in resources:
            f.write(json.dumps(r) + "\n")


def load_resources(base, resources, batch_size):
    """PUT every resource by id in batches. Returns (loaded, failed, created): created counts the
    resources the server did not have yet (201), so a rerun of the same data reports 0 new."""
    ok = fail = created = 0
    for batch in chunked(resources, batch_size):
        resp = http("POST", base, batch_bundle(batch))
        bad = failed_entries(resp)
        fail += len(bad)
        ok += len(batch) - len(bad)
        created += sum(1 for e in resp.get("entry", []) if e.get("response", {}).get("status", "").startswith("201"))
        for i, status, diag in bad[:3]:
            print(f"  failed {batch[i]['resourceType']}/{batch[i]['id']}: {status} {diag}", file=sys.stderr)
    return ok, fail, created


def print_report(report):
    routed = report["routing"]
    print(f"Routed {sum(routed.values())} messages: " + ", ".join(f"{k} {v}" for k, v in routed.items()))
    print("Mapped to FHIR: " + ", ".join(f"{k} {v}" for k, v in report["resources"].items()))
    m = report["lab_name_matching"]
    print(f"Lab names coded to LOINC: {report['pct_results_coded_loinc']}% "
          f"(at source {m.get('loinc_at_source', 0)}, local code {m.get('local_code', 0)}, "
          f"text synonym {m.get('text_synonym', 0)}, unmapped {m.get('unmapped', 0)})")
    if report["unmapped_lab_names"]:
        print("  Unmapped lab names: " + ", ".join(f"{k} ({v})" for k, v in report["unmapped_lab_names"].items()))
    print("Diagnoses: " + ", ".join(f"{k} {v}" for k, v in sorted(report["diagnosis_outcomes"].items())))
    for r in report["needs_review"][:5]:
        print(f"  needs review: visit {r['visit']} ICD 9 {r['icd9']} {r['text']} -> {' or '.join(r['options'])}")
    print("Values: " + ", ".join(f"{k} {v}" for k, v in sorted(report["other"].items()) if k.startswith("value") or k.startswith("corrected")))


def load_reference(base, batch, occupied=frozenset()):
    """Load the reference data group by group, so every reference points at something already there.
    Returns (loaded, failed, created)."""
    ok = fail = created = 0
    for group in reference.resource_groups(occupied):
        o, f, c = load_resources(base, group, batch)
        ok, fail, created = ok + o, fail + f, created + c
    return ok, fail, created


def reference_missing(base):
    """True when any marker resource of the reference data is not on the server."""
    for rtype, rid in reference.MARKERS:
        try:
            http("GET", f"{base}/{rtype}/{rid}", retries=2)
        except RuntimeError as e:
            if "HTTP 404" in str(e) or "HTTP 410" in str(e):
                return True
            raise
    return False


def ensure_reference(base, batch, state_path=feed_state.DEFAULT_PATH):
    """Load the reference data when the server does not have it yet. Raises RuntimeError when that fails."""
    if not reference_missing(base):
        return
    print("Reference data (organizations, locations, staff, services) not found on the server: loading it first")
    ok, fail, created = load_reference(base, batch, occupied_beds(feed_state.load(state_path)))
    print(f"Reference data: {ok} resources loaded ({created} new), failed {fail}")
    if fail:
        raise RuntimeError(f"{fail} reference resources failed to load")


def cmd_reference(a):
    occupied = occupied_beds(feed_state.load(a.state))
    resources = reference.resources(occupied)
    ok, fail, created = load_reference(a.base, a.batch, occupied)
    kinds = Counter(r["resourceType"] for r in resources)
    print("Riverside General reference data (demo, invented): " + ", ".join(f"{k} {v}" for k, v in kinds.items()))
    print(f"  {sum(1 for r in resources if r['resourceType'] == 'Location' and r.get('operationalStatus'))} beds, "
          f"{len(occupied)} occupied according to the feed state")
    print(f"Loaded {ok} resources into {a.base} ({created} new), failed {fail}")
    return 1 if fail else 0


def fetch_open_visits(base, visits):
    """The Encounter and CareTeam the server holds for each visit still open, to seed the mapper."""
    out = []
    for visit in visits:
        eid = fhir_id("rv", visit)
        for path in (f"Encounter/{eid}", f"CareTeam/{fhir_id(eid, 'careteam')}"):
            try:
                out.append(http("GET", f"{base}/{path}", retries=2))
            except RuntimeError as e:
                if "HTTP 404" not in str(e) and "HTTP 410" not in str(e):
                    raise
    return out


def cmd_load(a):
    files = read_feed(a.feed)
    if not files:
        print(f"No .hl7 files in {a.feed}. Run: python3 hl7v2/pipeline.py generate", file=sys.stderr)
        return 1
    if not a.dry_run:
        try:
            ensure_reference(a.base, a.batch)
        except RuntimeError as e:
            print(f"Cannot load: {e}", file=sys.stderr)
            return 1
    mapper = FeedMapper(translate=None if a.dry_run else make_translator(a.base))
    report = route_and_map(files, Router(), mapper)
    resources = mapper.resources()
    out = Path(a.out)
    write_resources(out, resources)
    print_report(report)

    if a.dry_run:
        report["loaded"] = 0
        print(f"\nDry run: nothing sent. Resources written to {out / 'fhir_resources.ndjson'}")
    else:
        ok, fail, _ = load_resources(a.base, resources, a.batch)
        report["loaded"], report["load_failed"] = ok, fail
        print(f"\nLoaded {ok} resources into {a.base}, failed {fail}")
        http("PUT", f"{a.base}/Binary/{REPORT_ID}", report_binary(report))
        print(f"Published the run report to {a.base}/Binary/{REPORT_ID} for the app's Ingestion page")
    (out / "load_report.json").write_text(json.dumps(report, indent=1), encoding="utf-8")
    print(f"Wrote {out / 'load_report.json'}")
    return 1 if report.get("load_failed") else 0


def read_history(base):
    """The run history on the server, [] when it does not exist yet. Any other error is raised, so a
    server hiccup never replaces 400 runs of history with one."""
    try:
        return binary_json(http("GET", f"{base}/Binary/{HISTORY_ID}", retries=2)) or []
    except RuntimeError as e:
        if "HTTP 404" in str(e) or "HTTP 410" in str(e):
            return []
        raise


def run_day(day, a):
    """Generate, route, map and load one day of the live feed. Returns an exit code."""
    started = datetime.now(timezone.utc)
    state = feed_state.load(a.state)
    folder = Path(a.daily) / day.isoformat()

    if state.last_day and day.isoformat() <= state.last_day:
        # Already loaded. Replaying its files through the router shows every message is a duplicate.
        files = read_feed(folder)
        router = Router(seen=state.seen_control_ids)
        routed = Counter(router.route(text).status for _, text in files)
        print(f"{day} was already generated and loaded (last generated day {state.last_day}).")
        if files:
            print(f"Rerouted its {len(files)} saved messages: " + ", ".join(f"{k} {v}" for k, v in sorted(routed.items())))
        print("0 new resources, nothing loaded.")
        return 0

    seeds = []
    if not a.dry_run:
        try:
            ensure_reference(a.base, a.batch, a.state)
            seeds = fetch_open_visits(a.base, [adm["visit"] for adm in state.open_admissions])
        except RuntimeError as e:
            print(f"Cannot start the run: {e}", file=sys.stderr)
            finished = datetime.now(timezone.utc)
            record = {"run_id": f"{day.isoformat()}-{started:%Y%m%dT%H%M%SZ}", "day": day.isoformat(),
                      "started_at": started.isoformat(timespec="seconds"),
                      "finished_at": finished.isoformat(timespec="seconds"),
                      "duration_s": round((finished - started).total_seconds(), 1), "status": "failed",
                      "reason": str(e)[:300], "messages": 0, "process": 0, "skip": 0, "duplicate": 0, "error": 0,
                      "resources_loaded": 0, "resources_created": 0, "load_failed": 0}
            try:
                http("PUT", f"{a.base}/Binary/{HISTORY_ID}", json_binary(HISTORY_ID, append_run(read_history(a.base), record)))
            except RuntimeError as e2:
                print(f"Could not record the failed run: {e2}", file=sys.stderr)
            return 1
    beds_before = occupied_beds(state)
    messages = build_day(day, state, seed=a.seed)
    write_feed(folder, messages)
    files = read_feed(folder)
    router = Router(seen=state.seen_control_ids)
    mapper = FeedMapper(translate=None if a.dry_run else make_translator(a.base))
    mapper.seed(seeds)
    report = route_and_map(files, router, mapper)
    beds = reference.bed_updates(beds_before, occupied_beds(state))  # bed status (occupied or not) as of tonight
    report["bed_status_updates"] = len(beds)
    resources = mapper.resources() + beds
    write_resources(folder, resources)
    print(f"Riverside General live feed (demo), {day}: {len(files)} messages in {folder}/")
    print_report(report)

    if a.dry_run:
        report["loaded"] = 0
        (folder / "load_report.json").write_text(json.dumps(report, indent=1), encoding="utf-8")
        print(f"\nDry run: nothing sent, feed state not saved. Resources written to {folder / 'fhir_resources.ndjson'}")
        return 0

    ok = fail = created = 0
    try:
        ok, fail, created = load_resources(a.base, resources, a.batch)
    except RuntimeError as e:
        print(f"Load failed: {e}", file=sys.stderr)
        fail = len(resources) - ok
    status = run_status(ok, fail)
    finished = datetime.now(timezone.utc)
    routed = report["routing"]
    record = {
        "run_id": f"{day.isoformat()}-{started:%Y%m%dT%H%M%SZ}",
        "day": day.isoformat(),
        "started_at": started.isoformat(timespec="seconds"),
        "finished_at": finished.isoformat(timespec="seconds"),
        "duration_s": round((finished - started).total_seconds(), 1),
        "status": status,
        "messages": len(files),
        **{k: routed.get(k, 0) for k in ("process", "skip", "duplicate", "error")},
        "resources_loaded": ok,
        "resources_created": created,
        "load_failed": fail,
        "pct_loinc": report["pct_results_coded_loinc"],
        **day_summary(messages, day),
        "open_admissions": len(state.open_admissions),
    }
    report.update(loaded=ok, load_failed=fail, day=day.isoformat(), run_id=record["run_id"], feed="daily")
    (folder / "load_report.json").write_text(json.dumps(report, indent=1), encoding="utf-8")
    print(f"\nLoaded {ok} resources into {a.base} ({created} new), failed {fail}: {status}")

    try:
        if status != "failed":
            http("PUT", f"{a.base}/Binary/{REPORT_ID}", report_binary(report))
            http("PUT", f"{a.base}/Binary/{day_report_id(day)}", report_binary(report, day_report_id(day)))
        history = append_run(read_history(a.base), record)
        http("PUT", f"{a.base}/Binary/{HISTORY_ID}", json_binary(HISTORY_ID, history))
        reports = "" if status == "failed" else f"Binary/{REPORT_ID}, Binary/{day_report_id(day)} and "
        print(f"Published {reports}Binary/{HISTORY_ID} ({len(history)} runs)")
    except RuntimeError as e:
        print(f"Could not publish the run report or history: {e}", file=sys.stderr)
        return 1
    if status != "success":
        print("Feed state not saved: run this day again to retry (same messages, loading is idempotent).", file=sys.stderr)
        return 1
    state.remember(router.seen, day)
    feed_state.save(state, a.state)
    print(f"Saved feed state to {a.state}: {len(state.open_admissions)} patients in hospital, {len(state.roster)} on the roster")
    return 0


def today_utc():
    return datetime.now(timezone.utc).date()


def parse_day(v):
    """YYYY-MM-DD, or "yesterday" / "today" in UTC."""
    if v in ("yesterday", "today"):
        return today_utc() - timedelta(days=1 if v == "yesterday" else 0)
    return date.fromisoformat(v)


def cmd_daily(a):
    return run_day(a.day or today_utc(), a)


def cmd_backfill(a):
    """Run daily for each of the last N days ending yesterday, oldest first, skipping days already
    generated. Ending yesterday keeps every event in the past, like a nightly batch."""
    today = today_utc()
    for k in range(a.days, 0, -1):
        day = today - timedelta(days=k)
        last = feed_state.load(a.state).last_day
        if last and day.isoformat() <= last:
            print(f"{day}: already generated, skipped")
            continue
        print(f"\n===== {day} =====")
        if run_day(day, a):
            print(f"Stopped at {day}. Fix the problem and run backfill again; done days are skipped.", file=sys.stderr)
            return 1
    return 0


def was_deleted(outcome):
    """False when HAPI answered a DELETE with "already deleted" or "not found" (200, nothing removed)."""
    codes = {c.get("code") for i in (outcome or {}).get("issue", []) for c in i.get("details", {}).get("coding", [])}
    return not codes & {"SUCCESSFUL_DELETE_ALREADY_DELETED", "SUCCESSFUL_DELETE_NOT_FOUND"}


def cmd_prune_reports(a):
    """Delete the day reports older than the newest --keep days of the run history. Deletes go to HAPI directly
    (localhost), never through a public proxy."""
    try:
        ids = reports_to_prune(read_history(a.base), a.keep)
    except RuntimeError as e:
        print(f"Cannot read the run history: {e}", file=sys.stderr)
        return 1
    deleted = failed = 0
    for rid in ids:
        try:
            deleted += was_deleted(http("DELETE", f"{a.base}/Binary/{rid}", retries=2))
        except RuntimeError as e:
            if "HTTP 404" in str(e) or "HTTP 410" in str(e):
                continue
            failed += 1
            print(f"  could not delete Binary/{rid}: {e}", file=sys.stderr)
    print(f"Day reports: kept the newest {a.keep} days, deleted {deleted} older, failed {failed}")
    return 1 if failed else 0


EMPI_FEED = Path("data/empi/feed")


def empi_patients(messages):
    """Route and map the EMPI test set registrations: one Patient each, with the test set tag."""
    router, mapper = Router(), FeedMapper(translate=None)
    for _, _, text in messages:
        o = router.route(text)
        if o.status != "process":
            raise RuntimeError(f"EMPI test message {o.control_id} not processed: {o.reason}")
        mapper.apply(o.message)
    patients = mapper.resources()
    for p in patients:
        p["meta"]["tag"].append(dict(empi.TEST_SET_TAG))
    return patients


def keep_links(patient, existing):
    """A rerun replaces the Patient but keeps the Patient.link the matcher added, so links survive a reseed."""
    if existing and existing.get("link"):
        return {**patient, "link": existing["link"]}
    return patient


def fetch_existing(base, rtype, rid):
    try:
        return http("GET", f"{base}/{rtype}/{rid}", retries=2)
    except RuntimeError as e:
        if "HTTP 404" in str(e) or "HTTP 410" in str(e):
            return None
        raise


def held_out_paths(seed, feed, truth):
    """Where a set's files go: the tuning set (seed 42) at the given paths, a held out seed next to them with its seed
    in the name, so it never replaces the tuning set (data/empi/feed-seed7/, data/empi/truth-seed7.json)."""
    if seed == empi_seed.DEFAULT_SEED:
        return Path(feed), Path(truth)
    t = Path(truth)
    return Path(f"{feed}-seed{seed}"), t.with_name(f"{t.stem}-seed{seed}{t.suffix}")


def cmd_empi_seed(a):
    base = a.base.rstrip("/")
    held_out = a.seed != empi_seed.DEFAULT_SEED
    feed, truth_path = held_out_paths(a.seed, a.feed, a.truth)
    try:
        synthea = empi.fetch_patients(base)
        # a held out set shares no person with the tuning set (Binary/empi-truth, else data/empi/truth.json)
        exclude = {t["existing_id"] for t in empi.read_truth(base)} if held_out else set()
    except RuntimeError as e:
        print(f"Cannot read the patients: {e}", file=sys.stderr)
        return 1
    if held_out:
        print(f"Held out set, seed {a.seed}: leaving out the {len(exclude)} Synthea patients of the tuning set")
    try:
        messages, truth = empi_seed.build_seed([empi.record(p) for p in synthea], empi.load_nicknames(), seed=a.seed,
                                               exclude=exclude)
    except ValueError as e:
        print(str(e), file=sys.stderr)
        return 1
    write_feed(feed, messages)
    patients = empi_patients(messages)
    out = truth_path
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(truth, indent=1), encoding="utf-8")
    kinds = Counter(t["kind"] for t in truth["pairs"])
    print(f"EMPI test set (demo): {len(messages)} ADT^A04 messages in {feed}/, "
          f"{kinds['match']} true matches and {kinds['hard_negative']} hard negatives")
    print(f"Ground truth written to {out}")
    if a.dry_run:
        print("Dry run: nothing sent")
        return 0
    patients = [keep_links(p, fetch_existing(base, "Patient", p["id"])) for p in patients]
    ok, fail, created = load_resources(base, patients, a.batch)
    if held_out:
        # the held out truth stays a local file: Binary/empi-truth (the warehouse, /empi) remains the tuning set
        print(f"Loaded {ok} Patients into {base} ({created} new), failed {fail}")
        print(f"Next: python3 empi/match.py run, then python3 empi/match.py evaluate --truth {out}")
    else:
        http("PUT", f"{base}/Binary/{empi.TRUTH_ID}", json_binary(empi.TRUTH_ID, truth))
        print(f"Loaded {ok} Patients into {base} ({created} new), failed {fail}; published Binary/{empi.TRUTH_ID}")
        print("Next: python3 empi/match.py run")
    return 1 if fail else 0


def cmd_show(a):
    print(describe(parse(Path(a.file).read_text(encoding="utf-8"))))
    return 0


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    g = sub.add_parser("generate")
    g.add_argument("--feed", default=str(FEED))
    g.add_argument("--seed", type=int, default=42)
    g.add_argument("--patients", type=int, default=25)
    g.set_defaults(func=cmd_generate)
    r = sub.add_parser("route")
    r.add_argument("--feed", default=str(FEED))
    r.add_argument("--out", default=str(OUT))
    r.set_defaults(func=cmd_route)
    ld = sub.add_parser("load")
    ld.add_argument("--feed", default=str(FEED))
    ld.add_argument("--out", default=str(OUT))
    ld.add_argument("--base", default=DEFAULT_BASE)
    ld.add_argument("--batch", type=int, default=200)
    ld.add_argument("--dry-run", action="store_true")
    ld.set_defaults(func=cmd_load)
    rf = sub.add_parser("reference")
    rf.add_argument("--base", default=DEFAULT_BASE)
    rf.add_argument("--state", default=str(feed_state.DEFAULT_PATH), help="feed state, for which beds are occupied")
    rf.add_argument("--batch", type=int, default=200)
    rf.set_defaults(func=cmd_reference)
    for name, func in (("daily", cmd_daily), ("backfill", cmd_backfill)):
        d = sub.add_parser(name)
        if name == "daily":
            d.add_argument("--day", type=parse_day, default=None, help="YYYY-MM-DD or yesterday, default today (UTC)")
            d.add_argument("--dry-run", action="store_true")
        else:
            d.add_argument("--days", type=int, required=True)
            d.set_defaults(dry_run=False)
        d.add_argument("--base", default=DEFAULT_BASE)
        d.add_argument("--state", default=str(feed_state.DEFAULT_PATH))
        d.add_argument("--daily", default=str(DAILY), help="folder for the daily feed files")
        d.add_argument("--seed", type=int, default=42)
        d.add_argument("--batch", type=int, default=200)
        d.set_defaults(func=func)
    pr = sub.add_parser("prune-reports")
    pr.add_argument("--base", default=DEFAULT_BASE)
    pr.add_argument("--keep", type=int, default=REPORTS_KEPT, help="days of reports to keep")
    pr.set_defaults(func=cmd_prune_reports)
    es = sub.add_parser("empi-seed")
    es.add_argument("--base", default=DEFAULT_BASE)
    es.add_argument("--seed", type=int, default=empi_seed.DEFAULT_SEED,
                    help="42 is the tuning set; any other seed writes a held out set next to it")
    es.add_argument("--feed", default=str(EMPI_FEED), help="folder for the test set's .hl7 files")
    es.add_argument("--truth", default=str(empi.TRUTH_FILE))
    es.add_argument("--batch", type=int, default=200)
    es.add_argument("--dry-run", action="store_true")
    es.set_defaults(func=cmd_empi_seed)
    s = sub.add_parser("show")
    s.add_argument("file")
    s.set_defaults(func=cmd_show)
    a = ap.parse_args(argv)
    return a.func(a)


if __name__ == "__main__":
    sys.exit(main())
