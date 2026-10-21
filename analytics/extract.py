"""Extract FHIR resources from the server into landing files (one NDJSON file per resource type).

This is the "E" of the pipeline. The landing files are what Bronze loads, in DuckDB from the local folder
and in Snowflake from a stage. Paging follows the server's "next" links; only the query part of each
link is used, so a misconfigured server address cannot send requests to another host.

The live feed's run history (Binary/pipeline-run-history, see hl7v2/pipeline.py) is saved next to them as
feed_runs.json. It is pipeline metadata, not a FHIR resource, so Bronze does not load it; publish reads it to say
which day was loaded last.

The EMPI's review queue and the ground truth of its test set (Binary/empi-review, Binary/empi-truth, see
empi/match.py) are FHIR Binaries, so they land as Binary.ndjson and Bronze loads them like any other resource.
Only these two: the other Binaries are the app's published reports.
"""
import base64
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from tools import FRESH, http  # noqa: E402

RESOURCE_TYPES = ["Patient", "Encounter", "Condition", "Observation", "Location", "Practitioner", "CareTeam"]
RUN_HISTORY_ID = "pipeline-run-history"
RUNS_FILE = "feed_runs.json"
BINARY_IDS = ["empi-review", "empi-truth"]


def next_path(bundle):
    for link in bundle.get("link", []):
        if link.get("relation") == "next":
            url = link.get("url", "")
            i = url.find("?")
            return url[i:] if i >= 0 else None
    return None


def extract_type(base, resource_type, out_file, page_size=500, progress=None):
    """Write every resource of one type to out_file. Returns the number written."""
    count, pages = 0, 0
    path = f"{resource_type}?_count={page_size}"
    with open(out_file, "w", encoding="utf-8") as f:
        while path:
            # a fresh search, every page: the nightly job extracts right after the feed and the EMPI wrote
            bundle = http("GET", f"{base}{path}" if path.startswith("?") else f"{base}/{path}", headers=FRESH)
            for e in bundle.get("entry", []):
                r = e.get("resource")
                if r and r.get("resourceType") == resource_type:
                    f.write(json.dumps(r, separators=(",", ":")) + "\n")
                    count += 1
            pages += 1
            if progress and pages % 50 == 0:
                progress(resource_type, count)
            path = next_path(bundle)
    return count


def read_run_history(base):
    """The live feed's run history, [] when the server has none. Any other error is raised."""
    try:
        binary = http("GET", f"{base}/Binary/{RUN_HISTORY_ID}", retries=2)
    except RuntimeError as e:
        if "HTTP 404" in str(e) or "HTTP 410" in str(e):
            return []
        raise
    runs = json.loads(base64.b64decode(binary.get("data", "")) or "null")
    return runs if isinstance(runs, list) else []


def extract_binaries(base, out_file, ids=BINARY_IDS):
    """Write the Binaries in ids that the server has to out_file (none: no file). Returns the ids written."""
    found = []
    for rid in ids:
        try:
            found.append(http("GET", f"{base}/Binary/{rid}", retries=2))
        except RuntimeError as e:
            if "HTTP 404" not in str(e) and "HTTP 410" not in str(e):
                raise
    if found:
        Path(out_file).write_text("".join(json.dumps(b, separators=(",", ":")) + "\n" for b in found), encoding="utf-8")
    return [b["id"] for b in found]


def extract_all(base, landing, types=RESOURCE_TYPES, page_size=500, quiet=False):
    landing = Path(landing)
    landing.mkdir(parents=True, exist_ok=True)
    for old in [*landing.glob("*.ndjson"), landing / RUNS_FILE]:
        old.unlink(missing_ok=True)
    say = (lambda *a: None) if quiet else print
    manifest = {"extracted_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"), "base": base, "counts": {}}
    for t in types:
        start = time.time()
        expected = http("GET", f"{base}/{t}?_summary=count", headers=FRESH).get("total")
        n = extract_type(base, t, landing / f"{t}.ndjson", page_size,
                         progress=lambda rt, c: say(f"  {rt}: {c:,} so far"))
        manifest["counts"][t] = {"extracted": n, "server_total": expected}
        flag = "" if expected in (None, n) else f"  (server reported {expected:,})"
        say(f"{t:<12} {n:>9,} resources in {time.time() - start:5.1f}s{flag}")
    runs = read_run_history(base)
    (landing / RUNS_FILE).write_text(json.dumps(runs), encoding="utf-8")
    manifest["feed_runs"] = len(runs)
    say(f"{'feed runs':<12} {len(runs):>9,} records of Binary/{RUN_HISTORY_ID}")
    binaries = extract_binaries(base, landing / "Binary.ndjson")
    manifest["binaries"] = binaries
    say(f"{'Binary':<12} {len(binaries):>9,} ({', '.join(binaries) or 'no EMPI review queue yet'})")
    (landing / "manifest.json").write_text(json.dumps(manifest, indent=1), encoding="utf-8")
    return manifest
