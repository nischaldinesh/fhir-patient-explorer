#!/usr/bin/env python3
"""FHIR Patient Explorer, Phase 1 data tools. Standard library only.

Commands:
  wait       Wait until the FHIR server answers /metadata
  download   Download MIMIC IV demo on FHIR NDJSON files from PhysioNet
  mimic      Load MIMIC NDJSON files into the FHIR server
  synthea    Load Synthea transaction bundles into the FHIR server
  verify     Print resource counts and check the Phase 1 success criteria
"""
import argparse
import gzip
import json
import re
import ssl
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path


def ssl_context():
    """Use certifi's CA bundle when available.

    Python from python.org on macOS ships without system CA certificates,
    which causes CERTIFICATE_VERIFY_FAILED. certifi fixes that.
    """
    try:
        import certifi
        return ssl.create_default_context(cafile=certifi.where())
    except ImportError:
        return ssl.create_default_context()


SSL_CTX = ssl_context()
SSL_HINT = ("SSL certificate check failed. Fix with ONE of:\n"
            "  pip3 install certifi\n"
            "  open \"/Applications/Python 3.12/Install Certificates.command\"")


def urlopen(req, timeout):
    return urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX)

DEFAULT_BASE = "http://localhost:8080/fhir"
MIMIC_URL = "https://physionet.org/files/mimic-iv-fhir-demo/2.1.0/fhir/"
USER_AGENT = "fhir-patient-explorer/0.1"
FHIR_HEADERS = {
    "Content-Type": "application/fhir+json",
    "Accept": "application/fhir+json",
    "User-Agent": USER_AGENT,
}
# HAPI reuses the result of an identical search for up to a minute. A search that must see what was just written
# (extract after a load, the EMPI after a seed, verify after loading) sends this. Reads by id are never cached.
FRESH = {"Cache-Control": "no-cache"}
# Load reference targets first so the data is consistent at every point.
TYPE_ORDER = ["Organization", "Location", "Medication", "Patient", "Encounter", "Specimen"]
# ICU bedside charting is very large; skipped unless --all is passed.
HEAVY_FILES = ("Chartevents",)
VERIFY_TYPES = ["Patient", "Encounter", "Observation", "Condition",
                "MedicationRequest", "Procedure"]


# ---------- HTTP ----------

def http(method, url, body=None, timeout=600, retries=3, headers=None):
    """headers: extra request headers, such as Cache-Control: no-cache (HAPI reuses an identical search for a minute)."""
    data = json.dumps(body).encode() if body is not None else None
    for attempt in range(1, retries + 1):
        req = urllib.request.Request(url, data=data, method=method, headers={**FHIR_HEADERS, **(headers or {})})
        try:
            with urlopen(req, timeout) as r:
                raw = r.read()
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as e:
            detail = e.read().decode(errors="replace")[:500]
            if e.code < 500 or attempt == retries:
                raise RuntimeError(f"{method} {url} -> HTTP {e.code}: {detail}") from None
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            if attempt == retries:
                raise RuntimeError(f"{method} {url} -> {e}") from None
        time.sleep(2 * attempt)


# ---------- pure helpers (unit tested) ----------

def iter_ndjson(path):
    path = Path(path)
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def first_type(path):
    for resource in iter_ndjson(path):
        return resource.get("resourceType")
    return None


def type_rank(resource_type):
    return TYPE_ORDER.index(resource_type) if resource_type in TYPE_ORDER else len(TYPE_ORDER)


def select_mimic_files(paths, include_all=False):
    """Drop heavy files unless asked, then sort reference targets first."""
    chosen = [p for p in paths if include_all or not any(h in p.name for h in HEAVY_FILES)]
    return sorted(chosen, key=lambda p: (type_rank(first_type(p)), p.name))


def chunked(items, size):
    buf = []
    for item in items:
        buf.append(item)
        if len(buf) == size:
            yield buf
            buf = []
    if buf:
        yield buf


def batch_bundle(resources):
    """PUT each resource at its own id so reloading is idempotent."""
    return {
        "resourceType": "Bundle",
        "type": "batch",
        "entry": [
            {
                "resource": r,
                "request": {"method": "PUT", "url": f"{r['resourceType']}/{r['id']}"},
            }
            for r in resources
        ],
    }


def failed_entries(response):
    """Return (index, status, server diagnostics) for every non 2xx entry."""
    out = []
    for i, entry in enumerate(response.get("entry", [])):
        resp = entry.get("response", {})
        status = resp.get("status", "")
        if not status.startswith("2"):
            issues = resp.get("outcome", {}).get("issue", [])
            diag = "; ".join(x.get("diagnostics", "") for x in issues if x.get("diagnostics"))
            out.append((i, status, diag[:300]))
    return out


def order_synthea_files(paths):
    """Shared hospital and practitioner bundles must load before patients."""
    first = [p for p in paths if p.name.startswith(("hospitalInformation", "practitionerInformation"))]
    rest = [p for p in paths if p not in first]
    return sorted(first, key=lambda p: p.name) + sorted(rest, key=lambda p: p.name)


def parse_mimic_index(html):
    names = re.findall(r'href="(?:[^"]*/)?([^"/]+\.ndjson\.gz)"', html)
    return sorted(set(names))


# ---------- commands ----------

def cmd_wait(args):
    deadline = time.time() + args.timeout
    while time.time() < deadline:
        try:
            meta = http("GET", f"{args.base}/metadata", timeout=10, retries=1)
            print(f"FHIR server ready: {meta.get('software', {}).get('name', 'unknown')} "
                  f"FHIR {meta.get('fhirVersion')}")
            return 0
        except RuntimeError:
            print("waiting for FHIR server ...")
            time.sleep(5)
    print("FHIR server did not become ready in time", file=sys.stderr)
    return 1


def cmd_download(args):
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    req = urllib.request.Request(MIMIC_URL, headers={"User-Agent": USER_AGENT})
    try:
        with urlopen(req, 60) as r:
            names = parse_mimic_index(r.read().decode(errors="replace"))
    except urllib.error.URLError as e:
        if isinstance(e.reason, ssl.SSLCertVerificationError):
            print(SSL_HINT, file=sys.stderr)
            return 1
        raise
    if not names:
        print(f"No .ndjson.gz files found at {MIMIC_URL}", file=sys.stderr)
        return 1
    for name in names:
        dest = out / name
        if dest.exists() and dest.stat().st_size > 0:
            print(f"skip   {name} (already downloaded)")
            continue
        print(f"fetch  {name}")
        tmp = out / (name + ".part")
        req = urllib.request.Request(MIMIC_URL + name, headers={"User-Agent": USER_AGENT})
        with urlopen(req, 600) as r, open(tmp, "wb") as f:
            while chunk := r.read(1 << 20):
                f.write(chunk)
        tmp.rename(dest)
    print(f"Done. {len(names)} files in {out}")
    return 0


def cmd_mimic(args):
    files = select_mimic_files(sorted(Path(args.dir).glob("*.ndjson*")), args.all)
    if args.only:
        files = [p for p in files if any(s.lower() in p.name.lower() for s in args.only)]
    if not files:
        print(f"No matching NDJSON files in {args.dir}. Run: python tools.py download", file=sys.stderr)
        return 1
    total_ok = total_fail = 0
    dead_batches = 0  # consecutive batches where every entry failed
    for path in files:
        ok = fail = 0
        resources = iter_ndjson(path)
        if args.max_per_file:
            resources = (r for i, r in zip(range(args.max_per_file), resources))
        for batch in chunked(resources, args.batch):
            try:
                resp = http("POST", args.base, batch_bundle(batch))
                bad = failed_entries(resp)
            except RuntimeError as e:
                bad = [(i, "request failed", str(e)[:300]) for i in range(len(batch))]
            fail += len(bad)
            ok += len(batch) - len(bad)
            if bad:
                i, status, diag = bad[0]
                print(f"  {len(bad)} failed in batch, first {batch[i]['resourceType']}/{batch[i]['id']}: "
                      f"{status} {diag}", file=sys.stderr)
            dead_batches = dead_batches + 1 if len(bad) == len(batch) else 0
            if dead_batches >= 3:
                print(f"{path.name:<45} loaded {ok:>7}  failed {fail}")
                print("\nSTOPPED: 3 batches in a row failed completely, so the server is unhealthy.\n"
                      "Check it with:  docker logs fhir --tail 80   and   docker stats --no-stream\n"
                      "Loading is idempotent, so resume later with:  "
                      f"python3 tools.py mimic --only {path.name.split('.')[0]} ...", file=sys.stderr)
                return 1
        print(f"{path.name:<45} loaded {ok:>7}  failed {fail}")
        total_ok += ok
        total_fail += fail
    print(f"MIMIC total: loaded {total_ok}, failed {total_fail}")
    return 1 if total_fail else 0


def cmd_synthea(args):
    files = order_synthea_files(list(Path(args.dir).glob("*.json")))
    if not files:
        print(f"No Synthea bundles in {args.dir}", file=sys.stderr)
        return 1
    ok = fail = 0
    for path in files:
        bundle = json.loads(path.read_text(encoding="utf-8"))
        try:
            http("POST", args.base, bundle)
            ok += 1
        except RuntimeError as e:
            fail += 1
            print(f"  failed {path.name}: {e}", file=sys.stderr)
        if (ok + fail) % 25 == 0:
            print(f"  {ok + fail}/{len(files)} bundles")
    print(f"Synthea total: {ok} bundles loaded, {fail} failed")
    return 1 if fail else 0


def cmd_verify(args):
    counts = {}
    for t in VERIFY_TYPES:
        counts[t] = http("GET", f"{args.base}/{t}?_summary=count", headers=FRESH).get("total", 0)
        print(f"{t:<20} {counts[t]:>8}")
    checks = [
        ("Patients >= %d" % args.min_patients, counts["Patient"] >= args.min_patients),
        ("Observations >= %d" % args.min_observations, counts["Observation"] >= args.min_observations),
    ]
    for label, passed in checks:
        print(f"[{'PASS' if passed else 'FAIL'}] {label}")
    return 0 if all(p for _, p in checks) else 1


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--base", default=DEFAULT_BASE, help="FHIR base URL")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("wait")
    s.add_argument("--timeout", type=int, default=300)
    s.set_defaults(func=cmd_wait)

    s = sub.add_parser("download")
    s.add_argument("--out", default="data/mimic")
    s.set_defaults(func=cmd_download)

    s = sub.add_parser("mimic")
    s.add_argument("--dir", default="data/mimic")
    s.add_argument("--batch", type=int, default=500)
    s.add_argument("--all", action="store_true", help="also load very large chartevents files")
    s.add_argument("--max-per-file", type=int, default=0, help="limit for quick test runs")
    s.add_argument("--only", nargs="+", default=[],
                   help="load only files whose name contains one of these words, e.g. --only Labevents Procedure")
    s.set_defaults(func=cmd_mimic)

    s = sub.add_parser("synthea")
    s.add_argument("--dir", default="data/synthea/output/fhir")
    s.set_defaults(func=cmd_synthea)

    s = sub.add_parser("verify")
    s.add_argument("--min-patients", type=int, default=200)
    s.add_argument("--min-observations", type=int, default=10000)
    s.set_defaults(func=cmd_verify)

    args = p.parse_args(argv)
    args.base = args.base.rstrip("/")
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())