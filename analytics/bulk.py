#!/usr/bin/env python3
"""FHIR Bulk Data client: the "E" of the pipeline through $export instead of paged searches.

The flow of the SMART / HL7 Bulk Data Access IG (v2):
  kickoff   GET [base]/$export (or [base]/Group/[id]/$export) with Prefer: respond-async, _type, and _since for an
            incremental export; the server answers 202 with the status URL in Content-Location
  poll      GET the status URL until 200, waiting as long as Retry-After asks (1 to 120 s) and printing X-Progress,
            for at most --max-wait seconds (the job is kept, so a rerun goes on polling the same job)
  download  every output NDJSON file and every error file (OperationOutcomes), streamed to a .part file and renamed
  delete    DELETE the status URL once everything is landed, so the server can drop the files

State per server, next to the landing folder (data/analytics/bulk/<server>/):
  job.json        the job in flight: request, status URL, the server's manifest, the files already downloaded
  watermark.json  transactionTime of the last completed export: the _since of the next incremental one
  store/          the current state per resource type, merged by id (a full export replaces it, a delta upserts)
  parts/          the downloaded files of the job in flight

A rerun after a crash resumes the job in job.json and skips the files it already has. Every state file is written to
a temp file and renamed. Requests are retried with backoff on 429, 5xx and connection errors, honoring Retry-After.

Then the store is copied to the landing folder in the same layout and with the same manifest.json as the search
extract (extract.py), so build.py builds from either. The run history and the EMPI Binaries are not bulk types: they
come from the same reads by id as before.

_since does not report deletes. A resource deleted on the server stays in the store until the next full export
(--since full).

Optional SMART backend services auth (analytics/smart_auth.py) when BULK_CLIENT_ID and BULK_PRIVATE_KEY_PATH are set.

Usage:
  python3 analytics/build.py --base http://localhost:8090/fhir extract --mode bulk
  python3 analytics/bulk.py --base http://localhost:8090/fhir export --since full --landing data/bulktest/landing
  python3 analytics/bulk.py --base http://localhost:8090/fhir status
  python3 analytics/bulk.py --base http://localhost:8090/fhir cancel
"""
import argparse
import json
import os
import random
import re
import shutil
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(ROOT))
import extract  # noqa: E402
from tools import DEFAULT_BASE, FRESH, USER_AGENT, urlopen  # noqa: E402

JOB_FILE = "job.json"
WATERMARK_FILE = "watermark.json"
POLL_MIN, POLL_MAX, POLL_DEFAULT = 1.0, 120.0, 2.0
RETRY_STATUSES = {429, 500, 502, 503, 504}


class BulkError(RuntimeError):
    pass


class JobGone(BulkError):
    """The server no longer knows the job (404 or 410 on the status URL): start a new one."""


class Timeout(BulkError):
    pass


# ---------- pure helpers (unit tested) ----------

def parse_retry_after(value, now=None):
    """Seconds to wait from a Retry-After header: delay seconds or an HTTP date. None when absent or unreadable."""
    if value is None or str(value).strip() == "":
        return None
    v = str(value).strip()
    if re.fullmatch(r"\d+(\.\d+)?", v):
        return float(v)
    try:
        when = parsedate_to_datetime(v)
    except (TypeError, ValueError):
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    now = now or datetime.now(timezone.utc)
    return max(0.0, (when - now).total_seconds())


def poll_delay(retry_after):
    """How long to wait before the next status request: what the server asked, kept between 1 and 120 seconds."""
    if retry_after is None:
        return POLL_DEFAULT
    return min(POLL_MAX, max(POLL_MIN, retry_after))


def backoff(attempt, retry_after=None, base=1.0, cap=60.0, rand=random.random):
    """Wait before retry number attempt (1, 2, ...): Retry-After when given, else exponential with jitter."""
    if retry_after is not None:
        return min(cap, retry_after)
    return min(cap, base * 2 ** (attempt - 1)) * (0.5 + rand() / 2)


def same_origin(base, url):
    """url moved onto the scheme, host and port of base: only its path and query are used, so a misconfigured
    server address (or a hostile manifest) cannot send our requests, or a token, to another host."""
    b, u = urllib.parse.urlsplit(base), urllib.parse.urlsplit(url)
    return urllib.parse.urlunsplit((b.scheme, b.netloc, u.path, u.query, ""))


def kickoff_url(base, types=(), since=None, group=None):
    path = f"{base.rstrip('/')}/Group/{urllib.parse.quote(group, safe='')}/$export" if group else f"{base.rstrip('/')}/$export"
    params = [("_outputFormat", "application/fhir+ndjson")]
    if types:
        params.append(("_type", ",".join(types)))
    if since:
        params.append(("_since", since))
    return path + "?" + urllib.parse.urlencode(params, safe=",:")


def server_slug(base):
    """A folder name per server: localhost:8090/fhir -> localhost-8090-fhir."""
    u = urllib.parse.urlsplit(base)
    return re.sub(r"[^A-Za-z0-9]+", "-", f"{u.netloc}{u.path}").strip("-").lower() or "server"


def state_dir(landing, base):
    return Path(landing).resolve().parent / "bulk" / server_slug(base)


def progress_text(headers):
    return (headers.get("x-progress") or "").strip()


def file_name(entry, index):
    t = re.sub(r"[^A-Za-z]", "", entry.get("type") or "Unknown") or "Unknown"
    return f"{t}-{index:04d}.ndjson"


def plan_downloads(manifest):
    """[(kind, type, url, file name, expected count)] for the output and error files of a completed job."""
    out = []
    for i, e in enumerate(manifest.get("output") or []):
        out.append(("output", e.get("type"), e["url"], file_name(e, i), e.get("count")))
    for i, e in enumerate(manifest.get("error") or []):
        out.append(("error", e.get("type") or "OperationOutcome", e["url"], f"errors-{i:04d}.ndjson", e.get("count")))
    return out


def classify_issues(outcomes):
    """(fatal, warnings): the issue texts of the error files' OperationOutcomes. fatal and error stop the export."""
    fatal, warnings = [], []
    for oo in outcomes:
        for issue in oo.get("issue") or [{}]:
            text = issue.get("diagnostics") or (issue.get("details") or {}).get("text") or issue.get("code") or "?"
            (fatal if issue.get("severity", "error") in ("fatal", "error") else warnings).append(text)
    return fatal, warnings


def resource_id(line):
    r = json.loads(line)
    return r.get("resourceType"), r.get("id")


def merge_type(old_file, part_files, out_file, resource_type, full):
    """Write the new state of one resource type to out_file. full: the parts are everything (old_file is ignored).
    Else the parts are a delta: every resource in them replaces the one with its id in old_file, the rest is kept.
    Returns the number of resources written. Lines of another resource type are an error."""
    delta = {}
    for p in part_files:
        with open(p, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rt, rid = resource_id(line)
                if rt != resource_type:
                    raise BulkError(f"{p}: a {rt} in the {resource_type} file")
                delta[rid] = line
    tmp = Path(str(out_file) + ".tmp")
    n = 0
    with open(tmp, "w", encoding="utf-8") as out:
        if not full and old_file and Path(old_file).exists():
            with open(old_file, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line and resource_id(line)[1] not in delta:
                        out.write(line + "\n")
                        n += 1
        for line in delta.values():
            out.write(line + "\n")
            n += 1
    os.replace(tmp, out_file)
    return n


def write_json(path, data):
    """Atomic: a temp file in the same folder, then rename."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(data, indent=1), encoding="utf-8")
    os.replace(tmp, path)


def read_json(path, default=None):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return default


def resolve_since(arg, watermark, store_ready):
    """The _since of this run: None for a full export. auto: the watermark when there is one and a store to merge
    into; full: None; else the timestamp given."""
    if arg in (None, "", "auto"):
        return (watermark or {}).get("transaction_time") if store_ready else None
    if arg == "full":
        return None
    return arg


# ---------- HTTP ----------

class Response:
    def __init__(self, status, headers, body=b""):
        self.status, self.headers, self.body = status, {k.lower(): v for k, v in headers.items()}, body

    def json(self):
        return json.loads(self.body) if self.body else {}


class Client:
    """Requests with retries on 429, 5xx and connection errors (honoring Retry-After) and the bearer token when
    an auth is configured. Status codes the flow handles itself (202, 404, 410) are returned, not raised."""

    def __init__(self, auth=None, retries=5, timeout=300, sleep=None, say=print):
        self.auth, self.retries, self.timeout, self.say = auth, retries, timeout, say
        self.sleep = sleep or (lambda s: time.sleep(s))

    def _headers(self, extra):
        h = {"User-Agent": USER_AGENT, **(extra or {})}
        if self.auth:
            try:
                h.update(self.auth.headers())
            except (RuntimeError, OSError, ValueError) as e:
                raise BulkError(f"SMART backend services auth failed: {e}") from None
        return h

    def _open(self, method, url, headers):
        refreshed = False
        for attempt in range(1, self.retries + 1):
            req = urllib.request.Request(url, method=method, headers=self._headers(headers))
            try:
                return urlopen(req, self.timeout)
            except urllib.error.HTTPError as e:
                if e.code == 401 and self.auth and not refreshed:
                    self.auth.invalidate()   # an expired token: one new one, then the same request again
                    refreshed = True
                    continue
                if e.code not in RETRY_STATUSES or attempt == self.retries:
                    return e
                wait = backoff(attempt, parse_retry_after(e.headers.get("Retry-After")))
                self.say(f"  {method} {urllib.parse.urlsplit(url).path}: HTTP {e.code}, retry in {wait:.1f}s")
            except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
                if attempt == self.retries:
                    raise BulkError(f"{method} {url} -> {e}") from None
                wait = backoff(attempt)
                self.say(f"  {method} {urllib.parse.urlsplit(url).path}: {e}, retry in {wait:.1f}s")
            self.sleep(wait)
        raise BulkError(f"{method} {url}: no response")

    def request(self, method, url, headers=None):
        r = self._open(method, url, headers)
        try:
            return Response(r.status if hasattr(r, "status") else r.code, dict(r.headers), r.read())
        finally:
            r.close()

    def download(self, url, dest, headers=None):
        """Stream url to dest (a .part file first, then renamed). Returns the number of non empty lines."""
        r = self._open("GET", url, headers)
        try:
            code = r.status if hasattr(r, "status") else r.code
            if code != 200:
                raise BulkError(f"GET {url} -> HTTP {code}: {r.read()[:300].decode(errors='replace')}")
            tmp = Path(str(dest) + ".part")
            lines, last = 0, b"\n"
            with open(tmp, "wb") as f:
                while True:
                    chunk = r.read(1 << 20)
                    if not chunk:
                        break
                    f.write(chunk)
                    lines += chunk.count(b"\n")
                    last = chunk[-1:]
                if last != b"\n":
                    f.write(b"\n")
                    lines += 1
        finally:
            r.close()
        os.replace(tmp, dest)
        return lines


def check(resp, what):
    if resp.status >= 400:
        raise BulkError(f"{what} -> HTTP {resp.status}: {resp.body[:500].decode(errors='replace')}")


# ---------- the export ----------

class Exporter:
    def __init__(self, base, state, client, say=print, sleep=None, clock=None):
        self.base, self.state, self.client = base.rstrip("/"), Path(state), client
        self.say = say
        self.sleep = sleep or (lambda s: time.sleep(s))
        self.clock = clock or (lambda: time.monotonic())
        self.parts = self.state / "parts"
        self.store = self.state / "store"

    # state files
    def job(self):
        return read_json(self.state / JOB_FILE)

    def save_job(self, job):
        write_json(self.state / JOB_FILE, job)

    def watermark(self):
        return read_json(self.state / WATERMARK_FILE)

    def drop_job(self):
        (self.state / JOB_FILE).unlink(missing_ok=True)
        shutil.rmtree(self.parts, ignore_errors=True)

    # steps
    def kickoff(self, types, since, group):
        url = kickoff_url(self.base, types, since, group)
        # no-cache: HAPI otherwise hands back a recent job with the same parameters instead of exporting afresh
        resp = self.client.request("GET", url, {"Accept": "application/fhir+json", "Prefer": "respond-async", **FRESH})
        if resp.status != 202 or not resp.headers.get("content-location"):
            check(resp, "kickoff")
            raise BulkError(f"kickoff: expected 202 with Content-Location, got HTTP {resp.status}")
        job = {"base": self.base, "request": {"types": list(types), "since": since, "group": group},
               "kickoff_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
               "status_url": resp.headers["content-location"], "manifest": None, "done": {}}
        self.save_job(job)
        self.say(f"kickoff: {'Group/' + group if group else 'system'} export of {', '.join(types)}"
                 + (f" since {since}" if since else " (full)"))
        return job

    def poll(self, job, max_wait):
        start, last = self.clock(), None
        url = same_origin(self.base, job["status_url"])
        while True:
            resp = self.client.request("GET", url, {"Accept": "application/json"})
            if resp.status == 200:
                manifest = resp.json()
                job["manifest"] = manifest
                self.save_job(job)
                n_out, n_err = len(manifest.get("output") or []), len(manifest.get("error") or [])
                self.say(f"complete: transactionTime {manifest.get('transactionTime')}, {n_out} files, {n_err} error files")
                return manifest
            if resp.status in (404, 410):
                raise JobGone(f"the server no longer knows the job (HTTP {resp.status})")
            if resp.status != 202:
                check(resp, "status")
                raise BulkError(f"status: unexpected HTTP {resp.status}")
            text = progress_text(resp.headers)
            if text and text != last:
                self.say(f"  in progress: {text}")
                last = text
            wait = poll_delay(parse_retry_after(resp.headers.get("retry-after")))
            if self.clock() - start + wait > max_wait:
                raise Timeout(f"not complete after {max_wait:.0f}s; the job is kept, run again to go on waiting "
                              f"(or: python3 analytics/bulk.py --base {self.base} cancel)")
            self.sleep(wait)

    def download_all(self, job):
        manifest = job["manifest"]
        self.parts.mkdir(parents=True, exist_ok=True)
        token = {"Accept": "application/fhir+ndjson"}
        got = skipped = 0
        for kind, rtype, url, name, expected in plan_downloads(manifest):
            dest = self.parts / name
            if url in job["done"] and dest.exists():
                skipped += 1
                continue
            lines = self.client.download(same_origin(self.base, url), dest, token)
            if expected is not None and kind == "output" and lines != expected:
                raise BulkError(f"{name}: {lines} lines, the manifest says {expected}")
            job["done"][url] = {"kind": kind, "type": rtype, "file": name, "lines": lines}
            self.save_job(job)   # after every file, so a crash loses at most the file in flight
            got += 1
            self.say(f"  {name:<28} {lines:>8,} lines")
        if skipped:
            self.say(f"  {skipped} file{'s' if skipped != 1 else ''} already downloaded, skipped")
        return got, skipped

    def errors(self, job):
        outcomes = []
        for d in job["done"].values():
            if d["kind"] == "error":
                with open(self.parts / d["file"], encoding="utf-8") as f:
                    outcomes += [json.loads(line) for line in f if line.strip()]
        return classify_issues(outcomes)

    def merge(self, job):
        """The store after this job: per requested type, a full replace or a delta upsert. Returns {type: count}."""
        full = job["request"]["since"] is None
        self.store.mkdir(parents=True, exist_ok=True)
        files = {}
        for d in job["done"].values():
            if d["kind"] == "output":
                files.setdefault(d["type"], []).append(self.parts / d["file"])
        counts = {}
        for t in job["request"]["types"]:
            counts[t] = merge_type(self.store / f"{t}.ndjson", sorted(files.get(t, [])), self.store / f"{t}.ndjson",
                                   t, full)
        return counts

    def finish(self, job):
        url = same_origin(self.base, job["status_url"])
        resp = self.client.request("DELETE", url)
        if resp.status >= 400 and resp.status not in (404, 410):
            self.say(f"  note: DELETE of the job answered HTTP {resp.status}; the server drops it on its own later")
        self.drop_job()

    def run(self, types, since_arg="auto", group=None, max_wait=3600):
        """Kick off (or resume) an export and merge it into the store. Returns a summary for the manifest."""
        job = self.job()
        if job and job.get("base") != self.base:
            raise BulkError(f"{self.state / JOB_FILE} belongs to {job.get('base')}")
        if job:
            self.say(f"resuming the job from {job['kickoff_at']}"
                     + (f" ({len(job['done'])} files already downloaded)" if job["done"] else ""))
            if job["request"]["types"] != list(types) or job["request"]["group"] != group:
                raise BulkError("the job in flight asked for other types or another group; finish it with the same "
                                f"options or cancel it: python3 analytics/bulk.py --base {self.base} cancel")
        else:
            wm = self.watermark()
            store_ready = all((self.store / f"{t}.ndjson").exists() for t in types)
            job = self.kickoff(types, resolve_since(since_arg, wm, store_ready), group)
        if not job["manifest"]:
            try:
                self.poll(job, max_wait)
            except JobGone as e:
                self.say(f"  {e}: starting a new export")
                self.drop_job()
                return self.run(types, since_arg, group, max_wait)
        got, skipped = self.download_all(job)
        fatal, warnings = self.errors(job)
        if fatal:
            raise BulkError(f"the export reported {len(fatal)} errors, nothing merged: " + "; ".join(fatal[:5]))
        for w in warnings[:5]:
            self.say(f"  server warning: {w}")
        counts = self.merge(job)
        manifest = job["manifest"]
        delta = {}
        for d in job["done"].values():
            if d["kind"] == "output":
                delta[d["type"]] = delta.get(d["type"], 0) + d["lines"]
        summary = {"transaction_time": manifest.get("transactionTime"), "since": job["request"]["since"],
                   "group": group, "files_downloaded": got, "files_skipped": skipped, "export_warnings": warnings,
                   "delta": {t: delta.get(t, 0) for t in types}, "counts": counts}
        return job, summary

    def commit(self, job, summary):
        """After the landing files are written: advance the watermark, delete the job on the server."""
        write_json(self.state / WATERMARK_FILE, {
            "transaction_time": summary["transaction_time"], "completed_at":
            datetime.now(timezone.utc).isoformat(timespec="seconds"), "types": job["request"]["types"],
            "since": summary["since"], "group": summary["group"]})
        self.finish(job)


def first_line(err, n=80):
    return str(err).split("\n")[0][:n]


def write_landing(store, landing, types, base, summary, total=None, say=print):
    """The landing folder as extract.extract_all writes it, from the store: one NDJSON file per type, feed_runs.json,
    Binary.ndjson and manifest.json (plus the bulk details)."""
    landing = Path(landing)
    landing.mkdir(parents=True, exist_ok=True)
    for old in [*landing.glob("*.ndjson"), landing / extract.RUNS_FILE]:
        old.unlink(missing_ok=True)
    manifest = {"extracted_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"), "base": base, "counts": {}}
    for t in types:
        src, dest = Path(store) / f"{t}.ndjson", landing / f"{t}.ndjson"
        tmp = Path(str(dest) + ".tmp")
        shutil.copyfile(src, tmp)
        os.replace(tmp, dest)
        with open(dest, encoding="utf-8") as f:
            n = sum(1 for line in f if line.strip())
        expected = None
        if total:
            try:
                expected = total(t)
            except (RuntimeError, ValueError) as e:
                say(f"  {t}: no server total ({first_line(e)})")
        manifest["counts"][t] = {"extracted": n, "server_total": expected}
        flag = "" if expected in (None, n) else f"  (server reported {expected:,})"
        say(f"{t:<12} {n:>9,} resources, {summary['delta'].get(t, 0):>7,} in this export{flag}")
    try:
        runs = extract.read_run_history(base)
    except RuntimeError as e:
        say(f"  feed runs: not read ({first_line(e)})")
        runs = []
    (landing / extract.RUNS_FILE).write_text(json.dumps(runs), encoding="utf-8")
    manifest["feed_runs"] = len(runs)
    try:
        binaries = extract.extract_binaries(base, landing / "Binary.ndjson")
    except RuntimeError as e:
        say(f"  Binary: not read ({first_line(e)})")
        binaries = []
    manifest["binaries"] = binaries
    say(f"{'feed runs':<12} {len(runs):>9,}   Binary {', '.join(binaries) or 'none'}")
    manifest.update({"mode": "bulk", "transaction_time": summary["transaction_time"], "since": summary["since"],
                     "group": summary["group"], "files_downloaded": summary["files_downloaded"],
                     "files_skipped": summary["files_skipped"], "export_warnings": summary["export_warnings"]})
    write_json(landing / "manifest.json", manifest)
    return manifest


def make_client(base, say=print):
    import smart_auth
    try:
        auth = smart_auth.from_env(base)
    except (RuntimeError, OSError, ValueError) as e:
        raise BulkError(f"SMART backend services auth is not set up: {e}") from None
    if auth:
        say(f"SMART backend services auth as client {auth.client_id[:12]}...")
    return Client(auth=auth, say=say)


def bulk_extract(base, landing, types=extract.RESOURCE_TYPES, since="auto", group=None, max_wait=3600,
                 client=None, say=print):
    """build.py extract --mode bulk: export (or resume), merge, write the landing folder, then commit."""
    base = base.rstrip("/")
    start = time.time()
    ex = Exporter(base, state_dir(landing, base), client or make_client(base, say), say=say)
    job, summary = ex.run(list(types), since, group, max_wait)
    def total(t):
        resp = ex.client.request("GET", f"{base}/{t}?_summary=count", {"Accept": "application/fhir+json", **FRESH})
        if resp.status >= 400:
            raise RuntimeError(f"HTTP {resp.status}")
        return resp.json().get("total")
    manifest = write_landing(ex.store, landing, types, base, summary, total, say=say)
    ex.commit(job, summary)
    say(f"bulk export done in {time.time() - start:.1f}s, watermark {summary['transaction_time']}")
    return manifest


# ---------- commands ----------

def cmd_export(a):
    bulk_extract(a.base, a.landing, split(a.types), a.since, a.group or None, a.max_wait)
    return 0


def cmd_status(a):
    st = state_dir(a.landing, a.base)
    print(f"state: {st}")
    job = read_json(st / JOB_FILE)
    wm = read_json(st / WATERMARK_FILE)
    print(f"watermark: {wm.get('transaction_time') if wm else 'none (the next export is full)'}")
    if not job:
        print("no job in flight")
        return 0
    print(f"job from {job['kickoff_at']}: {job['request']}, "
          f"{'complete' if job['manifest'] else 'not complete yet'}, {len(job['done'])} files downloaded")
    return 0


def cmd_cancel(a):
    st = state_dir(a.landing, a.base)
    job = read_json(st / JOB_FILE)
    if not job:
        print("no job in flight")
        return 0
    ex = Exporter(a.base, st, make_client(a.base))
    ex.finish(job)
    print("job deleted on the server and dropped here; the watermark is unchanged")
    return 0


def split(s):
    return [x.strip() for x in (s or "").split(",") if x.strip()]


def add_export_args(p):
    p.add_argument("--since", default="auto", help="auto (the watermark, if any), full, or a FHIR instant")
    p.add_argument("--group", default="", help="Group id for a Group level export (default: system level)")
    p.add_argument("--types", default=",".join(extract.RESOURCE_TYPES), help="_type, comma separated")
    p.add_argument("--max-wait", type=float, default=3600, help="seconds to wait for the server (default 3600)")


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--base", default=DEFAULT_BASE)
    ap.add_argument("--landing", default="data/analytics/landing")
    sub = ap.add_subparsers(dest="cmd", required=True)
    e = sub.add_parser("export")
    add_export_args(e)
    e.set_defaults(func=cmd_export)
    sub.add_parser("status").set_defaults(func=cmd_status)
    sub.add_parser("cancel").set_defaults(func=cmd_cancel)
    a = ap.parse_args(argv)
    a.base = a.base.rstrip("/")
    try:
        return a.func(a)
    except BulkError as err:
        print(f"bulk export failed: {err}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
