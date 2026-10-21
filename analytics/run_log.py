"""The build log: one record per `build.py build`, appended to <warehouse folder>/run_history/builds.ndjson.

dq_test_results holds the latest build only, and Binary/analytics-history is written by publish, not by build. This log
keeps every build, including failed ones, so the BI export (bi_pipeline_runs) can chart the pipeline over time. It
holds metadata about the build only: times, status, dbt test counts, models built, whether the extract was complete,
how old the landing data was, row counts of four tables and the git commit. No patient data.

Writes are appends of one line, under an exclusive lock, with O_APPEND and fsync; the file is never rewritten. A line
cut short by a crash is skipped when read (and counted), so a torn write can never hide the records before it.
"""
import fcntl
import json
import os
from datetime import datetime, timezone
from pathlib import Path

LOG_DIR = "run_history"
LOG_FILE = "builds.ndjson"
ROW_TABLES = ("bronze_resources", "silver_observation", "fact_encounter", "fact_lab_result")
# The landing files dbt source freshness watches (models/marts/_sources.yml): the stalest of them is the age.
FRESHNESS_TYPES = ("Patient", "Encounter", "Observation")
TEST_STATUSES = ("pass", "warn", "fail", "error", "skipped")
FIELDS = ("build_id", "started_at", "finished_at", "duration_s", "status", "tests_pass", "tests_warn", "tests_fail",
          "tests_error", "tests_skipped", "models_built", "extract_complete", "landing_age_hours",
          *(f"rows_{t}" for t in ROW_TABLES), "git_commit")


def log_path(db):
    """The build log of a warehouse file: next to it, so a copy of the warehouse keeps its own log."""
    return Path(db).parent / LOG_DIR / LOG_FILE


def utc_now():
    return datetime.now(timezone.utc)


def iso(t):
    return t.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_id(started):
    return "b" + started.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def summarize_results(results):
    """dbt results -> test counts by status and models built. results: (resource_type, status) pairs."""
    out = {f"tests_{s}": 0 for s in TEST_STATUSES}
    out["models_built"] = 0
    for rtype, status in results:
        status = str(status)
        if rtype == "test":
            key = f"tests_{status}" if status in TEST_STATUSES else "tests_error"
            out[key] += 1
        elif rtype == "model" and status == "success":
            out["models_built"] += 1
    return out


def extract_complete(manifest):
    """True when every resource type extracted equals the server's total. False when unknown (no manifest)."""
    counts = (manifest or {}).get("counts") or {}
    return bool(counts) and all(c.get("extracted") == c.get("server_total") for c in counts.values())


def read_manifest(landing):
    try:
        return json.loads((Path(landing) / "manifest.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def landing_age_hours(newest, at):
    """Hours from the newest meta.lastUpdated of the stalest watched type to `at`, rounded to 0.1. newest: {type:
    naive UTC datetime or None}. None when a watched type has no timestamp (nothing to measure)."""
    times = [newest.get(t) for t in FRESHNESS_TYPES]
    if not times or any(t is None for t in times):
        return None
    at = at.astimezone(timezone.utc).replace(tzinfo=None)
    return round((at - min(times)).total_seconds() / 3600, 1)


def warehouse_facts(con):
    """Row counts of ROW_TABLES and the newest meta.lastUpdated per watched type, read from the built warehouse. The
    timestamp expression is the one dbt source freshness uses (first 19 characters, UTC)."""
    rows = {}
    for t in ROW_TABLES:
        try:
            rows[f"rows_{t}"] = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        except Exception:
            rows[f"rows_{t}"] = None
    newest = {}
    try:
        newest = dict(con.execute(
            "SELECT resource_type, MAX(TRY_CAST(SUBSTR(json_extract_string(raw, '$.meta.lastUpdated'), 1, 19) "
            "AS TIMESTAMP)) FROM bronze_resources GROUP BY 1").fetchall())
    except Exception:
        pass
    return rows, newest


def git_commit(root):
    """The checked out commit, 12 characters, read from the .git files (no git binary needed, works on a read only
    mount). 'unknown' when it cannot be read."""
    try:
        git = Path(root) / ".git"
        if git.is_file():                                   # a worktree: "gitdir: <path>"
            git = Path(git.read_text(encoding="utf-8").split(":", 1)[1].strip())
        head = (git / "HEAD").read_text(encoding="utf-8").strip()
        if not head.startswith("ref:"):
            sha = head
        else:
            ref = head.split(":", 1)[1].strip()
            common = Path(git / "commondir").read_text().strip() if (git / "commondir").exists() else "."
            bases = [git, (git / common).resolve()]
            sha = next((((b / ref).read_text(encoding="utf-8").strip()) for b in bases if (b / ref).is_file()), "")
            if not sha:
                for b in bases:
                    packed = b / "packed-refs"
                    if packed.is_file():
                        for line in packed.read_text(encoding="utf-8").splitlines():
                            parts = line.split()
                            if len(parts) == 2 and parts[1] == ref:
                                sha = parts[0]
        sha = sha.lower()
        return sha[:12] if len(sha) >= 12 and all(c in "0123456789abcdef" for c in sha) else "unknown"
    except (OSError, IndexError):
        return "unknown"


def make_record(started, finished, status, summary, manifest, rows, newest, commit):
    """One build record, every field of FIELDS in order."""
    rec = {"build_id": build_id(started), "started_at": iso(started), "finished_at": iso(finished),
           "duration_s": round((finished - started).total_seconds(), 1), "status": status}
    for s in TEST_STATUSES:
        rec[f"tests_{s}"] = summary.get(f"tests_{s}")
    rec["models_built"] = summary.get("models_built")
    rec["extract_complete"] = extract_complete(manifest)
    rec["landing_age_hours"] = landing_age_hours(newest, finished)
    for t in ROW_TABLES:
        rec[f"rows_{t}"] = rows.get(f"rows_{t}")
    rec["git_commit"] = commit
    return {k: rec[k] for k in FIELDS}


def append(path, record):
    """Append one record as one line: exclusive lock, O_APPEND, a single write, fsync. Never rewrites the file."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    line = (json.dumps(record, separators=(",", ":"), sort_keys=False) + "\n").encode("utf-8")
    if b"\n" in line[:-1]:
        raise ValueError("a build record must be one line")
    with open(path.with_suffix(".lock"), "a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)
            try:
                # A crash mid line leaves a fragment without a newline: start on a fresh line so it stays alone.
                if os.fstat(fd).st_size and not _ends_with_newline(path):
                    line = b"\n" + line
                written = os.write(fd, line)
                if written != len(line):
                    raise OSError(f"short write to the build log ({written} of {len(line)} bytes)")
                os.fsync(fd)
            finally:
                os.close(fd)
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def _ends_with_newline(path):
    with open(path, "rb") as f:
        f.seek(-1, os.SEEK_END)
        return f.read(1) == b"\n"


def read(path):
    """(records in file order, lines skipped). A missing log is ([], 0); a line that is not a JSON object with a
    build_id (a torn write) is skipped."""
    path = Path(path)
    if not path.exists():
        return [], 0
    records, bad = [], 0
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            r = json.loads(line)
        except ValueError:
            bad += 1
            continue
        if isinstance(r, dict) and r.get("build_id"):
            records.append(r)
        else:
            bad += 1
    return records, bad
