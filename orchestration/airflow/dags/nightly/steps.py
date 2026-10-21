"""The steps of the nightly pipeline as commands, plus small summaries of what each step did.

Every command is a plain CLI you can also run by hand, started with the project venv (/opt/project-venv) from the repo root
(/opt/project), so the data paths (data/hl7v2, data/empi, data/analytics) land in the mounted data directory.
Summaries hold counts and names of files, never patient data.
"""
import json
import os
import re
import subprocess
from datetime import timedelta, timezone
from pathlib import Path

PROJECT_DIR = Path(os.environ.get("PROJECT_DIR", "/opt/project"))
PROJECT_PYTHON = os.environ.get("PROJECT_PYTHON", "/opt/project-venv/bin/python")
DEFAULT_FHIR_BASE = "http://host.docker.internal:8090/fhir"
REPORTS_KEPT = 30


def fhir_base(env=None):
    return ((os.environ if env is None else env).get("FHIR_BASE") or DEFAULT_FHIR_BASE).rstrip("/")


def feed_day(logical_date):
    """The feed day for a run: the UTC day before the logical date, like `date -u -d yesterday`.
    The schedule fires at 06:15 UTC and Airflow 3 sets the logical date to that time, so day D loads D minus 1."""
    if logical_date.tzinfo is None:
        raise ValueError("logical_date must be timezone aware")
    return (logical_date.astimezone(timezone.utc) - timedelta(days=1)).date().isoformat()


def commands(base, day=None, bi_target="csv"):
    """Task id -> the command it runs (argv after the Python interpreter), in DAG order."""
    return {
        "load_feed": ["hl7v2/pipeline.py", "daily", "--day", day or "yesterday", "--base", base],
        "prune_reports": ["hl7v2/pipeline.py", "prune-reports", "--keep", str(REPORTS_KEPT), "--base", base],
        "match_patients": ["empi/match.py", "run", "--base", base],
        "extract": ["analytics/build.py", "--base", base, "extract"],
        "dbt_build_and_test": ["analytics/build.py", "build"],
        # mf validate-configs on the semantic layer, semantic and warehouse checks
        "validate_semantic_layer": ["analytics/metrics.py", "validate"],
        "source_freshness": ["analytics/build.py", "freshness"],
        "publish": ["analytics/build.py", "--base", base, "publish"],
        # optional: the slim serving copy the metrics API and the BI export read
        "serving_snapshot": ["analytics/build.py", "snapshot"],
        # optional: suppressed aggregates to csv or Google Sheets. --no-env-file: the sheet
        # and key come from orchestration/airflow/.env only, never from analytics/.env in the mounted repo
        "export_bi": ["analytics/export_bi.py", "--no-env-file", "export", "--target", bi_target],
    }


def is_true(value):
    """An Airflow Variable used as a switch: only "true" (any case) turns it on."""
    return str(value or "").strip().lower() == "true"


def bi_target(value):
    """The Variable bi_export_target: csv unless it says sheets."""
    v = str(value or "").strip().lower() or "csv"
    if v not in ("csv", "sheets"):
        raise ValueError(f"bi_export_target must be csv or sheets, not {v!r}")
    return v


def should_force_failure(flag, try_number):
    """FORCE_EXTRACT_FAILURE, a test switch: "once" fails the first try only, "always" every try, else never."""
    flag = (flag or "").strip().lower()
    return flag == "always" or (flag == "once" and try_number <= 1)


def child_env(env=None):
    """The environment for a CLI step: everything except Airflow's own settings and secrets."""
    env = dict(os.environ if env is None else env)
    out = {k: v for k, v in env.items() if not k.startswith(("AIRFLOW__", "_AIRFLOW"))}
    out["PYTHONDONTWRITEBYTECODE"] = "1"   # the repo is mounted read only
    out["PYTHONUNBUFFERED"] = "1"
    return out


def run_cli(argv, cwd=None, python=None):
    """Run one CLI step, copy its output to the task log, return the output. Raises when it exits non zero."""
    cmd = [python or PROJECT_PYTHON, *argv]
    print("$ " + " ".join(cmd), flush=True)
    proc = subprocess.Popen(cmd, cwd=str(cwd or PROJECT_DIR), env=child_env(), stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True)
    lines = []
    for line in proc.stdout:
        print(line, end="", flush=True)
        lines.append(line)
    code = proc.wait()
    if code != 0:
        raise RuntimeError(f"{argv[0]} {argv[1] if len(argv) > 1 else ''} exited with code {code}")
    return "".join(lines)


def read_json(path):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


# ---------- summaries for XCom: counts and file names only ----------

def summarize_feed(day, output, daily_dir):
    """What load_feed did for the day: messages and resources from the day's load report, files in its folder."""
    folder = Path(daily_dir) / day
    report = read_json(folder / "load_report.json") or {}
    return {
        "day": day,
        "already_loaded": "was already generated and loaded" in output,
        "messages": len(report.get("messages", [])),
        "resources_loaded": report.get("loaded"),
        "load_failed": report.get("load_failed"),
        "files_written": len(list(folder.glob("*"))) if folder.is_dir() else 0,
    }


def last_int(pattern, text):
    m = None
    for m in re.finditer(pattern, text):
        pass
    return int(m.group(1)) if m else None


def summarize_prune(output):
    return {"deleted": last_int(r"deleted (\d+) older", output), "failed": last_int(r"failed (\d+)", output)}


def summarize_match(output):
    return {"candidate_pairs": last_int(r"Candidate pairs (\d+)", output),
            "auto_linked": last_int(r"auto linked (\d+)", output),
            "review": last_int(r"auto linked \d+, review (\d+)", output),
            "patients_updated": last_int(r"(\d+) Patients updated now", output)}


def summarize_extract(landing_dir):
    """Resources extracted per type (from manifest.json) and the landing files written."""
    landing = Path(landing_dir)
    manifest = read_json(landing / "manifest.json") or {}
    return {"extracted_at": manifest.get("extracted_at"),
            "resources": {k: v.get("extracted") for k, v in (manifest.get("counts") or {}).items() if isinstance(v, dict)},
            "files_written": sorted(p.name for p in landing.glob("*") if p.is_file())}


def summarize_freshness(output):
    """{table: status} from the lines `[PASS] Patient ...` that build.py freshness prints."""
    return {m.group(2): m.group(1).lower() for m in re.finditer(r"\[(\w+)\]\s+(\w+)", output)}


def summarize_semantic(output):
    """Errors and warnings mf validate-configs counted over its validation steps, and whether it passed."""
    def total(word):
        return sum(int(x) for x in re.findall(rf"\b{word}: (\d+)", output))
    return {"errors": total("ERRORS"), "future_errors": total("FUTURE_ERRORS"), "warnings": total("WARNINGS"),
            "passed": "Semantic layer validation passed" in output}


def summarize_snapshot(output):
    """Version, tables and size of the serving snapshot from the first line build.py snapshot prints."""
    m = re.search(r"Serving snapshot (\S+): (\d+) tables, ([\d.]+) MB", output)
    return {"version": m.group(1), "tables": int(m.group(2)), "size_mb": float(m.group(3))} if m else {}


def summarize_export_bi(output, target):
    """Rows per tab and the cells the check across grains hid, from what export_bi.py prints. Counts only."""
    rows = {m.group(1): int(m.group(2)) for m in re.finditer(r"^\s+(bi_\w+)\s+(\d+)\s+\d+\s*$", output, flags=re.M)}
    m = re.search(r"check across grains: (\d+) day cells, (\d+) week cells, (\d+) month cells", output)
    hidden = dict(zip(("day", "week", "month"), map(int, m.groups()))) if m else {}
    return {"target": target, "tabs": rows, "hidden_cross_grain": hidden,
            "written": "Wrote " in output and "guard passed" in output}


def summarize_publish(output):
    m = re.search(r"data as of (.+)", output)
    return {"binaries": len(re.findall(r"^\s+Binary/", output, flags=re.M)),
            "data_date": m.group(1).strip() if m else None}
