#!/usr/bin/env python3
"""BI export (demo): suppressed aggregates from the serving snapshot to a Google Sheet that Tableau Public reads.

Only aggregates leave, never identifiers, never a count from 1 to 10. Every number comes from the semantic layer through
analytics/metrics.py (small cell and complementary suppression) or from the quality marts, which are suppressed in dbt.
The export reads the serving snapshot (analytics/serving.py, `build.py snapshot`), never the warehouse itself, so it
never blocks the nightly build.

Tabs (one per dataset, written whole, so a rerun gives the same sheet):
  bi_ops_daily          admissions, emergency visits, occupied bed days per day and source (saved query ops_daily)
  bi_ops_weekly         the same per ISO week          } live feed only, with a check across the three grains:
  bi_ops_monthly        the same per month             } no sum of hidden cells of 1 to 10 can be worked out
  bi_occupancy_monthly  bed occupancy rate per unit and month
  bi_quality            mart_quality_period (all time and years) and saved query quality_all_time
  bi_population         people by sex, age band and source, and for all sources (complementary along source)
  bi_suppression        share of quality mart cells hidden per grain (percentages only)
  bi_pipeline_runs      one row per warehouse build, from the build log (analytics/run_log.py)       } pipeline
  bi_feed_runs          one row per feed day: routing and loading counts, no patient event field     } health:
  bi_data_quality       coding coverage, EMPI quality, LOINC review queue, freshness, completeness  } metadata
  bi_meta               data date, export time, snapshot version, rows per tab, the rules, the demo notice

A guard checks every tab before anything is written: declared columns and types, no key or identifier column, no
identifier looking value, no count of people from 1 to 10.

Two kinds of counts. `count` counts people, stays or bed days: a value from 1 to 10 could point at a person, so it is
never published. `op_count` counts things the pipeline handles: messages, resources, table rows, dbt tests, lab codes,
diagnosis records. Those say how the pipeline ran, not who was treated, so the 1 to 10 rule does not apply to them. To
keep it that way, a tab with an op_count column may have no people count, no demographic or hospital unit column and
no patient event field of the feed: operational counts are only ever totals per build, per feed day, per source or per
resource type, never split by anything that could narrow them down to a person.

Usage:
  python3 analytics/export_bi.py export --target csv --dry-run     # row counts only, nothing written
  python3 analytics/export_bi.py export --target csv               # data/bi/<tab>.csv
  python3 analytics/export_bi.py export --target sheets            # BI_SHEET_ID with the BI_CREDENTIALS key
  python3 analytics/export_bi.py verify --target sheets            # read the sheet back: guard and a hash per tab

BI_SHEET_ID and BI_CREDENTIALS (the path of a service account key) come from the environment or analytics/.env
(--no-env-file: the environment only). They are never printed or logged.
"""
import argparse
import csv
import hashlib
import json
import os
import re
import sys
from calendar import monthrange
from datetime import date, datetime, timedelta, timezone
from fractions import Fraction
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(HERE))
import build  # noqa: E402
import metrics  # noqa: E402
import run_log  # noqa: E402
import serving  # noqa: E402

OUT = Path("data/bi")
BUILD_LOG = run_log.log_path(build.DB)
REVIEW_QUEUE = ROOT / "terminology" / "loinc_review_queue.csv"
ENV_FILE = HERE / ".env"
OPS_QUERY = "ops_daily"
QUALITY_QUERY = "quality_all_time"
GRAINS = ("day", "week", "month")
GRAIN_TAB = {"day": "bi_ops_daily", "week": "bi_ops_weekly", "month": "bi_ops_monthly"}

# ---------- the declared schemas: (column, type, kind) ----------
# kind: dim, count (people, stays or bed days: never 1 to 10), op_count (messages, resources, rows, tests, lab codes:
# operational, any value of 0 or more), rate, value (a mean, median, duration or age), flag, text

OPS = (("period", "date", "dim"), ("source", "str", "dim"), ("admissions", "int", "count"),
       ("ed_visits", "int", "count"), ("bed_days", "int", "count"), ("is_suppressed", "bool", "flag"))
SCHEMAS = {
    "bi_ops_daily": OPS,
    "bi_ops_weekly": OPS + (("is_complete_period", "bool", "flag"),),
    "bi_ops_monthly": OPS + (("is_complete_period", "bool", "flag"),),
    "bi_occupancy_monthly": (("period", "date", "dim"), ("unit_name", "str", "dim"),
                             ("bed_occupancy_rate", "float", "rate"), ("is_suppressed", "bool", "flag"),
                             ("is_complete_period", "bool", "flag")),
    "bi_quality": (("origin", "str", "dim"), ("period_type", "str", "dim"), ("period_start", "date", "dim"),
                   ("measure_id", "str", "dim"), ("measure_name", "str", "dim"), ("source", "str", "dim"),
                   ("unit", "str", "dim"), ("numerator", "int", "count"), ("denominator", "int", "count"),
                   ("rate", "float", "rate"), ("value", "float", "value"), ("is_suppressed", "bool", "flag"),
                   ("suppression_reason", "str", "text")),
    "bi_population": (("sex", "str", "dim"), ("age_band", "str", "dim"), ("source", "str", "dim"),
                      ("people", "int", "count"), ("is_suppressed", "bool", "flag")),
    "bi_suppression": (("mart", "str", "dim"), ("grain", "str", "dim"), ("grain_order", "int", "dim"),
                       ("pct_suppressed", "float", "rate")),
    "bi_pipeline_runs": (("build_id", "str", "dim"), ("build_date", "date", "dim"), ("started_at", "str", "dim"),
                         ("finished_at", "str", "text"), ("status", "str", "dim"), ("duration_s", "float", "value"),
                         ("tests_pass", "int", "op_count"), ("tests_warn", "int", "op_count"),
                         ("tests_fail", "int", "op_count"), ("tests_error", "int", "op_count"),
                         ("tests_skipped", "int", "op_count"), ("models_built", "int", "op_count"),
                         ("extract_complete", "bool", "flag"), ("landing_age_hours", "float", "value"),
                         *((f"rows_{t}", "int", "op_count") for t in run_log.ROW_TABLES),
                         ("git_commit", "str", "text")),
    "bi_feed_runs": (("day", "date", "dim"), ("status", "str", "dim"), ("duration_s", "float", "value"),
                     ("messages", "int", "op_count"), ("processed", "int", "op_count"), ("skipped", "int", "op_count"),
                     ("duplicates", "int", "op_count"), ("errors", "int", "op_count"),
                     ("resources_loaded", "int", "op_count"), ("load_failed", "int", "op_count"),
                     ("pct_loinc", "float", "rate")),
    "bi_data_quality": (("check_group", "str", "dim"), ("measure", "str", "dim"), ("scope", "str", "dim"),
                        ("band", "str", "dim"), ("value", "float", "value"), ("numerator", "int", "op_count"),
                        ("denominator", "int", "op_count"), ("measure_unit", "str", "dim"), ("as_of", "date", "dim"),
                        ("label", "str", "text")),
    "bi_meta": (("item", "str", "dim"), ("value", "str", "text")),
}
TABS = tuple(SCHEMAS)
DATA_TABS = TABS[:-1]

SUPPRESSION_RULE = ("Counts from 1 to 10 are hidden (CMS cell size rule), and so is a rate or mean whose counts are. "
                    "More cells are hidden when the hidden ones could be worked out by subtraction: along each "
                    "column of a tab, between days, weeks and months, between units and the monthly total, and "
                    "between the quality marts and the semantic layer. A hidden cell is empty with is_suppressed true.")
DEMO_NOTICE = ("Demo data: the MIMIC IV demo (deidentified, dates shifted into the 2100s), Synthea (synthetic) and the "
               "invented Riverside General feed. Not a real hospital.")
TREND_NOTE = ("Trends exist only for the live feed (Riverside General, real dates). The archive spans many decades "
              "with a few stays per year, so it is shown all time and per year only.")


class ExportError(Exception):
    pass


class GuardError(ExportError):
    pass


# ---------- the guard (pure) ----------

ID_SUFFIXES = ("_sk", "_id", "key")
ALLOWED_ID_COLUMNS = {"measure_id", "build_id"}   # name a measure and a warehouse build, not a person or a record
# rows_<table>: the row count of a warehouse table, allowed for the tables the warehouse builds (not a patient column
# even when the table name holds a word such as encounter)
ROW_COUNT_TABLES = set(build.TABLES) | set(build.MART_TABLES)
PATIENT_TOKENS = {"patient", "person", "mrn", "pseudonym", "encounter", "visit", "birth", "birthdate", "dob",
                  "address", "street", "postal", "zip", "phone", "email", "ssn", "identifier", "fhir", "given",
                  "family"}
PATIENT_COLUMNS = {"name", "first_name", "last_name", "full_name", "birth_year", "city", "state"}
IDENTIFIER_VALUE = re.compile(r"(?i)\bRV\d{6}\b|^P-[0-9A-F]{10}$|^[0-9a-f]{32}$|riverside-rv\d|"
                              r"^(Patient|Encounter|Observation|Condition)/")
DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
# What a tab with operational counts may never hold next to them (see the module docstring)
DEMOGRAPHIC_OR_UNIT_COLUMNS = {"sex", "gender", "age", "age_band", "race", "ethnicity", "source_count", "unit_name",
                               "location_level", "room", "bed", "ward"}
FEED_PATIENT_FIELDS = {"new_admissions", "admissions", "discharges", "ed_visits", "transfers", "open_admissions"}


def column_problem(name):
    """Why a column name may not leave the warehouse, or None."""
    if name in ALLOWED_ID_COLUMNS:
        return None
    if name.startswith("rows_") and name[len("rows_"):] in ROW_COUNT_TABLES:
        return None
    if name.endswith(ID_SUFFIXES):
        return f"column {name} looks like a key or an id"
    if name in PATIENT_COLUMNS or set(name.split("_")) & PATIENT_TOKENS:
        return f"column {name} is patient level"
    return None


def operational_problems(schema):
    """A tab with operational counts may hold no people count, no demographic or unit column and no patient event
    field of the feed, so its counts can never be narrowed down to a person."""
    if not any(k == "op_count" for _, _, k in schema):
        return []
    out = [f"column {c} is a people count next to operational counts" for c, _, k in schema if k == "count"]
    out += [f"column {c} splits operational counts by a demographic or a unit" for c, _, _ in schema
            if c in DEMOGRAPHIC_OR_UNIT_COLUMNS]
    out += [f"column {c} is a patient event field of the feed" for c, _, _ in schema if c in FEED_PATIENT_FIELDS]
    return out


def type_ok(value, typ):
    if typ == "int":
        return isinstance(value, int) and not isinstance(value, bool)
    if typ == "float":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if typ == "bool":
        return isinstance(value, bool)
    if typ == "date":
        return isinstance(value, str) and bool(DATE.match(value))
    return isinstance(value, str)


def check_tab(tab, rows, schema=None):
    """Problems of one tab (a list of dicts) against its declared schema. Empty when it may leave."""
    if schema is None:
        if tab not in SCHEMAS:
            return [f"{tab}: not a declared tab"]
        schema = SCHEMAS[tab]
    names = [c for c, _, _ in schema]
    problems = [f"{tab}: {p}" for p in (column_problem(c) for c in names) if p]
    problems += [f"{tab}: {p}" for p in operational_problems(schema)]
    for i, row in enumerate(rows):
        if list(row) != names:
            extra, missing = [c for c in row if c not in names], [c for c in names if c not in row]
            problems.append(f"{tab} row {i}: columns differ from the schema (extra {extra}, missing {missing})"
                            if extra or missing else f"{tab} row {i}: columns out of order")
            for c in extra:
                p = column_problem(c)
                if p:
                    problems.append(f"{tab}: {p}")
            continue
        for col, typ, kind in schema:
            v = row[col]
            if v is None:
                if kind in ("dim", "flag"):
                    problems.append(f"{tab} row {i}: {col} is empty")
                continue
            if not type_ok(v, typ):
                problems.append(f"{tab} row {i}: {col} is {type(v).__name__}, declared {typ}")
                continue
            if kind == "count" and (v < 0 or metrics.is_small(v)):
                problems.append(f"{tab} row {i}: {col} is a count of {v}" if v > 0 else
                                f"{tab} row {i}: {col} is negative")
            if kind == "op_count" and v < 0:
                problems.append(f"{tab} row {i}: {col} is negative")
            if typ == "str" and IDENTIFIER_VALUE.search(v):
                problems.append(f"{tab} row {i}: {col} holds an identifier looking value")
        if len(problems) > 50:
            problems.append(f"{tab}: more problems not listed")
            break
    return problems


def check_all(tabs):
    """Raise GuardError unless every tab passes; nothing is written before this."""
    problems = [f"unexpected tab {t}" for t in tabs if t not in SCHEMAS]
    problems += [f"missing tab {t}" for t in TABS if t not in tabs]
    for t, rows in tabs.items():
        if t in SCHEMAS:
            problems += check_tab(t, rows)
    if problems:
        raise GuardError("guard refused the export:\n  " + "\n  ".join(problems))


# ---------- the check across grains (pure) ----------

def day_range(start, end):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def period_end(start, grain):
    if grain == "day":
        return start
    if grain == "week":
        return start + timedelta(days=6)
    return start.replace(day=monthrange(start.year, start.month)[1])


class Span:
    """Membership in the span of 0/1 vectors, exact (fractions)."""

    def __init__(self, vectors):
        self.rows = []                      # (pivot, row) in echelon form
        for v in vectors:
            r = self.reduce([Fraction(x) for x in v])
            pivot = next((i for i, x in enumerate(r) if x), None)
            if pivot is not None:
                r = [x / r[pivot] for x in r]
                self.rows.append((pivot, r))

    def reduce(self, v):
        v = list(v)
        for pivot, r in self.rows:
            if v[pivot]:
                f = v[pivot]
                v = [a - f * b for a, b in zip(v, r)]
        return v

    def contains(self, v):
        return not any(self.reduce([Fraction(x) for x in v]))


MAX_ATOMS = 16


def interval_leaks(days, aggs):
    """Sums of hidden days a reader can work out from the shown cells, and that are 1 to 10.

    days: {date: (value, shown)}, every day; a day missing is a known zero. aggs: [{start, end, shown}], the weeks and
    months (or any intervals of days) published next to the days. A shown aggregate minus its shown days is the sum of
    its hidden days, and sums and differences of those give more. Split the days at every boundary of a shown
    aggregate (atoms): a reader can only work out unions of atoms whose indicator lies in the span of the shown
    aggregates. Returns the leaking unions as lists of atoms (start, end), smallest sum first."""
    shown = [a for a in aggs if a["shown"]]
    if not shown:
        return []
    cuts = sorted({a["start"] for a in shown} | {a["end"] + timedelta(days=1) for a in shown})
    atoms = [(s, e - timedelta(days=1)) for s, e in zip(cuts, cuts[1:])
             if any(a["start"] <= s and e - timedelta(days=1) <= a["end"] for a in shown)]
    if len(atoms) > MAX_ATOMS:
        raise ExportError(f"the check across grains has {len(atoms)} intervals, more than {MAX_ATOMS}")
    span = Span([[1 if a["start"] <= s and e <= a["end"] else 0 for s, e in atoms] for a in shown])
    hidden = [sum(v for d, (v, ok) in days.items() if s <= d <= e and not ok and v) for s, e in atoms]
    leaks = []
    for mask in range(1, 1 << len(atoms)):
        total = sum(h for i, h in enumerate(hidden) if mask >> i & 1)
        if metrics.is_small(total):
            vec = [mask >> i & 1 for i in range(len(atoms))]
            if span.contains(vec):
                leaks.append((total, [atoms[i] for i in range(len(atoms)) if vec[i]]))
    return [a for _, a in sorted(leaks, key=lambda x: (x[0], x[1]))]


def protect_grains(days, aggs):
    """Hide more cells until interval_leaks finds nothing. Prefers hiding a day (the smallest shown non zero day
    inside a leaking union: shown days are 0 or above 10, so one is enough); when there is none, the first shown
    aggregate, weeks before months and smallest first, whose hiding stops that union from being worked out. Changes
    the shown flags in place; returns (days hidden, aggregates hidden) as lists of dates and of aggs."""
    hid_days, hid_aggs = [], []
    while True:
        leaks = interval_leaks(days, aggs)
        if not leaks:
            return hid_days, hid_aggs
        union = leaks[0]
        inside = [d for d, (v, ok) in days.items() if ok and v and any(s <= d <= e for s, e in union)]
        if inside:
            d = min(inside, key=lambda x: (days[x][0], x))
            days[d] = (days[d][0], False)
            hid_days.append(d)
            continue
        order = sorted((a for a in aggs if a["shown"]), key=lambda x: (x["end"] - x["start"], x["value"], x["start"]))
        for a in order:
            a["shown"] = False
            if union not in interval_leaks(days, aggs):
                break
            a["shown"] = True
        else:
            a = order[0]
            a["shown"] = False
        hid_aggs.append(a)


def complement_against_total(values, shown):
    """Parts of a known total (the units of a month against the month's bed days): while the hidden parts add up to
    1 to 10, hide the smallest shown non zero part too. values, shown: lists; returns the indexes hidden."""
    added = []
    while metrics.is_small(sum(v or 0 for v, ok in zip(values, shown) if not ok)):
        cand = [i for i, (v, ok) in enumerate(zip(values, shown)) if ok and v]
        if not cand:
            break
        i = min(cand, key=lambda j: (values[j], j))
        shown[i] = False
        added.append(i)
    return added


# ---------- the datasets ----------

def day_of(value):
    """'2026-08-20 00:00:00' (MetricFlow), a date or a datetime -> date."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def as_int(v):
    return None if v is None else int(round(v))


def ops_tabs(run, cat, data_date):
    """bi_ops_daily, bi_ops_weekly, bi_ops_monthly from the saved query ops_daily at three grains, and the cells the
    check across grains hid: {"tabs": {tab: rows}, "stats": {...}, "bed_days_month": {(month, source): raw}}."""
    names, group_by, where = metrics.resolve_saved_query(OPS_QUERY)
    others = [g for g in group_by if not g.startswith("metric_time")]
    raw, out = {}, {}
    for grain in GRAINS:
        gb = [f"metric_time__{grain}", *others]
        rows = metrics.drop_empty_time_rows(run(metrics.components(names, cat), gb, where, gb), cat)
        for r in rows:
            r["period"] = day_of(r.pop(f"metric_time__{grain}"))
        rows.sort(key=lambda r: (r["period"], *(str(r[g]) for g in others)))
        raw[grain] = rows
        out[grain] = metrics.complement(rows, metrics.suppress(rows, names, cat), names, cat, ["period", *others])
    stats = {f"hidden_small_{g}": sum(1 for r in out[g] for m in names if r[m] is None) for g in GRAINS}
    added = {g: 0 for g in GRAINS}
    keys = sorted({tuple(r[g] for g in others) for grain in GRAINS for r in raw[grain]}, key=str)
    for key in keys:
        idx = {g: [i for i, r in enumerate(raw[g]) if tuple(r[o] for o in others) == key] for g in GRAINS}
        for m in names:
            days = {}
            for i in idx["day"]:
                days[raw["day"][i]["period"]] = (raw["day"][i][m] or 0, out["day"][i][m] is not None)
            aggs = []
            for g in ("week", "month"):
                for i in idx[g]:
                    start = raw[g][i]["period"]
                    end = period_end(start, g)
                    value = raw[g][i][m] or 0
                    inside = sum(v for d, (v, _) in days.items() if start <= d <= end)
                    if inside != value:
                        raise ExportError(f"{m} for the {g} of {start} is {value}, its days add up to {inside}")
                    aggs.append({"grain": g, "i": i, "start": start, "end": end, "value": value,
                                 "shown": out[g][i][m] is not None})
            if not aggs:
                continue
            lo = min(a["start"] for a in aggs)
            hi = max(a["end"] for a in aggs)
            for d in day_range(lo, hi):
                days.setdefault(d, (0, True))
            hid_days, hid_aggs = protect_grains(days, aggs)
            by_day = {raw["day"][i]["period"]: i for i in idx["day"]}
            for d in hid_days:
                out["day"][by_day[d]][m] = None
                out["day"][by_day[d]]["is_suppressed"] = True
                added["day"] += 1
            for a in hid_aggs:
                out[a["grain"]][a["i"]][m] = None
                out[a["grain"]][a["i"]]["is_suppressed"] = True
                added[a["grain"]] += 1
    stats.update({f"hidden_cross_grain_{g}": added[g] for g in GRAINS})
    stats.update({f"cells_{g}": len(out[g]) * len(names) for g in GRAINS})
    tabs = {}
    for g in GRAINS:
        rows = []
        for r, o in zip(raw[g], out[g]):
            row = {"period": r["period"].isoformat(), "source": str(r[others[0]]) if others else "all"}
            for m in names:
                row[m] = as_int(o[m])
            row["is_suppressed"] = bool(o["is_suppressed"])
            if g != "day":
                row["is_complete_period"] = data_date is not None and period_end(r["period"], g) <= data_date
            rows.append(row)
        tabs[GRAIN_TAB[g]] = rows
    bed_days_month = {(r["period"], str(r[others[0]]) if others else "all"): r.get("bed_days") or 0
                      for r in raw["month"]}
    return {"tabs": tabs, "stats": stats, "bed_days_month": bed_days_month}


def occupancy_tab(run, cat, data_date):
    """bi_occupancy_monthly: bed occupancy rate per unit and month. A shown rate times the unit's capacity (known
    master data) gives its occupied bed days, and the month's bed days are on bi_ops_monthly, so the hidden units of
    a month must not add up to 1 to 10 either. A unit with beds but no patient at midnight has a rate of 0."""
    name = "bed_occupancy_rate"
    gb = ["metric_time__month", "location__unit_name"]
    comps = metrics.components([name], cat)
    num, den = cat[name]["numerator"], cat[name]["denominator"]
    rows = metrics.drop_empty_time_rows(run(comps, gb, [], gb), cat)
    for r in rows:
        r["period"] = day_of(r.pop("metric_time__month"))
        r["unit_name"] = r.pop("location__unit_name") or "unknown"
        if r.get(num) is None and r.get(den):
            r[num], r[name] = 0, 0.0
    rows = [r for r in rows if r.get(den)]
    rows.sort(key=lambda r: (r["period"], r["unit_name"]))
    out = metrics.suppress(rows, [name], cat)
    added = 0
    for month in sorted({r["period"] for r in rows}):
        idx = [i for i, r in enumerate(rows) if r["period"] == month]
        shown = [out[i][name] is not None for i in idx]
        for j in complement_against_total([rows[i][num] for i in idx], shown):
            out[idx[j]][name] = None
            out[idx[j]]["is_suppressed"] = True
            added += 1
    tab = [{"period": r["period"].isoformat(), "unit_name": r["unit_name"],
            "bed_occupancy_rate": None if o[name] is None else round(o[name], 4),
            "is_suppressed": bool(o["is_suppressed"]),
            "is_complete_period": data_date is not None and period_end(r["period"], "month") <= data_date}
           for r, o in zip(rows, out)]
    return tab, {"hidden_occupancy_complement": added}


SEMANTIC_MEASURE = {"readmission_rate_30d": "readmission_30d", "average_length_of_stay_days": "los_mean_days"}
MART_DECIMALS = {"readmission_30d": 4, "los_mean_days": 2}


def quality_mart_rows(con):
    """The bi_quality rows of mart_quality_period: all time and every year. For a mean the numerator is a total of
    days, not a count, so only proportions keep it."""
    sql = """SELECT period_type, CAST(period_start AS VARCHAR), measure_id, measure_name, source, unit, numerator,
                    denominator, rate, value, is_suppressed, suppression_reason
             FROM mart_quality_period WHERE period_type IN ('all_time', 'year')
             ORDER BY CASE period_type WHEN 'all_time' THEN 0 ELSE 1 END, period_start, measure_id, source"""
    return [{"origin": "quality_mart", "period_type": ptype, "period_start": pstart, "measure_id": mid,
             "measure_name": mname, "source": src, "unit": unit,
             "numerator": as_int(numer) if unit == "proportion" else None,
             "denominator": as_int(denom), "rate": rate, "value": value, "is_suppressed": bool(supp),
             "suppression_reason": reason}
            for (ptype, pstart, mid, mname, src, unit, numer, denom, rate, value, supp, reason)
            in con.execute(sql).fetchall()]


def quality_tab(mart_rows, run, cat):
    """bi_quality: the mart rows, then saved query quality_all_time. A semantic layer cell is shown only when the
    mart shows the same cell (the mart's complementary suppression hides cells the semantic layer alone would show),
    and the two must agree."""
    rows = list(mart_rows)
    mart = {(r["measure_id"], r["source"]): r for r in rows if r["period_type"] == "all_time"}
    names, group_by, where = metrics.resolve_saved_query(QUALITY_QUERY)
    raw = metrics.drop_empty_time_rows(run(metrics.components(names, cat), group_by, where, group_by), cat)
    out = metrics.suppress(raw, names, cat)
    src_col = next(g for g in group_by if g.endswith("source"))
    semantic = []
    for o in sorted(out, key=lambda r: str(r[src_col])):
        src = str(o[src_col])
        for m in names:
            mid = SEMANTIC_MEASURE[m]
            cell = mart.get((mid, src))
            v = o[m]
            if v is None:
                shown, reason = False, "small cell"
            elif cell is None or cell["is_suppressed"]:
                shown, reason = False, "hidden in the quality mart"
            else:
                shown, reason = True, None
                d = MART_DECIMALS[mid]
                if abs(round(v, d) - cell["value"]) > 10 ** -d / 2 + 1e-9:
                    raise ExportError(f"semantic layer and quality mart disagree on {mid} for {src}")
            value = round(v, MART_DECIMALS[mid]) if shown else None
            semantic.append({"origin": "semantic_layer", "period_type": "all_time",
                             "period_start": cell["period_start"] if cell else None, "measure_id": mid,
                             "measure_name": cell["measure_name"] if cell else cat[m]["label"], "source": src,
                             "unit": cell["unit"] if cell else None, "numerator": None, "denominator": None,
                             "rate": value if mid == "readmission_30d" else None, "value": value,
                             "is_suppressed": not shown, "suppression_reason": reason})
    return rows + semantic


POPULATION_CAT = {"people": {"suppression": "count"}}


def population_tab(con):
    """bi_population: people (dim_patient, one row per person across linked records) by sex, age band and source,
    complementary along each column; then all sources by sex and age band. Complementary suppression along source
    keeps every shown all sources cell from giving back its hidden sources."""
    def counts(cols, sql_cols):
        q = f"""SELECT {sql_cols}, COUNT(*) FROM dim_patient GROUP BY ALL ORDER BY ALL"""
        return [dict(zip([*cols, "people"], r)) for r in con.execute(q).fetchall()]

    by_source = counts(["sex", "age_band", "source"],
                       "COALESCE(sex, 'unknown'), COALESCE(age_band, 'unknown'), "
                       "REPLACE(COALESCE(source_list, 'unknown'), ',', ' and ')")
    total = counts(["sex", "age_band"], "COALESCE(sex, 'unknown'), COALESCE(age_band, 'unknown')")
    for r in total:
        r["source"] = "all"
    out_s = metrics.complement(by_source, metrics.suppress(by_source, ["people"], POPULATION_CAT), ["people"],
                               POPULATION_CAT, ["sex", "age_band", "source"])
    out_t = metrics.complement(total, metrics.suppress(total, ["people"], POPULATION_CAT), ["people"],
                               POPULATION_CAT, ["sex", "age_band"])
    for t, o in zip(total, out_t):
        if o["people"] is None:
            continue
        hidden = sum(r["people"] for r, x in zip(by_source, out_s)
                     if x["people"] is None and (r["sex"], r["age_band"]) == (t["sex"], t["age_band"]))
        if metrics.is_small(hidden):
            raise ExportError("population: an all sources cell gives back its hidden sources")
    return [{"sex": r["sex"], "age_band": r["age_band"], "source": r["source"], "people": o["people"],
             "is_suppressed": bool(o["is_suppressed"])}
            for r, o in list(zip(total, out_t)) + list(zip(by_source, out_s))]


def suppression_tab(con):
    """bi_suppression: percent of quality mart cells hidden per grain; the cell counts stay inside (some are 1 to 10,
    counts of cells, not of people)."""
    q = "SELECT mart, grain, grain_order, pct_suppressed FROM mart_suppression_report ORDER BY grain_order, mart"
    return [{"mart": m, "grain": g, "grain_order": int(o), "pct_suppressed": None if p is None else float(p)}
            for m, g, o, p in con.execute(q).fetchall()]


# ---------- pipeline health (metadata about runs and data quality, no patient data) ----------

def pipeline_runs_tab(records):
    """bi_pipeline_runs: one row per record of the build log, in the order they were appended."""
    rows = []
    for r in records:
        row = {"build_id": str(r["build_id"]), "build_date": str(r.get("started_at") or "")[:10],
               "started_at": str(r.get("started_at") or ""), "finished_at": r.get("finished_at"),
               "status": str(r.get("status") or "unknown"),
               "duration_s": None if r.get("duration_s") is None else float(r["duration_s"])}
        for c, typ, kind in SCHEMAS["bi_pipeline_runs"][6:]:
            v = r.get(c)
            if kind == "op_count":
                v = None if v is None else int(v)
            elif typ == "float":
                v = None if v is None else float(v)
            elif typ == "bool":
                v = bool(v)
            row[c] = v
        rows.append(row)
    return rows


# The feed run fields that may leave, by an allow list: export column -> run history field. Every other field of a
# run (patient events such as new_admissions, discharges, ed_visits, transfers, open_admissions, and ids) stays out.
FEED_RUN_FIELDS = {"day": "day", "status": "status", "duration_s": "duration_s", "messages": "messages",
                   "processed": "process", "skipped": "skip", "duplicates": "duplicate", "errors": "error",
                   "resources_loaded": "resources_loaded", "load_failed": "load_failed", "pct_loinc": "pct_loinc"}


def feed_runs_tab(runs):
    """bi_feed_runs: one row per feed day, oldest first, the newest run of each day (a retry or a rerun replaces the
    earlier one,). Only FEED_RUN_FIELDS are read."""
    from publish import latest_runs_by_day
    rows = []
    for r in reversed(latest_runs_by_day(runs)):
        row = {}
        for col, typ, kind in SCHEMAS["bi_feed_runs"]:
            v = r.get(FEED_RUN_FIELDS[col])
            if v is not None:
                v = int(v) if typ == "int" else float(v) if typ == "float" else str(v)
            row[col] = v
        rows.append(row)
    return rows


QUEUE_BANDS = ("high", "medium", "low", "no suggestion")


def confidence_band(row):
    """The review queue's confidence bands: high 0.9 and above (the threshold chosen in docs/LLM_LOINC_EVAL.md),
    medium 0.6 to under 0.9, low under 0.6, and no suggestion when the model answered NONE."""
    if not (row.get("suggested_loinc") or "").strip():
        return "no suggestion"
    try:
        c = float(row.get("confidence") or "")
    except ValueError:
        return "low"
    return "high" if c >= 0.9 else "medium" if c >= 0.6 else "low"


def read_queue(path):
    path = Path(path)
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def pct(num, den):
    return None if not den else round(100 * num / den, 1)


def data_quality_tab(con, records, manifest, queue, as_of):
    """bi_data_quality, long: check group, measure, scope (a source, a resource type or the test set), band, value,
    numerator and denominator (operational counts), unit, as of, label. Freshness is the one of the last build, so a
    rerun without a new build gives the same rows."""
    out = []

    def add(group, measure, scope, value, num=None, den=None, unit="percent", band="all", label=""):
        out.append({"check_group": group, "measure": measure, "scope": scope, "band": band,
                    "value": None if value is None else float(value), "numerator": None if num is None else int(num),
                    "denominator": None if den is None else int(den), "measure_unit": unit, "as_of": as_of,
                    "label": label})

    for src, labs, loinc, diags, icd10, review in con.execute(
            "SELECT source, numeric_lab_results, with_loinc, diagnoses, with_icd10, need_review "
            "FROM gold_terminology_quality ORDER BY source").fetchall():
        add("coding", "loinc_coverage", src, pct(loinc, labs), loinc, labs,
            label="Numeric lab results with a LOINC code")
        add("coding", "icd10_coverage", src, pct(icd10, diags), icd10, diags, label="Diagnoses with an ICD 10 code")
        add("coding", "diagnoses_need_review", src, pct(review, diags), review, diags,
            label="Diagnoses waiting for a coder (an ICD 9 code with several ICD 10 options)")
    q = con.execute("SELECT precision, recall FROM gold_empi_quality").fetchone()
    if q:
        add("empi", "precision", "synthetic test set", q[0], unit="ratio",
            label="Share of automatic links that are true matches, on the seeded synthetic test set (demo)")
        add("empi", "recall", "synthetic test set", q[1], unit="ratio",
            label="Share of true matches linked automatically, on the seeded synthetic test set (demo)")
    for status in sorted({(r.get("status") or "pending").strip() or "pending" for r in queue}):
        for band in QUEUE_BANDS:
            n = sum(1 for r in queue if ((r.get("status") or "pending").strip() or "pending") == status
                    and confidence_band(r) == band)
            add("terminology_queue", f"review_{status}", "llm loinc suggestions", None, n, None, unit="lab codes",
                band=band, label="Lab codes in the LLM assisted LOINC review queue")
    fresh = con.execute("SELECT CAST(data_date AS VARCHAR), last_event_at, extracted_at FROM gold_feed_freshness"
                        ).fetchone()
    if fresh:
        lag = None if not (fresh[1] and fresh[2]) else round((fresh[2] - fresh[1]).total_seconds() / 3600, 1)
        add("freshness", "data_date", "live feed", None, unit="date", label=fresh[0] or "no live feed data")
        add("freshness", "feed_lag_hours", "live feed", lag, unit="hours",
            label="Hours from the last feed event to the extract")
    last = records[-1] if records else None
    add("freshness", "landing_age_hours", "landing", last and last.get("landing_age_hours"), unit="hours",
        label="Hours from the newest FHIR meta.lastUpdated (Patient, Encounter, Observation) to the end of the last "
              "build; warn after 36, error after 72")
    for rtype, c in sorted(((manifest or {}).get("counts") or {}).items()):
        add("completeness", "extracted_of_server_total", rtype, pct(c.get("extracted") or 0, c.get("server_total")),
            c.get("extracted"), c.get("server_total"), label="Resources extracted out of the server's total")
    add("completeness", "extract_complete", "all", None if last is None else float(bool(last.get("extract_complete"))),
        unit="flag", label="1 when every resource type was extracted in full at the last build")
    return out


# ---------- cells, hashes, meta ----------

def cell_text(v):
    """One canonical text per value: CSV cells and the content hash."""
    if v is None:
        return ""
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    return str(v)


def table_values(tab, rows):
    """Header plus rows as lists, in schema order."""
    cols = [c for c, _, _ in SCHEMAS[tab]]
    return [cols] + [[r[c] for c in cols] for r in rows]


def content_hash(tabs):
    h = hashlib.sha256()
    for t in DATA_TABS:
        for line in table_values(t, tabs[t]):
            h.update(("\x1f".join(cell_text(v) for v in line) + "\n").encode("utf-8"))
        h.update(b"\x1e")
    return h.hexdigest()


def meta_rows(tabs, snap, stats, previous=None, now=None):
    """bi_meta. The export time stays as it was when nothing else changed, so a rerun leaves the sheet identical."""
    digest = content_hash(tabs)
    previous = previous or {}
    if previous.get("content_hash") == digest and previous.get("exported_at"):
        exported_at = previous["exported_at"]
    else:
        exported_at = (now or datetime.now(timezone.utc)).strftime("%Y-%m-%dT%H:%M:%SZ")
    items = [("data_as_of", snap.get("data_date") or "no live feed data"), ("exported_at", exported_at),
             ("snapshot_version", snap.get("version", "")), ("snapshot_created_at", snap.get("created_at", "")),
             ("content_hash", digest)]
    items += [(f"rows_{t}", str(len(tabs[t]))) for t in DATA_TABS]
    items += [(k, str(v)) for k, v in sorted(stats.items())]
    items += [("suppression_rule", SUPPRESSION_RULE), ("trend_note", TREND_NOTE), ("demo_notice", DEMO_NOTICE)]
    return [{"item": k, "value": v} for k, v in items]


# ---------- building everything from a snapshot ----------

def load_snapshot(serving_dir):
    snap = serving.read_pointer(serving_dir)
    if not snap:
        raise ExportError(f"no serving snapshot in {serving_dir}: run python3 analytics/build.py snapshot")
    missing = [t for t in serving.BI_TABLES if t not in (snap.get("tables") or {})]
    if missing:
        raise ExportError(f"the serving snapshot {snap['version']} has no {', '.join(missing)}: "
                          "run python3 analytics/build.py snapshot")
    return snap


def build_datasets(snap, run=None, cat=None, build_log=None, landing=None, review_queue=None):
    """Every tab but bi_meta, and the counts of hidden cells. run: metrics.raw_query's signature (tests pass a fake).
    The pipeline health tabs read the build log, the landing folder's feed_runs.json and manifest.json, and the
    LOINC review queue."""
    import duckdb
    cat = cat or metrics.catalog()
    records, bad_lines = run_log.read(build_log or BUILD_LOG)
    landing = Path(landing or build.LANDING)
    runs = build.feed_runs(landing)
    as_of = (records[-1]["finished_at"] if records else snap.get("created_at") or "")[:10]
    if run is None:
        def run(names, group_by, where, order):
            return metrics.raw_query(names, group_by, where, order, db=snap["db"], target=snap["target"], parse=False)
    data_date = date.fromisoformat(snap["data_date"]) if snap.get("data_date") else None
    con = duckdb.connect(str(snap["db"]), read_only=True)
    try:
        population = population_tab(con)
        suppression = suppression_tab(con)
        mart_rows = quality_mart_rows(con)
        data_quality = data_quality_tab(con, records, run_log.read_manifest(landing),
                                        read_queue(review_queue or REVIEW_QUEUE), as_of)
    finally:
        con.close()          # before mf opens the same file
    quality_rows = quality_tab(mart_rows, run, cat)
    ops = ops_tabs(run, cat, data_date)
    occupancy, occ_stats = occupancy_tab(run, cat, data_date)
    tabs = dict(ops["tabs"])
    tabs["bi_occupancy_monthly"] = occupancy
    tabs["bi_quality"] = quality_rows
    tabs["bi_population"] = population
    tabs["bi_suppression"] = suppression
    tabs["bi_pipeline_runs"] = pipeline_runs_tab(records)
    tabs["bi_feed_runs"] = feed_runs_tab(runs)
    tabs["bi_data_quality"] = data_quality
    health = {"build_log_records": len(records), "build_log_bad_lines": bad_lines, "feed_run_records": len(runs)}
    return {t: tabs[t] for t in DATA_TABS}, {**ops["stats"], **occ_stats, **health}


# ---------- targets ----------

class CsvTarget:
    """data/bi/<tab>.csv, written to a temporary file and renamed. Same rows, same bytes."""

    def __init__(self, folder):
        self.folder = Path(folder)

    def read_meta(self):
        path = self.folder / "bi_meta.csv"
        if not path.exists():
            return {}
        with open(path, newline="", encoding="utf-8") as f:
            return {r["item"]: r["value"] for r in csv.DictReader(f)}

    def write(self, tab, values):
        self.folder.mkdir(parents=True, exist_ok=True)
        tmp = self.folder / f".{tab}.csv.tmp"
        with open(tmp, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, lineterminator="\n")
            for line in values:
                w.writerow([cell_text(v) for v in line])
        os.replace(tmp, self.folder / f"{tab}.csv")

    def read(self, tab):
        with open(self.folder / f"{tab}.csv", newline="", encoding="utf-8") as f:
            return [list(r) for r in csv.reader(f)]


def sheet_cell(v):
    return "" if v is None else v


def normalize(v):
    """A cell as the Sheets API gives it back unformatted: whole floats are integers, empty is ''."""
    if v is None:
        return ""
    if isinstance(v, float) and v.is_integer():
        return int(v)
    return v


class SheetsTarget:
    """One worksheet per tab, written whole: resize to the exact size, clear, write the values as RAW (no parsing by
    Sheets), freeze the header. Worksheets that are not ours are left alone."""

    def __init__(self, spreadsheet):
        self.sh = spreadsheet

    def worksheet(self, tab, rows, cols):
        for ws in self.sh.worksheets():
            if ws.title == tab:
                return ws
        return self.sh.add_worksheet(title=tab, rows=rows, cols=cols)

    def read_meta(self):
        values = self.read("bi_meta") if any(ws.title == "bi_meta" for ws in self.sh.worksheets()) else []
        return {r[0]: r[1] for r in values[1:] if len(r) >= 2}

    def write(self, tab, values):
        rows, cols = max(len(values), 2), len(values[0])     # Sheets cannot freeze every row
        ws = self.worksheet(tab, rows, cols)
        ws.resize(rows=rows, cols=cols)
        ws.clear()
        ws.update(values=[[sheet_cell(v) for v in line] for line in values], range_name="A1",
                  value_input_option="RAW")
        ws.freeze(rows=1)

    def read(self, tab):
        ws = next(w for w in self.sh.worksheets() if w.title == tab)
        values = ws.get(value_render_option="UNFORMATTED_VALUE")
        width = max((len(r) for r in values), default=0)
        return [[normalize(v) for v in r] + [""] * (width - len(r)) for r in values]


def values_hash(values):
    return hashlib.sha256(json.dumps(values, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()


def expected_sheet_values(tab, rows):
    return [[normalize(sheet_cell(v)) for v in line] for line in table_values(tab, rows)]


def typed_rows(tab, values):
    """Rows read back from a target as dicts of typed values, for the guard."""
    schema = SCHEMAS[tab]
    if not values or values[0] != [c for c, _, _ in schema]:
        raise GuardError(f"{tab}: the header differs from the schema")

    def parse(v, typ):
        if v == "" or v is None:
            return None
        if typ == "bool":
            return v if isinstance(v, bool) else str(v).lower() == "true"
        if typ == "int":
            return int(v) if not isinstance(v, bool) else v
        if typ == "float":
            return float(v) if not isinstance(v, bool) else v
        return str(v)
    return [{c: parse(v, t) for (c, t, _), v in zip(schema, r)} for r in values[1:]]


def export(tabs, target):
    """Guard every tab, then write the data tabs and bi_meta last (its data date changes only when all others
    were written)."""
    check_all(tabs)
    for t in DATA_TABS + ("bi_meta",):
        target.write(t, table_values(t, tabs[t]))


# ---------- settings and secrets ----------

def read_env_file(path):
    env = {}
    if path and Path(path).exists():
        for line in Path(path).read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def settings(env_file, env=None):
    """BI_SHEET_ID and the key path, from the environment first, then env_file (None: the environment only)."""
    env = os.environ if env is None else env
    file = read_env_file(env_file)
    sheet_id = env.get("BI_SHEET_ID") or file.get("BI_SHEET_ID")
    cred = env.get("BI_CREDENTIALS") or file.get("BI_CREDENTIALS")
    if not sheet_id or not cred:
        raise ExportError("BI_SHEET_ID and BI_CREDENTIALS must be set (environment or analytics/.env)")
    path = Path(cred)
    return sheet_id, path if path.is_absolute() else ROOT / path


def redact(text, secrets):
    for s in sorted({s for s in secrets if s}, key=len, reverse=True):
        text = text.replace(s, "<redacted>")
    return text


def open_spreadsheet(env_file):
    """The gspread Spreadsheet, and the strings that must never be printed."""
    sheet_id, path = settings(env_file)
    secrets = [sheet_id, str(path), path.name]
    if not path.is_file():
        raise ExportError("the BI_CREDENTIALS file does not exist")
    try:
        secrets.append(json.loads(path.read_text(encoding="utf-8")).get("client_email", ""))
    except (OSError, ValueError):
        raise ExportError("the BI_CREDENTIALS file is not a service account key (JSON)") from None
    try:
        import gspread
    except ImportError:
        raise ExportError("gspread is not installed: python3 -m pip install -r analytics/requirements-bi.txt") from None
    try:
        return gspread.service_account(filename=str(path)).open_by_key(sheet_id), secrets
    except Exception as e:
        raise ExportError(redact(f"could not open the sheet: {type(e).__name__}: {e}", secrets)) from None


# ---------- commands ----------

def make_target(a):
    if a.target == "csv":
        return CsvTarget(a.out), []
    sh, secrets = open_spreadsheet(None if a.no_env_file else a.env_file)
    return SheetsTarget(sh), secrets


def print_counts(tabs):
    print(f"  {'tab':<22} {'rows':>6} {'suppressed':>11}")
    for t in TABS:
        rows = tabs[t]
        supp = sum(1 for r in rows if r.get("is_suppressed")) if "is_suppressed" in (rows[0] if rows else {}) else 0
        print(f"  {t:<22} {len(rows):>6} {supp:>11}")


def cmd_export(a):
    snap = load_snapshot(a.serving)
    print(f"Serving snapshot {snap['version']}, data as of {snap.get('data_date') or 'no live feed data'}")
    tabs, stats = build_datasets(snap, build_log=a.build_log, landing=a.landing, review_queue=a.review_queue)
    secrets = []
    try:
        target, secrets = (None, []) if a.dry_run and a.target == "sheets" else make_target(a)
        previous = target.read_meta() if target else {}
        tabs["bi_meta"] = meta_rows(tabs, snap, stats, previous)
        check_all(tabs)
        print(f"{'Dry run, nothing written' if a.dry_run else 'Export to ' + a.target}: guard passed")
        print_counts(tabs)
        print("  hidden by the check across grains: " + ", ".join(
            f"{stats[f'hidden_cross_grain_{g}']} {g} cells" for g in GRAINS)
            + f"; by the unit check on occupancy: {stats['hidden_occupancy_complement']}")
        print(f"  pipeline health: {stats['build_log_records']} builds in the log"
              + (f" ({stats['build_log_bad_lines']} unreadable lines skipped)" if stats["build_log_bad_lines"] else "")
              + f", {stats['feed_run_records']} feed run records")
        if a.dry_run:
            return 0
        export(tabs, target)
    except ExportError:
        raise
    except Exception as e:
        raise ExportError(redact(f"export failed: {type(e).__name__}: {e}", secrets)) from None
    where = f"{a.out}/" if a.target == "csv" else "the sheet"
    print(f"Wrote {len(TABS)} tabs to {where} (full replace)")
    return 0


def cmd_verify(a):
    """Read every tab back: header, guard, rows and a hash (the same rows give the same hash)."""
    target, secrets = make_target(a)
    try:
        tabs = {}
        print(f"  {'tab':<22} {'rows':>6}  sha256")
        for t in TABS:
            values = target.read(t)
            tabs[t] = typed_rows(t, values)
            print(f"  {t:<22} {len(tabs[t]):>6}  {values_hash(values)}")
        check_all(tabs)
    except ExportError:
        raise
    except Exception as e:
        raise ExportError(redact(f"verify failed: {type(e).__name__}: {e}", secrets)) from None
    counts = [(t, c) for t in TABS for c, _, k in SCHEMAS[t] if k == "count"]
    small = sum(1 for t, c in counts for r in tabs[t] if metrics.is_small(r[c]))
    print(f"Guard passed on what the {a.target} target holds: {len(counts)} count columns, {small} values from 1 to 10")
    ops = [(t, c) for t in TABS for c, _, k in SCHEMAS[t] if k == "op_count"]
    print(f"  {len(ops)} operational count columns (messages, resources, rows, tests, lab codes; the 1 to 10 rule "
          "applies to people only)")
    leaked = sorted({c for t in TABS for c, _, _ in SCHEMAS[t] if c in FEED_PATIENT_FIELDS
                     and any(k == "op_count" for _, _, k in SCHEMAS[t])})
    print(f"  patient event fields of the feed in an operational tab: {', '.join(leaked) or 'none'}")
    meta = {r["item"]: r["value"] for r in tabs["bi_meta"]}
    print("  hidden by the check across grains: " + ", ".join(
        f"{meta.get(f'hidden_cross_grain_{g}', '?')} {g} cells" for g in GRAINS)
        + f"; by the unit check on occupancy: {meta.get('hidden_occupancy_complement', '?')}")
    return 0


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--serving", default=str(build.SERVING), help="serving folder (default data/analytics/serving)")
    ap.add_argument("--out", default=str(OUT), help="csv folder (default data/bi)")
    ap.add_argument("--env-file", default=str(ENV_FILE), help="where BI_SHEET_ID and BI_CREDENTIALS are read")
    ap.add_argument("--no-env-file", action="store_true", help="read them from the environment only")
    ap.add_argument("--build-log", default=str(BUILD_LOG), help="build log (default data/analytics/run_history/builds.ndjson)")
    ap.add_argument("--landing", default=str(build.LANDING), help="landing folder with feed_runs.json and manifest.json")
    ap.add_argument("--review-queue", default=str(REVIEW_QUEUE), help="LLM assisted LOINC review queue (csv)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    e = sub.add_parser("export")
    e.add_argument("--target", choices=["sheets", "csv"], required=True)
    e.add_argument("--dry-run", action="store_true", help="build and guard every tab, print row counts, write nothing")
    e.set_defaults(func=cmd_export)
    v = sub.add_parser("verify")
    v.add_argument("--target", choices=["sheets", "csv"], required=True)
    v.set_defaults(func=cmd_verify)
    a = ap.parse_args(argv)
    try:
        return a.func(a)
    except ExportError as err:
        print(f"export_bi: {err}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
