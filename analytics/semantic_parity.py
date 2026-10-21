#!/usr/bin/env python3
"""Prove the semantic layer counts what the marts count: every metric from MetricFlow against a hand written SQL query
on the marts, all time by source and monthly for the live Riverside General feed (demo). Exits non zero on any
difference. Counts must be equal; ratios and sums of days may differ by at most 1e-9. Unsuppressed, internal only.

  python3 analytics/semantic_parity.py [--db data/analytics/warehouse.duckdb]

Checks (metric, grain): the encounter metrics by source over all time and by month for the live feed; bed days by
source and by month; bed occupancy by unit over all time and by unit and month; admissions_rolling_28d per day of the
live feed (the admissions of the 28 days ending that day).
"""
import argparse
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import build  # noqa: E402
import metrics as mx  # noqa: E402

TOLERANCE = 1e-9
LIVE = "{{ Dimension('encounter__is_live_feed') }} = TRUE"
ENCOUNTER_METRICS = ["admissions", "ed_visits", "index_stays", "readmissions_30d", "length_of_stay_days_total",
                     "average_length_of_stay_days", "readmission_rate_30d"]
# The index stays of int_quality_events: hospital admissions with a discharge and a length of stay
INDEX = "is_admission AND discharge_at IS NOT NULL AND length_of_stay_days IS NOT NULL"

def encounter_sql(by, live):
    """Hand written SQL for the encounter metrics. by: 'source' or 'month'. Admissions and emergency visits by the day
    the visit started, the stay metrics by the day of discharge (as the quality marts do)."""
    where = "is_live_feed" if live else "TRUE"
    adm = "source" if by == "source" else "STRFTIME(DATE_TRUNC('month', admit_at), '%Y-%m-%d')"
    dis = "source" if by == "source" else "STRFTIME(DATE_TRUNC('month', discharge_at), '%Y-%m-%d')"
    return f"""
        WITH e AS (SELECT * FROM fact_encounter WHERE {where}),
        started AS (
            SELECT {adm} AS k,
                   SUM(CASE WHEN is_admission THEN 1 ELSE 0 END) AS admissions,
                   SUM(CASE WHEN encounter_kind = 'emergency visit' THEN 1 ELSE 0 END) AS ed_visits
            FROM e GROUP BY 1
        ),
        ended AS (
            SELECT {dis} AS k,
                   COUNT(*) AS index_stays,
                   SUM(CASE WHEN readmitted_30d THEN 1 ELSE 0 END) AS readmissions_30d,
                   SUM(length_of_stay_days) AS length_of_stay_days_total
            FROM e WHERE {INDEX} GROUP BY 1
        )
        SELECT COALESCE(s.k, d.k) AS k, s.admissions, s.ed_visits, d.index_stays, d.readmissions_30d,
               d.length_of_stay_days_total,
               d.length_of_stay_days_total / NULLIF(d.index_stays, 0) AS average_length_of_stay_days,
               CAST(d.readmissions_30d AS DOUBLE) / NULLIF(d.index_stays, 0) AS readmission_rate_30d
        FROM started s FULL OUTER JOIN ended d ON d.k = s.k
    """


def bed_days_sql(by, live):
    where = "is_live_feed" if live else "TRUE"
    key = "source" if by == "source" else "STRFTIME(DATE_TRUNC('month', census_date), '%Y-%m-%d')"
    return f"SELECT {key} AS k, COUNT(*) AS bed_days FROM fact_bed_day WHERE {where} GROUP BY 1"


def occupancy_sql(by_month):
    """Occupied over available bed days per unit (and month): fact_bed_day over fact_bed_capacity_day."""
    m_b = "STRFTIME(DATE_TRUNC('month', b.census_date), '%Y-%m-%d')" if by_month else "''"
    m_c = "STRFTIME(DATE_TRUNC('month', c.capacity_date), '%Y-%m-%d')" if by_month else "''"
    key = "COALESCE(c.unit, o.unit, '') || '|' || COALESCE(c.m, o.m)" if by_month else "COALESCE(c.unit, o.unit, '')"
    return f"""
        WITH occ AS (
            SELECT l.unit_name AS unit, {m_b} AS m, COUNT(*) AS bed_days
            FROM fact_bed_day b JOIN dim_location l ON l.location_sk = b.location_sk GROUP BY 1, 2
        ),
        cap AS (
            SELECT l.unit_name AS unit, {m_c} AS m, COUNT(*) AS capacity_bed_days
            FROM fact_bed_capacity_day c JOIN dim_location l ON l.location_sk = c.location_sk GROUP BY 1, 2
        )
        SELECT {key} AS k, o.bed_days, c.capacity_bed_days,
               CAST(o.bed_days AS DOUBLE) / NULLIF(c.capacity_bed_days, 0) AS bed_occupancy_rate
        FROM cap c FULL OUTER JOIN occ o ON o.unit IS NOT DISTINCT FROM c.unit AND o.m = c.m
    """


ROLLING_SQL = """
    WITH adm AS (
        SELECT CAST(admit_at AS DATE) AS d, COUNT(*) AS n
        FROM fact_encounter WHERE is_live_feed AND is_admission GROUP BY 1
    ),
    days AS (
        SELECT calendar_date AS d FROM dim_date
        WHERE calendar_date >= (SELECT MIN(d) FROM adm) AND calendar_date <= (SELECT MAX(d) FROM adm) + 27
    )
    SELECT STRFTIME(days.d, '%Y-%m-%d') AS k, SUM(adm.n) AS admissions_rolling_28d
    FROM days JOIN adm ON adm.d BETWEEN days.d - 27 AND days.d
    GROUP BY 1
"""


# ---------- comparing (pure) ----------

def key_of(v):
    """Group keys as text: a timestamp '2026-09-01 00:00:00' becomes its day '2026-09-01'."""
    if v is None:
        return ""
    s = str(v)
    return s[:10] if len(s) >= 10 and s[4:5] == "-" and s[7:8] == "-" else s


def present(v):
    """Missing, NULL and 0 all mean nothing to count: the two sides may leave such rows out differently."""
    return v is not None and v != 0


def differences(expected, actual, metrics, tol=TOLERANCE):
    """expected, actual: {key: {metric: value}}. Returns a list of (key, metric, expected, actual) that differ:
    counts must be equal, other numbers within tol."""
    out = []
    for k in sorted(set(expected) | set(actual)):
        for m in metrics:
            e, a = (expected.get(k) or {}).get(m), (actual.get(k) or {}).get(m)
            if not present(e) and not present(a):
                continue
            if not present(e) or not present(a) or abs(float(e) - float(a)) > tol:
                out.append((k, m, e, a))
    return out


def by_key(rows, key_cols):
    return {"|".join(key_of(r.get(c)) for c in key_cols): r for r in rows}


# ---------- running ----------

def sql_rows(con, sql):
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def checks():
    """(name, metrics, mf group by, mf where, key columns of the mf rows, hand written SQL)."""
    return [
        ("encounters, all time by source", ENCOUNTER_METRICS, ["encounter__source"], [], ["encounter__source"],
         encounter_sql("source", live=False)),
        ("encounters, monthly, live feed", ENCOUNTER_METRICS, ["metric_time__month"], [LIVE], ["metric_time__month"],
         encounter_sql("month", live=True)),
        ("bed days, all time by source", ["bed_days"], ["encounter__source"], [], ["encounter__source"],
         bed_days_sql("source", live=False)),
        ("bed days, monthly, live feed", ["bed_days"], ["metric_time__month"], [LIVE], ["metric_time__month"],
         bed_days_sql("month", live=True)),
        ("bed occupancy, all time by unit", ["bed_days", "capacity_bed_days", "bed_occupancy_rate"],
         ["location__unit_name"], [], ["location__unit_name"], occupancy_sql(by_month=False)),
        ("bed occupancy, monthly by unit", ["bed_days", "capacity_bed_days", "bed_occupancy_rate"],
         ["location__unit_name", "metric_time__month"], [], ["location__unit_name", "metric_time__month"],
         occupancy_sql(by_month=True)),
        ("admissions rolling 28 days, daily, live feed", ["admissions_rolling_28d"], ["metric_time__day"], [LIVE],
         ["metric_time__day"], ROLLING_SQL),
    ]


def run(db, target=None):
    """Every check; returns the number of differences (0 means parity)."""
    target = Path(target) if target else build.default_target()
    mx.parse_project(target, db)
    total = 0
    for name, metrics, group_by, where, keys, sql in checks():
        actual = by_key(mx.raw_query(metrics, group_by, where, db=db, target=target, parse=False), keys)
        con = build.connect(db, read_only=True)
        try:
            expected = {r["k"]: r for r in sql_rows(con, sql)}
        finally:
            con.close()
        diff = differences(expected, actual, metrics)
        total += len(diff)
        print(f"  [{'PASS' if not diff else 'FAIL'}] {name}: {len(expected)} keys, {len(metrics)} metrics, "
              f"{len(diff)} differences")
        for k, m, e, a in diff[:10]:
            print(f"      {k} {m}: SQL {e} vs MetricFlow {a}")
    return total


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=str(build.DB))
    ap.add_argument("--target", default="")
    a = ap.parse_args(argv)
    if not Path(a.db).exists():
        sys.exit(f"No warehouse at {a.db}. Run: python3 analytics/build.py build")
    n = run(a.db, a.target or None)
    print(f"\nSemantic parity: {n} differences")
    return 1 if n else 0


if __name__ == "__main__":
    sys.exit(main())
