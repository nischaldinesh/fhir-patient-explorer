"""What the metrics API accepts and what it may return (pure, unit tested).

Every request is turned into an mf query here, from allowlists only: public metric names, dimension names, time
grains, filter fields with their allowed values, ISO dates. Nothing a caller sends reaches MetricFlow as text, so
there is no free text where clause and no way to group by an entity key (patient, encounter, bed day ...).

check_rows is the last line of defense after analytics/metrics.py has suppressed the rows: it fails the response if
a count from 1 to 10, or a column that is not on the allowlist, is about to leave.
"""
import re
from datetime import date

# The seven business metrics of analytics/SEMANTIC_LAYER.md. Their components (index_stays, readmissions_30d,
# length_of_stay_days_total, capacity_bed_days) stay internal.
PUBLIC_METRICS = ("admissions", "ed_visits", "average_length_of_stay_days", "readmission_rate_30d", "bed_days",
                  "bed_occupancy_rate", "admissions_rolling_28d")

# API name -> (MetricFlow name, entity it is reached through). No patient__state (a quasi identifier), no entity keys.
DIMENSIONS = {
    "source": ("encounter__source", "encounter"),
    "class_code": ("encounter__class_code", "encounter"),
    "encounter_kind": ("encounter__encounter_kind", "encounter"),
    "is_live_feed": ("encounter__is_live_feed", "encounter"),
    "sex": ("patient__sex", "patient"),
    "age_band": ("patient__age_band", "patient"),
    "source_count": ("patient__source_count", "patient"),
    "unit_name": ("location__unit_name", "location"),
    "location_level": ("location__location_level", "location"),
}

# The entities each metric's semantic model reaches (SEMANTIC_LAYER.md, metrics catalog)
METRIC_ENTITIES = {
    "admissions": {"encounter", "patient"},
    "ed_visits": {"encounter", "patient"},
    "average_length_of_stay_days": {"encounter", "patient"},
    "readmission_rate_30d": {"encounter", "patient"},
    "admissions_rolling_28d": {"encounter", "patient"},
    "bed_days": {"encounter", "patient", "location"},
    "bed_occupancy_rate": {"location"},
}

# A cumulative metric is a window over time: MetricFlow needs metric_time in the query
NEEDS_GRAIN = ("admissions_rolling_28d",)

FILTERS = {
    "source": ("encounter__source", ("mimic", "synthea", "hl7v2")),
    "is_live_feed": ("encounter__is_live_feed", ("true", "false")),
    "encounter_kind": ("encounter__encounter_kind", ("hospital admission", "emergency visit", "part of another stay",
                                                      "other")),
}

GRAINS = ("day", "week", "month", "quarter", "year")
MAX_GROUP_BY = 2
DEFAULT_LIMIT, MAX_LIMIT = 1000, 5000
PERIOD = "period"


class Rejected(ValueError):
    """A request outside the allowlist: answered with 422 and this message (never the raw input echoed back)."""


def split(value):
    return [x.strip() for x in (value or "").split(",") if x.strip()]


def parse_metrics(value):
    names = split(value)
    if not names:
        raise Rejected("give at least one metric")
    unknown = [n for n in names if n not in PUBLIC_METRICS]
    if unknown:
        raise Rejected(f"unknown metric; allowed: {', '.join(PUBLIC_METRICS)}")
    return list(dict.fromkeys(names))


def parse_group_by(value, metrics):
    names = list(dict.fromkeys(split(value)))
    if len(names) > MAX_GROUP_BY:
        raise Rejected(f"at most {MAX_GROUP_BY} group_by dimensions per query")
    bad = [n for n in names if n not in DIMENSIONS]
    if bad:
        raise Rejected(f"group_by not allowed; allowed: {', '.join(DIMENSIONS)}")
    for n in names:
        entity = DIMENSIONS[n][1]
        for m in metrics:
            if entity not in METRIC_ENTITIES[m]:
                raise Rejected(f"{m} cannot be split by {n}")
    return names


def parse_grain(value):
    if value in (None, ""):
        return None
    if value not in GRAINS:
        raise Rejected(f"grain must be one of {', '.join(GRAINS)}")
    return value


def parse_date(value, name):
    if value in (None, ""):
        return None
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise Rejected(f"{name} must be a date, YYYY-MM-DD")
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise Rejected(f"{name} must be a date, YYYY-MM-DD") from None


def parse_where(values, metrics):
    """where=field:value items -> [(mf dimension, value)], each field and value from FILTERS."""
    out = []
    for item in values or []:
        field, sep, val = item.partition(":")
        field, val = field.strip(), val.strip()
        if not sep or field not in FILTERS:
            raise Rejected(f"where must be field:value with field one of {', '.join(FILTERS)}")
        dim, allowed = FILTERS[field]
        if val not in allowed:
            raise Rejected(f"where {field} must be one of {', '.join(allowed)}")
        for m in metrics:
            if "encounter" not in METRIC_ENTITIES[m]:
                raise Rejected(f"{m} cannot be filtered by {field}")
        out.append((dim, val))
    return out


def parse_limit(value):
    if value is None:
        return DEFAULT_LIMIT
    if not 1 <= value <= MAX_LIMIT:
        raise Rejected(f"limit must be 1 to {MAX_LIMIT}")
    return value


def where_clauses(filters, start=None, end=None):
    """MetricFlow where strings, built only from allowlisted names and checked values."""
    out = []
    for dim, val in filters:
        if val in ("true", "false"):
            out.append(f"{{{{ Dimension('{dim}') }}}} = {val.upper()}")
        else:
            out.append(f"{{{{ Dimension('{dim}') }}}} = '{val}'")
    if start:
        out.append(f"{{{{ TimeDimension('metric_time', 'day') }}}} >= '{start.isoformat()}'")
    if end:
        out.append(f"{{{{ TimeDimension('metric_time', 'day') }}}} <= '{end.isoformat()}'")
    return out


def build_query(metrics, group_by=None, grain=None, start=None, end=None, where=None, limit=None):
    """A validated request -> {metrics, group_by (mf names), where, limit, ...}. Raises Rejected."""
    m = parse_metrics(metrics)
    dims = parse_group_by(group_by, m)
    g = parse_grain(grain)
    if not g and any(x in NEEDS_GRAIN for x in m):
        raise Rejected(f"{', '.join(x for x in m if x in NEEDS_GRAIN)} needs a grain")
    s, e = parse_date(start, "start"), parse_date(end, "end")
    if s and e and s > e:
        raise Rejected("start must not be after end")
    filters = parse_where(where, m)
    mf_group = ([f"metric_time__{g}"] if g else []) + [DIMENSIONS[d][0] for d in dims]
    return {"metrics": m, "group_by": mf_group, "where": where_clauses(filters, s, e), "limit": parse_limit(limit),
            "request": {"metrics": m, "group_by": dims, "grain": g, "start": s and s.isoformat(),
                        "end": e and e.isoformat(), "where": [f"{f.split('__')[1]}:{v}" for f, v in filters]}}


# ---------- responses ----------

API_NAME = {mf: api for api, (mf, _) in DIMENSIONS.items()}


def column_name(mf_name):
    """encounter__source -> source; metric_time__month -> period."""
    if mf_name.startswith("metric_time"):
        return PERIOD
    return API_NAME.get(mf_name, mf_name)


def present(rows):
    """Rows with API column names; a period as a plain date."""
    out = []
    for r in rows:
        row = {}
        for k, v in r.items():
            name = column_name(k)
            if name == PERIOD and isinstance(v, str):
                v = v[:10]
            row[name] = v
        out.append(row)
    return out


def is_count_value(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool) and 1 <= v <= 10


def check_rows(rows, metrics, cat):
    """Raise ValueError if a row holds a count from 1 to 10 or a column outside the allowlist."""
    allowed = {PERIOD, "is_suppressed", *DIMENSIONS, *metrics}
    for r in rows:
        extra = set(r) - allowed
        if extra:
            raise ValueError(f"column not allowed in a response: {sorted(extra)}")
        for m in metrics:
            if cat[m].get("suppression") == "count" and is_count_value(r.get(m)):
                raise ValueError(f"{m}: a count from 1 to 10 reached the response")
