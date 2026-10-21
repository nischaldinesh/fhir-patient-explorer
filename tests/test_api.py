"""The metrics API (api/): request allowlists, auth, suppression, saved queries and the catalog with MetricFlow mocked
(metrics.raw_query replaced by made up rows), the serving snapshot helpers (analytics/serving.py), and one
integration test on a slim snapshot of the local warehouse when it exists."""
import json
import logging
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "analytics"))
pytest.importorskip("fastapi")
pytest.importorskip("httpx")
yaml = pytest.importorskip("yaml")
from fastapi.testclient import TestClient  # noqa: E402

import metrics as mx  # noqa: E402
import serving  # noqa: E402
from api import schema  # noqa: E402
from api.app import create_app  # noqa: E402
from api.engine import Engine  # noqa: E402

KEY = "test-key-not-a-secret"
H = {"X-API-Key": KEY}
WAREHOUSE = ROOT / "data" / "analytics" / "warehouse.duckdb"

# made up raw MetricFlow answers per source: synthea's admissions are a small cell
RAW = {
    "hl7v2": {"admissions": 250, "ed_visits": 90, "bed_days": 900, "index_stays": 240, "readmissions_30d": 50,
              "length_of_stay_days_total": 1300.0, "average_length_of_stay_days": 5.4, "readmission_rate_30d": 0.21,
              "admissions_rolling_28d": 80, "capacity_bed_days": 3000, "bed_occupancy_rate": 0.3},
    "mimic": {"admissions": 120, "ed_visits": 0, "bed_days": 0, "index_stays": 110, "readmissions_30d": 20,
              "length_of_stay_days_total": 700.0, "average_length_of_stay_days": 6.4, "readmission_rate_30d": 0.18,
              "admissions_rolling_28d": 30, "capacity_bed_days": 0, "bed_occupancy_rate": None},
    "synthea": {"admissions": 7, "ed_visits": 15, "bed_days": 0, "index_stays": 7, "readmissions_30d": 2,
                "length_of_stay_days_total": 40.0, "average_length_of_stay_days": 5.7, "readmission_rate_30d": 0.28,
                "admissions_rolling_28d": 3, "capacity_bed_days": 0, "bed_occupancy_rate": None},
}


class FakeMF:
    """Stands in for metrics.raw_query: rows per source (or one total row), remembers what it was asked."""

    def __init__(self):
        self.calls = []

    def __call__(self, metric_names, group_by=(), where=(), order=(), db=None, target=None, parse=True):
        self.calls.append({"metrics": list(metric_names), "group_by": list(group_by), "where": list(where)})
        others = [g for g in group_by if g != "encounter__source"]
        values = {"metric_time__day": "2026-10-14 00:00:00", "metric_time__month": "2026-10-01 00:00:00",
                  "patient__age_band": "40s", "encounter__is_live_feed": True, "location__unit_name": "Medical Unit"}
        if "encounter__source" in group_by:
            rows = []
            for src, vals in RAW.items():
                r = {g: values.get(g, "x") for g in others}
                r["encounter__source"] = src
                rows.append({**r, **{m: vals[m] for m in metric_names}})
            return rows
        total = {m: sum(v[m] or 0 for v in RAW.values()) for m in metric_names}
        return [{**{g: values.get(g, "x") for g in group_by}, **total}]


@pytest.fixture
def fake_mf(monkeypatch):
    fake = FakeMF()
    monkeypatch.setattr(mx, "raw_query", fake)
    return fake


def snapshot_dir(tmp_path, version="20261019T000000Z"):
    d = tmp_path / "serving"
    d.mkdir(exist_ok=True)
    serving.write_pointer(d, {"version": version, "db": "unused.duckdb", "target": str(d / "t"),
                              "data_date": "2026-10-14"})
    return d


@pytest.fixture
def engine(tmp_path, fake_mf):
    return Engine(snapshot_dir(tmp_path), warm_in_background=False)


@pytest.fixture
def client(engine):
    with TestClient(create_app(api_key=KEY, engine=engine)) as c:
        yield c


def count_values(body):
    """Every value of a count metric in a response body."""
    cat = mx.catalog()
    return [r[m] for r in body["rows"] for m in r if m in cat and cat[m]["suppression"] == "count"]


# ---------- schema (pure) ----------

def test_public_metrics_are_the_catalog_minus_components():
    cat = mx.catalog()
    components = {"index_stays", "readmissions_30d", "length_of_stay_days_total", "capacity_bed_days"}
    assert set(schema.PUBLIC_METRICS) == set(cat) - components
    assert set(schema.METRIC_ENTITIES) == set(schema.PUBLIC_METRICS)


def test_allowlisted_dimensions_exist_in_the_semantic_models():
    models = yaml.safe_load((ROOT / "analytics/dbt/models/semantic/_semantic_models.yml").read_text())["semantic_models"]
    entity_dims = {}
    for m in models:
        primary = [e["name"] for e in m.get("entities", []) if e["type"] == "primary"]
        for d in m.get("dimensions", []):
            for e in primary:
                entity_dims.setdefault(e, set()).add(d["name"])
    for api_name, (mf_name, entity) in schema.DIMENSIONS.items():
        ent, dim = mf_name.split("__")
        assert ent == entity and dim in entity_dims[ent], api_name
    assert "patient__state" not in {mf for mf, _ in schema.DIMENSIONS.values()}
    for field, (mf_name, _) in schema.FILTERS.items():
        assert mf_name in {mf for mf, _ in schema.DIMENSIONS.values()}, field


def test_build_query():
    q = schema.build_query("admissions,admissions", "source,age_band", "month", "2026-08-01", "2026-10-31",
                           ["is_live_feed:true", "encounter_kind:hospital admission"], 50)
    assert q["metrics"] == ["admissions"]
    assert q["group_by"] == ["metric_time__month", "encounter__source", "patient__age_band"]
    assert q["where"] == ["{{ Dimension('encounter__is_live_feed') }} = TRUE",
                          "{{ Dimension('encounter__encounter_kind') }} = 'hospital admission'",
                          "{{ TimeDimension('metric_time', 'day') }} >= '2026-08-01'",
                          "{{ TimeDimension('metric_time', 'day') }} <= '2026-10-31'"]
    assert q["limit"] == 50
    assert q["request"]["where"] == ["is_live_feed:true", "encounter_kind:hospital admission"]
    assert schema.build_query("bed_occupancy_rate", "unit_name")["group_by"] == ["location__unit_name"]
    assert schema.build_query("admissions")["limit"] == schema.DEFAULT_LIMIT


@pytest.mark.parametrize("kw", [
    {"metrics": ""},
    {"metrics": "index_stays"},                                   # a component, internal
    {"metrics": "admissions", "group_by": "patient"},             # an entity key
    {"metrics": "admissions", "group_by": "encounter"},
    {"metrics": "admissions", "group_by": "patient_sk"},
    {"metrics": "admissions", "group_by": "state"},               # a quasi identifier, left out
    {"metrics": "admissions", "group_by": "source,sex,age_band"},  # more than 2
    {"metrics": "bed_occupancy_rate", "group_by": "source"},      # capacity has no visit
    {"metrics": "admissions", "grain": "hour"},
    {"metrics": "admissions", "start": "2026-13-01"},
    {"metrics": "admissions", "start": "1 OR 1=1"},
    {"metrics": "admissions", "start": "2026-10-02", "end": "2026-10-01"},
    {"metrics": "admissions", "where": ["source:hl7v2' OR 1=1"]},
    {"metrics": "admissions", "where": ["state:MA"]},
    {"metrics": "admissions", "where": ["{{ Dimension('patient__state') }} = 'MA'"]},
    {"metrics": "bed_occupancy_rate", "where": ["source:hl7v2"]},
    {"metrics": "admissions", "limit": 0},
    {"metrics": "admissions_rolling_28d", "group_by": "source"},   # a window needs a grain
    {"metrics": "admissions", "limit": schema.MAX_LIMIT + 1},
])
def test_build_query_rejects(kw):
    with pytest.raises(schema.Rejected):
        schema.build_query(**kw)


def test_present_and_check_rows():
    rows = schema.present([{"metric_time__month": "2026-10-01 00:00:00", "encounter__source": "hl7v2",
                            "admissions": 12, "is_suppressed": False}])
    assert rows == [{"period": "2026-10-01", "source": "hl7v2", "admissions": 12, "is_suppressed": False}]
    cat = mx.catalog()
    schema.check_rows(rows, ["admissions"], cat)
    schema.check_rows([{"admissions": 0, "is_suppressed": False}], ["admissions"], cat)
    schema.check_rows([{"readmission_rate_30d": 0.5, "average_length_of_stay_days": 5.0, "is_suppressed": False}],
                      ["readmission_rate_30d", "average_length_of_stay_days"], cat)
    with pytest.raises(ValueError):
        schema.check_rows([{"admissions": 10, "is_suppressed": False}], ["admissions"], cat)
    with pytest.raises(ValueError):
        schema.check_rows([{"admissions": 1.0, "is_suppressed": False}], ["admissions"], cat)
    with pytest.raises(ValueError):
        schema.check_rows([{"patient_sk": "abc", "admissions": 50, "is_suppressed": False}], ["admissions"], cat)


# ---------- auth ----------

def test_app_refuses_to_start_without_a_key(monkeypatch, engine):
    monkeypatch.delenv("METRICS_API_KEY", raising=False)
    with pytest.raises(RuntimeError):
        create_app(engine=engine)


@pytest.mark.parametrize("path", ["/v1/metrics", "/v1/metrics/query?metrics=admissions",
                                  "/v1/saved-queries/ops_daily"])
def test_key_required(client, path):
    assert client.get(path).status_code == 401
    assert client.get(path, headers={"X-API-Key": "wrong"}).status_code == 401
    assert client.get(path, headers={"X-API-Key": KEY + "x"}).status_code == 401
    assert client.get(path, headers=H).status_code == 200


def test_health_needs_no_key_and_shows_the_snapshot(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok", "snapshot": {"version": "20261019T000000Z", "data_date": "2026-10-14"}}


def test_no_snapshot(tmp_path, fake_mf):
    with TestClient(create_app(api_key=KEY, engine=Engine(tmp_path / "none"))) as c:
        assert c.get("/health").json() == {"status": "no snapshot", "snapshot": None}
        assert c.get("/v1/metrics/query?metrics=admissions", headers=H).status_code == 503


# ---------- catalog ----------

def test_catalog(client):
    body = client.get("/v1/metrics", headers=H).json()
    names = [m["name"] for m in body["metrics"]]
    assert names == list(schema.PUBLIC_METRICS)
    by = {m["name"]: m for m in body["metrics"]}
    assert all(by[n]["description"] and by[n]["label"] and by[n]["suppression"] for n in names)
    assert by["bed_occupancy_rate"]["group_by"] == ["unit_name", "location_level"]
    assert by["bed_occupancy_rate"]["filters"] == []
    assert "unit_name" not in by["admissions"]["group_by"] and "source" in by["admissions"]["group_by"]
    assert body["max_group_by"] == 2 and body["grains"] == list(schema.GRAINS)
    assert body["saved_queries"] == ["ops_daily", "quality_all_time"]


def test_openapi_docs(client):
    assert client.get("/docs").status_code == 200
    spec = client.get("/openapi.json").json()
    assert {"/health", "/v1/metrics", "/v1/metrics/query", "/v1/saved-queries/{name}"} <= set(spec["paths"])
    params = {p["name"]: p for p in spec["paths"]["/v1/metrics/query"]["get"]["parameters"]}
    assert params["metrics"]["examples"] and params["group_by"]["examples"]
    assert "X-API-Key" in json.dumps(spec["components"]["securitySchemes"])


# ---------- queries ----------

@pytest.mark.parametrize("qs", ["group_by=patient", "group_by=patient__state", "group_by=state",
                                "group_by=source,sex,age_band", "group_by=encounter_sk",
                                "where=source:x' OR '1'='1", "limit=999999", "limit=abc"])
def test_rejected_with_422(client, fake_mf, qs):
    before = len(fake_mf.calls)                      # the saved queries warmed at startup
    r = client.get(f"/v1/metrics/query?metrics=admissions&{qs}", headers=H)
    assert r.status_code == 422
    assert len(fake_mf.calls) == before              # never reached MetricFlow


def test_422_does_not_echo_the_input(client):
    r = client.get("/v1/metrics/query?metrics=admissions&group_by=secret_value_xyz", headers=H)
    assert r.status_code == 422 and "secret_value_xyz" not in r.text
    r = client.get("/v1/metrics/query?metrics=admissions&limit=secret_value_xyz", headers=H)
    assert r.status_code == 422 and "secret_value_xyz" not in r.text


def test_query_passes_only_built_arguments(client, fake_mf):
    r = client.get("/v1/metrics/query?metrics=readmission_rate_30d&group_by=source&grain=month"
                   "&where=is_live_feed:true&start=2026-08-01", headers=H)
    assert r.status_code == 200
    call = fake_mf.calls[-1]
    assert call["metrics"] == ["readmission_rate_30d", "readmissions_30d", "index_stays"]
    assert call["group_by"] == ["metric_time__month", "encounter__source"]
    assert call["where"] == ["{{ Dimension('encounter__is_live_feed') }} = TRUE",
                             "{{ TimeDimension('metric_time', 'day') }} >= '2026-08-01'"]
    body = r.json()
    assert body["columns"] == ["period", "source", "readmission_rate_30d", "is_suppressed"]
    assert body["rows"][0]["period"] == "2026-10-01"
    # the components never leave
    assert all(set(row) == set(body["columns"]) for row in body["rows"])


def test_small_counts_are_hidden_with_their_complement(client):
    body = client.get("/v1/metrics/query?metrics=admissions,ed_visits&group_by=source", headers=H).json()
    rows = {r["source"]: r for r in body["rows"]}
    assert rows["synthea"]["admissions"] is None and rows["synthea"]["is_suppressed"]
    # total admissions (377) minus the shown cells must not give synthea's 7 back: the next smallest is hidden too
    assert rows["mimic"]["admissions"] is None
    assert rows["hl7v2"]["admissions"] == 250
    # ed_visits: hl7v2 90, mimic 0 (a zero is shown), synthea 15
    assert [rows[s]["ed_visits"] for s in ("hl7v2", "mimic", "synthea")] == [90, 0, 15]
    assert body["suppressed_rows"] == 2
    assert not [v for v in count_values(body) if v is not None and 1 <= v <= 10]


def test_rates_hidden_when_their_counts_are_small(client):
    body = client.get("/v1/metrics/query?metrics=readmission_rate_30d,average_length_of_stay_days&group_by=source",
                      headers=H).json()
    rows = {r["source"]: r for r in body["rows"]}
    assert rows["synthea"]["readmission_rate_30d"] is None             # 2 of 7
    assert rows["synthea"]["average_length_of_stay_days"] is None      # 7 stays
    assert rows["hl7v2"]["readmission_rate_30d"] == 0.21


def query_by_source(m):
    """Each metric split by source (occupancy has no visit: by unit), with a grain where the metric needs one."""
    group = "unit_name" if m == "bed_occupancy_rate" else "source"
    grain = "&grain=month" if m in schema.NEEDS_GRAIN else ""
    return f"/v1/metrics/query?metrics={m}&group_by={group}{grain}"


def test_every_metric_by_source_has_no_small_count(client):
    for m in schema.PUBLIC_METRICS:
        r = client.get(query_by_source(m), headers=H)
        assert r.status_code == 200, m
        assert not [v for v in count_values(r.json()) if v is not None and 1 <= v <= 10], m


def test_limit_truncates(client):
    body = client.get("/v1/metrics/query?metrics=admissions&group_by=source&limit=2", headers=H).json()
    assert body["row_count"] == 2 and body["truncated"] is True


def test_guard_blocks_a_small_count(tmp_path, monkeypatch):
    def leaky(metric_names, group_by, where, order, **kw):
        return [{"admissions": 4, "is_suppressed": False}]
    eng = Engine(snapshot_dir(tmp_path), runner=leaky, warm_in_background=False)
    with TestClient(create_app(api_key=KEY, engine=eng)) as c:
        r = c.get("/v1/metrics/query?metrics=admissions", headers=H)
    assert r.status_code == 500 and "4" not in r.json()["detail"]


def test_mf_failure_is_a_400_without_details(tmp_path):
    def broken(*a, **kw):
        raise RuntimeError("mf query failed: SELECT secret FROM somewhere")
    eng = Engine(snapshot_dir(tmp_path), runner=broken, warm_in_background=False)
    with TestClient(create_app(api_key=KEY, engine=eng, warm=False)) as c:
        r = c.get("/v1/metrics/query?metrics=admissions", headers=H)
    assert r.status_code == 400 and "secret" not in r.text


# ---------- saved queries ----------

def test_saved_queries(client, fake_mf):
    calls = len(fake_mf.calls)                       # the startup warmed both
    assert calls == 2
    for name in ("ops_daily", "quality_all_time"):
        r = client.get(f"/v1/saved-queries/{name}", headers=H)
        assert r.status_code == 200, name
        body = r.json()
        assert body["query"]["saved_query"] == name
        assert not [v for v in count_values(body) if v is not None and 1 <= v <= 10]
    assert len(fake_mf.calls) == calls              # both answered from the cache
    ops = client.get("/v1/saved-queries/ops_daily", headers=H).json()
    assert ops["columns"] == ["period", "source", "admissions", "ed_visits", "bed_days", "is_suppressed"]
    assert ops["query"]["where"] == ["{{ Dimension('encounter__is_live_feed') }} = TRUE"]
    assert client.get("/v1/saved-queries/nope", headers=H).status_code == 404


def test_new_snapshot_is_picked_up(tmp_path, fake_mf):
    d = snapshot_dir(tmp_path)
    eng = Engine(d, warm_in_background=False)
    with TestClient(create_app(api_key=KEY, engine=eng)) as c:
        c.get("/v1/metrics/query?metrics=admissions", headers=H)
        n = len(fake_mf.calls)
        c.get("/v1/metrics/query?metrics=admissions", headers=H)
        assert len(fake_mf.calls) == n
        import os
        import time
        time.sleep(0.01)
        serving.write_pointer(d, {"version": "20261020T000000Z", "db": "u", "target": "t", "data_date": "2026-10-15"})
        os.utime(d / serving.POINTER, None)
        r = c.get("/v1/metrics/query?metrics=admissions", headers=H)
        assert r.json()["snapshot"]["version"] == "20261020T000000Z"
        assert len(fake_mf.calls) > n


def test_log_holds_no_key_or_values(client, caplog):
    with caplog.at_level(logging.INFO, logger="metrics_api"):
        client.get("/v1/metrics/query?metrics=admissions&group_by=source&where=source:hl7v2", headers=H)
        client.get("/v1/metrics/query?metrics=admissions&group_by=leak_me", headers=H)
    text = caplog.text
    assert "/v1/metrics/query" in text
    for secret in (KEY, "hl7v2", "leak_me", "admissions"):
        assert secret not in text


# ---------- serving snapshot (analytics/serving.py) ----------

def test_semantic_tables():
    manifest = {"semantic_models": [{"node_relation": {"alias": "fact_encounter"}},
                                    {"node_relation": {"alias": "dim_patient"}},
                                    {"node_relation": {"alias": "fact_encounter"}}],
                "project_configuration": {"time_spines": [{"node_relation": {"alias": "dim_date"}}],
                                          "time_spine_table_configurations": []}}
    assert serving.semantic_tables(manifest) == ["fact_encounter", "dim_patient", "dim_date"]


def test_snapshot_tables_add_the_bi_marts():
    manifest = {"semantic_models": [{"node_relation": {"alias": "fact_encounter"}}],
                "project_configuration": {"time_spines": [{"node_relation": {"alias": "dim_date"}}]}}
    assert serving.snapshot_tables(manifest) == ["fact_encounter", "dim_date", "mart_quality_period",
                                                 "mart_suppression_report", "gold_terminology_quality",
                                                 "gold_empi_quality", "gold_feed_freshness"]


def test_versions_to_remove():
    names = ["20261017T000000Z", "20261018T000000Z", "20261019T000000Z", ".20261020T000000Z.tmp"]
    assert serving.versions_to_remove(names, "20261019T000000Z", 2) == ["20261017T000000Z"]
    assert serving.versions_to_remove(names, "20261017T000000Z", 1) == ["20261018T000000Z"]


def test_pointer_round_trip(tmp_path):
    assert serving.read_pointer(tmp_path) is None
    (tmp_path / serving.POINTER).write_text("not json")
    assert serving.read_pointer(tmp_path) is None
    serving.write_pointer(tmp_path, {"version": "v"})
    assert serving.read_pointer(tmp_path) == {"version": "v"}


def test_copy_tables_copies_only_the_named_tables(tmp_path):
    duckdb = pytest.importorskip("duckdb")
    src = tmp_path / "src.duckdb"
    con = duckdb.connect(str(src))
    con.execute("CREATE TABLE fact_encounter AS SELECT 1 AS a UNION ALL SELECT 2")
    con.execute("CREATE TABLE silver_patient AS SELECT 'name' AS family")
    con.execute("CREATE TABLE gold_feed_freshness AS SELECT DATE '2026-10-14' AS data_date")
    con.close()
    rows, data_date = serving.copy_tables(src, tmp_path / "warehouse.duckdb", ["fact_encounter"])
    assert rows == {"fact_encounter": 2} and data_date == "2026-10-14"
    con = duckdb.connect(str(tmp_path / "warehouse.duckdb"), read_only=True)
    assert [r[0] for r in con.execute("SELECT table_name FROM information_schema.tables").fetchall()] == ["fact_encounter"]
    con.close()
    with pytest.raises(SystemExit, match="predates"):
        serving.copy_tables(src, tmp_path / "other.duckdb", ["fact_encounter", "fact_bed_capacity_day"])


# ---------- integration: the local warehouse, if there is one ----------

@pytest.fixture(scope="module")
def real_snapshot(tmp_path_factory):
    if not WAREHOUSE.exists():
        pytest.skip("no local warehouse (python3 analytics/build.py build)")
    d = tmp_path_factory.mktemp("serving")
    info = serving.create_snapshot(WAREHOUSE, d)
    return d, info


def test_integration_snapshot_is_slim(real_snapshot):
    d, info = real_snapshot
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(info["db"], read_only=True)
    tables = {r[0] for r in con.execute("SELECT table_name FROM information_schema.tables").fetchall()}
    con.close()
    assert tables == {"fact_encounter", "dim_patient", "dim_location", "fact_bed_day", "fact_bed_capacity_day",
                      "dim_date", *serving.BI_TABLES}
    # the only Gold tables are the three of pipeline metadata the BI export reads
    assert not [t for t in tables - set(serving.BI_TABLES)
                if t.startswith(("bronze", "silver", "gold", "analyst", "int_", "snap"))]
    assert info["size_bytes"] < WAREHOUSE.stat().st_size / 10


def test_integration_api_on_the_real_snapshot(real_snapshot):
    d, _ = real_snapshot
    with TestClient(create_app(api_key=KEY, engine=Engine(d, warm_in_background=False))) as c:
        for m in schema.PUBLIC_METRICS:
            r = c.get(query_by_source(m), headers=H)
            assert r.status_code == 200, (m, r.text)
            assert r.json()["rows"], m
            assert not [v for v in count_values(r.json()) if v is not None and 1 <= v <= 10], m
        for name in ("ops_daily", "quality_all_time"):
            r = c.get(f"/v1/saved-queries/{name}", headers=H)
            assert r.status_code == 200, name
            assert not [v for v in count_values(r.json()) if v is not None and 1 <= v <= 10], name
        assert c.get("/v1/metrics/query?metrics=admissions&group_by=patient", headers=H).status_code == 422
