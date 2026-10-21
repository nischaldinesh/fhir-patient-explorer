"""The semantic layer (analytics/dbt/models/semantic/, dbt MetricFlow): the catalog, small cell suppression in
analytics/metrics.py, the parity check in analytics/semantic_parity.py, and real mf queries on a small warehouse."""
import itertools
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "analytics"))
sys.path.insert(0, str(ROOT / "hl7v2"))
yaml = pytest.importorskip("yaml")
import metrics as mx  # noqa: E402
import semantic_parity as sp  # noqa: E402

REQUIRED = ["admissions", "ed_visits", "average_length_of_stay_days", "readmission_rate_30d", "bed_days",
            "bed_occupancy_rate", "admissions_rolling_28d"]
COUNTS = [m for m, v in mx.catalog().items() if v["suppression"] == "count"]


@pytest.fixture(scope="module")
def cat():
    return mx.catalog()


# ---------- the catalog ----------

def test_every_metric_has_label_owner_definition_and_rule(cat):
    assert set(REQUIRED) <= set(cat)
    for name, m in cat.items():
        assert m["label"] and m["owner"] and m["description"], name
        assert "\n" not in m["description"].strip(), f"{name}: one line definition"
        assert m["suppression"] in mx.RULES, name
        assert "invented demo persona" in m["owner"], name
        for part in ("numerator", "denominator"):
            if m[part]:
                assert m[part] in cat, f"{name}: {part} {m[part]} is not a metric"
        if m["suppression"] in ("rate", "mean"):
            assert m["denominator"], name
        if m["suppression"] == "rate":
            assert cat[m["numerator"]]["suppression"] == "count", name
    assert cat["admissions_rolling_28d"]["type"] == "cumulative"
    assert {cat[m]["type"] for m in ("readmission_rate_30d", "average_length_of_stay_days", "bed_occupancy_rate")} == {"ratio"}


def test_dim_date_is_the_time_spine():
    marts = yaml.safe_load((ROOT / "analytics/dbt/models/marts/_marts.yml").read_text())
    dim_date = next(m for m in marts["models"] if m["name"] == "dim_date")
    assert dim_date["time_spine"]["standard_granularity_column"] == "calendar_date"
    col = next(c for c in dim_date["columns"] if c["name"] == "calendar_date")
    assert col["granularity"] == "day"


def test_saved_queries_resolve_to_mf_arguments():
    metrics, group_by, where = mx.resolve_saved_query("ops_daily")
    assert metrics == ["admissions", "ed_visits", "bed_days"]
    assert group_by == ["metric_time__day", "encounter__source"]
    assert where == ["{{ Dimension('encounter__is_live_feed') }} = TRUE"]
    assert mx.resolve_saved_query("quality_all_time")[1] == ["encounter__source"]
    with pytest.raises(ValueError):
        mx.resolve_saved_query("nope")


def test_group_by_name():
    assert mx.group_by_name("TimeDimension('metric_time', 'month')") == "metric_time__month"
    assert mx.group_by_name("TimeDimension('metric_time')") == "metric_time"
    assert mx.group_by_name(" Dimension('patient__age_band') ") == "patient__age_band"
    assert mx.group_by_name("Entity('encounter')") == "encounter"
    with pytest.raises(ValueError):
        mx.group_by_name("Metric('x')")


def test_components_add_what_the_rule_needs(cat):
    assert mx.components(["readmission_rate_30d"], cat) == ["readmission_rate_30d", "readmissions_30d", "index_stays"]
    assert mx.components(["admissions", "average_length_of_stay_days"], cat) == [
        "admissions", "average_length_of_stay_days", "length_of_stay_days_total", "index_stays"]
    with pytest.raises(ValueError):
        mx.components(["nope"], cat)


# ---------- suppression ----------

def test_is_small_cell_follows_the_macro():
    # (numerator, denominator, numerator is a count) -> hidden; the same cases as is_small_cell in macros/marts.sql
    cases = [((0, 0, True), False), ((0, 11, True), False), ((0, 5, True), True), ((5, 50, True), True),
             ((45, 50, True), True), ((39, 50, True), False), ((40, 50, True), True), ((11, 11, True), False), ((3, 50, False), False),
             ((3.5, 10, False), True), ((None, None, True), False), ((None, 12, True), False)]
    for (n, d, c), want in cases:
        assert mx.is_small_cell(n, d, c) is want, (n, d, c)
    assert [mx.is_small(v) for v in (None, 0, 1, 10, 11, 0.5)] == [False, False, True, True, False, False]


def test_counts_are_hidden_from_1_to_10(cat):
    rows = [{"encounter__source": "hl7v2", "admissions": n} for n in range(0, 25)]
    out = mx.suppress(rows, ["admissions"], cat)
    assert [r["admissions"] for r in out] == [0] + [None] * 10 + list(range(11, 25))
    assert [r["is_suppressed"] for r in out] == [False] + [True] * 10 + [False] * 14
    assert out[0]["encounter__source"] == "hl7v2"


def test_rates_means_and_components(cat):
    row = {"encounter__source": "synthea", "readmission_rate_30d": 4 / 76, "readmissions_30d": 4, "index_stays": 76,
           "average_length_of_stay_days": 6.8, "length_of_stay_days_total": 516.5}
    out = mx.suppress([row], ["readmission_rate_30d", "average_length_of_stay_days"], cat)[0]
    assert out["readmission_rate_30d"] is None                 # 4 readmissions: a small numerator
    assert out["average_length_of_stay_days"] == 6.8           # 76 stays: the mean is shown
    assert out["is_suppressed"] is True
    assert "index_stays" not in out and "readmissions_30d" not in out   # components not asked for are dropped

    small = {"average_length_of_stay_days": 4.0, "length_of_stay_days_total": 20.0, "index_stays": 5}
    assert mx.suppress([small], ["average_length_of_stay_days"], cat)[0]["average_length_of_stay_days"] is None

    occupancy = {"bed_occupancy_rate": 0.9, "bed_days": 90, "capacity_bed_days": 100}
    assert mx.suppress([occupancy], ["bed_occupancy_rate"], cat)[0]["bed_occupancy_rate"] is None   # 10 empty
    assert mx.suppress([occupancy], ["capacity_bed_days"], cat)[0]["capacity_bed_days"] == 100      # rule none


def test_numerator_and_denominator_side_by_side_hide_the_complement(cat):
    row = {"readmissions_30d": 45, "index_stays": 50}            # both over 10, but 50 - 45 = 5 people
    out = mx.suppress([row], ["readmissions_30d", "index_stays"], cat)[0]
    assert out["readmissions_30d"] is None and out["index_stays"] is None and out["is_suppressed"]
    fine = mx.suppress([{"readmissions_30d": 30, "index_stays": 50}], ["readmissions_30d", "index_stays"], cat)[0]
    assert fine == {"readmissions_30d": 30, "index_stays": 50, "is_suppressed": False}


def test_no_output_ever_shows_a_count_from_1_to_10(cat):
    """Every combination of small and large counts: nothing from 1 to 10 survives, directly or by subtraction."""
    values = [0, 1, 5, 10, 11, 12, 20, 21, 40, 100]
    asked = ["admissions", "index_stays", "readmissions_30d", "readmission_rate_30d", "bed_days",
             "bed_occupancy_rate", "average_length_of_stay_days"]
    rows = []
    for adm, den, num, beds, cap in itertools.product(values, values, values, values, [0, 50, 100, 105]):
        if num > den or beds > cap:
            continue
        rows.append({"admissions": adm, "index_stays": den, "readmissions_30d": num,
                     "readmission_rate_30d": num / den if den else None, "length_of_stay_days_total": den * 5.0,
                     "average_length_of_stay_days": 5.0 if den else None, "bed_days": beds, "capacity_bed_days": cap,
                     "bed_occupancy_rate": beds / cap if cap else None})
    out = mx.suppress(rows, asked, cat)
    for src, r in zip(rows, out):
        for m in ("admissions", "index_stays", "readmissions_30d", "bed_days"):
            assert not mx.is_small(r[m]), (m, r)
        if r["index_stays"] is not None and r["readmissions_30d"] is not None:
            assert not mx.is_small(r["index_stays"] - r["readmissions_30d"])
        if r["readmission_rate_30d"] is not None:
            n, d = src["readmissions_30d"], src["index_stays"]
            assert not (mx.is_small(n) or mx.is_small(d) or mx.is_small(d - n)), src
        if r["bed_occupancy_rate"] is not None:
            n, d = src["bed_days"], src["capacity_bed_days"]
            assert not (mx.is_small(n) or mx.is_small(d) or mx.is_small(d - n)), src
        if r["average_length_of_stay_days"] is not None:
            assert not mx.is_small(src["index_stays"])
        assert r["is_suppressed"] == any(r[m] is None and src[m] is not None for m in asked)


def test_rows_without_a_day_and_without_values_are_dropped(cat):
    rows = [{"metric_time__week": None, "admissions": None, "index_stays": 0},
            {"metric_time__week": None, "admissions": 12, "index_stays": 0},
            {"metric_time__week": "2026-09-07 00:00:00", "admissions": 0, "index_stays": 0},
            {"encounter__source": "hl7v2", "admissions": 0}]
    assert mx.drop_empty_time_rows(rows, cat) == rows[1:]


def test_complement_hides_what_a_total_would_give_back(cat):
    raw = [{"encounter__source": "hl7v2", "admissions": 250},
           {"encounter__source": "mimic", "admissions": 120},
           {"encounter__source": "synthea", "admissions": 7}]
    out = mx.complement(raw, mx.suppress(raw, ["admissions"], cat), ["admissions"], cat, ["encounter__source"])
    # total 377 minus the shown 250 would be 127: no longer a small cell
    assert [r["admissions"] for r in out] == [250, None, None]
    assert [r["is_suppressed"] for r in out] == [False, True, True]
    # hidden cells already adding up to more than 10 need nothing more
    raw = [{"s": "a", "admissions": 100}, {"s": "b", "admissions": 7}, {"s": "c", "admissions": 6}]
    assert [r["admissions"] for r in mx.complement(raw, mx.suppress(raw, ["admissions"], cat), ["admissions"],
                                                   cat, ["s"])] == [100, None, None]
    # zeros are shown and do not count as cover
    raw = [{"s": "a", "admissions": 0}, {"s": "b", "admissions": 3}, {"s": "c", "admissions": 40}]
    assert [r["admissions"] for r in mx.complement(raw, mx.suppress(raw, ["admissions"], cat), ["admissions"],
                                                   cat, ["s"])] == [0, None, None]


def test_complement_works_along_each_group_by_column(cat):
    raw = [{"m": "jan", "s": "a", "admissions": 50}, {"m": "jan", "s": "b", "admissions": 4},
           {"m": "feb", "s": "a", "admissions": 60}, {"m": "feb", "s": "b", "admissions": 30}]
    out = mx.complement(raw, mx.suppress(raw, ["admissions"], cat), ["admissions"], cat, ["m", "s"])
    hidden = {(r["m"], r["s"]) for r in out if r["admissions"] is None}
    # jan b is small; jan a is hidden for the jan total, then feb b for the b total, then feb a for the feb total
    assert ("jan", "b") in hidden and ("jan", "a") in hidden
    for col in ("m", "s"):
        for key in {r[col] for r in raw}:
            cells = [(x, y) for x, y in zip(raw, out) if x[col] == key]
            assert not mx.is_small(sum(x["admissions"] for x, y in cells if y["admissions"] is None))
    # rates and means are not touched, and without group by nothing changes
    raw = [{"readmission_rate_30d": 0.2, "readmissions_30d": 20, "index_stays": 100}]
    out = mx.suppress(raw, ["readmission_rate_30d"], cat)
    assert mx.complement(raw, out, ["readmission_rate_30d"], cat, []) == out


def test_a_metric_without_a_rule_is_refused():
    with pytest.raises(ValueError):
        mx.suppress([{"x": 3}], ["x"], {"x": {"suppression": None}})


def test_parse_value():
    assert [mx.parse_value(v) for v in ("", "12", "0.25", "2026-09-01 00:00:00", "hl7v2")] == [
        None, 12, 0.25, "2026-09-01 00:00:00", "hl7v2"]


# ---------- the parity comparison ----------

def test_differences():
    e = {"hl7v2": {"admissions": 10, "rate": 0.25}, "mimic": {"admissions": 0, "rate": None}}
    assert sp.differences(e, {"hl7v2": {"admissions": 10, "rate": 0.25 + 1e-12}}, ["admissions", "rate"]) == []
    assert sp.differences(e, {"hl7v2": {"admissions": 11, "rate": 0.25}}, ["admissions"]) == [("hl7v2", "admissions", 10, 11)]
    assert sp.differences(e, {"hl7v2": {"admissions": 10, "rate": 0.25 + 1e-8}}, ["rate"]) == [
        ("hl7v2", "rate", 0.25, 0.25 + 1e-8)]
    assert sp.differences(e, {}, ["admissions"]) == [("hl7v2", "admissions", 10, None)]   # a missing row counts
    assert sp.differences({}, {"synthea": {"admissions": 3}}, ["admissions"]) == [("synthea", "admissions", None, 3)]


def test_key_of():
    assert sp.key_of("2026-09-01 00:00:00") == "2026-09-01"
    assert sp.key_of(None) == "" and sp.key_of("hl7v2") == "hl7v2" and sp.key_of(24) == "24"
    assert sp.by_key([{"a": "x", "b": "2026-01-01 00:00:00", "v": 1}], ["a", "b"]) == {
        "x|2026-01-01": {"a": "x", "b": "2026-01-01 00:00:00", "v": 1}}


# ---------- real MetricFlow queries on a small warehouse ----------

@pytest.fixture(scope="module")
def small(tmp_path_factory):
    """A warehouse with MIMIC, Synthea and 30 days of the live feed, built with dbt; dbt's target folder is not
    analytics/dbt/target, as in local Airflow, so mf goes through the linked project folder."""
    pytest.importorskip("duckdb")
    pytest.importorskip("dbt.cli.main")
    pytest.importorskip("dbt_metricflow")
    from test_analytics import build_at, live_feed_resources, mimic_and_synthea, write_landing
    tmp = tmp_path_factory.mktemp("semantic")
    write_landing(tmp / "landing", mimic_and_synthea() + live_feed_resources(days=30))
    build_at(tmp, tmp / "landing").close()
    mx.parse_project(tmp / "target", tmp / "w.duckdb")
    return {"db": tmp / "w.duckdb", "target": tmp / "target"}


def test_validate_configs_passes(small, capsys):
    assert mx.main(["--db", str(small["db"]), "--target", str(small["target"]), "validate"]) == 0
    out = capsys.readouterr().out
    assert "Successfully validated" in out and "Semantic layer validation passed" in out


@pytest.mark.parametrize("grain", ["day", "week", "month"])
def test_every_metric_answers_by_time_and_two_dimensions(small, grain):
    groups = [
        (["admissions", "ed_visits", "average_length_of_stay_days", "readmission_rate_30d", "admissions_rolling_28d"],
         ["encounter__source", "patient__sex"]),
        (["bed_days"], ["encounter__source", "location__unit_name"]),
        (["bed_occupancy_rate"], ["location__unit_name", "location__unit_bed_capacity"]),
    ]
    seen = set()
    for metrics, dims in groups:
        rows = mx.raw_query(metrics, [f"metric_time__{grain}", *dims], db=small["db"], target=small["target"],
                            parse=False)
        assert rows, metrics
        assert {f"metric_time__{grain}", *dims, *metrics} <= set(rows[0])
        seen |= {m for m in metrics if any(r[m] for r in rows)}
    assert seen == set(REQUIRED)


def test_query_never_outputs_a_count_from_1_to_10(small, cat):
    rows = mx.query(["admissions", "ed_visits", "bed_days", "admissions_rolling_28d", "readmission_rate_30d"],
                    ["metric_time__day", "encounter__source"], db=small["db"], target=small["target"], parse=False,
                    cat=cat)
    assert rows and any(r["is_suppressed"] for r in rows)         # the demo days are small: some cells are hidden
    for r in rows:
        for m in ("admissions", "ed_visits", "bed_days", "admissions_rolling_28d"):
            assert not mx.is_small(r[m]), r
    assert any(r["admissions_rolling_28d"] and r["admissions_rolling_28d"] > 10 for r in rows)   # and some shown


def test_parity_on_the_small_warehouse(small, capsys):
    assert sp.run(small["db"], small["target"]) == 0
    assert "[FAIL]" not in capsys.readouterr().out


def test_parity_on_the_local_warehouse(capsys):
    """Against data/analytics/warehouse.duckdb when it exists and has the semantic layer's model (read only)."""
    duckdb = pytest.importorskip("duckdb")
    pytest.importorskip("dbt_metricflow")
    db = ROOT / "data" / "analytics" / "warehouse.duckdb"
    if not db.exists():
        pytest.skip("no local warehouse")
    try:
        con = duckdb.connect(str(db), read_only=True)
        has = con.execute("SELECT COUNT(*) FROM information_schema.tables "
                          "WHERE table_name = 'fact_bed_capacity_day'").fetchone()[0]
        con.close()
    except duckdb.Error as e:
        pytest.skip(f"local warehouse not readable: {e}")
    if not has:
        pytest.skip("local warehouse predates the semantic layer: run python3 analytics/build.py build")
    assert sp.run(db, ROOT / "analytics" / "dbt" / "target") == 0
    assert "[FAIL]" not in capsys.readouterr().out
