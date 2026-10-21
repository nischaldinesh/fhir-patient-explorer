# Metrics API (demo)

A small HTTP API on the semantic layer (`analytics/SEMANTIC_LAYER.md`): the seven business metrics by name, split by
allowlisted dimensions, with small cells always hidden. Aggregates only. The data is demo data: the MIMIC IV demo
(deidentified), Synthea (synthetic) and the invented Riverside General feed.

| Piece | Where |
|---|---|
| App (FastAPI) | `api/app.py` |
| Allowlists, request checks, the response guard | `api/schema.py` |
| Engine: serving snapshot, lock, cache | `api/engine.py` |
| Suppression (the only way to data) | `analytics/metrics.py` |
| Serving snapshot | `analytics/serving.py`, `python3 analytics/build.py snapshot` |
| Tests | `tests/test_api.py` |

## Run it

```bash
python3 -m pip install -r analytics/requirements.txt -r api/requirements.txt
python3 analytics/build.py snapshot          # the slim serving copy, after a build
cp api/.env.example api/.env                 # then set METRICS_API_KEY, for example: openssl rand -hex 32
api/run.sh                                   # http://127.0.0.1:8000, docs at http://127.0.0.1:8000/docs
```

`api/run.sh` starts uvicorn on 127.0.0.1 only, port 8000 (`PORT=8001 api/run.sh` for another). Without
`METRICS_API_KEY` the app does not start.

## Endpoints

Every endpoint except `/health` needs the header `X-API-Key`.

| Endpoint | Answers |
|---|---|
| `GET /health` | `ok` and the snapshot in use (version, data date). No data, no key |
| `GET /v1/metrics` | The catalog: label, definition, type, suppression rule, the dimensions and filters each metric allows |
| `GET /v1/metrics/query` | `metrics`, `group_by`, `grain`, `start`, `end`, `where`, `limit` (below) |
| `GET /v1/saved-queries/{name}` | A saved query of the semantic layer: `ops_daily`, `quality_all_time` |

```bash
KEY=...   # your METRICS_API_KEY
curl -s -H "X-API-Key: $KEY" "http://127.0.0.1:8000/v1/metrics/query?metrics=admissions,ed_visits&group_by=source&grain=month&where=is_live_feed:true"
curl -s -H "X-API-Key: $KEY" http://127.0.0.1:8000/v1/saved-queries/quality_all_time
```

Query parameters, all checked against allowlists:

| Parameter | Allowed |
|---|---|
| `metrics` | `admissions`, `ed_visits`, `average_length_of_stay_days`, `readmission_rate_30d`, `bed_days`, `bed_occupancy_rate`, `admissions_rolling_28d` (comma separated) |
| `group_by` | Up to 2 of `source`, `class_code`, `encounter_kind`, `is_live_feed`, `sex`, `age_band`, `source_count`, `unit_name`, `location_level` |
| `grain` | `day`, `week`, `month`, `quarter`, `year` (required for `admissions_rolling_28d`) |
| `start`, `end` | Dates, `YYYY-MM-DD` |
| `where` | Repeatable `field:value`: `source` (mimic, synthea, hl7v2), `is_live_feed` (true, false), `encounter_kind` (hospital admission, emergency visit, part of another stay, other) |
| `limit` | 1 to 5000 rows, default 1000; `truncated` says when it cut rows |

A metric only takes the dimensions its semantic model reaches: occupancy splits by unit and time only, so
`bed_occupancy_rate` by `source` is a 422. Anything outside the lists is a 422 with the reason, never the value sent.

The response: `snapshot` (version, data date), `query` (as understood), `columns`, `rows`, `row_count`, `truncated`,
`suppressed_rows` and a `note` on suppression. The time column is `period`.

## Safety

* **Key.** `X-API-Key` against `METRICS_API_KEY`, compared in constant time (`hmac.compare_digest`). Placeholder in
  `api/.env.example`; the real key lives only in `api/.env` (ignored by git) or the environment.
* **No entity keys, no free text.** `group_by` never takes a patient, encounter or other entity, and never
  `patient__state` (a quasi identifier). Filters are fields and values from a list; the API writes the MetricFlow where
  clause itself.
* **Small cells.** Every number goes through `analytics/metrics.py`: a count from 1 to 10 is hidden, and so is a rate
  or mean whose counts are. The API also turns on **complementary suppression** there: within a response, along each
  group by column, while the hidden cells add up to 1 to 10, the smallest shown cell is hidden too, so a total from
  another query (the same metric without that column) cannot give a hidden cell back by subtraction.
* **Guard.** Before a response leaves, `schema.check_rows` fails it with a 500 if a count from 1 to 10, or a column
  outside the allowlist, is in it.
* **Logs.** One line per request: method, route template, status, time. No query values, no key. uvicorn's own access
  log (which prints query strings) is off.
* **Limits.** Suppression protects one response and simple totals. Someone who asks many overlapping questions (two
  date ranges one day apart, two filters) can still subtract their way to a small number; that needs query auditing
  or noise, which this demo does not do. Keep the key to people under a data use agreement.

## Reads never block the nightly build

The API never opens `data/analytics/warehouse.duckdb`. `python3 analytics/build.py snapshot` (after a build) writes
`data/analytics/serving/<stamp>/warehouse.duckdb` with eleven tables: the six the semantic layer reads (`fact_encounter`,
`dim_patient`, `dim_location`, `fact_bed_day`, `fact_bed_capacity_day`, `dim_date`), and five the BI export reads and the
API never does: the two suppressed quality marts (`mart_quality_period`, `mart_suppression_report`) and three small Gold
tables of pipeline metadata (`gold_terminology_quality`, `gold_empi_quality`, `gold_feed_freshness`), copied with ATTACH
(read only) and CREATE TABLE AS, plus the parsed semantic manifest. Then it swaps `data/analytics/serving/current.json`
by rename and keeps the two newest versions. The API picks up a new version on the next request. On this repo's data
the snapshot is 4.5 MB, the warehouse 669 MB.

MetricFlow runs one query at a time (two mf processes on one DuckDB file would fight over its lock), and answers are
cached per snapshot version and query. Both saved queries are answered once at startup and after every new snapshot,
so they come from memory: under 1 ms on this repo's data. A new ad hoc query takes about 1.2 seconds (one mf process).

In local Airflow the optional task `serving_snapshot` writes the snapshot after the build (set the Airflow Variable
`serving_snapshot_enabled` or `bi_export_enabled` to `true`).

The API and the BI export (`analytics/export_bi.py`) are separate channels, each suppressed on its own. A reader with
both could in theory subtract one from the other, which is one more reason the API stays local and keyed.
