# Nightly pipeline on Apache Airflow

The nightly pipeline runs as one Airflow 3 DAG, `fhir_nightly`, on your machine in Docker. Each step is its own task
with retries, a timeout and logs; each run covers one date and can be rerun or backfilled; a failing task sends an
alert with a link to its log; and the UI keeps the history of every run. The DAG stays thin: every task calls a plain
CLI command you can also run by hand, so there is one implementation of each step.

## What runs

| Task | Command (from the repo root, project venv) | Notes |
|---|---|---|
| `load_feed` | `hl7v2/pipeline.py daily --day <day> --base $FHIR_BASE` | day = the UTC day before the run's logical date, like `date -u -d yesterday` |
| `prune_reports` | `hl7v2/pipeline.py prune-reports --keep 30 --base $FHIR_BASE` | |
| `match_patients` | `empi/match.py run --base $FHIR_BASE` | |
| `extract` | `analytics/build.py --base $FHIR_BASE extract` | |
| `dbt_build_and_test` | `analytics/build.py build` | dbt seed and build, every test; then mart row counts; appends one record to the build log `data/analytics/run_history/builds.ndjson`, which is inside `DATA_DIR`, so the `export_bi` task reads it for `bi_pipeline_runs` |
| `validate_semantic_layer` | `analytics/metrics.py validate` | dbt parse, then `mf validate-configs` against the new warehouse |
| `source_freshness` | `analytics/build.py freshness` | |
| `publish` | `analytics/build.py --base $FHIR_BASE publish` | |
| `serving_snapshot` | `analytics/build.py snapshot` | optional, skipped unless the Variable `serving_snapshot_enabled` or `bi_export_enabled` is `true`; the slim copy the metrics API and the BI export read (`api/README.md`) |
| `export_bi` | `analytics/export_bi.py --no-env-file export --target <csv or sheets>` | optional, skipped unless the Variable `bi_export_enabled` is `true`; reads tonight's snapshot; csv unless the Variable `bi_export_target` is `sheets` (below) |

Schedule: `15 6 * * *` in UTC (`CronTriggerTimetable`), every day at 06:15 UTC. Airflow 3 sets the logical date to
that time, so the run of 18 October loads the feed of 17 October. Each task pushes a small summary to XCom (counts,
file names, statuses), never data.

Built on Airflow **3.3.2** (`apache/airflow:3.3.2-python3.12`), LocalExecutor, Postgres for Airflow's metadata.
Three containers: `airflow-db`, `airflow-scheduler` (scheduler, DAG processor and triggerer; tasks run here) and
`airflow-apiserver` (UI and API on http://localhost:8088). The project's requirements (DuckDB, dbt) live in their own
venv in the image, `/opt/project-venv`, so they never conflict with Airflow's. The repo is mounted read only; only the
data directory is writable.

## Start

You need Docker and a FHIR server for the DAG to write to. **Never point it at your local HAPI on 8080.** Seed a
throwaway HAPI on port 8090 first. By default it holds only the live feed: an empty HAPI, the invented hospital's
reference data, then the Riverside General feed for the 3 days before a start day (default 3 days ago). The DAG then
loads from the start day on. It takes under a minute and needs no local data.

```bash
orchestration/airflow/throwaway-hapi/seed.sh --start-day 2026-10-17   # feed of 14 to 16 October; omit for 3 days ago

cd orchestration/airflow
cp .env.example .env     # set PSEUDONYM_SALT, AIRFLOW_DB_PASSWORD, the JWT and secret keys (openssl rand -hex 32)
                         # and AIRFLOW__CORE__FERNET_KEY:
                         # python3 -c "import base64,os;print(base64.urlsafe_b64encode(os.urandom(32)).decode())"
mkdir -p ../../data/airflow/airflow-logs
docker compose up -d --build
```

Open http://localhost:8088 (local only, no login: everyone who reaches it is an admin). The DAG starts paused.

Seeding starts the throwaway HAPI over and deletes the data directory's feed state, landing files and warehouse (not
its Airflow logs). It refuses to use `data/` itself.

**Full copy instead (opt in).** `seed.sh --from-local-db` copies your whole local database (MIMIC, Synthea and the
feed, with `pg_dump` of the running local `fhir-db`, read only) and your local feed state, so the archive models have
data too. **It needs about 3.5 GB of free disk**: a 130 MB dump in `data/airflow/seed/` and a 3.4 GB Docker volume
(`fhir-throwaway_pgdata`). Start the local Postgres first (`docker compose up -d db`, not HAPI). Remove it all
afterwards with `docker compose -f throwaway-hapi/compose.yaml down -v` and `rm -r data/airflow/seed`.

With the feed only seed, MIMIC and Synthea are absent, so the archive tables and the marts hold only the live feed:
after 5 feed days, 32 patient records (26 people), 31 visits and 469 lab results, and almost every quality cell is
suppressed. All 406 dbt tests still run and pass, and the semantic layer validates.

| Setting (`.env`) | Default | |
|---|---|---|
| `FHIR_BASE` | `http://host.docker.internal:8090/fhir` | the throwaway HAPI |
| `DATA_DIR` | `../../data/airflow` | mounted over `/opt/project/data`: feed state, landing files, warehouse, logs |
| `PSEUDONYM_SALT` | placeholder | the salt for pseudonyms and keys; keep it the same every run |
| `ALERT_WEBHOOK_URL` | empty | Slack or Discord incoming webhook; empty means alerts are only logged |
| `FORCE_EXTRACT_FAILURE` | empty | test switch: `once` or `always` |
| `NIGHTLY_DEADLINE_MINUTES` | 45 | the deadline alert |

The patient matcher writes only to HAPI on localhost. In the containers HAPI is `host.docker.internal`, so the compose
file sets `EMPI_EXTRA_LOCAL_HOSTS=host.docker.internal`; outside Docker it is unset.

## BI export (optional)

The task writes `data/airflow/bi/*.csv` by default. Only with the Variable `bi_export_target` set to `sheets` does it
write to Google Sheets, and then to the sheet of `BI_SHEET_ID` in `orchestration/airflow/.env`, never the one in
`analytics/.env` (`--no-env-file`): this stack holds throwaway data, so give it a sheet of its own.

```bash
docker compose exec airflow-scheduler airflow variables set bi_export_enabled true     # csv
# for Sheets: a second sheet shared with the service account as Editor, BI_SHEET_ID in .env, and the key file at
# data/airflow/secrets/bi_service_account.json (the containers see /opt/project/data/secrets/...), then
docker compose up -d                                                                   # picks up .env
docker compose exec airflow-scheduler airflow variables set bi_export_target sheets
```

The image installs `analytics/requirements-bi.txt` into the project venv, so after pulling this change run
`docker compose up -d --build` once.

## Trigger and backfill

```bash
docker compose exec airflow-scheduler airflow dags unpause fhir_nightly
docker compose exec airflow-scheduler airflow dags trigger fhir_nightly      # logical date now: loads yesterday
```

Unpausing also starts one run for the most recent 06:15 slot (Airflow does that even with catchup off). A backfill
only runs while the DAG is unpaused; create it first, then unpause, and the slot's run waits behind the backfill (see
Order and overlap below).

Backfill two past dates (the runs of 18 and 19 October load the feed of 17 and 18 October, right after a seed with
`--start-day 2026-10-17`). A feed day can only load after the last generated day, so backfill only days after it
(`last_day` in `data/airflow/hl7v2/feed_state.json`); an older day is reported as already loaded and loads nothing.
Including the latest 06:15 slot in the backfill means unpausing adds no extra run.

```bash
docker compose exec airflow-scheduler airflow backfill create --dag-id fhir_nightly \
  --from-date 2026-10-18 --to-date 2026-10-19T23:59:59+00:00 --max-active-runs 1 --reprocess-behavior completed
```

`--reprocess-behavior` is required because `load_feed` has `depends_on_past`. Run the same command again to rerun both
dates (a DAG has one backfill at a time: if it creates no runs, the last one is not marked done yet, try again a minute
later); the warehouse then has the same row counts:

```bash
.venv/bin/python orchestration/airflow/scripts/compare_counts.py --data-dir data/airflow --save /tmp/first.json
# ... the second backfill ...
.venv/bin/python orchestration/airflow/scripts/compare_counts.py --data-dir data/airflow --against /tmp/first.json
```

Rerunning is safe because every step is: a day already loaded routes its saved messages again (all duplicates) and
loads nothing, matching links only pairs not linked yet, extract and build replace their output.

**Order and overlap.** All tasks share the pool `fhir_pipeline` with one slot, so a backfill and a scheduled run never
work on the data at the same time. `load_feed` waits until the previous run's `load_feed` succeeded, so days load in
date order, and tasks are weighted so that a run that has started finishes before the next one begins. After a failed
day, fix it and clear that task; later runs wait meanwhile.

## Retries, timeouts and alerts

* Every task: 2 retries with a 5 minute base delay that doubles, and its own execution timeout (10 to 30 minutes).
  Airflow adds jitter of up to the same again, so the first retry comes 5 to 10 minutes later, the second 10 to 20.
  If your computer sleeps during a run, the running task loses its token and is stopped; it is retried after the wake.
* **Failure alert.** When a task fails its last try, `on_failure_callback` posts the DAG id, task id, logical date, try
  number and the log URL to `ALERT_WEBHOOK_URL`, as `text` (Slack) and `content` (Discord). No patient
  data. Without the URL it only logs the payload.
* **Runtime limit.** An Airflow 3 deadline alert (`DeadlineAlert`, reference: when the run was queued, 45 minutes)
  posts "did not succeed within 45 minutes" to the same webhook. Airflow clears the deadline only when the run
  succeeds, so a run that failed earlier gets this alert too, after its failure alert. It alerts and does not stop the run; the
  per task timeouts stop a stuck step.

Try it with the test switch (set it in `.env`, then `docker compose up -d` to apply):

```bash
FORCE_EXTRACT_FAILURE=once      # extract fails on try 1, succeeds on try 2 (5 to 10 minutes later)
FORCE_EXTRACT_FAILURE=always    # all 3 tries fail; the alert payload shows in the extract log
```

## Tests

```bash
docker compose exec airflow-scheduler python -m pytest -q -p no:cacheprovider /opt/project/orchestration/airflow/tests
```

They check that the DAG imports with no errors and no network or database calls at import, the task ids and their
order, retries, timeouts, pool and alerts on every task, the schedule, catchup and the deadline, and the pure helpers
(commands, feed day, summaries, the alert payload).

## Stop

```bash
docker compose down              # keep the metadata
docker compose down -v           # also delete it
docker compose -f throwaway-hapi/compose.yaml down -v   # throw the test HAPI away
```
