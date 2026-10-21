"""fhir_nightly: the nightly pipeline as one Airflow DAG.

Thin on purpose: every task runs a plain CLI command (see nightly/steps.py) and pushes a small
summary (counts, file names) to XCom. Nothing at the top level of this file touches the network or a database.
"""
import os
from datetime import datetime, timedelta, timezone

from airflow.sdk import DeadlineAlert, DeadlineReference, SyncCallback, Variable, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowSkipException
from airflow.timetables.trigger import CronTriggerTimetable

from nightly import alerts, steps

DAG_ID = "fhir_nightly"
SCHEDULE = "15 6 * * *"          # every day at 06:15 UTC
DEADLINE_MINUTES = int(os.environ.get("NIGHTLY_DEADLINE_MINUTES", "45"))
DATA = steps.PROJECT_DIR / "data"
POOL = "fhir_pipeline"            # one slot (start.sh): a backfill and a scheduled run never overlap on the data

default_args = {
    "owner": "data_engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": 2.0,     # 5 to 10 minutes, then 10 to 20 (Airflow adds jitter)
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(minutes=20),
    "on_failure_callback": alerts.on_task_failure,
    "pool": POOL,
    "weight_rule": "upstream",           # a started run finishes before the next run's first step takes the slot
}


def run(task_id, **kw):
    return steps.run_cli(steps.commands(steps.fhir_base(), **kw)[task_id])


@dag(
    dag_id=DAG_ID,
    description="Nightly Riverside General feed (demo), patient matching and analytics refresh",
    schedule=CronTriggerTimetable(SCHEDULE, timezone="UTC"),
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=timedelta(minutes=DEADLINE_MINUTES),
        callback=SyncCallback(alerts.on_deadline_missed, kwargs={"dag_id": DAG_ID, "minutes": DEADLINE_MINUTES}),
    ),
    tags=["demo", "fhir", "dbt"],
    doc_md=__doc__,
)
def fhir_nightly():

    # Feed days must load in date order: wait until the previous run's load_feed succeeded (a backfill, then the
    # next scheduled run). After a failed day, fix it and clear that task; later runs wait meanwhile.
    @task(execution_timeout=timedelta(minutes=30), depends_on_past=True)
    def load_feed():
        ctx = get_current_context()
        day = steps.feed_day(ctx.get("logical_date") or ctx["dag_run"].run_after)
        out = run("load_feed", day=day)
        return steps.summarize_feed(day, out, DATA / "hl7v2" / "daily")

    @task(execution_timeout=timedelta(minutes=10))
    def prune_reports():
        return steps.summarize_prune(run("prune_reports"))

    @task(execution_timeout=timedelta(minutes=20))
    def match_patients():
        return steps.summarize_match(run("match_patients"))

    @task(execution_timeout=timedelta(minutes=20))
    def extract():
        ti = get_current_context()["ti"]
        if steps.should_force_failure(os.environ.get("FORCE_EXTRACT_FAILURE"), ti.try_number):
            raise RuntimeError(f"FORCE_EXTRACT_FAILURE={os.environ['FORCE_EXTRACT_FAILURE']}: "
                               f"extract fails on purpose (try {ti.try_number})")
        run("extract")
        return steps.summarize_extract(DATA / "analytics" / "landing")

    @task(execution_timeout=timedelta(minutes=30))
    def dbt_build_and_test():
        run("dbt_build_and_test")
        counts = DATA / "orchestration" / "mart_counts.json"
        counts.parent.mkdir(parents=True, exist_ok=True)
        steps.run_cli(["orchestration/airflow/scripts/compare_counts.py", "--data-dir", "data", "--save", str(counts)])
        return {"mart_rows": steps.read_json(counts), "warehouse": "data/analytics/warehouse.duckdb"}

    # The metric definitions (analytics/dbt/models/semantic/) still parse and match the warehouse just built
    @task(execution_timeout=timedelta(minutes=10))
    def validate_semantic_layer():
        return steps.summarize_semantic(run("validate_semantic_layer"))

    @task(execution_timeout=timedelta(minutes=10))
    def source_freshness():
        return steps.summarize_freshness(run("source_freshness"))

    @task(execution_timeout=timedelta(minutes=10))
    def publish():
        return steps.summarize_publish(run("publish"))

    # Optional: the slim serving copy for the metrics API (api/) and the BI export. Runs when either is switched on.
    @task(execution_timeout=timedelta(minutes=10))
    def serving_snapshot():
        if not (steps.is_true(Variable.get("serving_snapshot_enabled", default="false"))
                or steps.is_true(Variable.get("bi_export_enabled", default="false"))):
            raise AirflowSkipException("Variables serving_snapshot_enabled and bi_export_enabled are not true")
        return steps.summarize_snapshot(run("serving_snapshot"))

    # Optional: suppressed aggregates from tonight's serving snapshot, to data/bi/*.csv by default, or to the Google
    # Sheet of BI_SHEET_ID in orchestration/airflow/.env when the Variable bi_export_target is sheets.
    # Aggregates only, never identifiers, never a count from 1 to 10 (the guard in analytics/export_bi.py).
    @task(execution_timeout=timedelta(minutes=10))
    def export_bi():
        if not steps.is_true(Variable.get("bi_export_enabled", default="false")):
            raise AirflowSkipException("Variable bi_export_enabled is not true")
        target = steps.bi_target(Variable.get("bi_export_target", default="csv"))
        return steps.summarize_export_bi(run("export_bi", bi_target=target), target)

    (load_feed() >> prune_reports() >> match_patients() >> extract() >> dbt_build_and_test()
     >> validate_semantic_layer() >> source_freshness() >> publish() >> serving_snapshot() >> export_bi())


fhir_nightly()
