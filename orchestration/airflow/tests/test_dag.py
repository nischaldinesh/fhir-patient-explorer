"""The fhir_nightly DAG as Airflow parses it. Runs inside the Airflow container:
docker compose exec airflow-scheduler python -m pytest -q -p no:cacheprovider /opt/project/orchestration/airflow/tests
"""
import importlib.util
import socket
import subprocess
from datetime import timedelta

import pytest
from airflow.dag_processing.dagbag import DagBag

from conftest import DAGS

ORDER = ["load_feed", "prune_reports", "match_patients", "extract", "dbt_build_and_test", "validate_semantic_layer",
         "source_freshness", "publish", "serving_snapshot", "export_bi"]


@pytest.fixture(scope="module")
def dag():
    bag = DagBag(dag_folder=str(DAGS))
    assert bag.import_errors == {}
    assert list(bag.dag_ids) == ["fhir_nightly"]
    return bag.dags["fhir_nightly"]


def test_task_ids_and_order(dag):
    assert sorted(dag.task_ids) == sorted(ORDER)
    for up, down in zip(ORDER, ORDER[1:]):
        assert dag.get_task(up).downstream_task_ids == {down}
        assert dag.get_task(down).upstream_task_ids == {up}
    assert dag.get_task(ORDER[0]).upstream_task_ids == set()
    assert dag.get_task(ORDER[-1]).downstream_task_ids == set()


def test_export_reads_the_snapshot_built_the_same_night(dag):
    """The BI export reads the serving snapshot, so it runs after it; a skipped snapshot skips the export."""
    assert dag.get_task("serving_snapshot").downstream_task_ids == {"export_bi"}
    for t in ("serving_snapshot", "export_bi"):
        assert str(dag.get_task(t).trigger_rule) in ("all_success", "TriggerRule.ALL_SUCCESS"), t


def test_retries_timeouts_and_alerts_on_every_task(dag):
    for t in dag.tasks:
        assert t.retries == 2, t.task_id
        assert t.retry_delay == timedelta(minutes=5), t.task_id
        assert t.retry_exponential_backoff == 2.0, t.task_id
        assert t.execution_timeout is not None and t.execution_timeout <= timedelta(minutes=30), t.task_id
        assert t.on_failure_callback, t.task_id
        assert t.pool == "fhir_pipeline", t.task_id
        assert str(t.weight_rule) in ("upstream", "WeightRule.UPSTREAM"), t.task_id
        assert t.depends_on_past == (t.task_id == "load_feed"), t.task_id


def test_schedule_is_daily_at_0615_utc(dag):
    tt = dag.timetable
    assert type(tt).__name__ == "CronTriggerTimetable"
    assert tt._expression == "15 6 * * *"
    assert str(tt._timezone) in ("UTC", "Timezone('UTC')")
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.start_date.tzinfo is not None


def test_deadline_alert(dag):
    deadline = dag.deadline[0] if isinstance(dag.deadline, list) else dag.deadline
    assert deadline.interval == timedelta(minutes=45)
    assert deadline.callback.path == "nightly.alerts.on_deadline_missed"


def test_no_network_or_database_at_import(monkeypatch):
    """Parsing the file must not open a socket (HAPI, Postgres, the API server) or start a process."""
    def refuse(*a, **k):
        raise AssertionError("network or process call at import time")
    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)
    monkeypatch.setattr(subprocess, "Popen", refuse)
    spec = importlib.util.spec_from_file_location("fhir_nightly_isolated", DAGS / "fhir_nightly.py")
    spec.loader.exec_module(importlib.util.module_from_spec(spec))
