"""Alerts for the fhir_nightly DAG: a task that fails after its last retry, and a run that misses its deadline.

An alert is a short JSON message POSTed to ALERT_WEBHOOK_URL (a Slack or Discord
incoming webhook: Slack reads "text", Discord reads "content"). It names the DAG, task, logical date, try number and
the log URL, never patient data. Without ALERT_WEBHOOK_URL nothing is sent and the payload is only logged.
"""
import json
import logging
import os
import urllib.request

log = logging.getLogger(__name__)


def failure_text(dag_id, task_id, logical_date, try_number, log_url):
    return (f"Airflow {dag_id} FAILED at task '{task_id}' for logical date {logical_date} "
            f"(try {try_number}). Log: {log_url}")


def build_alert_payload(dag_id, task_id, logical_date, try_number, log_url):
    """The webhook body for a failed task. Pure: no network, no Airflow."""
    text = failure_text(dag_id, task_id, logical_date, try_number, log_url)
    return {"text": text, "content": text}


def deadline_payload(dag_id, run_id, minutes):
    text = f"Airflow {dag_id} run {run_id} did not succeed within {minutes} minutes (deadline missed)."
    return {"text": text, "content": text}


def post_alert(payload, url=None, timeout=20):
    """POST the payload to ALERT_WEBHOOK_URL. Returns True when sent; never raises (an alert must not fail a run)."""
    url = url if url is not None else os.environ.get("ALERT_WEBHOOK_URL", "")
    log.warning("alert payload: %s", json.dumps(payload))
    if not url:
        log.warning("ALERT_WEBHOOK_URL is not set: alert not sent")
        return False
    req = urllib.request.Request(url, data=json.dumps(payload).encode("utf-8"), method="POST",
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            ok = 200 <= r.status < 300
    except Exception as e:  # noqa: BLE001 the alert is best effort, never fails the run
        log.warning("could not send the alert: %s", e)
        return False
    log.warning("alert sent" if ok else "alert not accepted")
    return ok


def payload_from_context(context):
    """The failure payload from an Airflow task context (only ids, dates and the log URL are read)."""
    ti = context["ti"]
    dag_id = getattr(ti, "dag_id", None) or context["dag"].dag_id
    logical = context.get("logical_date") or getattr(context.get("dag_run"), "run_after", None)
    return build_alert_payload(dag_id, ti.task_id, logical.isoformat() if logical else "none",
                               ti.try_number, getattr(ti, "log_url", "") or "")


def on_task_failure(context):
    """on_failure_callback: runs once a task has failed its last try."""
    post_alert(payload_from_context(context))


def on_deadline_missed(context=None, dag_id="fhir_nightly", minutes=45, **_):
    """SyncCallback of the DAG's DeadlineAlert: the run did not succeed within `minutes`."""
    run = (context or {}).get("dag_run")
    # Airflow passes the run as a dict (the REST API's DAGRunResponse: dag_run_id)
    run_id = (run.get("dag_run_id") or run.get("run_id")) if isinstance(run, dict) else getattr(run, "run_id", None)
    post_alert(deadline_payload(dag_id, run_id or "unknown", minutes))
