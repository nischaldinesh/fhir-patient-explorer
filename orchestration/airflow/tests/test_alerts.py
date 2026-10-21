import json
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace

from nightly import alerts

TEXT = ("Airflow fhir_nightly FAILED at task 'extract' for logical date 2026-10-18T06:15:00+00:00 (try 3). "
        "Log: http://localhost:8088/dags/fhir_nightly/runs/r1/tasks/extract?try_number=3")


def test_build_alert_payload():
    p = alerts.build_alert_payload("fhir_nightly", "extract", "2026-10-18T06:15:00+00:00", 3,
                                   "http://localhost:8088/dags/fhir_nightly/runs/r1/tasks/extract?try_number=3")
    assert p == {"text": TEXT, "content": TEXT}


def test_payload_from_context_reads_only_ids():
    ti = SimpleNamespace(dag_id="fhir_nightly", task_id="extract", try_number=3,
                         log_url="http://localhost:8088/dags/fhir_nightly/runs/r1/tasks/extract?try_number=3")
    ctx = {"ti": ti, "logical_date": datetime(2026, 10, 18, 6, 15, tzinfo=timezone.utc),
           "params": {"patient": "Jane Doe"}}
    p = alerts.payload_from_context(ctx)
    assert p["text"] == TEXT and "Jane" not in json.dumps(p)


def test_payload_without_logical_date_uses_run_after():
    ti = SimpleNamespace(dag_id="d", task_id="t", try_number=1, log_url="")
    run = SimpleNamespace(run_after=datetime(2026, 10, 18, 7, 0, tzinfo=timezone.utc))
    assert "2026-10-18T07:00:00+00:00" in alerts.payload_from_context({"ti": ti, "logical_date": None,
                                                                        "dag_run": run})["text"]


def test_deadline_payload():
    assert alerts.deadline_payload("fhir_nightly", "manual__1", 45)["text"] == (
        "Airflow fhir_nightly run manual__1 did not succeed within 45 minutes (deadline missed).")


def test_on_deadline_missed_reads_the_run_id(monkeypatch):
    sent = []
    monkeypatch.setattr(alerts, "post_alert", sent.append)
    alerts.on_deadline_missed(context={"dag_run": {"dag_run_id": "manual__1", "conf": {"x": 1}}}, minutes=45)
    alerts.on_deadline_missed(context={"dag_run": SimpleNamespace(run_id="scheduled__2")}, minutes=45)
    alerts.on_deadline_missed(context=None)
    assert [p["text"].split(" run ")[1].split(" did ")[0] for p in sent] == ["manual__1", "scheduled__2", "unknown"]


def test_post_alert_is_a_no_op_without_url():
    assert alerts.post_alert({"text": "x"}, url="") is False


def test_post_alert_sends_json():
    got = []

    class H(BaseHTTPRequestHandler):
        def do_POST(self):
            got.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
            self.send_response(204)
            self.end_headers()

        def log_message(self, *a):
            pass

    srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        assert alerts.post_alert({"text": "x", "content": "x"}, url=f"http://127.0.0.1:{srv.server_port}/") is True
    finally:
        srv.shutdown()
    assert got == [{"text": "x", "content": "x"}]
    assert alerts.post_alert({"text": "x"}, url="http://127.0.0.1:1/") is False   # never raises
