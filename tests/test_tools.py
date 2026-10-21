import gzip
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import tools  # noqa: E402


def write_ndjson(path, resources, compress=True):
    text = "\n".join(json.dumps(r) for r in resources) + "\n"
    if compress:
        with gzip.open(path, "wt", encoding="utf-8") as f:
            f.write(text)
    else:
        path.write_text(text, encoding="utf-8")
    return path


def test_iter_ndjson_reads_gzip_and_plain(tmp_path):
    rows = [{"resourceType": "Patient", "id": "a"}, {"resourceType": "Patient", "id": "b"}]
    gz = write_ndjson(tmp_path / "p.ndjson.gz", rows)
    plain = write_ndjson(tmp_path / "p.ndjson", rows, compress=False)
    assert list(tools.iter_ndjson(gz)) == rows
    assert list(tools.iter_ndjson(plain)) == rows


def test_chunked_sizes():
    assert [len(c) for c in tools.chunked(range(7), 3)] == [3, 3, 1]
    assert list(tools.chunked([], 3)) == []


def test_batch_bundle_uses_put_with_ids():
    b = tools.batch_bundle([{"resourceType": "Observation", "id": "x1"}])
    assert b["type"] == "batch"
    assert b["entry"][0]["request"] == {"method": "PUT", "url": "Observation/x1"}


def test_failed_entries_detects_non_2xx():
    resp = {"entry": [
        {"response": {"status": "201 Created"}},
        {"response": {"status": "400 Bad Request", "outcome": {"issue": [{"diagnostics": "bad code"}]}}},
        {"response": {"status": "200 OK"}},
    ]}
    assert tools.failed_entries(resp) == [(1, "400 Bad Request", "bad code")]


def test_select_mimic_files_orders_and_skips_heavy(tmp_path):
    obs = write_ndjson(tmp_path / "MimicObservationLabevents.ndjson.gz", [{"resourceType": "Observation", "id": "o"}])
    pat = write_ndjson(tmp_path / "MimicPatient.ndjson.gz", [{"resourceType": "Patient", "id": "p"}])
    org = write_ndjson(tmp_path / "MimicOrganization.ndjson.gz", [{"resourceType": "Organization", "id": "g"}])
    heavy = write_ndjson(tmp_path / "MimicObservationChartevents.ndjson.gz", [{"resourceType": "Observation", "id": "c"}])
    paths = [obs, pat, org, heavy]
    assert [p.name for p in tools.select_mimic_files(paths)] == [org.name, pat.name, obs.name]
    assert heavy in tools.select_mimic_files(paths, include_all=True)


def test_order_synthea_files_puts_shared_bundles_first():
    names = ["Zoe_1.json", "practitionerInformation1.json", "Adam_2.json", "hospitalInformation1.json"]
    ordered = [p.name for p in tools.order_synthea_files([Path(n) for n in names])]
    assert ordered == ["hospitalInformation1.json", "practitionerInformation1.json", "Adam_2.json", "Zoe_1.json"]


def test_parse_mimic_index_handles_relative_and_absolute_links():
    html = ('<a href="MimicPatient.ndjson.gz">x</a>'
            '<a href="/files/mimic-iv-fhir-demo/2.1.0/fhir/MimicEncounter.ndjson.gz">y</a>'
            '<a href="../">up</a><a href="MimicPatient.ndjson.gz">dup</a>')
    assert tools.parse_mimic_index(html) == ["MimicEncounter.ndjson.gz", "MimicPatient.ndjson.gz"]


def test_mimic_load_end_to_end_against_fake_server(tmp_path):
    """Spin up a tiny fake FHIR server and load two files through the real CLI path."""
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    received = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            received.extend(e["request"]["url"] for e in body["entry"])
            resp = {"resourceType": "Bundle", "type": "batch-response",
                    "entry": [{"response": {"status": "201 Created"}} for _ in body["entry"]]}
            data = json.dumps(resp).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/fhir+json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *a):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        write_ndjson(tmp_path / "MimicPatient.ndjson.gz", [{"resourceType": "Patient", "id": f"p{i}"} for i in range(3)])
        write_ndjson(tmp_path / "MimicObservationLabevents.ndjson.gz", [{"resourceType": "Observation", "id": f"o{i}"} for i in range(5)])
        base = f"http://127.0.0.1:{server.server_port}/fhir"
        rc = tools.main(["--base", base, "mimic", "--dir", str(tmp_path), "--batch", "2"])
    finally:
        server.shutdown()
    assert rc == 0
    assert len(received) == 8
    assert received[0].startswith("Patient/")  # patients load before observations


def _fake_server(status_for_entry):
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    calls = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            calls.append(len(body["entry"]))
            resp = {"resourceType": "Bundle", "type": "batch-response",
                    "entry": [{"response": {"status": status_for_entry}} for _ in body["entry"]]}
            data = json.dumps(resp).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *a):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, calls


def test_loader_stops_when_server_is_unhealthy(tmp_path):
    write_ndjson(tmp_path / "MimicObservationLabevents.ndjson.gz",
                 [{"resourceType": "Observation", "id": f"o{i}"} for i in range(50)])
    server, calls = _fake_server("500 Internal Server Error")
    try:
        rc = tools.main(["--base", f"http://127.0.0.1:{server.server_port}/fhir",
                         "mimic", "--dir", str(tmp_path), "--batch", "5"])
    finally:
        server.shutdown()
    assert rc == 1
    assert len(calls) == 3  # gave up after 3 dead batches instead of 10


def test_only_filter_selects_files(tmp_path):
    write_ndjson(tmp_path / "MimicPatient.ndjson.gz", [{"resourceType": "Patient", "id": "p"}])
    write_ndjson(tmp_path / "MimicProcedure.ndjson.gz", [{"resourceType": "Procedure", "id": "x"}])
    server, calls = _fake_server("200 OK")
    try:
        rc = tools.main(["--base", f"http://127.0.0.1:{server.server_port}/fhir",
                         "mimic", "--dir", str(tmp_path), "--only", "procedure"])
    finally:
        server.shutdown()
    assert rc == 0
    assert calls == [1]