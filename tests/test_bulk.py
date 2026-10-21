"""The Bulk Data client (analytics/bulk.py) and SMART backend services auth (analytics/smart_auth.py): the pure helpers,
and the whole flow against a small fake Bulk Data server on localhost (kickoff, poll, download, error file, delete,
resume after a crash, incremental _since, retries)."""
import json
import sys
import threading
import time
import urllib.parse
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "analytics"))
import bulk  # noqa: E402
import smart_auth  # noqa: E402

TYPES = ["Patient", "Observation"]


# ---------- pure helpers ----------

def test_parse_retry_after():
    assert bulk.parse_retry_after("5") == 5.0
    assert bulk.parse_retry_after(" 2.5 ") == 2.5
    assert bulk.parse_retry_after(None) is None
    assert bulk.parse_retry_after("soon") is None
    now = datetime(2026, 10, 19, 12, 0, 0, tzinfo=timezone.utc)
    assert bulk.parse_retry_after("Mon, 19 Oct 2026 12:00:30 GMT", now) == 30.0
    assert bulk.parse_retry_after("Mon, 19 Oct 2026 11:00:00 GMT", now) == 0.0


def test_poll_delay_is_clamped():
    assert bulk.poll_delay(None) == bulk.POLL_DEFAULT
    assert bulk.poll_delay(0) == 1.0
    assert bulk.poll_delay(30) == 30
    assert bulk.poll_delay(3600) == 120.0


def test_backoff():
    assert bulk.backoff(1, retry_after=7) == 7
    assert bulk.backoff(1, retry_after=999) == 60.0
    assert bulk.backoff(1, rand=lambda: 1.0) == 1.0
    assert bulk.backoff(3, rand=lambda: 1.0) == 4.0
    assert bulk.backoff(3, rand=lambda: 0.0) == 2.0
    assert bulk.backoff(20, rand=lambda: 1.0) == 60.0


def test_same_origin_keeps_only_path_and_query():
    base = "http://localhost:8090/fhir"
    assert bulk.same_origin(base, "http://hapi-internal:8080/fhir/Binary/1?x=2") == "http://localhost:8090/fhir/Binary/1?x=2"
    assert bulk.same_origin(base, "https://evil.example/steal") == "http://localhost:8090/steal"


def test_kickoff_url():
    u = bulk.kickoff_url("http://h/fhir/", ["Patient", "Observation"], "2026-10-01T00:00:00Z")
    assert u.startswith("http://h/fhir/$export?")
    q = urllib.parse.parse_qs(urllib.parse.urlsplit(u).query)
    assert q == {"_outputFormat": ["application/fhir+ndjson"], "_type": ["Patient,Observation"],
                 "_since": ["2026-10-01T00:00:00Z"]}
    assert bulk.kickoff_url("http://h/fhir", [], None, group="g 1").startswith("http://h/fhir/Group/g%201/$export?")


def test_server_slug_and_state_dir(tmp_path):
    assert bulk.server_slug("http://localhost:8090/fhir") == "localhost-8090-fhir"
    assert bulk.state_dir(tmp_path / "x" / "landing", "http://h:1/fhir") == (tmp_path / "x" / "bulk" / "h-1-fhir").resolve()


def test_plan_downloads_and_classify_issues():
    m = {"output": [{"type": "Patient", "url": "u1", "count": 2}, {"type": "Patient", "url": "u2"}],
         "error": [{"type": "OperationOutcome", "url": "e1"}]}
    assert bulk.plan_downloads(m) == [("output", "Patient", "u1", "Patient-0000.ndjson", 2),
                                      ("output", "Patient", "u2", "Patient-0001.ndjson", None),
                                      ("error", "OperationOutcome", "e1", "errors-0000.ndjson", None)]
    fatal, warn = bulk.classify_issues([
        {"issue": [{"severity": "warning", "diagnostics": "slow"}, {"severity": "error", "diagnostics": "bad"}]},
        {"issue": [{"severity": "fatal", "details": {"text": "dead"}}]}])
    assert fatal == ["bad", "dead"] and warn == ["slow"]


def test_resolve_since():
    wm = {"transaction_time": "2026-10-01T00:00:00Z"}
    assert bulk.resolve_since("auto", wm, True) == "2026-10-01T00:00:00Z"
    assert bulk.resolve_since("auto", wm, False) is None
    assert bulk.resolve_since("auto", None, True) is None
    assert bulk.resolve_since("full", wm, True) is None
    assert bulk.resolve_since("2026-01-01", wm, True) == "2026-01-01"


def line(rt, rid, v=1):
    return json.dumps({"resourceType": rt, "id": rid, "v": v}) + "\n"


def test_merge_type_full_and_delta(tmp_path):
    store = tmp_path / "Patient.ndjson"
    p1 = tmp_path / "p1.ndjson"
    p1.write_text(line("Patient", "a") + line("Patient", "b"))
    assert bulk.merge_type(store, [p1], store, "Patient", full=True) == 2
    d = tmp_path / "d.ndjson"
    d.write_text(line("Patient", "b", 2) + line("Patient", "c"))
    assert bulk.merge_type(store, [d], store, "Patient", full=False) == 3
    rows = {r["id"]: r["v"] for r in map(json.loads, store.read_text().splitlines())}
    assert rows == {"a": 1, "b": 2, "c": 1}
    # the same delta again changes nothing (a resumed job merges twice)
    assert bulk.merge_type(store, [d], store, "Patient", full=False) == 3
    bad = tmp_path / "bad.ndjson"
    bad.write_text(line("Observation", "x"))
    with pytest.raises(bulk.BulkError):
        bulk.merge_type(store, [bad], store, "Patient", full=False)


def test_write_json_is_atomic(tmp_path):
    f = tmp_path / "s" / "job.json"
    bulk.write_json(f, {"a": 1})
    assert json.loads(f.read_text()) == {"a": 1}
    assert not list(f.parent.glob("*.tmp"))


# ---------- a fake Bulk Data server ----------

class FakeBulk:
    """Serves one job at a time. files: {name: text}; manifest outputs refer to them."""

    def __init__(self):
        self.requests = []
        self.files = {}
        self.outputs = []
        self.errors = []
        self.polls_before_done = 2
        self.polls = 0
        self.fail = {}            # path -> remaining failures (HTTP 503)
        self.status_gone = False
        self.transaction_time = "2026-10-19T10:00:00Z"
        self.totals = {}
        self.deleted = []
        self.require_token = None

    def handler(self):
        fake = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def send(self, code, body=b"", headers=None):
                self.send_response(code)
                for k, v in (headers or {}).items():
                    self.send_header(k, v)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_DELETE(self):
                fake.requests.append(("DELETE", self.path, dict(self.headers)))
                fake.deleted.append(self.path)
                self.send(202)

            def do_GET(self):
                fake.requests.append(("GET", self.path, dict(self.headers)))
                path = urllib.parse.urlsplit(self.path).path
                if fake.require_token and self.headers.get("Authorization") != f"Bearer {fake.require_token}":
                    return self.send(401, b"{}")
                if fake.fail.get(path):
                    fake.fail[path] -= 1
                    return self.send(503, b"busy", {"Retry-After": "0"})
                if path == "/fhir/$export" or path.endswith("/$export"):
                    fake.polls = 0
                    return self.send(202, b"", {"Content-Location": "http://hapi-internal:8080/fhir/status/1"})
                if path == "/fhir/status/1":
                    if fake.status_gone:
                        return self.send(404, b"{}")
                    fake.polls += 1
                    if fake.polls <= fake.polls_before_done:
                        return self.send(202, b"", {"X-Progress": f"{fake.polls * 40}% done", "Retry-After": "3"})
                    m = {"transactionTime": fake.transaction_time, "request": "x", "requiresAccessToken": True,
                         "output": fake.outputs, "error": fake.errors}
                    return self.send(200, json.dumps(m).encode(), {"Content-Type": "application/json"})
                if path.startswith("/fhir/files/"):
                    name = path.rsplit("/", 1)[1]
                    return self.send(200, fake.files[name].encode(), {"Content-Type": "application/fhir+ndjson"})
                if path.startswith("/fhir/Binary/"):
                    return self.send(404, b"{}")
                rt = path.rsplit("/", 1)[1]
                if rt in fake.totals:
                    return self.send(200, json.dumps({"total": fake.totals[rt]}).encode())
                self.send(404, b"{}")

        return H

    def publish(self, files, errors=None):
        """files: {type: [text of each file]}"""
        self.files, self.outputs = {}, []
        for t, texts in files.items():
            for i, text in enumerate(texts):
                name = f"{t}-{i}.ndjson"
                self.files[name] = text
                self.outputs.append({"type": t, "url": f"http://hapi-internal:8080/fhir/files/{name}",
                                     "count": text.count("\n")})
        self.errors = []
        for i, text in enumerate(errors or []):
            name = f"err-{i}.ndjson"
            self.files[name] = text
            self.errors.append({"type": "OperationOutcome", "url": f"http://hapi-internal:8080/fhir/files/{name}"})

    def count(self, method, prefix):
        return sum(1 for m, p, _ in self.requests if m == method and urllib.parse.urlsplit(p).path.startswith(prefix))


@pytest.fixture
def server():
    fake = FakeBulk()
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), fake.handler())
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    fake.base = f"http://127.0.0.1:{httpd.server_address[1]}/fhir"
    yield fake
    httpd.shutdown()


def quiet_client(**kw):
    return bulk.Client(sleep=lambda s: None, say=lambda *a: None, **kw)


def run_export(fake, landing, since="auto", client=None, max_wait=3600):
    return bulk.bulk_extract(fake.base, landing, TYPES, since, None, max_wait, client=client or quiet_client(),
                             say=lambda *a: None)


def no_sleep(monkeypatch):
    waits = []
    monkeypatch.setattr(bulk.time, "sleep", lambda s: waits.append(s))
    return waits


def test_full_export_lands_like_the_search_extract(server, tmp_path, monkeypatch):
    waits = no_sleep(monkeypatch)
    server.publish({"Patient": [line("Patient", "a") + line("Patient", "b"), line("Patient", "c")],
                    "Observation": [line("Observation", "o1")]},
                   errors=[json.dumps({"resourceType": "OperationOutcome",
                                       "issue": [{"severity": "warning", "diagnostics": "skipped one"}]}) + "\n"])
    server.totals = {"Patient": 3, "Observation": 1}
    landing = tmp_path / "landing"
    m = run_export(server, landing)
    assert m["mode"] == "bulk" and m["since"] is None
    assert m["counts"] == {"Patient": {"extracted": 3, "server_total": 3}, "Observation": {"extracted": 1, "server_total": 1}}
    assert m["transaction_time"] == "2026-10-19T10:00:00Z"
    assert m["export_warnings"] == ["skipped one"]
    assert set(m) >= {"extracted_at", "base", "counts", "feed_runs", "binaries"}
    assert sorted(p.name for p in landing.iterdir()) == ["Observation.ndjson", "Patient.ndjson", "feed_runs.json",
                                                          "manifest.json"]
    assert len((landing / "Patient.ndjson").read_text().splitlines()) == 3
    # kickoff asked for an async export, without cache, of the types; polls honored Retry-After (3 s)
    kick = next(h for m_, p, h in server.requests if "$export" in p)
    assert kick["Prefer"] == "respond-async" and kick["Cache-Control"] == "no-cache"
    assert waits.count(3) == 2
    # files came from the base host, not the host in the manifest; the job was deleted, the state cleaned up
    assert server.count("GET", "/fhir/files/") == 4
    assert server.deleted == ["/fhir/status/1"]
    state = bulk.state_dir(landing, server.base)
    assert not (state / "job.json").exists() and not (state / "parts").exists()
    assert json.loads((state / "watermark.json").read_text())["transaction_time"] == "2026-10-19T10:00:00Z"


def test_resume_skips_files_already_downloaded(server, tmp_path, monkeypatch):
    no_sleep(monkeypatch)
    server.publish({"Patient": [line("Patient", "a"), line("Patient", "b")], "Observation": [line("Observation", "o")]})
    server.fail["/fhir/files/Patient-1.ndjson"] = 99     # the second file never comes: the run stops there
    landing = tmp_path / "landing"
    with pytest.raises(bulk.BulkError):
        run_export(server, landing, client=quiet_client(retries=2))
    state = bulk.state_dir(landing, server.base)
    job = json.loads((state / "job.json").read_text())
    assert len(job["done"]) == 1 and job["manifest"]
    assert not (state / "watermark.json").exists()
    assert server.count("GET", "/fhir/files/Patient-0") == 1
    server.fail.clear()
    kickoffs = server.count("GET", "/fhir/$export")
    m = run_export(server, landing)
    assert server.count("GET", "/fhir/$export") == kickoffs        # the same job, no new kickoff
    assert server.count("GET", "/fhir/files/Patient-0") == 1       # not downloaded again
    assert m["files_skipped"] == 1 and m["files_downloaded"] == 2
    assert m["counts"]["Patient"]["extracted"] == 2


def test_retry_on_503_with_retry_after(server, tmp_path, monkeypatch):
    no_sleep(monkeypatch)
    server.publish({"Patient": [line("Patient", "a")], "Observation": []})
    server.fail["/fhir/files/Patient-0.ndjson"] = 2
    m = run_export(server, tmp_path / "landing")
    assert m["counts"]["Patient"]["extracted"] == 1
    assert server.count("GET", "/fhir/files/Patient-0") == 3


def test_incremental_since_upserts_the_delta(server, tmp_path, monkeypatch):
    no_sleep(monkeypatch)
    landing = tmp_path / "landing"
    server.publish({"Patient": [line("Patient", "a") + line("Patient", "b")], "Observation": [line("Observation", "o")]})
    run_export(server, landing)
    server.transaction_time = "2026-10-20T10:00:00Z"
    server.publish({"Patient": [line("Patient", "b", 2) + line("Patient", "c")]})   # only what changed
    m = run_export(server, landing)
    kick = [p for meth, p, _ in server.requests if "$export" in p][-1]
    assert urllib.parse.parse_qs(urllib.parse.urlsplit(kick).query)["_since"] == ["2026-10-19T10:00:00Z"]
    assert m["since"] == "2026-10-19T10:00:00Z"
    rows = {r["id"]: r["v"] for r in map(json.loads, (landing / "Patient.ndjson").read_text().splitlines())}
    assert rows == {"a": 1, "b": 2, "c": 1}
    assert (landing / "Observation.ndjson").read_text() == line("Observation", "o")   # untouched by the delta
    state = bulk.state_dir(landing, server.base)
    assert json.loads((state / "watermark.json").read_text())["transaction_time"] == "2026-10-20T10:00:00Z"
    # --since full starts over
    server.publish({"Patient": [line("Patient", "z")], "Observation": []})
    m = run_export(server, landing, since="full")
    assert m["since"] is None and m["counts"]["Patient"]["extracted"] == 1


def test_fatal_error_file_stops_before_merge(server, tmp_path, monkeypatch):
    no_sleep(monkeypatch)
    server.publish({"Patient": [line("Patient", "a")]},
                   errors=[json.dumps({"resourceType": "OperationOutcome",
                                       "issue": [{"severity": "error", "diagnostics": "export broke"}]}) + "\n"])
    landing = tmp_path / "landing"
    with pytest.raises(bulk.BulkError, match="export broke"):
        run_export(server, landing)
    assert not (bulk.state_dir(landing, server.base) / "watermark.json").exists()
    assert not landing.exists() or not list(landing.glob("*.ndjson"))


def test_job_gone_starts_a_new_export(server, tmp_path, monkeypatch):
    no_sleep(monkeypatch)
    server.publish({"Patient": [line("Patient", "a")], "Observation": []})
    landing = tmp_path / "landing"
    state = bulk.state_dir(landing, server.base)
    bulk.write_json(state / "job.json", {"base": server.base, "request": {"types": TYPES, "since": None, "group": None},
                                         "kickoff_at": "x", "status_url": "http://h/fhir/status/1", "manifest": None,
                                         "done": {}})
    server.status_gone = True
    calls = {"n": 0}
    orig = bulk.Exporter.poll

    def poll_once_gone(self, job, max_wait):
        calls["n"] += 1
        if calls["n"] > 1:
            server.status_gone = False
        return orig(self, job, max_wait)
    monkeypatch.setattr(bulk.Exporter, "poll", poll_once_gone)
    m = run_export(server, landing)
    assert m["counts"]["Patient"]["extracted"] == 1
    assert server.count("GET", "/fhir/$export") == 1


def test_max_wait_keeps_the_job(server, tmp_path):
    server.polls_before_done = 10**6
    server.publish({"Patient": []})
    landing = tmp_path / "landing"
    clock = iter(range(0, 10**6, 50))
    ex = bulk.Exporter(server.base, bulk.state_dir(landing, server.base), quiet_client(), say=lambda *a: None,
                       sleep=lambda s: None, clock=lambda: next(clock))
    with pytest.raises(bulk.Timeout):
        ex.run(TYPES, "auto", None, max_wait=120)
    assert (bulk.state_dir(landing, server.base) / "job.json").exists()


def test_other_options_while_a_job_is_in_flight(server, tmp_path):
    landing = tmp_path / "landing"
    state = bulk.state_dir(landing, server.base)
    bulk.write_json(state / "job.json", {"base": server.base, "request": {"types": ["Patient"], "since": None,
                                                                          "group": None},
                                         "kickoff_at": "x", "status_url": "u", "manifest": None, "done": {}})
    with pytest.raises(bulk.BulkError, match="cancel"):
        bulk.Exporter(server.base, state, quiet_client(), say=lambda *a: None).run(TYPES)


def test_group_export_and_401_refresh(server, tmp_path, monkeypatch):
    no_sleep(monkeypatch)
    server.publish({"Patient": [line("Patient", "a")], "Observation": []})

    class Auth:
        tokens = iter(["old", "new", "new2"])

        def __init__(self):
            self.token = next(self.tokens)

        def headers(self):
            return {"Authorization": f"Bearer {self.token}"}

        def invalidate(self):
            self.token = next(self.tokens)
    server.require_token = "new"
    m = bulk.bulk_extract(server.base, tmp_path / "landing", TYPES, "auto", "cohort-1", 3600,
                          client=quiet_client(auth=Auth()), say=lambda *a: None)
    assert m["group"] == "cohort-1"
    assert any(p.startswith("/fhir/Group/cohort-1/$export") for _, p, _ in server.requests)


# ---------- SMART backend services auth ----------

@pytest.fixture(scope="module")
def rsa_pem():
    pytest.importorskip("jwt")
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private = key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                serialization.NoEncryption())
    public = key.public_key().public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
    return private, public


def test_claims():
    c = smart_auth.claims("cid", "https://t/token", now=1000, lifetime=900, jti="j")
    assert c == {"iss": "cid", "sub": "cid", "aud": "https://t/token", "exp": 1300, "jti": "j"}


def test_client_assertion_is_rs384_and_verifies(rsa_pem):
    import jwt
    private, public = rsa_pem
    now = int(time.time())
    token = smart_auth.client_assertion("cid", "https://t/token", private, kid="k1", now=now)
    header = jwt.get_unverified_header(token)
    assert header["alg"] == "RS384" and header["kid"] == "k1" and header["typ"] == "JWT"
    body = jwt.decode(token, public, algorithms=["RS384"], audience="https://t/token")
    assert body["iss"] == body["sub"] == "cid" and body["exp"] - now <= 300 and body["jti"]
    other = smart_auth.client_assertion("cid", "https://t/token", private, now=now)
    assert jwt.decode(other, public, algorithms=["RS384"], audience="https://t/token")["jti"] != body["jti"]


def test_es384_for_an_ec_key():
    pytest.importorskip("jwt")
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    pem = ec.generate_private_key(ec.SECP384R1()).private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
    assert smart_auth.algorithm(pem) == "ES384"


def test_backend_auth_caches_and_refreshes(rsa_pem):
    private, _ = rsa_pem
    posts = []
    now = [1000.0]

    def post(url, body):
        form = urllib.parse.parse_qs(body)
        posts.append(form)
        return {"access_token": f"tok{len(posts)}", "expires_in": 300}
    auth = smart_auth.BackendAuth("cid", private, "https://t/token", post=post, clock=lambda: now[0])
    assert auth.headers() == {"Authorization": "Bearer tok1"}
    assert auth.headers() == {"Authorization": "Bearer tok1"}
    assert posts[0]["grant_type"] == ["client_credentials"] and posts[0]["scope"] == ["system/*.read"]
    assert posts[0]["client_assertion_type"] == [smart_auth.ASSERTION_TYPE]
    now[0] += 250                                    # within 60 s of expiry: a new token
    assert auth.headers() == {"Authorization": "Bearer tok2"}
    auth.invalidate()
    assert auth.headers() == {"Authorization": "Bearer tok3"}


def test_from_env(tmp_path, rsa_pem):
    assert smart_auth.from_env("http://h/fhir", env={}) is None
    with pytest.raises(RuntimeError):
        smart_auth.from_env("http://h/fhir", env={"BULK_CLIENT_ID": "c"})
    key = tmp_path / "k.pem"
    key.write_bytes(rsa_pem[0])
    auth = smart_auth.from_env("http://h/fhir", env={"BULK_CLIENT_ID": "c", "BULK_PRIVATE_KEY_PATH": str(key)},
                               fetch=lambda url: {"token_endpoint": "https://h/auth/token"})
    assert auth.token_url == "https://h/auth/token" and auth.scope == "system/*.read"
    with pytest.raises(RuntimeError):
        smart_auth.discover_token_url("http://h/fhir", fetch=lambda url: {})


def test_auth_failure_is_a_bulk_error(server, tmp_path, monkeypatch):
    class Refused:
        def headers(self):
            raise RuntimeError("token request -> HTTP 400: invalid_grant")

        def invalidate(self):
            pass
    with pytest.raises(bulk.BulkError, match="auth failed"):
        run_export(server, tmp_path / "landing", client=quiet_client(auth=Refused()))
    monkeypatch.setenv("BULK_CLIENT_ID", "c")
    monkeypatch.delenv("BULK_PRIVATE_KEY_PATH", raising=False)
    with pytest.raises(bulk.BulkError, match="not set up"):
        bulk.make_client("http://h/fhir", say=lambda *a: None)
