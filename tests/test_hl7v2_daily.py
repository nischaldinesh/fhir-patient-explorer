import base64
import hashlib
import json
import re
import sys
import threading
from datetime import date, datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "hl7v2"))
import feed_state  # noqa: E402
import generate  # noqa: E402
import pipeline  # noqa: E402
import reference  # noqa: E402
from hl7 import parse  # noqa: E402
from route import Router  # noqa: E402
from to_fhir import FeedMapper  # noqa: E402

DAY = date(2026, 9, 1)


def simulate(days, start=DAY, seed=42):
    state, out = feed_state.FeedState(), []
    for k in range(days):
        out.append(generate.build_day(start + timedelta(days=k), state, seed=seed))
    return state, out


def control_ids(messages):
    return [parse(t).control_id for _, label, t in messages if label != "BROKEN"]


def mapped(messages, seen=()):
    router, mapper = Router(seen=seen), FeedMapper(translate=None)
    for _, _, text in messages:
        o = router.route(text)
        if o.status == "process":
            mapper.apply(o.message)
    return mapper


def test_original_feed_is_unchanged():
    msgs = generate.build()
    digest = hashlib.sha256("".join(t for _, _, t in msgs).encode()).hexdigest()
    assert len(msgs) == 198
    assert digest == "fe258141c8cb6c83799f20ac33539b0f485516d627abb8dd376dc34c125558a7"


def test_same_day_and_seed_give_identical_messages():
    a, b = feed_state.FeedState(), feed_state.FeedState()
    assert generate.build_day(DAY, a) == generate.build_day(DAY, b)
    assert a == b
    assert generate.build_day(DAY, feed_state.FeedState(), seed=7) != generate.build_day(DAY, feed_state.FeedState())


def test_a_day_has_the_expected_traffic():
    _, days = simulate(14)
    for msgs in days:
        labels = [label for _, label, _ in msgs]
        assert 2 <= labels.count("ADT_A01") <= 5 + 1  # plus at most one resend
        assert labels.count("ADT_A04") >= 1 and "QRY_A19" in labels and "ORU_R01" in labels
        for _, label, text in msgs:
            if label == "ADT_A01":
                v = parse(text).first("PV1")
                assert v.get(2) == "I" and v.get(4) in {"E", "U", "R"} and v.get(19).startswith("V2026")
                assert parse(text).first("PID").get(3, 1) >= "RV200000"
            if label == "ADT_A04":
                assert parse(text).first("PV1").get(2) == "E"
    admission_types = [parse(t).first("PV1").get(4) for msgs in days for _, label, t in msgs if label == "ADT_A01"]
    assert 0.4 < admission_types.count("E") / len(admission_types) < 0.8


def test_new_mrns_never_reuse_the_original_range():
    state, _ = simulate(10)
    numbers = [int(p["mrn"][2:]) for p in state.roster]
    assert min(numbers) == feed_state.FIRST_MRN and len(set(numbers)) == len(numbers)
    assert not any(100200 <= n <= 100368 for n in numbers)


def test_consecutive_days_do_not_collide():
    _, (d1, d2) = simulate(2)
    resent = {t for _, _, t in d1}  # resends carried over midnight repeat yesterday's message on purpose
    new_d2 = [m for m in d2 if m[2] not in resent]
    assert not set(control_ids(d1)) & set(control_ids(new_d2))
    visits = [{parse(t).first("PV1").get(19) for _, label, t in d if label in ("ADT_A01", "ADT_A04")} for d in (d1, d2)]
    assert visits[0] and visits[1] and not visits[0] & visits[1]
    m1, m2 = mapped(d1), mapped(d2)
    assert not set(m1.observations) & set(m2.observations)
    assert not set(m1.conditions) & set(m2.conditions)
    # an Encounter id is shared only by a visit that is still open on day 2
    assert set(m1.encounters) & set(m2.encounters) <= {f"rv-{v.lower()}" for v in visits[0]}


def test_returning_patient_keeps_patient_id_and_readmissions_happen():
    _, days = simulate(28)
    admits, discharges = {}, {}
    for msgs in days:
        for _, label, text in msgs:
            m = parse(text) if label.startswith(("ADT", "ORU")) else None
            if label == "ADT_A01":
                admits.setdefault(m.first("PID").get(3, 1), set()).add((m.first("PV1").get(44), m.first("PV1").get(19)))
            if label == "ADT_A03" and m.first("PV1").get(2) == "I":
                discharges.setdefault(m.first("PID").get(3, 1), set()).add(m.first("PV1").get(45))
    returning = {mrn for mrn, a in admits.items() if len(a) > 1}
    assert returning
    mrn = sorted(returning)[0]
    ids = {pid for msgs in days for pid in mapped(msgs).patients if pid == f"riverside-{mrn.lower()}"}
    assert ids == {f"riverside-{mrn.lower()}"}
    readmitted = 0
    for mrn, stays in admits.items():
        for out in discharges.get(mrn, ()):
            out_day = date(int(out[:4]), int(out[4:6]), int(out[6:8]))
            if any(start > out and date(int(start[:4]), int(start[4:6]), int(start[6:8])) - out_day <= timedelta(days=30)
                   for start, _ in stays):
                readmitted += 1
    assert readmitted >= 1


def test_open_admissions_are_eventually_discharged():
    _, days = simulate(28)
    admitted, discharged = {}, set()
    for k, msgs in enumerate(days):
        for _, label, text in msgs:
            v = parse(text).first("PV1") if label in ("ADT_A01", "ADT_A03") else None
            if label == "ADT_A01":
                admitted[v.get(19)] = k
            elif label == "ADT_A03":
                discharged.add(v.get(19))
    early = {visit for visit, k in admitted.items() if k < 18}  # stays last 2 to 9 days
    assert early and early <= discharged


def test_kidney_injury_stays_show_a_creatinine_rise():
    _, days = simulate(10)
    by_visit = {}
    for msgs in days:
        for _, label, text in msgs:
            if label == "ORU_R01":
                m = parse(text)
                for obx in m.all("OBX"):
                    if obx.get(3, 2).lower().startswith("creat") and obx.get(2) == "NM":
                        by_visit.setdefault(m.first("PV1").get(19), []).append((obx.get(14), float(obx.get(5))))
    rises = [v for v, series in by_visit.items()
             if any(b[1] - a[1] >= 0.3 for a, b in zip(sorted(series), sorted(series)[1:]))]
    assert rises


def test_state_round_trips_through_json(tmp_path):
    state, _ = simulate(5)
    assert feed_state.FeedState.from_json(state.to_json()) == state
    path = tmp_path / "s" / "feed_state.json"
    feed_state.save(state, path)
    assert feed_state.load(path) == state
    assert feed_state.load(tmp_path / "missing.json") == feed_state.FeedState()


def test_state_forgets_old_control_ids_and_refuses_old_days():
    s = feed_state.FeedState()
    s.remember({"A"}, DAY)
    s.remember({"B"}, DAY + timedelta(days=31))
    assert set(s.seen_control_ids) == {"B"}
    generate.build_day(DAY, s)
    with pytest.raises(ValueError):
        generate.build_day(DAY, s)


def test_router_catches_a_resend_from_an_earlier_run():
    _, (d1,) = simulate(1)
    text = next(t for _, label, t in d1 if label == "ADT_A01")
    cid = parse(text).control_id
    assert Router().route(text).status == "process"
    assert Router(seen={cid}).route(text).status == "duplicate"


def test_patient_class_and_admission_type_mapping():
    _, (d1,) = simulate(1)
    m = mapped(d1)
    kinds = {(e["class"]["code"], e["type"][0]["text"]) for e in m.encounters.values()}
    assert ("IMP", "Hospital admission") in kinds and ("EMER", "Emergency department visit") in kinds
    for e in m.encounters.values():
        code = e["priority"]["coding"][0]
        assert code["system"].endswith("v3-ActPriority") and code["code"] in {"EM", "UR", "EL"}
    original = mapped(generate.build())
    assert all("priority" not in e and e["type"] == [{"text": "Hospital admission"}] for e in original.encounters.values())


def test_daily_dry_run_writes_files_and_keeps_state(tmp_path, capsys):
    state_path, daily = tmp_path / "feed_state.json", tmp_path / "daily"
    state = feed_state.FeedState()
    generate.build_day(DAY, state)
    feed_state.save(state, state_path)
    before = state_path.read_bytes()
    day = (DAY + timedelta(days=1)).isoformat()
    args = ["daily", "--day", day, "--dry-run", "--state", str(state_path), "--daily", str(daily)]
    assert pipeline.main(args) == 0
    assert len(list((daily / day).glob("*.hl7"))) > 5
    assert (daily / day / "fhir_resources.ndjson").exists()
    assert state_path.read_bytes() == before
    assert "Routed" in capsys.readouterr().out


def test_run_history_is_newest_first_and_capped():
    history = []
    for k in range(405):
        history = pipeline.append_run(history, {"run_id": k})
    assert len(history) == 400 and history[0]["run_id"] == 404 and history[-1]["run_id"] == 5
    assert pipeline.append_run(None, {"run_id": 1}) == [{"run_id": 1}]
    assert [pipeline.run_status(*x) for x in ((5, 0), (5, 1), (0, 3))] == ["success", "partial", "failed"]


class FakeFhir:
    """Stores batch PUTs by id (201 when new, 200 when replaced) and Binary resources, and reads them back
    by id; 404 for unknown."""

    def __init__(self, fail_posts=False):
        store, binaries, fail = {}, {}, fail_posts
        self.store, self.binaries = store, binaries

        class H(BaseHTTPRequestHandler):
            def reply(self, obj, code=200):
                d = json.dumps(obj).encode()
                self.send_response(code)
                self.send_header("Content-Length", str(len(d)))
                self.end_headers()
                self.wfile.write(d)

            def do_GET(self):
                rid = self.path.rsplit("/", 1)[-1]
                if "/Binary/" in self.path and rid in binaries:
                    return self.reply(binaries[rid])
                if self.path.split("/fhir/", 1)[-1] in store:
                    return self.reply(store[self.path.split("/fhir/", 1)[-1]])
                if "$translate" in self.path:
                    return self.reply({"resourceType": "Parameters", "parameter": [{"name": "result", "valueBoolean": False}]})
                self.reply({"resourceType": "OperationOutcome"}, 404)

            def do_PUT(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                binaries[self.path.rsplit("/", 1)[-1]] = body
                self.reply(body)

            def do_DELETE(self):
                rid = self.path.rsplit("/", 1)[-1]
                if binaries.pop(rid, None) is None:
                    return self.reply({"resourceType": "OperationOutcome"}, 404)
                self.reply({"resourceType": "OperationOutcome"})

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                entries = []
                for e in body["entry"]:
                    url = e["request"]["url"]
                    if fail:
                        entries.append({"response": {"status": "422 Unprocessable Entity"}})
                        continue
                    entries.append({"response": {"status": "200 OK" if url in store else "201 Created"}})
                    store[url] = e["resource"]
                self.reply({"resourceType": "Bundle", "entry": entries})

            def log_message(self, *a):
                pass

        self.srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()
        self.base = f"http://127.0.0.1:{self.srv.server_port}/fhir"

    def history(self):
        return json.loads(base64.b64decode(self.binaries[pipeline.HISTORY_ID]["data"]))


@pytest.fixture
def fhir():
    f = FakeFhir()
    yield f
    f.srv.shutdown()


def daily_args(tmp_path, base, *extra):
    return [*extra, "--base", base, "--state", str(tmp_path / "state.json"), "--daily", str(tmp_path / "daily")]


def test_daily_loads_records_history_and_rerun_adds_nothing(tmp_path, fhir, capsys):
    day = DAY.isoformat()
    assert pipeline.main(daily_args(tmp_path, fhir.base, "daily", "--day", day)) == 0
    first = dict(fhir.store)
    (record,) = fhir.history()
    # the reference data was loaded first, on its own, because the server did not have it
    assert record["day"] == day and record["status"] == "success"
    assert record["resources_created"] == len(first) - len(reference.resources())
    assert record["messages"] == sum(record[k] for k in ("process", "skip", "duplicate", "error"))
    assert record["new_admissions"] >= 2 and record["open_admissions"] >= 2
    assert set(record) >= {"run_id", "started_at", "finished_at", "duration_s", "resources_loaded", "load_failed", "pct_loinc", "discharges"}
    report = json.loads(base64.b64decode(fhir.binaries[pipeline.REPORT_ID]["data"]))
    assert report["day"] == day and report["routing"]["process"] == record["process"]
    # the same report is kept under the day's own id
    day_report = fhir.binaries["hl7v2-ingestion-report-20260901"]
    assert day_report["id"] == "hl7v2-ingestion-report-20260901" and day_report["data"] == fhir.binaries[pipeline.REPORT_ID]["data"]
    assert feed_state.load(tmp_path / "state.json").last_day == day
    capsys.readouterr()

    assert pipeline.main(daily_args(tmp_path, fhir.base, "daily", "--day", day)) == 0
    assert "0 new resources" in capsys.readouterr().out
    assert fhir.store == first and len(fhir.history()) == 1


def test_a_retried_day_creates_nothing_new(tmp_path, fhir):
    state_path = tmp_path / "state.json"
    feed_state.save(feed_state.FeedState(), state_path)
    before = state_path.read_bytes()
    assert pipeline.main(daily_args(tmp_path, fhir.base, "daily", "--day", DAY.isoformat())) == 0
    state_path.write_bytes(before)  # as if the state was never saved
    assert pipeline.main(daily_args(tmp_path, fhir.base, "daily", "--day", DAY.isoformat())) == 0
    newest, older = fhir.history()
    assert newest["resources_created"] == 0 and newest["resources_loaded"] == older["resources_loaded"]


def test_failed_load_keeps_state_for_a_retry(tmp_path):
    f = FakeFhir(fail_posts=True)
    try:
        assert pipeline.main(daily_args(tmp_path, f.base, "daily", "--day", DAY.isoformat())) == 1
        (record,) = f.history()
    finally:
        f.srv.shutdown()
    assert record["status"] == "failed" and record["resources_loaded"] == 0
    assert pipeline.REPORT_ID not in f.binaries and pipeline.day_report_id(DAY) not in f.binaries
    assert not (tmp_path / "state.json").exists()


def test_backfill_runs_missing_days_in_order(tmp_path, fhir, monkeypatch):
    monkeypatch.setattr(pipeline, "today_utc", lambda: DAY + timedelta(days=3))  # backfill ends yesterday
    assert pipeline.main(daily_args(tmp_path, fhir.base, "backfill", "--days", "3")) == 0
    assert [r["day"] for r in fhir.history()] == ["2026-09-03", "2026-09-02", "2026-09-01"]
    assert pipeline.main(daily_args(tmp_path, fhir.base, "backfill", "--days", "3")) == 0
    assert len(fhir.history()) == 3
    starts = {e["period"]["start"][:10] for url, e in fhir.store.items() if url.startswith("Encounter/")}
    assert starts == {"2026-09-01", "2026-09-02", "2026-09-03"}


def test_yesterday_means_the_previous_utc_day(monkeypatch):
    monkeypatch.setattr(pipeline, "today_utc", lambda: date(2026, 3, 1))
    assert pipeline.parse_day("yesterday") == date(2026, 2, 28)
    assert pipeline.parse_day("today") == date(2026, 3, 1)
    assert pipeline.parse_day("2026-01-05") == date(2026, 1, 5)


def test_a_run_for_yesterday_has_no_event_in_the_future(tmp_path):
    """The nightly job loads yesterday: every timestamp in the messages is before the run time."""
    state_path, daily = tmp_path / "state.json", tmp_path / "daily"
    yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
    state = feed_state.FeedState()  # some patients already in hospital, plus resends carried over midnight
    for k in range(5, 1, -1):
        generate.build_day(yesterday - timedelta(days=k), state)
    feed_state.save(state, state_path)
    started = datetime.now(timezone.utc).replace(tzinfo=None)
    assert pipeline.main(["daily", "--day", "yesterday", "--dry-run", "--state", str(state_path), "--daily", str(daily)]) == 0
    files = list((daily / yesterday.isoformat()).glob("*.hl7"))
    stamps = [datetime.strptime(t, "%Y%m%d%H%M%S")
              for f in files for t in re.findall(r"(?<!\d)(20\d{12})(?!\d)", f.read_text(encoding="utf-8"))]
    assert len(files) > 5 and len(stamps) > 20
    assert max(stamps) <= started
    assert min(stamps).date() >= yesterday - timedelta(days=9)  # admit times of stays that began earlier


def test_day_report_id():
    assert pipeline.day_report_id(date(2026, 10, 4)) == "hl7v2-ingestion-report-20261004"
    assert pipeline.day_report_id("2026-10-04") == "hl7v2-ingestion-report-20261004"
    with pytest.raises(ValueError):
        pipeline.day_report_id("../Patient")


def test_reports_to_prune_keeps_the_newest_days():
    history = [{"day": (DAY + timedelta(days=k)).isoformat()} for k in range(35)][::-1]
    history.insert(1, {"day": history[0]["day"]})        # a retried day counts once
    history.append({"status": "failed"})                 # a record without a day is ignored
    ids = pipeline.reports_to_prune(history, keep=30)
    assert ids == [pipeline.day_report_id(DAY + timedelta(days=k)) for k in range(4, -1, -1)]
    assert pipeline.reports_to_prune(history[:10], keep=30) == []
    assert pipeline.reports_to_prune(None) == [] and pipeline.reports_to_prune([], keep=0) == []


def test_was_deleted_reads_the_hapi_outcome():
    def outcome(code):
        return {"resourceType": "OperationOutcome", "issue": [{"details": {"coding": [{"code": code}]}}]}
    assert pipeline.was_deleted(outcome("SUCCESSFUL_DELETE")) and pipeline.was_deleted({}) and pipeline.was_deleted(None)
    assert not pipeline.was_deleted(outcome("SUCCESSFUL_DELETE_ALREADY_DELETED"))
    assert not pipeline.was_deleted(outcome("SUCCESSFUL_DELETE_NOT_FOUND"))


def test_prune_reports_deletes_old_day_reports(fhir, capsys):
    days = [(DAY + timedelta(days=k)).isoformat() for k in range(4)]
    history = [{"day": d} for d in reversed(days)]
    fhir.binaries[pipeline.HISTORY_ID] = pipeline.json_binary(pipeline.HISTORY_ID, history)
    for d in days[1:]:   # the oldest day has no report (as for days run before reports were kept)
        fhir.binaries[pipeline.day_report_id(d)] = pipeline.json_binary(pipeline.day_report_id(d), {})
    assert pipeline.main(["prune-reports", "--keep", "2", "--base", fhir.base]) == 0
    assert "deleted 1 older, failed 0" in capsys.readouterr().out
    assert sorted(k for k in fhir.binaries if k.startswith("hl7v2-ingestion-report-")) == [
        pipeline.day_report_id(d) for d in days[2:]]


def test_a_resend_after_midnight_is_not_a_new_admission():
    a01 = "\r".join([
        "MSH|^~\\&|ADT|RIVERSIDE|FHIR|RIVERSIDE|20260901235000||ADT^A01^ADT_A01|RVG2026090199|P|2.5.1",
        "EVN|A01|20260901235000",
        "PID|1||RV200001^^^RIVERSIDE^MR||Doe^Jane||19600101|F",
        "PV1|1|I|MED^214^A|E|||1234^Morgan^Dana||||||||||||V2026090101",
    ]) + "\r"
    before_midnight = [(datetime(2026, 9, 1, 23, 50), "ADT_A01", a01)]
    resend_after_midnight = [(datetime(2026, 9, 2, 0, 20), "ADT_A01", a01)]
    first = generate.day_summary(before_midnight, DAY)
    second = generate.day_summary(resend_after_midnight, DAY + timedelta(days=1))
    assert first["new_admissions"] == 1 and second["new_admissions"] == 0
    assert first["new_admissions"] + second["new_admissions"] == 1
    assert generate.msh_day(a01) == "2026-09-01" and generate.msh_day("PID|1||X") is None


def test_every_admission_counts_once_over_many_days():
    """Resends, including those carried over midnight, never add an admission to the run history."""
    _, days = simulate(40)
    carried = [t for prev, msgs in zip(days, days[1:]) for _, label, t in msgs
               if label == "ADT_A01" and t in {x[2] for x in prev}]
    assert carried, "the simulation should include an A01 resent after midnight"
    counted = sum(generate.day_summary(msgs, DAY + timedelta(days=k))["new_admissions"] for k, msgs in enumerate(days))
    assert counted == len({t for msgs in days for _, label, t in msgs if label == "ADT_A01"})
