"""EMPI (empi/match.py) and its seeded test set (hl7v2/empi_seed.py, pipeline.py empi-seed): string comparison,
blocking, scoring of every variation type, thresholds, the review queue, idempotent linking, and precision and recall
on a seeded test set."""
import base64
import json
import random
import sys
import threading
import urllib.parse
from datetime import date, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "hl7v2"), str(ROOT / "empi"), str(ROOT)]
import empi_seed as es  # noqa: E402
import match as m  # noqa: E402
import pipeline  # noqa: E402

CFG = m.load_config()
NICK = m.load_nicknames()
SYNTHEA = "https://github.com/synthetichealth/synthea"


def fhir_patient(pid, first, last, middle="", sex="female", born="1970-05-14", phone="555-210-3344",
                 line="12 Elm Street", city="Salem", zip_="01970", source="synthea", tags=None):
    """A Patient as the server holds it. Synthea style appends digits to names, as Synthea does."""
    digits = source == "synthea"
    given = [f"{first}{'42' if digits else ''}"] + ([f"{middle}{'7' if digits else ''}"] if middle else [])
    p = {"resourceType": "Patient", "id": pid, "meta": {"tag": tags or []},
         "name": [{"use": "official", "family": f"{last}{'96' if digits else ''}", "given": given}],
         "gender": sex, "birthDate": born, "telecom": [{"system": "phone", "value": phone}],
         "address": [{"line": [line], "city": city, "state": "MA", "postalCode": zip_}]}
    if source == "synthea":
        p["identifier"] = [{"system": SYNTHEA, "value": pid}]
    if source == "hl7v2":
        p["meta"]["tag"].append({"system": f"{m.REPO}/CodeSystem/source", "code": "hl7v2"})
    if source == "mimic":
        p["meta"]["profile"] = ["http://mimic.mit.edu/fhir/mimic/StructureDefinition/mimic-patient"]
        p["name"] = [{"use": "official", "family": f"Patient_{pid}"}]
        for k in ("telecom", "address"):
            p.pop(k)
    return p


def rec(**kw):
    return m.record(fhir_patient(kw.pop("pid", "x"), **kw))


BASE = dict(first="Pamela", last="McDermott", middle="Gilma", sex="female", born="1968-01-09", phone="555-238-8373",
            line="822 Lubowitz Estate Unit 52", city="Boston", zip_="02119")


def score(**changes):
    a = rec(pid="a", **BASE)
    b = rec(pid="b", **{**BASE, **changes, "source": "hl7v2"})
    s = m.score_pair(a, b, CFG, NICK)
    return s["score"], m.classify(s["score"], CFG["thresholds"]), {f["field"]: f for f in s["fields"]}


# ---------- string comparison ----------

@pytest.mark.parametrize("name,code", [("Robert", "R163"), ("Rupert", "R163"), ("Rubin", "R150"), ("Ashcraft", "A261"),
                                       ("Tymczak", "T522"), ("Pfister", "P236"), ("Honeyman", "H555"), ("Lee", "L000"),
                                       ("O'Kon", "O250"), ("Kuhn96", "K500"), ("Jesús", "J220"), ("", "")])
def test_soundex(name, code):
    assert m.soundex(name) == code


def test_jaro_winkler():
    assert m.jaro_winkler("martha", "marhta") == pytest.approx(0.961, abs=0.001)
    assert m.jaro_winkler("dwayne", "duane") == pytest.approx(0.840, abs=0.001)
    assert m.jaro_winkler("dixon", "dicksonx") == pytest.approx(0.813, abs=0.001)
    assert m.jaro_winkler("crate", "trace") == pytest.approx(0.733, abs=0.001)
    assert m.jaro_winkler("same", "same") == 1.0
    assert m.jaro_winkler("", "abc") == 0.0 and m.jaro_winkler("abc", "xyz") == 0.0


def test_normalize_name():
    assert m.normalize_name("Jesús") == "jesus"
    assert m.normalize_name("O'Kon") == "okon"
    assert m.normalize_name("Brennan-Ross") == "brennanross"
    assert m.normalize_name("Hickle134") == "hickle"


def test_nicknames_both_ways_and_shared_names():
    assert m.is_nickname("Pam", "Pamela", NICK) and m.is_nickname("Pamela", "Pam", NICK)
    assert m.is_nickname("Bill", "William", NICK) and m.is_nickname("Ted", "Ned", NICK)   # both Edward
    assert m.is_nickname("Pamela262", "Pam", NICK)                                        # Synthea digits
    assert not m.is_nickname("Pam", "Peter", NICK)
    assert not m.is_nickname("Pam", "Pam", NICK) and not m.is_nickname("", "Pam", NICK)


def test_transposed_birth_dates():
    assert m.transposed("1986-05-24", "1986-05-42") is True        # two neighbouring digits (not a date, still a slip)
    assert m.transposed("1972-11-03", "1972-11-30") is True
    assert m.transposed("1986-05-04", "1986-04-05") is True        # day and month swapped
    assert m.transposed("1986-05-24", "1986-05-24") is False
    assert m.transposed("1986-05-24", "1986-06-24") is False
    assert m.transposed("1986-05-24", "1986-04-25") is False       # not a swap of neighbours, not day for month
    assert es.transpositions("1972-11-03") == ["1972-03-11", "1972-10-13", "1972-11-30"]
    assert all(t[:4] == "1966" for t in es.transpositions("1966-02-20"))


# ---------- reading a Patient ----------

def test_record_cleans_names_and_skips_mimic():
    r = m.record(fhir_patient("s", "Pamela", "McDermott", middle="Gilma", zip_="00000"))
    assert (r["first"], r["middle"], r["last"], r["source"]) == ("Pamela", "Gilma", "McDermott", "synthea")
    assert r["phone"] == "5552103344" and r["postal_code"] == "" and r["comparable"]
    mimic = m.record(fhir_patient("10014729", "x", "y", source="mimic"))
    assert mimic["source"] == "mimic" and mimic["last"] == "" and not mimic["comparable"]
    assert m.blocking_keys(mimic) == set()


# ---------- scoring each variation type ----------

def test_identical_records_score_100():
    s, status, _ = score()
    assert (s, status) == (100.0, "auto_link")


def test_nickname():
    s, status, f = score(first="Pam")
    assert status == "auto_link" and f["first_name"]["result"] == "nickname" and 95 <= s < 100


def test_typo_in_last_name():
    s, status, f = score(last="McDremott")
    assert status == "auto_link" and f["last_name"]["result"] == "close_spelling"
    a, b = rec(pid="a", **{**BASE, "last": "Kuhn"}), rec(pid="b", **{**BASE, "last": "Kuhm"})
    f = {x["field"]: x for x in m.score_pair(a, b, CFG, NICK)["fields"]}
    assert f["last_name"]["result"] == "sounds_alike"   # Jaro Winkler 0.88, both Soundex K500


def test_missing_middle_name_counts_neither_way():
    s, status, f = score(middle="")
    assert (s, status) == (100.0, "auto_link") and f["middle_name"]["points"] is None
    assert score(middle="Gail")[2]["middle_name"]["result"] == "different"
    assert score(middle="G")[2]["middle_name"]["result"] == "same_initial"


def test_new_address():
    s, status, f = score(line="40 Harbor St", city="Quincy", zip_="02169")
    assert status == "auto_link" and f["address"]["points"] == 0 and f["postal_code"]["points"] == 0 and s == 91.0


def test_address_written_differently():
    _, _, f = score(line="822 Lubowitz Est Unit 52")
    assert f["address"]["result"] == "written_differently"
    assert score(line="822 lubowitz estate apt 52")[2]["address"]["result"] == "same"   # apt and unit are one word
    assert score(line="823 Lubowitz Estate Unit 52")[2]["address"]["result"] == "different"


def test_transposed_birth_date():
    s, status, f = score(born="1968-09-01")
    assert status == "auto_link" and f["birth_date"]["result"] == "transposed" and s == 92.0
    assert f["birth_date"]["reason"] == "Day and month swapped"
    assert score(born="1968-10-09")[2]["birth_date"]["reason"] == "Two digits swapped"


def test_different_phone_and_phone_formats():
    s, status, f = score(phone="555-901-2222")
    assert status == "auto_link" and f["phone"]["points"] == 0 and s == 91.0
    assert score(phone="(555)238-8373")[2]["phone"]["result"] == "same"


def test_moved_and_new_phone_goes_to_review():
    s, status, _ = score(line="40 Harbor St", city="Quincy", zip_="02169", phone="555-901-2222")
    assert status == "review" and 70 <= s < 90


def test_same_name_another_person_is_not_linked():
    s, status, _ = score(born="1968-07-30", middle="R", line="5 Mill St", city="Lowell", zip_="01852", phone="555-111-2222")
    assert status == "no_match" and s < 70
    s, status, _ = score(sex="male", middle="", line="5 Mill St", city="Lowell", zip_="01852", phone="555-111-2222")
    assert status == "review"   # same name and birthday, other sex: a person decides


def test_every_field_has_a_reason_and_points_add_up():
    s, _, f = score(first="Pam", phone="555-901-2222")
    assert [x["field"] for x in f.values()] == [k for k, _ in m.FIELDS]
    assert all(x["reason"] and "-" not in x["reason"] and "–" not in x["reason"] for x in f.values())
    earned = sum(x["points"] for x in f.values() if x["points"] is not None)
    possible = sum(x["max"] for x in f.values() if x["points"] is not None)
    assert round(100 * earned / possible, 1) == pytest.approx(s, abs=0.1)
    assert sum(CFG["weights"].values()) == 100


# ---------- thresholds ----------

def test_thresholds():
    t = CFG["thresholds"]
    assert t == {"auto_link": 90, "review": 70}
    assert m.classify(100, t) == "auto_link" and m.classify(90, t) == "auto_link"
    assert m.classify(89.9, t) == "review" and m.classify(70, t) == "review"
    assert m.classify(69.9, t) == "no_match" and m.classify(0, t) == "no_match"
    assert m.classify(85, {"auto_link": 80, "review": 60}) == "auto_link"


# ---------- blocking ----------

def test_blocking():
    records = [rec(pid="a", first="Ann", last="Smith", born="1970-01-01"),
               rec(pid="b", first="Bob", last="Smyth", born="1970-12-31"),     # same year, Soundex S530
               rec(pid="c", first="Cy", last="Jones", born="1970-01-01"),      # exact birth date only
               rec(pid="d", first="Di", last="Smith", born="1971-01-01"),      # another year, another day
               m.record(fhir_patient("e", "x", "y", source="mimic"))]
    pairs = [(a["id"], b["id"]) for a, b in m.candidate_pairs(records)]
    assert pairs == [("a", "b"), ("a", "c")]


def test_pair_id_is_stable():
    assert m.pair_id("a", "b") == m.pair_id("b", "a")
    assert m.pair_id("a", "b").startswith("pair-") and len(m.pair_id("a", "b")) == 15


# ---------- decisions, quality, links ----------

def test_decisions_stand_whatever_the_new_score():
    records = [rec(pid="a", **BASE), rec(pid="b", **{**BASE, "sex": "male", "phone": "555-000-1111", "zip_": "01852",
                                                    "line": "1 Mill St", "middle": ""})]
    first = m.match_all(records, CFG, NICK)
    assert [p["status"] for p in first] == ["review"]
    again = m.match_all(records, CFG, NICK, previous=[{**first[0], "status": "rejected", "decided_at": "t"}])
    assert again[0]["status"] == "rejected" and again[0]["decided_at"] == "t"
    assert m.to_link(again) == []


def test_decide():
    doc = {"counts": {"no_match": 4, "candidate_pairs": 7}, "pairs": [
        {"pair_id": "pair-1", "status": "review", "score": 80}, {"pair_id": "pair-2", "status": "auto_link", "score": 95},
        {"pair_id": "pair-3", "status": "accepted", "score": 85}]}
    out = m.decide(doc, "pair-1", "accepted", "2026-10-18T10:00:00+00:00")
    assert out["pairs"][0]["status"] == "accepted" and doc["pairs"][0]["status"] == "review"   # a copy
    assert out["counts"] == {"auto_link": 1, "review": 0, "accepted": 2, "rejected": 0, "no_match": 4, "candidate_pairs": 7}
    assert m.decide(doc, "pair-1", "rejected", "t")["counts"]["rejected"] == 1
    for pid, status in (("pair-2", "rejected"), ("pair-3", "rejected"), ("pair-9", "accepted")):
        with pytest.raises(ValueError):
            m.decide(doc, pid, status, "t")


def test_quality():
    truth = [{"seed_id": "s1", "existing_id": "e1", "kind": "match"}, {"seed_id": "s2", "existing_id": "e2", "kind": "match"},
             {"seed_id": "s3", "existing_id": "e3", "kind": "hard_negative"}]
    pair = lambda a, b, status: {"a": {"id": a}, "b": {"id": b}, "status": status}  # noqa: E731
    pairs = [pair("e1", "s1", "auto_link"), pair("e2", "s2", "review"), pair("e3", "s3", "auto_link"),
             pair("x", "y", "auto_link")]   # outside the test set: not counted
    q = m.quality(pairs, truth)
    assert (q["precision"], q["recall"], q["false_positives"], q["hard_negatives_auto_linked"]) == (0.5, 0.5, 1, 1)
    assert (q["pairs_in_review"], q["true_matches_in_review"], q["false_negatives"]) == (1, 1, 1)
    assert m.quality(pairs, []) is None


def test_with_link_is_idempotent():
    p = {"resourceType": "Patient", "id": "a", "meta": {"versionId": "3", "lastUpdated": "x", "tag": [1]}}
    once = m.with_link(p, "b")
    assert once["link"] == [{"other": {"reference": "Patient/b"}, "type": "seealso"}]
    assert "versionId" not in once["meta"] and once["meta"]["tag"] == [1] and "link" not in p
    assert m.with_link(once, "b") is None
    assert len(m.with_link(once, "c")["link"]) == 2


def test_writes_only_to_localhost():
    for ok in ("http://localhost:8080/fhir", "http://127.0.0.1:18080/fhir"):
        m.require_local(ok)
    with pytest.raises(SystemExit, match="localhost only"):
        m.require_local("https://fhir.example.org/fhir")


def test_extra_local_hosts(monkeypatch):
    assert m.local_hosts({}) == m.LOCAL_HOSTS
    assert m.local_hosts({"EMPI_EXTRA_LOCAL_HOSTS": " Host.Docker.Internal, ,"}) == m.LOCAL_HOSTS | {"host.docker.internal"}
    monkeypatch.delenv("EMPI_EXTRA_LOCAL_HOSTS", raising=False)
    with pytest.raises(SystemExit, match="localhost only"):
        m.require_local("http://host.docker.internal:8090/fhir")
    monkeypatch.setenv("EMPI_EXTRA_LOCAL_HOSTS", "host.docker.internal")
    m.require_local("http://host.docker.internal:8090/fhir")
    with pytest.raises(SystemExit, match="localhost only"):
        m.require_local("https://fhir.example.org/fhir")


def test_keep_links_on_reseed():
    new = {"resourceType": "Patient", "id": "a"}
    assert pipeline.keep_links(new, None) == new
    assert pipeline.keep_links(new, {"link": [1]})["link"] == [1]


# ---------- the seeded test set ----------

FIRSTS = ["Pamela", "Kenneth", "Raymond", "Jonathan", "Katherine", "Edmond", "Wallace", "Chester", "Stuart", "Jefferson",
          "Marvin", "Jessica", "Jody", "Rosario", "Kerry", "Dallas", "Riley", "Agnes", "Lena", "Hiram", "Tanner", "Galen",
          "Mauro", "Lois", "Gerda", "Hans", "Yuk", "Dong", "Hye", "Mignon", "Zina", "Tristan", "Ivan", "Aron", "Wilfredo",
          "Lavina", "Retta", "Verdell", "Jona", "Sabra"]
LASTS = ["Carroll", "Hoppe", "Garza", "McDermott", "Runolfsdottir", "Jaskolski", "Spinka", "Rutherford", "Parker",
         "Jones", "Borer", "Parisian", "Sanford", "Miller", "Ritchie", "Hayes", "Rippin", "VonRueden", "Kozey", "Abernathy",
         "Weissnat", "Runte", "Hilpert", "Hickle", "Cervantez", "Monahan", "Dach", "Mills", "Romero", "Tremblay"]
STREETS = ["Lubowitz Estate", "Kuhn Gate", "Hirthe Road", "Fay Way", "Beer Branch", "Ondricka Mill"]


def synthea_population(n=60, seed=7):
    """Synthea style patients to draw the test set from: living, with phones and addresses, most with a middle name."""
    rnd = random.Random(seed)
    out = []
    for i in range(n):
        first, last = FIRSTS[i % len(FIRSTS)], rnd.choice(LASTS)
        born = date(1940, 1, 1) + timedelta(days=rnd.randint(0, 365 * 75))
        out.append(fhir_patient(f"syn-{i:03d}", first, last, middle=rnd.choice(["", "Ann", "Lee", "Rose", "James", "Mae"]),
                                sex="male" if first in {"Kenneth", "Raymond", "Jonathan", "Edmond", "Wallace", "Chester",
                                                        "Stuart", "Jefferson", "Marvin", "Hiram", "Tanner", "Galen",
                                                        "Mauro", "Hans", "Dong", "Tristan", "Ivan", "Aron", "Wilfredo",
                                                        "Jody", "Rosario", "Dallas", "Riley"} else "female",
                                born=born.isoformat(), phone=f"555-{rnd.randint(200, 989)}-{rnd.randint(1000, 9999)}",
                                line=f"{rnd.randint(1, 999)} {rnd.choice(STREETS)}", city="Boston",
                                zip_=rnd.choice(["02119", "02215", "01940", "00000"])))
    return out


def seeded():
    population = synthea_population()
    messages, truth = es.build_seed([m.record(p) for p in population], NICK)
    return population, messages, truth


def test_seed_is_fixed_and_shaped_as_specified():
    _, messages, truth = seeded()
    _, again, truth2 = seeded()
    assert [t for _, _, t in messages] == [t for _, _, t in again] and truth == truth2
    assert len(messages) == 30 and all(label == "ADT_A04" for _, label, _ in messages)
    kinds = [t["kind"] for t in truth["pairs"]]
    assert kinds.count("match") == 20 and kinds.count("hard_negative") == 10
    assert sorted(t["seed_mrn"] for t in truth["pairs"]) == [f"RV{300001 + k}" for k in range(30)]
    assert len({t["existing_id"] for t in truth["pairs"]}) == 30   # each Synthea patient used once
    variations = [v for t in truth["pairs"] if t["kind"] == "match" for v in t["variations"]]
    for v in ("nickname", "typo", "missing_middle", "new_address", "transposed_birth_date", "different_phone"):
        assert v in variations
    patients = pipeline.empi_patients(messages)
    assert [p["id"] for p in patients] == [t["seed_id"] for t in truth["pairs"]]
    assert all(m.TEST_SET_TAG in p["meta"]["tag"] and m.source_of(p) == "hl7v2" for p in patients)


def test_hard_negatives_share_the_name():
    population, _, truth = seeded()
    patients = {p["id"]: m.record(p) for p in population + pipeline.empi_patients(seeded()[1])}
    for t in truth["pairs"]:
        s, e = patients[t["seed_id"]], patients[t["existing_id"]]
        if t["kind"] == "hard_negative":
            assert (s["first"], s["last"]) == (e["first"], e["last"]) and (s["birth_date"], s["sex"]) != (e["birth_date"], e["sex"])


def test_acceptance_on_the_seeded_set():
    population, messages, truth = seeded()
    records = [m.record(p) for p in population + pipeline.empi_patients(messages)]
    pairs = m.match_all(records, CFG, NICK)
    q = m.quality(pairs, truth["pairs"])
    assert q["precision"] >= 0.95 and q["recall"] >= 0.90, q
    assert q["hard_negatives_auto_linked"] == 0


def test_daily_and_original_feeds_unchanged():
    import feed_state
    import generate
    state = feed_state.FeedState()
    before = [t for _, _, t in generate.build_day(date(2026, 10, 1), state)]
    assert not any("RVGEMPI" in t for t in before) and len(generate.build()) == 198


# ---------- against a FHIR server: linking twice creates nothing new ----------

class FakeFhir:
    """Patient search, read and PUT, batch POST, and Binary GET and PUT, in memory."""

    def __init__(self, resources):
        self.store = {(r["resourceType"], r["id"]): r for r in resources}
        self.puts = []

    def __enter__(self):
        fake = self

        class H(BaseHTTPRequestHandler):
            def reply(self, code, body):
                d = json.dumps(body).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/fhir+json")
                self.send_header("Content-Length", str(len(d)))
                self.end_headers()
                self.wfile.write(d)

            def do_GET(self):
                path = urllib.parse.urlparse(self.path).path.split("/fhir/", 1)[-1]
                if path == "Patient":
                    entries = [{"resource": r} for (t, _), r in sorted(fake.store.items()) if t == "Patient"]
                    return self.reply(200, {"resourceType": "Bundle", "entry": entries})
                key = tuple(path.split("/", 1))
                if key not in fake.store:
                    return self.reply(404, {"resourceType": "OperationOutcome"})
                self.reply(200, fake.store[key])

            def do_PUT(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                key = tuple(urllib.parse.urlparse(self.path).path.split("/fhir/", 1)[-1].split("/", 1))
                fake.puts.append("/".join(key))
                fake.store[key] = body
                self.reply(200, body)

            def do_POST(self):
                bundle = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                out = []
                for e in bundle["entry"]:
                    r = e["resource"]
                    new = (r["resourceType"], r["id"]) not in fake.store
                    fake.store[(r["resourceType"], r["id"])] = r
                    out.append({"response": {"status": "201 Created" if new else "200 OK"}})
                self.reply(200, {"resourceType": "Bundle", "entry": out})

            def log_message(self, *a):
                pass

        self.srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()
        self.base = f"http://127.0.0.1:{self.srv.server_port}/fhir"
        return self

    def __exit__(self, *a):
        self.srv.shutdown()

    def binary(self, rid):
        return json.loads(base64.b64decode(self.store[("Binary", rid)]["data"]))


def test_seed_run_link_and_rerun(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    with FakeFhir(synthea_population()) as fake:
        assert pipeline.main(["empi-seed", "--base", fake.base]) == 0
        assert (tmp_path / "data/empi/truth.json").exists() and fake.binary("empi-truth")["pairs"]
        assert m.main(["run", "--base", fake.base]) == 0
        doc = fake.binary("empi-review")
        assert doc["contains_phi"] and doc["quality"]["precision"] >= 0.95 and doc["quality"]["recall"] >= 0.90
        linked = [p for p in doc["pairs"] if p["status"] == "auto_link"]
        assert linked and doc["links"]["patients_updated"] == 2 * len(linked)
        a, b = linked[0]["a"]["id"], linked[0]["b"]["id"]
        assert fake.store[("Patient", a)]["link"] == [{"other": {"reference": f"Patient/{b}"}, "type": "seealso"}]
        assert fake.store[("Patient", b)]["link"] == [{"other": {"reference": f"Patient/{a}"}, "type": "seealso"}]

        # a second run, and a reseed, create no new links and keep the old ones
        puts = len([p for p in fake.puts if p.startswith("Patient/")])
        assert m.main(["run", "--base", fake.base]) == 0
        assert fake.binary("empi-review")["links"]["patients_updated"] == 0
        assert len([p for p in fake.puts if p.startswith("Patient/")]) == puts
        assert pipeline.main(["empi-seed", "--base", fake.base]) == 0
        assert "(0 new)" in capsys.readouterr().out
        seed_side = a if a.startswith("riverside-") else b
        assert fake.store[("Patient", seed_side)]["link"]

        # a person accepts the pair in review: linked like an automatic match; a rerun keeps the decision
        pending = next(p for p in fake.binary("empi-review")["pairs"] if p["status"] == "review")
        assert m.main(["decide", pending["pair_id"], "--accept", "--base", fake.base]) == 0
        assert fake.store[("Patient", pending["a"]["id"])]["link"]
        assert m.main(["run", "--base", fake.base]) == 0
        after = {p["pair_id"]: p for p in fake.binary("empi-review")["pairs"]}
        assert after[pending["pair_id"]]["status"] == "accepted"
        assert fake.binary("empi-review")["links"]["patients_updated"] == 0
        assert m.main(["decide", linked[0]["pair_id"], "--reject", "--base", fake.base]) == 1   # auto links stay


def test_run_refuses_a_public_server():
    with pytest.raises(SystemExit, match="localhost only"):
        m.main(["run", "--base", "https://fhir.example.org/fhir"])


# ---------- a held out set, and evaluate ----------

def test_held_out_set_is_independent():
    population = synthea_population(90)
    records = [m.record(p) for p in population]
    _, tuning = es.build_seed(records, NICK)
    used = {t["existing_id"] for t in tuning["pairs"]}
    messages, held = es.build_seed(records, NICK, seed=7, exclude=used)
    assert used.isdisjoint(t["existing_id"] for t in held["pairs"])                      # no shared person
    assert {t["seed_mrn"] for t in tuning["pairs"]}.isdisjoint(t["seed_mrn"] for t in held["pairs"])
    assert held["seed"] == 7 and [t["seed_mrn"] for t in held["pairs"]][:1] == ["RV310701"]
    assert all("RVGEMPIS7N" in text for _, _, text in messages)
    assert es.mrn_start(42) == 300001 and min(es.mrn_start(s) for s in range(1000) if s != 42) > 300030
    assert pipeline.held_out_paths(42, "data/empi/feed", "data/empi/truth.json") == (Path("data/empi/feed"), Path("data/empi/truth.json"))
    assert pipeline.held_out_paths(7, "data/empi/feed", "data/empi/truth.json") == (Path("data/empi/feed-seed7"), Path("data/empi/truth-seed7.json"))


def test_read_truth_file(tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({"pairs": [{"seed_id": "x"}]}))
    with pytest.raises(ValueError, match="not an EMPI ground truth"):
        m.read_truth_file(bad)


def test_held_out_seed_and_evaluate(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    with FakeFhir(synthea_population(90)) as fake:
        assert pipeline.main(["empi-seed", "--base", fake.base]) == 0
        tuning = fake.binary("empi-truth")
        assert m.main(["run", "--base", fake.base]) == 0
        capsys.readouterr()

        # evaluate refuses a held out truth whose records are not loaded yet
        assert pipeline.main(["empi-seed", "--base", fake.base, "--seed", "7", "--dry-run"]) == 0
        assert m.main(["evaluate", "--truth", "data/empi/truth-seed7.json", "--base", fake.base]) == 1

        assert pipeline.main(["empi-seed", "--base", fake.base, "--seed", "7"]) == 0
        out = capsys.readouterr().out
        assert "leaving out the 30 Synthea patients of the tuning set" in out and "Binary/empi-truth" not in out
        assert fake.binary("empi-truth") == tuning                                     # the tuning truth stays
        assert json.loads((tmp_path / "data/empi/truth.json").read_text()) == tuning
        held = json.loads((tmp_path / "data/empi/truth-seed7.json").read_text())
        assert held["seed"] == 7 and (tmp_path / "data/empi/feed-seed7").is_dir()

        puts = len(fake.puts)
        assert m.main(["evaluate", "--truth", "data/empi/truth-seed7.json", "--base", fake.base]) == 0
        assert len(fake.puts) == puts                                                  # evaluate writes nothing
        out = capsys.readouterr().out
        assert "seed 7" in out and "precision" in out and "recall" in out
        q = m.quality(m.match_all([m.record(r) for (t, _), r in fake.store.items() if t == "Patient"], CFG, NICK),
                      held["pairs"])
        assert f"precision {q['precision']}" in out and q["hard_negatives_auto_linked"] == 0
        # the tuning quality is unaffected by the held out records
        assert m.main(["run", "--base", fake.base]) == 0
        assert fake.binary("empi-review")["quality"]["true_match_pairs"] == 20
