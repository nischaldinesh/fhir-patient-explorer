import json
import sys
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "hl7v2"))
import pipeline  # noqa: E402
import to_fhir as tf  # noqa: E402
from hl7 import parse  # noqa: E402

MSH = "MSH|^~\\&|EPICADT|RIVERSIDE^RIVERSIDE GENERAL HOSPITAL|FHIRBRIDGE|FHIRBRIDGE|{ts}||{type}|{cid}|P|2.5.1"
PID = "PID|1||RV100200^^^RIVERSIDE^MR||O'Brien^Maya^A||19700101|F|||{street}^^Lowell^MA^01852^USA||(555)555-1000"


def pv1(f=None):
    values = [""] * 46
    for k, v in {1: "1", 2: "I", 19: "V70000", **(f or {})}.items():
        values[k] = v
    return "PV1|" + "|".join(values[1:])


def msg(*segments):
    return parse("\r".join(segments))


def admit(dg1s=(), street="12 Elm St"):
    return msg(MSH.format(ts="20260302083000", type="ADT^A01", cid="A1"), PID.format(street=street),
               pv1({3: "ICU^301^A", 7: "1234^Morgan^Dana", 44: "20260302083000"}), *dg1s)


TRANSLATIONS = {
    "4019": [{"code": "I10", "display": "Essential (primary) hypertension", "equivalence": "inexact"},
             {"code": "I16.9", "display": "Hypertensive crisis, unspecified", "equivalence": "inexact"}],
    "25000": [{"code": "E11.9", "display": "Type 2 diabetes mellitus without complications", "equivalence": "inexact"}],
    "0389": [{"code": "A41.9", "display": "Sepsis, unspecified organism", "equivalence": "equivalent"}],
    "E8889": [],
}


def translate(code):
    return TRANSLATIONS.get(code)


def test_ids_dates_and_names():
    assert tf.fhir_id("RIVERSIDE", "RV100200") == "riverside-rv100200"
    long_id = tf.fhir_id("x" * 80)
    assert len(long_id) <= 64 and long_id == tf.fhir_id("x" * 80)
    assert tf.hl7_datetime("20260304081100") == "2026-03-04T08:11:00"
    assert tf.hl7_datetime("202603040811") == "2026-03-04T08:11:00"
    assert tf.hl7_datetime("19900725") == "1990-07-25"
    assert tf.hl7_datetime("garbage") == ""
    assert tf.normalize_name("  GLUCOSE, SERUM ") == "glucose serum"
    assert tf.normalize_name("K+") == "k"


def test_lab_matching_order():
    syn = tf.load_synonyms()
    assert tf.match_lab("2345-7", "Glucose", "LN", syn)[1] == "loinc_at_source"
    assert tf.match_lab("GLUC", "GLUCOSE, SERUM", "L", syn)[0]["loinc"] == "2345-7"
    hit, how = tf.match_lab("", "Blood sugar", "", syn)
    assert hit["loinc"] == "2345-7" and how == "text_synonym" and "Judgment call" in hit["note"]
    assert tf.match_lab("", "K+", "", syn)[0]["loinc"] == "2823-3"
    assert tf.match_lab("", "Body Height", "", syn)[0]["loinc"] == "8302-2"
    assert tf.match_lab("", "Pain score", "", syn) == (None, "unmapped")


def test_patient_encounter_and_lifecycle():
    m = tf.FeedMapper(translate=translate)
    m.apply(admit())
    p = m.patients["riverside-rv100200"]
    assert p["name"][0] == {"use": "official", "family": "O'Brien", "given": ["Maya", "A"]}
    assert p["gender"] == "female" and p["birthDate"] == "1970-01-01"
    assert p["identifier"][0]["value"] == "RV100200" and p["meta"]["tag"][0]["code"] == "hl7v2"
    e = m.encounters["rv-v70000"]
    assert e["status"] == "in-progress" and e["class"]["code"] == "IMP"
    assert e["period"]["start"] == "2026-03-02T08:30:00" and e["location"][0]["location"]["display"] == "ICU 301 A"

    # A08 replaces demographics, A03 closes the visit but keeps what the admission said
    m.apply(msg(MSH.format(ts="20260303090000", type="ADT^A08", cid="A2"), PID.format(street="99 Harbor St"), pv1()))
    m.apply(msg(MSH.format(ts="20260306100000", type="ADT^A03", cid="A3"), PID.format(street="99 Harbor St"),
                pv1({44: "20260302083000", 45: "20260306100000"})))
    assert m.patients["riverside-rv100200"]["address"][0]["line"] == ["99 Harbor St"]
    e = m.encounters["rv-v70000"]
    assert e["status"] == "finished" and e["period"]["end"] == "2026-03-06T10:00:00"
    assert e["location"][0]["location"]["display"] == "ICU 301 A"
    assert len(m.patients) == 1 and len(m.encounters) == 1


def test_diagnoses_translation_outcomes():
    m = tf.FeedMapper(translate=translate)
    m.apply(admit([
        "DG1|1||4019^HYPERTENSION NOS^I9||20260302083000|A",
        "DG1|2||0389^SEPTICEMIA NOS^I9||20260302083000|A",
        "DG1|3||25000^DMII^I9||20260302083000|A",
        "DG1|4||250.00^DIABETES TYPE II^I9C||20260302083000|A",
        "DG1|5||E8889^FALL NOS^I9||20260302083000|A",
        "DG1|6||J189^Pneumonia, unspecified organism^I10||20260302083000|A",
        "DG1|7||^Chest pain, unspecified||20260302083000|A",
        "DG1|8||99999^UNKNOWN^I9||20260302083000|A",
    ]))
    c = m.conditions
    review = c["rv-v70000-dx-4019"]
    assert [k["system"] for k in review["code"]["coding"]] == [tf.ICD9CM]  # no guessed ICD 10 code
    assert "I10" in review["note"][0]["text"] and "I16.9" in review["note"][0]["text"]
    assert tf.REVIEW_TAG in review["meta"]["tag"]
    sepsis = c["rv-v70000-dx-0389"]["code"]["coding"]
    assert sepsis[0]["code"] == "038.9" and sepsis[1] == {"system": tf.ICD10CM, "code": "A41.9", "display": "Sepsis, unspecified organism"}
    assert c["rv-v70000-dx-e8889"]["code"]["coding"][0]["code"] == "E888.9"
    assert c["rv-v70000-dx-j189"]["code"]["coding"][0]["code"] == "J18.9"
    assert "coding" not in next(x for x in c.values() if x["code"]["text"].startswith("Chest pain"))["code"]
    assert len([k for k in c if k == "rv-v70000-dx-25000"]) == 1  # "25000" and "250.00" merged
    assert m.stats["duplicate_diagnosis_merged"] == 1 and m.stats["icd9_dotted_normalized"] == 1
    assert m.dx_outcomes == {"icd9_needs_review": 1, "icd9_upgraded_exact": 1, "icd9_upgraded_approximate": 2,
                             "icd9_no_icd10_match": 1, "icd10_at_source": 1, "no_code_free_text": 1,
                             "icd9_not_translated": 1}
    assert m.review == [{"visit": "V70000", "icd9": "4019", "text": "HYPERTENSION NOS", "options": ["I10", "I16.9"],
                         "labels": ["Essential (primary) hypertension", "Hypertensive crisis, unspecified"],
                         "patient": "riverside-rv100200", "condition": "rv-v70000-dx-4019"}]


def test_observations_values_ranges_and_flags():
    m = tf.FeedMapper()
    m.apply(admit())
    m.apply(msg(MSH.format(ts="20260302120000", type="ORU^R01", cid="R1"), PID.format(street="12 Elm St"), pv1(),
                "OBR|1||L1|BMP^Basic metabolic panel^L|||20260302110000",
                "OBX|1|NM|^Blood sugar||188|mg/dL|70-105|H|||F|||20260302110000",
                "OBX|2|ST|TROP^Troponin T^L||<0.01|ng/mL|0-0.01|N|||C|||20260302110000",
                "OBX|3|NM|NA^Sodium^L||abc|mmol/L|133-145||||F",
                "OBR|2||N1|VS^Vital signs^L|||20260302100000",
                "OBX|1|NM|^Body Height||160|cm||||||F",
                "OBX|2|ST|^Pain score||Moderate|||||||F"))
    o = m.observations
    glu = o["rv-l1-1"]
    assert glu["code"]["coding"][0]["code"] == "2345-7" and glu["valueQuantity"]["value"] == 188
    assert glu["valueQuantity"]["system"] == tf.UCUM and glu["interpretation"][0]["coding"][0]["code"] == "H"
    assert glu["referenceRange"][0]["low"]["value"] == 70 and glu["encounter"]["reference"] == "Encounter/rv-v70000"
    trop = o["rv-l1-2"]
    assert trop["valueQuantity"]["comparator"] == "<" and trop["valueQuantity"]["value"] == 0.01 and trop["status"] == "corrected"
    assert o["rv-l1-3"]["valueString"] == "abc"  # NM that is not a number is kept as text, not dropped
    height = o["rv-n1-1"]
    assert height["category"][0]["coding"][0]["code"] == "vital-signs" and "vital-signs" in height["meta"]["profile"][0]
    assert height["code"]["coding"][0]["code"] == "8302-2"
    pain = o["rv-n1-2"]
    assert pain["valueString"] == "Moderate" and "coding" not in pain["code"]
    assert m.unmapped_labs == {"Pain score": 1}


def test_full_feed_maps_deterministically(tmp_path):
    import generate
    from route import Router

    def run():
        r, mp = Router(), tf.FeedMapper(translate=translate)
        for _, _, t in generate.build():
            o = r.route(t)
            if o.status == "process":
                mp.apply(o.message)
        return mp

    a, b = run(), run()
    assert json.dumps(a.resources(), sort_keys=True) == json.dumps(b.resources(), sort_keys=True)
    rep = a.report()
    assert rep["resources"]["Patient"] == 25 and rep["resources"]["Encounter"] == 25
    assert rep["pct_results_coded_loinc"] > 90
    assert list(rep["unmapped_lab_names"]) == ["Pain score"]
    ids = [x["id"] for x in a.resources()]
    assert len(ids) == len(set(ids))
    for x in a.resources():
        assert x["id"] == tf.fhir_id(x["id"]) and len(x["id"]) <= 64


def test_load_twice_against_fake_server(tmp_path, capsys):
    """Fake FHIR server: answers $translate and stores batch PUTs by id."""
    store, binaries = {}, {}

    class H(BaseHTTPRequestHandler):
        def reply(self, obj):
            d = json.dumps(obj).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(d)))
            self.end_headers()
            self.wfile.write(d)

        def do_GET(self):
            q = {k: v[0] for k, v in urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query).items()}
            matches = TRANSLATIONS.get(q.get("code"), [])
            self.reply({"resourceType": "Parameters", "parameter": [{"name": "result", "valueBoolean": bool(matches)}] + [
                {"name": "match", "part": [{"name": "equivalence", "valueCode": m["equivalence"]},
                                           {"name": "concept", "valueCoding": {"code": m["code"], "display": m["display"]}}]}
                for m in matches]})

        def do_PUT(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            binaries[self.path.rsplit("/", 1)[-1]] = body
            self.reply(body)

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            for e in body["entry"]:
                store[e["request"]["url"]] = e["resource"]
            self.reply({"resourceType": "Bundle", "entry": [{"response": {"status": "200 OK"}} for _ in body["entry"]]})

        def log_message(self, *a):
            pass

    srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    feed, out = tmp_path / "feed", tmp_path / "out"
    base = f"http://127.0.0.1:{srv.server_port}/fhir"
    try:
        assert pipeline.main(["generate", "--feed", str(feed)]) == 0
        assert pipeline.main(["load", "--feed", str(feed), "--out", str(out), "--base", base]) == 0
        first = dict(store)
        assert pipeline.main(["load", "--feed", str(feed), "--out", str(out), "--base", base]) == 0
    finally:
        srv.shutdown()
    assert store == first  # second run changed nothing
    report = json.loads((out / "load_report.json").read_text())
    assert report["loaded"] == len(first) and report["load_failed"] == 0
    assert report["diagnosis_outcomes"].get("icd9_needs_review", 0) > 0
    assert "Loaded" in capsys.readouterr().out
    import base64
    published = json.loads(base64.b64decode(binaries["hl7v2-ingestion-report"]["data"]))
    assert published["routing"] == {"process": 122, "skip": 62, "duplicate": 10, "error": 4}
    assert len(published["messages"]) == 198 and published["needs_review"][0]["patient"].startswith("riverside-")


def test_dry_run_needs_no_server(tmp_path):
    feed, out = tmp_path / "feed", tmp_path / "out"
    pipeline.main(["generate", "--feed", str(feed)])
    assert pipeline.main(["load", "--dry-run", "--feed", str(feed), "--out", str(out)]) == 0
    lines = (out / "fhir_resources.ndjson").read_text().splitlines()
    assert len(lines) > 400 and json.loads((out / "load_report.json").read_text())["loaded"] == 0
