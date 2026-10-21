import json
import sys
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "terminology"))
import conceptmaps as cm  # noqa: E402

ICD_CSV = """icd9,icd9_display,rows,category,needs_review,icd10,icd10_display
25000,Diabetes,10,exact,no,E119,Type 2 diabetes mellitus without complications
2765,Fluid disorder,4,one_to_many,yes,E870 | E871,Hyperosmolality | Hypo-osmolality
V5861,Long term anticoagulants,3,approximate,no,Z7901,Long term (current) use of anticoagulants
E8889,Fall,2,no_map,yes,,
"""


def write(tmp_path, name, text):
    p = tmp_path / name
    p.write_text(text, encoding="utf-8")
    return p


def test_dot_icd10():
    assert cm.dot_icd10("E119") == "E11.9"
    assert cm.dot_icd10("I10") == "I10"
    assert cm.dot_icd10("w19xxxa") == "W19.XXXA"
    assert cm.dot_icd10("E11.9") == "E11.9"


def test_icd9_elements_equivalence_and_review_comments(tmp_path):
    els = {e["code"]: e for e in cm.icd9_elements(cm.read_csv(write(tmp_path, "icd.csv", ICD_CSV)))}
    assert els["25000"]["target"] == [{"code": "E11.9", "equivalence": "equivalent",
                                       "display": "Type 2 diabetes mellitus without complications"}]
    assert [t["code"] for t in els["2765"]["target"]] == ["E87.0", "E87.1"]
    assert all(t["equivalence"] == "inexact" and "Needs review" in t["comment"] for t in els["2765"]["target"])
    assert els["V5861"]["target"][0]["equivalence"] == "inexact"
    assert "E8889" not in els  # no_map codes are left out, so $translate answers "no match"


def test_build_real_tables_produces_valid_conceptmaps():
    lab, icd = cm.build(ROOT / "terminology" / "lab_loinc_map.csv", write_tmp_icd())
    for m in (lab, icd):
        assert m["resourceType"] == "ConceptMap"
        assert m["url"].endswith(m["id"])
        g = m["group"][0]
        assert g["source"] and g["target"] and g["element"]
        assert len({e["code"] for e in g["element"]}) == len(g["element"])  # no duplicate source codes
    glucose = next(e for e in lab["group"][0]["element"] if e["code"] == "50931")
    assert glucose["target"][0]["code"] == "2345-7"


def write_tmp_icd():
    import tempfile
    p = Path(tempfile.mkdtemp()) / "icd.csv"
    p.write_text(ICD_CSV, encoding="utf-8")
    return p


def test_translate_path_and_parse():
    path = cm.translate_path("icd9", "25000")
    q = urllib.parse.parse_qs(path.split("?", 1)[1])
    assert path.startswith("ConceptMap/$translate?")
    assert q["code"] == ["25000"] and q["system"] == [cm.MIMIC_ICD9] and q["targetsystem"] == [cm.ICD10CM]
    params = {"resourceType": "Parameters", "parameter": [
        {"name": "result", "valueBoolean": True},
        {"name": "match", "part": [{"name": "equivalence", "valueCode": "equivalent"},
                                   {"name": "concept", "valueCoding": {"code": "E11.9", "display": "T2DM"}}]},
    ]}
    assert cm.parse_translate(params) == {"result": True, "message": "",
                                          "matches": [{"equivalence": "equivalent", "code": "E11.9", "display": "T2DM"}]}


def test_load_and_verify_against_fake_server(tmp_path, capsys):
    """A tiny server that stores ConceptMaps on PUT and answers $translate from them."""
    store = {}

    class H(BaseHTTPRequestHandler):
        def reply(self, obj):
            d = json.dumps(obj).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(d)))
            self.end_headers()
            self.wfile.write(d)

        def do_PUT(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            store[body["url"]] = body
            self.reply(body)

        def do_GET(self):
            q = {k: v[0] for k, v in urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query).items()}
            m = store[q["url"]]
            params = [{"name": "result", "valueBoolean": False}]
            for e in m["group"][0]["element"]:
                if e["code"] == q["code"]:
                    params = [{"name": "result", "valueBoolean": True}] + [
                        {"name": "match", "part": [{"name": "equivalence", "valueCode": t["equivalence"]},
                                                   {"name": "concept", "valueCoding": {"code": t.get("code", "")}}]}
                        for t in e["target"]]
            self.reply({"resourceType": "Parameters", "parameter": params})

        def log_message(self, *a):
            pass

    srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    icd = write(tmp_path, "icd.csv", ICD_CSV + "4019,Hypertension,9,exact,no,I10,Essential (primary) hypertension\n")
    base = ["--base", f"http://127.0.0.1:{srv.server_port}/fhir", "--icd-csv", str(icd), "--out", str(tmp_path / "out")]
    try:
        assert cm.main(base + ["load"]) == 0
        assert len(store) == 2
        assert (tmp_path / "out" / "ConceptMap-mimic-icd9-to-icd10.json").exists()
        assert cm.main(base + ["verify"]) == 0
        assert cm.main(base + ["translate", "icd9", "99999"]) == 1
    finally:
        srv.shutdown()
    out = capsys.readouterr().out
    assert out.count("[PASS]") == 5
