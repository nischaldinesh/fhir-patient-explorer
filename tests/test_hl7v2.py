import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "hl7v2"))
import generate  # noqa: E402
import pipeline  # noqa: E402
from hl7 import HL7ParseError, parse, unescape, Separators  # noqa: E402
from route import Router  # noqa: E402

ADT = "\r".join([
    "MSH|^~\\&|EPICADT|RIVERSIDE^RIVERSIDE GENERAL HOSPITAL|FHIRBRIDGE|FHIRBRIDGE|20260302083000||ADT^A01^ADT_A01|RVG000001|P|2.5.1",
    "EVN|A01|20260302083000",
    "PID|1||RV100200^^^RIVERSIDE^MR~999-00-1234^^^SSA^SS||O'Brien^Maya^A||19700101|F|||12 Elm St \\T\\ Rear^^Lowell^MA^01852^USA",
    "PV1|1|I|ICU^301^A||||||||||||||||V70000",
    "DG1|1||4019^HYPERTENSION NOS^I9||20260302083000|A",
]) + "\r"


def test_msh_numbering_and_message_type():
    m = parse(ADT)
    assert m.msh.get(1) == "|"
    assert m.msh.get(2) == "^~\\&"
    assert m.msh.get(3) == "EPICADT"
    assert m.msh.get(4, 2) == "RIVERSIDE GENERAL HOSPITAL"
    assert m.msh.get(7) == "20260302083000"
    assert m.message_type == "ADT^A01"
    assert m.control_id == "RVG000001"


def test_components_repetitions_and_escapes():
    pid = parse(ADT).first("PID")
    assert pid.get(3, 1) == "RV100200"
    assert pid.get(3, 4) == "RIVERSIDE"
    assert pid.repetitions(3) == 2
    assert pid.get(3, 1, rep=2) == "999-00-1234"
    assert pid.get(3, 1, rep=3) == ""
    assert pid.get(5, 1) == "O'Brien"
    assert pid.get(11, 1) == "12 Elm St & Rear"  # \T\ decodes to the subcomponent separator
    assert parse(ADT).first("PV1").get(19) == "V70000"
    assert pid.get(99) == ""


def test_unescape_all_sequences():
    s = Separators()
    assert unescape("a\\F\\b\\S\\c\\T\\d\\R\\e\\E\\f", s) == "a|b^c&d~e\\f"
    assert unescape("line1\\.br\\line2", s) == "line1\nline2"
    assert unescape("no escapes", s) == "no escapes"
    assert unescape("lonely \\ backslash", s) == "lonely \\ backslash"


def test_custom_separators_are_read_from_msh():
    m = parse("MSH#*~\\&#APP#FAC#R#R#20260101##ORU*R01#X1#P#2.5\rPID#1##MRN1\r")
    assert m.message_type == "ORU^R01"
    assert m.first("PID").get(3) == "MRN1"


def test_line_endings_and_mllp_framing():
    framed = "\x0b" + ADT.replace("\r", "\r\n") + "\x1c\r"
    assert len(parse(framed).segments) == 5


@pytest.mark.parametrize("text,msg", [
    ("", "empty"),
    ("PID|1||X", "does not start with an MSH"),
    ("MSH|^^^^|A", "invalid encoding"),
    ("MSH|^~\\&|A|B|C|D|2026|||X1|P|2.5", "MSH-9"),
    ("MSH|^~\\&|A|B|C|D|2026||ADT^A01|X1|P|2.5\rP!D|1", "malformed segment"),
])
def test_parse_errors(text, msg):
    with pytest.raises(HL7ParseError, match=msg):
        parse(text)


def test_router_outcomes():
    r = Router()
    assert r.route(ADT).status == "process"
    dup = r.route(ADT)
    assert dup.status == "duplicate" and "RVG000001" in dup.reason
    ack = r.route("MSH|^~\\&|FHIRBRIDGE|X|Y|Z|2026||ACK^A01^ACK|A1|P|2.5.1\rMSA|AA|RVG000001")
    assert ack.status == "skip" and "Acknowledgment" in ack.reason
    assert r.route("MSH|^~\\&|A|B|C|D|2026||SIU^S12|S1|P|2.5.1\rPID|1||M1").status == "skip"
    assert r.route("MSH|^~\\&|A|B|C|D|2026||ZZZ^Z99|Z1|P|2.5.1").reason == "message type ZZZ^Z99 is not handled"
    no_pid = r.route("MSH|^~\\&|A|B|C|D|2026||ORU^R01|O1|P|2.5.1\rOBX|1|NM|GLU||99")
    assert no_pid.status == "error" and "PID" in no_pid.reason
    no_obx = r.route("MSH|^~\\&|A|B|C|D|2026||ORU^R01|O2|P|2.5.1\rPID|1||M1")
    assert no_obx.status == "error" and "OBX" in no_obx.reason
    assert r.route("garbage").status == "error"


def test_generator_is_deterministic_and_messy():
    a, b = generate.build(seed=42), generate.build(seed=42)
    assert [m[2] for m in a] == [m[2] for m in b]
    texts = [m[2] for m in a]
    assert all(t.endswith("\r") for t in texts)
    joined = "".join(texts)
    for messy in ["^Blood sugar", "|<0.01|", "^I9", "250.00^", "^Body Height", "\\T\\", "ACK^A01", "QRY^A19"]:
        assert messy in joined, messy
    assert generate.build(seed=7)[0][2] != a[0][2]


def test_pipeline_generate_and_route(tmp_path, capsys):
    feed, out = tmp_path / "feed", tmp_path / "out"
    assert pipeline.main(["generate", "--feed", str(feed)]) == 0
    files = sorted(feed.glob("*.hl7"))
    assert len(files) > 150
    assert "\r" in files[0].read_bytes().decode()  # real HL7 carriage returns kept on disk
    assert pipeline.main(["route", "--feed", str(feed), "--out", str(out)]) == 0
    report = json.loads((out / "route_report.json").read_text())
    s = report["by_status"]
    assert s["process"] > 100 and s["skip"] > 40 and s["duplicate"] == 10 and s["error"] == 4
    assert len(list((out / "quarantine").glob("*.hl7"))) == 4
    assert pipeline.main(["show", str(files[0])]) == 0
    assert "MSH-9" in capsys.readouterr().out


def test_generated_fields_sit_at_their_hl7_positions():
    msgs = generate.build()
    adt = parse(next(t for _, label, t in msgs if label == "ADT_A01"))
    assert adt.first("PV1").get(19).startswith("V")      # visit number
    assert adt.first("PV1").get(44).isdigit()            # admit date/time
    assert adt.first("PID").get(3, 4) == "RIVERSIDE"     # assigning authority
    assert adt.first("DG1").get(3, 3) in {"I9", "I9C", "I10", ""}
    dis = parse(next(t for _, label, t in msgs if label == "ADT_A03"))
    assert dis.first("PV1").get(45) > dis.first("PV1").get(44)
    oru = parse(next(t for _, label, t in msgs if label == "ORU_R01" and "LABSYS" in t))
    obx = oru.first("OBX")
    assert obx.get(2) in {"NM", "ST"} and obx.get(6) and obx.get(11) in {"F", "C"} and obx.get(14).isdigit()
