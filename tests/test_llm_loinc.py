"""LLM assisted LOINC mapper: retrieval, prompt, parser, guard, cache, metrics, eval and review. No network, no key.

The LOINC fixture is invented (codes F0001-1 ..., analytes Fakeose and Blorbium), so no LOINC content is committed.
"""
import csv
import json
import re
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "terminology" / "llm_loinc"))
import cache as C  # noqa: E402
import config as cfg  # noqa: E402
import eval as E  # noqa: E402
import gold as G  # noqa: E402
import local_labs as L  # noqa: E402
import loinc_index as I  # noqa: E402
import rankers as R  # noqa: E402
import report  # noqa: E402
import suggest as S  # noqa: E402
import validate as V  # noqa: E402

LOINC_COLUMNS = ["LOINC_NUM", "COMPONENT", "PROPERTY", "TIME_ASPCT", "SYSTEM", "SCALE_TYP", "METHOD_TYP", "CLASS",
                 "STATUS", "CLASSTYPE", "RELATEDNAMES2", "SHORTNAME", "LONG_COMMON_NAME", "EXAMPLE_UCUM_UNITS",
                 "COMMON_TEST_RANK"]
FIXTURE = [
    # code, component, property, system, scale, method, status, classtype, related, short, long, units, rank
    ("F0001-1", "Fakeose", "MCnc", "Ser/Plas", "Qn", "", "ACTIVE", "1", "Fake sugar; Chemistry", "Fakeose SerPl-mCnc",
     "Fakeose [Mass/volume] in Serum or Plasma", "mg/dL", "5"),
    ("F0002-1", "Fakeose", "MCnc", "Bld", "Qn", "", "ACTIVE", "1", "Fake sugar; Whole blood", "Fakeose Bld-mCnc",
     "Fakeose [Mass/volume] in Blood", "mg/dL", "50"),
    ("F0003-1", "Fakeose", "MCnc", "Urine", "Qn", "", "ACTIVE", "1", "Fake sugar; Urine", "Fakeose Ur-mCnc",
     "Fakeose [Mass/volume] in Urine", "mg/dL", "0"),
    ("F0004-1", "Fakeose", "SCnc", "Ser/Plas", "Qn", "", "ACTIVE", "1", "Fake sugar", "Fakeose SerPl-sCnc",
     "Fakeose [Moles/volume] in Serum or Plasma", "mmol/L", "0"),
    ("F0005-1", "Fakeose", "PrThr", "Urine", "Ord", "Test strip", "ACTIVE", "1", "Fake sugar; Dipstick",
     "Fakeose Ur Ql Strip", "Fakeose [Presence] in Urine by Test strip", "", "0"),
    ("F0006-1", "Blorbium", "SCnc", "Ser/Plas", "Qn", "", "ACTIVE", "1", "Blb; Blorb", "Blorbium SerPl-sCnc",
     "Blorbium [Moles/volume] in Serum or Plasma", "mmol/L", "9"),
    ("F0007-1", "Fakeose", "MCnc", "Ser/Plas", "Qn", "", "DEPRECATED", "1", "", "Fakeose old",
     "Deprecated Fakeose [Mass/volume] in Serum or Plasma", "mg/dL", "0"),
    ("F0008-1", "Fakeose", "Find", "^Patient", "Nom", "", "ACTIVE", "2", "", "Fakeose clinical",
     "Fakeose finding", "", "0"),
]
SERUM = {"label": "Fakeose", "specimen": "Blood", "category": "Chemistry", "unit": "mg/dL", "value_type": "number"}
GAS = {"label": "Fakeose", "specimen": "Blood", "category": "Blood Gas", "unit": "mg/dL", "value_type": "number"}


def write_fixture(tmp_path):
    loinc = tmp_path / "Loinc.csv"
    with open(loinc, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(LOINC_COLUMNS)
        for (code, comp, prop, system, scale, method, status, ct, rel, short, long, units, rank) in FIXTURE:
            w.writerow([code, comp, prop, "Pt", system, scale, method, "CHEM", status, ct, rel, short, long, units, rank])
    mapto = tmp_path / "MapTo.csv"
    mapto.write_text('"LOINC","MAP_TO","COMMENT"\n"F0007-1","F0001-1",""\n', encoding="utf-8")
    return loinc, mapto


@pytest.fixture
def index(tmp_path):
    loinc, mapto = write_fixture(tmp_path)
    return I.load(loinc, mapto, tmp_path / "index.pkl")


class FakeClient:
    """Stands in for anthropic.Anthropic: records every request and answers with answer(request)."""

    def __init__(self, answer=None, delay=0.0):
        self.requests, self.delay = [], delay
        self.answer = answer or (lambda req: {"choice": first_code(req), "top3": [first_code(req)], "confidence": 0.9,
                                              "reason": "first candidate"})
        self.messages = SimpleNamespace(create=self.create)
        self._lock = threading.Lock()

    def create(self, **request):
        with self._lock:
            self.requests.append(request)
        time.sleep(self.delay)
        answer = self.answer(request)
        stop = "refusal" if answer == "refusal" else "end_turn"
        text = "" if answer == "refusal" else json.dumps(answer)
        return SimpleNamespace(content=[SimpleNamespace(type="text", text=text)], stop_reason=stop,
                               model=request["model"], usage=SimpleNamespace(input_tokens=1000, output_tokens=100))


class NoCallClient:
    messages = SimpleNamespace(create=lambda **kw: pytest.fail("the API must not be called"))


def first_code(request):
    m = re.search(r"\b(F\d{4}-\d)\b", request["messages"][0]["content"].split("Candidates:")[1])
    return m.group(1) if m else "NONE"


# ---------------------------------------------------------------- retrieval

def test_tokens_and_unit_properties():
    assert I.tokens("Calcium, Total (in Serum)") == ["calcium", "total", "serum"]
    assert "SCnc" in I.expected_properties("mEq/L") and "MCnc" in I.expected_properties("MG/DL")
    assert I.expected_properties("") == set()
    assert "Ser/Plas" in I.specimen_systems("Blood", "Chemistry")[0]
    assert "Bld" in I.specimen_systems("Blood", "Blood Gas")[0]
    assert I.specimen_systems("Urine", "Hematology")[0] == {"Urine", "Urine sed"}  # falls back to the fluid alone


def test_index_only_active_lab_terms(index):
    codes = {d["code"] for d in index.docs}
    assert codes == {"F0001-1", "F0002-1", "F0003-1", "F0004-1", "F0005-1", "F0006-1"}
    assert index.status("F0007-1") == "DEPRECATED" and index.status("F0008-1") == "ACTIVE"
    assert index.status("F9999-9") is None
    assert index.equivalents("F0007-1") == {"F0007-1", "F0001-1"}


def test_retrieval_uses_category_unit_and_scale(index):
    assert index.search(SERUM)[0]["code"] == "F0001-1"
    assert index.search(GAS)[0]["code"] == "F0002-1"
    assert index.search(dict(SERUM, unit="mmol/L"))[0]["code"] == "F0004-1"
    assert index.search({"label": "Fakeose", "specimen": "Urine", "value_type": "text"})[0]["code"] == "F0005-1"
    assert index.search({"label": "Blb", "specimen": "Blood", "category": "Chemistry"})[0]["code"] == "F0006-1"
    assert index.search({"label": "nothing like it"}) == []


def test_retrieval_top_k_scores_and_exclude(index):
    top = index.search(SERUM, k=3)
    assert len(top) == 3 and top[0]["score"] >= top[1]["score"] >= top[2]["score"]
    assert "F0001-1" not in [c["code"] for c in index.search(SERUM, exclude={"F0001-1"})]


def test_index_is_pickled_and_rebuilt_when_loinc_changes(tmp_path):
    loinc, mapto = write_fixture(tmp_path)
    path = tmp_path / "index.pkl"
    first = I.load(loinc, mapto, path)
    assert path.exists() and I.load(loinc, mapto, path).stamp == first.stamp
    with open(loinc, "a", encoding="utf-8") as f:
        f.write("F0009-1,Zap,MCnc,Pt,Ser/Plas,Qn,,CHEM,ACTIVE,1,,Zap,Zap [Mass/volume] in Serum or Plasma,mg/dL,0\n")
    time.sleep(0.01)
    assert "F0009-1" in {d["code"] for d in I.load(loinc, mapto, path).docs}


# ---------------------------------------------------------------- local labs

@pytest.mark.parametrize("raw,clean", [
    ("NEG.", "NEG"), ("LG*.", "LG"), ("SM .", "SM"), ("Yellow", "YELLOW"), ("1+", "1+"), ("VERY LOW*.", "VERY LOW"),
    ("<1", "<1"), ("2+*.", "2+"),
    ("1+ mod", None), ("Using this patient's age, gender, and serum creatinine", None), ("___", None),
    ("see comment: hemolyzed", None), ("a b c d", None), ("", None), (None, None), ("x" * 30, None),
])
def test_clean_answer(raw, clean):
    assert L.clean_answer(raw) == clean


def test_summarize_hides_small_cells():
    few = [{"value": 5.0, "unit": "mg/dL", "low": 1, "high": 9}] * 10
    s = L.summarize(few)
    assert s["unit"] == "mg/dL" and s["value_type"] == "number" and (s["normal_low"], s["normal_high"]) == ("1", "9")
    assert s["value_p5"] == "" and s["value_p95"] == ""
    many = [{"value": float(v), "unit": "mg/dL"} for v in range(1, 101)]
    s = L.summarize(many)
    assert (s["value_p5"], s["value_p95"]) == ("6", "95")


def test_summarize_text_answers_need_11_each():
    results = [{"text": "NEG."}] * 11 + [{"text": "TR."}] * 10 + [{"text": "1+ mod"}] * 20
    s = L.summarize(results)
    assert s["value_type"] == "text" and s["answers"] == "NEG"


def test_feed_rows_skip_vital_signs_and_add_pain_score():
    labs = {"glucose": (["GLU^Glucose^L", "^Blood sugar", "2345-7^Glucose^LN"], "mg/dL", (70, 105), (72, 260), 0)}
    panels = {"BMP^Basic metabolic panel^L": ["glucose"]}
    synonyms = [{"match_type": "code", "match": "GLU", "loinc": "X"}, {"match_type": "text", "match": "blood sugar",
                                                                       "loinc": "X"},
                {"match_type": "text", "match": "body height", "loinc": "8302-2"}]
    norm = lambda t: (t or "").lower().strip()  # noqa: E731
    rows = L.feed_rows(labs, panels, synonyms, norm)
    assert [r["local_id"] for r in rows] == ["hl7v2:code:GLU", "hl7v2:text:blood sugar", "hl7v2:text:pain score"]
    assert rows[0]["label"] == "Glucose" and rows[1]["label"] == "Blood sugar"
    assert rows[0]["specimen"] == "Serum or plasma" and rows[0]["category"] == "Basic metabolic panel"


def test_committed_local_labs_hold_only_lab_metadata():
    rows = list(csv.DictReader(open(cfg.LOCAL_LABS, encoding="utf-8")))
    assert list(rows[0]) == L.COLUMNS
    for r in rows:
        for a in filter(None, (x.strip() for x in r["answers"].split("|"))):
            assert L.clean_answer(a) == a, a
    ids = {r["local_id"] for r in rows}
    assert all(i in ids for i in G.load_gold())


# ---------------------------------------------------------------- gold and queue

def test_gold_and_queue(tmp_path):
    lab_map = tmp_path / "map.csv"
    lab_map.write_text("mimic_code,mimic_display,loinc,loinc_display,unit,equivalence,note\n1,A,F0001-1,x,,,\n")
    syn = tmp_path / "syn.csv"
    syn.write_text("match_type,match,loinc,loinc_display,note\ncode,glu,F0001-1,x,\ntext,Blood  Sugar!,F0001-1,x,\n"
                   "text,body height,8302-2,x,\n")
    gold = G.load_gold(lab_map, syn)
    assert gold == {"mimic:1": "F0001-1", "hl7v2:code:GLU": "F0001-1", "hl7v2:text:blood sugar": "F0001-1"}
    labs = {"mimic:1": {}, "mimic:2": {}, "mimic:3": {}, "hl7v2:code:GLU": {}}
    assert G.queue_ids(labs, gold, {"mimic:3"}) == ["mimic:2"]
    assert G.slice_of("mimic:2") == "mimic" and G.slice_of("hl7v2:code:GLU") == "feed"


# ---------------------------------------------------------------- prompt and payload

def test_payload_sends_only_allowlisted_lab_metadata(index):
    lab = dict(SERUM, local_id="mimic:99", local_code="99", source="mimic", normal_low="70", normal_high="100",
               value_p5="", answers="", gold="F0001-1", note="judgment call", patient="Jane Doe", results="1234")
    req = R.build_request("claude-haiku-5-5", lab, index.search(lab))
    text = req["messages"][0]["content"]
    sent = json.loads(text.split("Local test:\n")[1].split("\n\nCandidates:")[0])
    assert set(sent) <= set(R.ALLOWED_LAB_FIELDS)
    assert set(sent) == {"label", "specimen", "category", "unit", "value_type", "normal_low", "normal_high"}
    for leaked in ("mimic:99", "Jane Doe", "judgment call", "1234"):
        assert leaked not in json.dumps(req)
    assert set(R.ALLOWED_LAB_FIELDS) <= set(L.COLUMNS) and "local_code" not in R.ALLOWED_LAB_FIELDS
    assert set(req) == {"model", "max_tokens", "system", "messages", "output_config", "thinking"}
    assert req["output_config"]["format"]["schema"] == R.OUTPUT_SCHEMA
    assert "temperature" not in req


def test_candidates_carry_only_candidate_fields(index):
    cands = index.search(SERUM)
    assert all(set(c) == set(R.CANDIDATE_FIELDS) for c in R.candidate_payload(cands))
    v2 = R.build_message(SERUM, cands, "v2")
    assert "F0001-1 | Fakeose [Mass/volume] in Serum or Plasma | Ser/Plas | MCnc | Qn | none" in v2
    assert not re.search(r"^\d+\. ", v2.split("Candidates:")[1], re.M)
    assert re.search(r"^1\. F0001-1", R.build_message(SERUM, cands, "v1").split("Candidates:\n")[1], re.M)
    assert R.build_message(SERUM, [], "v2").endswith("(none)")


def test_prompts_and_model_settings():
    assert set(R.PROMPTS) == set(cfg.EVAL_PROMPTS) and cfg.PROMPT_VERSION in R.PROMPTS
    for m in cfg.EVAL_MODELS:
        assert m in cfg.PRICES and m in cfg.REQUEST
        assert "temperature" not in cfg.REQUEST[m]
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", cfg.PRICES_CHECKED)
    assert cfg.DEFAULT_MODEL == "claude-haiku-5-5"


# ---------------------------------------------------------------- response parser and guard

def test_parse_response():
    ok = R.parse_response('{"choice": "F0001-1", "top3": ["F0001-1"], "confidence": "0.7", "reason": "r"}')
    assert ok["confidence"] == 0.7
    for bad in ["not json", "[1, 2]", '{"choice": "x"}', None,
                '{"choice": "x", "top3": "x", "confidence": 1, "reason": ""}',
                '{"choice": "x", "top3": [1], "confidence": 1, "reason": ""}',
                '{"choice": "x", "top3": [], "confidence": "high", "reason": ""}']:
        with pytest.raises(R.ParseError):
            R.parse_response(bad)


def test_validator_rejects_codes_outside_the_candidates(index):
    cands = index.search(SERUM, k=2)  # F0001-1 and one more
    d = V.validate({"choice": "F0006-1", "top3": ["F0006-1", "3", "F0001-1"], "confidence": 2, "reason": " r "},
                   cands, index)
    assert d.choice is None and d.top3 == ["F0001-1"] and d.confidence == 1.0 and d.reason == "r"
    assert len(d.violations) == 3 and all("not in the candidate list" in v for v in d.violations)


def test_validator_none_unknown_deprecated_and_order(index):
    cands = [{"code": c} for c in ("F0001-1", "F0002-1", "F0007-1", "F9999-9")]
    assert V.validate({"choice": "NONE", "top3": []}, cands, index).choice is None
    assert V.validate({"choice": " none ", "top3": []}, cands, index).violations == []
    d = V.validate({"choice": "F9999-9", "top3": []}, cands, index)
    assert d.choice is None and d.violations == ["F9999-9: not in Loinc.csv"]
    assert V.validate({"choice": "F0007-1", "top3": []}, cands, index).choice == "F0001-1"
    d = V.validate({"choice": "F0002-1", "top3": ["F0001-1", "F0001-1"]}, cands, index)
    assert d.top3 == ["F0002-1", "F0001-1"]


# ---------------------------------------------------------------- the Claude ranker, cache, cap, ledger

def ranker(index, tmp_path, client, **kw):
    return R.ClaudeRanker("claude-haiku-5-5", index, C.Cache(tmp_path / "cache"), kw.pop("budget", C.Budget(10)),
                          client=client, **kw)


def test_claude_ranker_calls_once_then_answers_from_cache(index, tmp_path):
    client = FakeClient()
    cands = index.search(SERUM)
    d = ranker(index, tmp_path, client).rank(SERUM, cands)
    assert d.choice == "F0001-1" and not d.cached and d.input_tokens == 1000 and d.latency_ms is not None
    again = ranker(index, tmp_path, NoCallClient()).rank(SERUM, cands)
    assert again.cached and again.choice == "F0001-1" and again.latency_ms == d.latency_ms
    ledger = C.Ledger(tmp_path / "cache" / "_ledger.jsonl").entries()
    assert len(ledger) == 1 and ledger[0]["model"] == "claude-haiku-5-5" and ledger[0]["purpose"] == "eval"
    assert "api_key" not in json.dumps(ledger) and len(client.requests) == 1


def test_cache_key_depends_on_prompt_version_and_model(index, tmp_path):
    client = FakeClient()
    cands = index.search(SERUM)
    ranker(index, tmp_path, client, prompt_version="v1").rank(SERUM, cands)
    ranker(index, tmp_path, client, prompt_version="v2").rank(SERUM, cands)
    assert len(client.requests) == 2 and client.requests[0]["system"] != client.requests[1]["system"]
    assert C.key_of({"a": 1, "b": 2}) == C.key_of({"b": 2, "a": 1})


def test_call_cap_stops_before_the_call(index, tmp_path):
    client = FakeClient()
    r = ranker(index, tmp_path, client, budget=C.Budget(1))
    r.rank(SERUM, index.search(SERUM))
    with pytest.raises(C.CallCapReached):
        r.rank(GAS, index.search(GAS))
    assert len(client.requests) == 1


def test_dry_run_counts_and_never_calls(index, tmp_path):
    r = ranker(index, tmp_path, NoCallClient(), dry_run=True)
    d = r.rank(SERUM, index.search(SERUM))
    assert d.dry_run and r.planned_calls == 1 and r.planned_tokens[0] > 100
    assert not (tmp_path / "cache" / "claude-haiku-5-5").exists()


def test_identical_requests_in_flight_make_one_call(index, tmp_path):
    client = FakeClient(delay=0.05)
    r = ranker(index, tmp_path, client)
    cands = index.search(SERUM)
    threads = [threading.Thread(target=r.rank, args=(SERUM, cands)) for _ in range(4)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert len(client.requests) == 1


def test_no_cache_mode_and_refusal(index, tmp_path):
    client = FakeClient(answer=lambda req: "refusal")
    r = ranker(index, tmp_path, client, use_cache=False)
    d = r.rank(SERUM, index.search(SERUM))
    assert d.choice is None and d.error == "refusal"
    r.rank(SERUM, index.search(SERUM))
    assert len(client.requests) == 2 and not (tmp_path / "cache" / "claude-haiku-5-5").exists()
    assert {e["purpose"] for e in C.Ledger(tmp_path / "cache" / "_ledger.jsonl").entries()} == {"stability"}


def test_lexical_baseline(index):
    d = R.LexicalBaseline().rank(SERUM, index.search(SERUM))
    assert d.choice == "F0001-1" and len(d.top3) == 3
    assert R.LexicalBaseline().rank(SERUM, []).choice is None


def test_cost_and_env_file(tmp_path):
    assert R.cost("claude-sonnet-5-5", 1_000_000, 100_000) == pytest.approx(2.0 + 1.0)
    env = tmp_path / ".env"
    env.write_text("# comment\nLLM_MODEL='claude-sonnet-5-5'\nOTHER=1\n", encoding="utf-8")
    assert R.read_env_file(env) == {"LLM_MODEL": "claude-sonnet-5-5", "OTHER": "1"}
    assert R.read_env_file(tmp_path / "missing") == {}
    with pytest.raises(SystemExit):
        cfg.price("claude-unknown")


# ---------------------------------------------------------------- metrics

def test_wilson():
    assert E.wilson(0, 0) == (None, None, None)
    p, lo, hi = E.wilson(5, 10)
    assert p == 0.5 and lo == pytest.approx(0.2366, abs=1e-4) and hi == pytest.approx(0.7634, abs=1e-4)
    p, lo, hi = E.wilson(10, 10)
    assert p == 1.0 and hi == 1.0 and lo == pytest.approx(0.7225, abs=1e-4)
    assert E.wilson(0, 10)[1] == 0.0


def test_recall_position_percentile_calibration():
    assert E.position(["a", "b", "c"], {"c", "x"}) == 2 and E.position(["a"], {"x"}) is None
    assert E.recall_at([0, 4, 19, None], 1) == (1, 4) and E.recall_at([0, 4, 19, None], 5) == (2, 4)
    assert E.recall_at([0, 4, 19, None], 20) == (3, 4)
    assert E.percentile([1, 2, 3, 4], 50) == 2.5 and E.percentile([None, 7], 95) == 7 and E.percentile([], 50) is None
    bands = E.calibration([(0.2, False), (0.9, True), (0.9, False), (0.99, True), (None, True)])
    assert [b["n"] for b in bands] == [1, 0, 2, 1] and bands[2]["accuracy"] == 0.5 and bands[1]["accuracy"] is None


def test_abstention_slice_is_seeded_half():
    ids = [f"mimic:{i}" for i in range(10)]
    a = E.abstention_ids(ids)
    assert len(a) == 5 and a == E.abstention_ids(list(reversed(ids))) and set(a) <= set(ids)


# ---------------------------------------------------------------- the eval end to end, on the fixture

@pytest.fixture
def eval_world(index, tmp_path, monkeypatch):
    labs = {
        "mimic:1": dict(SERUM, local_id="mimic:1", source="mimic", local_code="1"),
        "mimic:2": dict(GAS, local_id="mimic:2", source="mimic", local_code="2"),
        "mimic:3": dict(SERUM, label="Blorbium", unit="mmol/L", local_id="mimic:3", source="mimic", local_code="3"),
        "hl7v2:code:FAK": dict(SERUM, specimen="Serum or plasma", category="", local_id="hl7v2:code:FAK",
                               source="hl7v2", local_code="FAK"),
    }
    gold = {"mimic:1": "F0001-1", "mimic:2": "F0002-1", "mimic:3": "F0006-1", "hl7v2:code:FAK": "F0007-1"}
    monkeypatch.setattr(E.local_labs, "load", lambda: labs)
    monkeypatch.setattr(E.G, "load_gold", lambda: gold)
    monkeypatch.setattr(E.loinc_index, "load", lambda: index)
    monkeypatch.setattr(E, "HARD_PAIRS", [("mimic:1", "mimic:2")])
    return labs, gold


def test_eval_end_to_end_and_rerun_from_cache(eval_world, tmp_path):
    client = FakeClient()
    kw = dict(cache_dir=tmp_path / "cache", stability_path=tmp_path / "stability.json")
    res = E.evaluate(["claude-haiku-5-5"], 100, client=client, **kw)
    assert res["gold"] == {"mimic": 3, "feed": 1, "all": 4}
    assert res["retrieval"]["all"]["lenient"]["at_1"]["k"] == 4  # F0007-1 counts through MapTo.csv
    assert res["retrieval"]["all"]["strict"]["at_20"]["k"] == 3  # the deprecated gold code is never retrieved
    base = res["rankers"]["lexical_baseline"]["slices"]["all"]
    empty = sum(1 for r in res["records"] if r["case"] == "abstention" and not r["candidates"])
    assert base["top1_lenient"]["k"] == 4 and base["top1_strict"]["k"] == 3 and base["abstention"]["k"] == empty
    assert res["rankers"]["claude-haiku-5-5 v2"]["calls"] > 0 and res["suggested_outside_candidates"] == 0
    assert len(res["hard_pairs"]) == 2 and res["spend"]["total_calls"] == len(client.requests)
    calls = len(client.requests)
    again = E.evaluate(["claude-haiku-5-5"], 0, client=NoCallClient(), **kw)  # cap 0: any call would fail
    assert again["rankers"] == res["rankers"] and again["spend"]["total_calls"] == calls


def test_eval_dry_run_and_stability(eval_world, tmp_path):
    kw = dict(cache_dir=tmp_path / "cache", stability_path=tmp_path / "stability.json")
    plan = E.evaluate(["claude-haiku-5-5"], 100, client=NoCallClient(), dry_run=True, **kw)
    assert plan["dry_run"] and plan["calls"] > 0 and plan["estimated_cost"] > 0
    client = FakeClient()
    res = E.evaluate(["claude-haiku-5-5"], 500, client=client, stability=2, **kw)
    st = res["stability"]["models"]["claude-haiku-5-5 v2"]
    assert st["agreement"]["k"] == st["agreement"]["n"] == 4 and (tmp_path / "stability.json").exists()
    purposes = res["spend"]["purposes"]
    assert purposes["stability"]["calls"] == 8 and purposes["eval"]["calls"] + 8 == len(client.requests)


# ---------------------------------------------------------------- the review queue

@pytest.fixture
def review(tmp_path, index):
    paths = {k: tmp_path / f"{k}.csv" for k in ("queue", "map", "syn", "prov")}
    paths["map"].write_text(",".join(S.MAP_COLUMNS) + "\n", encoding="utf-8")
    paths["syn"].write_text(",".join(S.SYNONYM_COLUMNS) + "\n", encoding="utf-8")
    base = {c: "" for c in S.QUEUE_COLUMNS}
    rows = [dict(base, local_id="mimic:7", source="mimic", local_code="7", label="Fakeose", unit="mg/dL",
                 suggested_loinc="F0001-1", confidence="0.90", model="claude-haiku-5-5", prompt_version="v2",
                 status="pending"),
            dict(base, local_id="hl7v2:text:fake sugar", source="hl7v2", label="Fake sugar", suggested_loinc="",
                 model="claude-haiku-5-5", prompt_version="v2", status="pending")]
    S.write_csv(paths["queue"], rows, S.QUEUE_COLUMNS)

    def run(local_id, status="accepted", loinc=None, reviewer="Alex Kim"):
        return S.decide(local_id, reviewer, status, loinc, on="2026-10-19", index=index, queue_path=paths["queue"],
                        lab_map=paths["map"], synonyms=paths["syn"], provenance=paths["prov"], already_mapped=set())
    return paths, run


def test_queue_rows_keep_decisions_and_never_apply(index, tmp_path):
    labs = {"mimic:7": dict(SERUM, local_id="mimic:7", source="mimic", local_code="7"),
            "mimic:8": dict(GAS, local_id="mimic:8", source="mimic", local_code="8")}
    r = ranker(index, tmp_path, FakeClient(), purpose="queue")
    decided = [dict({c: "" for c in S.QUEUE_COLUMNS}, local_id="mimic:8", status="rejected", reviewed_by="A")]
    rows = S.build_queue(labs, ["mimic:7", "mimic:8"], index, r, decided)
    assert [x["status"] for x in rows] == ["pending", "rejected"] and rows[0]["suggested_loinc"] == "F0001-1"
    assert rows[0]["suggested_name"].startswith("Fakeose") and rows[0]["confidence"] == "0.90"


def test_accept_appends_map_and_provenance(review):
    paths, run = review
    row = run("mimic:7")
    assert row["status"] == "accepted" and row["reviewed_by"] == "Alex Kim"
    mapped = list(csv.DictReader(open(paths["map"])))
    assert mapped[0]["mimic_code"] == "7" and mapped[0]["loinc"] == "F0001-1" and "reviewed by Alex Kim" in mapped[0]["note"]
    prov = list(csv.DictReader(open(paths["prov"])))
    assert prov[0]["source"] == "llm_reviewed" and prov[0]["reviewer"] == "Alex Kim" and prov[0]["reviewed_on"] == "2026-10-19"
    with pytest.raises(SystemExit, match="already accepted"):
        run("mimic:7")


def test_accept_feed_row_with_another_code_and_guards(review):
    paths, run = review
    with pytest.raises(SystemExit, match="no suggestion"):
        run("hl7v2:text:fake sugar")
    with pytest.raises(SystemExit, match="deprecated"):
        run("hl7v2:text:fake sugar", loinc="F0007-1")
    with pytest.raises(SystemExit, match="not in Loinc.csv"):
        run("hl7v2:text:fake sugar", loinc="F9999-9")
    with pytest.raises(SystemExit, match="reviewer"):
        run("hl7v2:text:fake sugar", loinc="F0001-1", reviewer=" ")
    run("hl7v2:text:fake sugar", loinc="F0001-1")
    syn = list(csv.DictReader(open(paths["syn"])))
    assert (syn[0]["match_type"], syn[0]["match"], syn[0]["loinc"]) == ("text", "fake sugar", "F0001-1")
    assert "another code than the suggestion" in syn[0]["note"]


def test_reject_changes_only_the_queue(review):
    paths, run = review
    run("mimic:7", status="rejected")
    assert list(csv.DictReader(open(paths["map"]))) == [] and not paths["prov"].exists()
    assert list(csv.DictReader(open(paths["queue"])))[0]["status"] == "rejected"


def test_committed_queue_is_all_pending():
    rows = list(csv.DictReader(open(cfg.REVIEW_QUEUE, encoding="utf-8")))
    assert rows and list(rows[0]) == S.QUEUE_COLUMNS
    assert {r["status"] for r in rows} == {"pending"}
    gold = G.load_gold()
    assert not any(r["local_id"] in gold for r in rows)


# ---------------------------------------------------------------- freeze, holdout and threshold

import hashlib  # noqa: E402

import holdout as H  # noqa: E402


def test_prompt_v2_and_retrieval_are_frozen():
    """The holdout result belongs to exactly this prompt and this retrieval code: changing either fails here."""
    assert hashlib.sha256(R.PROMPT_V2.encode()).hexdigest() == cfg.FROZEN["prompt_v2_sha256"]
    source = (ROOT / "terminology" / "llm_loinc" / "loinc_index.py").read_bytes()
    assert hashlib.sha256(source).hexdigest() == cfg.FROZEN["loinc_index_py_sha256"]
    assert cfg.HOLDOUT_PROMPT == cfg.PROMPT_VERSION == "v2"


def write_mimic3(tmp_path, rows):
    p = tmp_path / "D_LABITEMS.csv"
    with open(p, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["ROW_ID", "ITEMID", "LABEL", "FLUID", "CATEGORY", "LOINC_CODE"])
        for i, r in enumerate(rows, 1):
            w.writerow([i, *r])
    return p


def test_load_holdout_and_dev_agreement(tmp_path):
    path = write_mimic3(tmp_path, [("1", "A", "Blood", "Chemistry", "F0001-1"), ("2", "B", "Blood", "Chemistry", "F0002-1"),
                                   ("3", "C", "Blood", "Chemistry", ""), ("4", "D", "Urine", "Hematology", "F0003-1"),
                                   ("5", "E", "Blood", "Chemistry", "F0004-1"), ("9", "Z", "Blood", "Chemistry", "F0006-1")])
    labs = {f"mimic:{i}": {} for i in (1, 2, 3, 4, 5)}
    dev = {"mimic:1": "F0001-1", "mimic:2": "F0006-1"}
    ho = H.load_holdout(labs, dev, {"mimic:5"}, path)
    assert list(ho) == ["mimic:4"] and ho["mimic:4"]["loinc"] == "F0003-1" and ho["mimic:4"]["mimic3_label"] == "D"
    agree = H.dev_agreement(dev, path)
    assert agree["both"] == 2 and agree["agree"] == 1 and agree["differ"][0]["mimic3"] == "F0002-1"
    with pytest.raises(SystemExit):
        H.load_holdout(labs, dev, set(), tmp_path / "missing.csv")


def test_doubt_flags(index):
    assert H.flags(SERUM, "F0001-1", index) == []
    assert H.flags(SERUM, "F9999-9", index) == ["not in Loinc.csv"]
    assert "deprecated in LOINC" in H.flags(SERUM, "F0007-1", index)
    assert "not a laboratory term (CLASSTYPE 2)" in H.flags(SERUM, "F0008-1", index)
    assert H.flags(dict(SERUM, specimen="Urine"), "F0001-1", index) == ["specimen Ser/Plas for a Urine test"]
    assert H.flags(dict(SERUM, unit="mmol/L"), "F0001-1", index) == ["property MCnc for unit mmol/L"]
    assert H.flag_kind("property MCnc for unit mmol/L") == "property does not fit the unit"
    assert H.flag_kind("deprecated in LOINC") == "deprecated in LOINC"


def test_near_match(index):
    terms = {d["code"]: d for d in index.docs}
    terms["F0010-1"] = dict(terms["F0001-1"], code="F0010-1", system="Ser", method="Immunoassay")
    assert H.near_match("F0010-1", {"F0001-1"}, terms)
    assert not H.near_match("F0002-1", {"F0001-1"}, terms)   # whole blood is another specimen
    assert not H.near_match("F0004-1", {"F0001-1"}, terms)   # moles, not mass
    assert not H.near_match(None, {"F0001-1"}, terms)


def test_choose_threshold_and_route():
    pairs = [(0.95, True)] * 12 + [(0.85, True)] * 10 + [(0.85, False)] * 3 + [(0.6, False)] * 5 + [(None, True)]
    t = H.choose_threshold(pairs, target=0.95, grid=[0.5, 0.8, 0.9], min_auto=10)
    assert t == {"threshold": 0.9, "auto": 12, "correct": 12, "accuracy": 1.0, "met_target": True}
    t = H.choose_threshold(pairs, target=0.99, grid=[0.5, 0.8], min_auto=10)
    assert t["threshold"] == 0.8 and not t["met_target"]
    assert H.choose_threshold([(0.9, True)], min_auto=10)["threshold"] is None
    auto, review = H.route([(0.95, True), (0.5, False), (None, True)], 0.9)
    assert auto == [(0.95, True)] and len(review) == 2
    assert H.route([(0.95, True)], None) == ([], [(0.95, True)])


def test_seeded_subsets():
    ids = [f"mimic:{i}" for i in range(100, 150)]
    assert H.sample_ids(ids, 10) == H.sample_ids(list(reversed(ids)), 10) and len(H.sample_ids(ids, 10)) == 10
    assert H.sample_ids(ids, None) == ids and H.sample_ids(ids, 500) == ids
    assert len(H.spot_check_ids(ids)) == 10
    assert set(H.SPOT_CHECK_NOTES) == set(H.spot_check_ids(ids)) or all(
        v[0] in ("ok", "plausible", "doubtful", "wrong") for v in H.SPOT_CHECK_NOTES.values())


def test_mcnemar():
    assert E.mcnemar(0, 0) == 1.0
    assert E.mcnemar(5, 5) == 1.0
    assert E.mcnemar(31, 6) == pytest.approx(4.1e-05, rel=0.05)
    assert E.mcnemar(6, 31) == E.mcnemar(31, 6)


def test_holdout_end_to_end(eval_world, tmp_path, monkeypatch):
    labs, gold = eval_world
    labs["mimic:4"] = dict(SERUM, label="Blorbium", unit="mmol/L", local_id="mimic:4", source="mimic", local_code="4")
    labs["mimic:5"] = dict(GAS, local_id="mimic:5", source="mimic", local_code="5")
    path = write_mimic3(tmp_path, [("4", "Blorb", "Blood", "Chemistry", "F0006-1"), ("5", "Fake", "Blood", "Blood Gas", "F0002-1"),
                                   ("1", "Fake", "Blood", "Chemistry", "F0002-1")])
    monkeypatch.setattr(cfg, "MIMIC3_LABITEMS", path)
    monkeypatch.setattr(H.load_holdout, "__defaults__", (path,))
    monkeypatch.setattr(H.dev_agreement, "__defaults__", (path,))
    client = FakeClient()
    kw = dict(cache_dir=tmp_path / "cache", stability_path=tmp_path / "stability.json")
    budget = C.Budget(500)
    dev = E.evaluate(["claude-haiku-5-5"], 500, client=client, budget=budget, **kw)
    ho = E.evaluate_holdout(["claude-haiku-5-5"], budget, dev["thresholds"], client=client,
                            cache_dir=tmp_path / "cache", sample_models=["claude-haiku-5-5"], sample_size=1)
    assert ho["pairs"] == 2 and ho["sample_size"] == 1 and set(ho["subsets"]) == {"all", "sample"}
    assert "claude-haiku-5-5 v2" not in ho["subsets"]["all"]["rankers"]   # it only answered the sample
    x = ho["subsets"]["sample"]["rankers"]["claude-haiku-5-5 v2"]
    assert x["top1_lenient"]["n"] == 1 and "vs_baseline" in x and x["threshold"] == dev["thresholds"]["claude-haiku-5-5 v2"]["threshold"]
    assert ho["dev_agreement"] == {"both": 1, "agree": 0, "differ": [
        {"local_id": "mimic:1", "dev": "F0001-1", "mimic3": "F0002-1", "label": "Fake"}]}
    res = dict(dev, holdout=ho)
    doc = report.render(res)
    assert doc.index("## Headline: holdout") < doc.index("## Dev set") < doc.index("## Limits")
    assert cfg.FROZEN["prompt_v2_sha256"] in doc and cfg.LOINC_NOTICE in doc and "—" not in doc
    calls = len(client.requests)
    E.evaluate_holdout(["claude-haiku-5-5"], C.Budget(0), dev["thresholds"], client=NoCallClient(),
                       cache_dir=tmp_path / "cache", sample_models=["claude-haiku-5-5"], sample_size=1)
    assert len(client.requests) == calls
