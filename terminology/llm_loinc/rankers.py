"""Rankers: pick the LOINC code for one local lab out of the retrieved candidates.

  LexicalBaseline  the top retrieval score; never abstains
  ClaudeRanker     asks a Claude model (LLM_MODEL, default claude-haiku-5-5) with structured output

What the model sees is only the lab metadata in ALLOWED_LAB_FIELDS and the candidates' CANDIDATE_FIELDS: no local
code (a MIMIC item id could be recalled from training data rather than reasoned about), no gold code, no notes, no
patient data, no counts. tests/test_llm_loinc.py checks the payload against these lists.

The API key is read from the environment or analytics/.env (ANTHROPIC_API_KEY) and never printed or logged.
"""
import json
import os
import sys
import threading
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import config as cfg  # noqa: E402
from cache import Budget, Cache, Ledger, key_of  # noqa: E402
from validate import Decision, validate  # noqa: E402

ALLOWED_LAB_FIELDS = ("label", "specimen", "category", "unit", "value_type", "normal_low", "normal_high",
                      "value_p5", "value_p95", "answers")
CANDIDATE_FIELDS = ("code", "name", "system", "property", "scale", "method")

# v1: written once before any result was seen (the blind baseline of the eval). Kept as sent, so its cached answers
# stay valid.
PROMPT_V1 = """You map a hospital laboratory's local test to LOINC.

You get the local test as the lab system describes it and a numbered list of LOINC candidates from a text search.
Choose the candidate that names the same measurement as the local test:
- the same analyte (LOINC component),
- the same kind of quantity (LOINC property, which follows from the unit: mmol/L or mEq/L is a substance
  concentration, mg/dL a mass concentration, % a fraction, a count per volume a number concentration),
- the same specimen (LOINC system; serum or plasma differs from whole blood, blood gas analyzers measure whole blood),
- the same scale (quantitative for numeric results, ordinal or nominal for coded text results).
A candidate with no method is fine when the local test names none. Prefer the general code over one for a special
timing or challenge (fasting, post dose) unless the local test says so.

If no candidate is the same measurement, answer NONE. A near miss with another specimen, property or scale is not
the same measurement, so answer NONE rather than the closest one.

Use only codes from the candidate list. Answer with:
- choice: the chosen candidate's code, or NONE
- top3: up to three candidate codes, best first (empty when nothing fits)
- confidence: from 0 to 1, how likely your choice (or NONE) is correct
- reason: one sentence"""

# v2: written after reading v1's errors on the gold set (see docs/LLM_LOINC_EVAL.md). Only general mapping rules were
# added, none about a particular test: how lab systems label specimens, the analyzer method, when NONE is right, and
# to answer with the code itself (v1's numbered list made a model answer with a list position).
PROMPT_V2 = """You map a hospital laboratory's local test to LOINC.

You get the local test as the lab system describes it and a list of LOINC candidates from a text search, one per
line: code | name | system | property | scale | method. Choose the candidate that names the same measurement:
- the same analyte (LOINC component),
- the same kind of quantity (LOINC property, which follows from the unit: mmol/L or mEq/L is a substance
  concentration, mg/dL a mass concentration, % a fraction, a count per volume a number concentration),
- the same specimen (LOINC system),
- the same scale (quantitative for numeric results, ordinal or nominal for coded text results).

How lab systems describe specimens: many record the fluid as "Blood" for every test drawn from a vein, so read the
category too.
- Chemistry tests are run on serum or plasma: choose the Ser/Plas code, not whole blood (Bld) and not the combined
  "Serum, Plasma or Blood" code, unless the label says whole blood.
- Blood gas tests are run on whole blood: choose the plain Bld code when arterial or venous is not stated.
- Hematology cell counts and red cell indices come from an automated analyzer: choose the "Automated count" code over
  the code with no method. Coagulation times and the INR are measured on platelet poor plasma (PPP).
- Urine, body fluid and other specimens: choose the code for that specimen.
Prefer the general code over one for a special timing or challenge (fasting, post dose) unless the local test says so.

Answer NONE only when no candidate is the same measurement, for example when every candidate has another specimen,
property or scale. A detail the local test does not state (arterial or venous, a method) is not a reason for NONE.

Use only codes from the candidate list, written exactly as listed. Answer with:
- choice: the chosen candidate's code, or NONE
- top3: up to three candidate codes, best first (empty when nothing fits)
- confidence: from 0 to 1, how likely your choice (or NONE) is correct
- reason: one sentence"""

PROMPTS = {"v1": PROMPT_V1, "v2": PROMPT_V2}

OUTPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "choice": {"type": "string"},
        "top3": {"type": "array", "items": {"type": "string"}},
        "confidence": {"type": "number"},
        "reason": {"type": "string"},
    },
    "required": ["choice", "top3", "confidence", "reason"],
    "additionalProperties": False,
}


class ParseError(ValueError):
    pass


def lab_payload(lab):
    """Only the allowlisted metadata of a local lab, empty fields left out."""
    return {k: lab[k] for k in ALLOWED_LAB_FIELDS if lab.get(k) not in (None, "")}


def candidate_payload(candidates):
    return [{k: c.get(k, "") for k in CANDIDATE_FIELDS} for c in candidates]


def build_message(lab, candidates, version=cfg.PROMPT_VERSION):
    cands = candidate_payload(candidates)
    if version == "v1":
        lines = [f"{i}. {c['code']} | {c['name']} | system {c['system']} | property {c['property']} | "
                 f"scale {c['scale']} | method {c['method'] or 'none'}" for i, c in enumerate(cands, 1)]
    else:
        lines = [f"{c['code']} | {c['name']} | {c['system']} | {c['property']} | {c['scale']} | "
                 f"{c['method'] or 'none'}" for c in cands]
    return ("Local test:\n" + json.dumps(lab_payload(lab), indent=1) + "\n\nCandidates:\n" +
            ("\n".join(lines) if lines else "(none)"))


def build_request(model, lab, candidates, version=cfg.PROMPT_VERSION):
    """Everything sent to the API for one lab; also what the cache key is made of."""
    settings = cfg.request_settings(model)
    output_config = dict(settings.get("output_config", {}),
                         format={"type": "json_schema", "schema": OUTPUT_SCHEMA})
    req = {"model": model, "max_tokens": cfg.MAX_TOKENS, "system": PROMPTS[version],
           "messages": [{"role": "user", "content": build_message(lab, candidates, version)}],
           "output_config": output_config}
    if "thinking" in settings:
        req["thinking"] = settings["thinking"]
    return req


def parse_response(text):
    """The model's JSON answer as a dict; ParseError when it is not the agreed shape."""
    try:
        data = json.loads(text)
    except (TypeError, json.JSONDecodeError) as e:
        raise ParseError(f"not JSON: {e}") from None
    if not isinstance(data, dict):
        raise ParseError("not a JSON object")
    missing = [k for k in OUTPUT_SCHEMA["required"] if k not in data]
    if missing:
        raise ParseError(f"missing {', '.join(missing)}")
    if not isinstance(data["top3"], list) or not all(isinstance(x, str) for x in data["top3"]):
        raise ParseError("top3 is not a list of codes")
    try:
        data["confidence"] = float(data["confidence"])
    except (TypeError, ValueError):
        raise ParseError("confidence is not a number") from None
    return data


def cost(model, input_tokens, output_tokens):
    p_in, p_out = cfg.price(model)
    return (input_tokens * p_in + output_tokens * p_out) / 1e6


def estimate_tokens(request):
    """Rough count for the dry run only (about 3.5 characters per token); real runs use the API's counts."""
    chars = len(request["system"]) + sum(len(m["content"]) for m in request["messages"]) + len(json.dumps(OUTPUT_SCHEMA))
    return int(chars / 3.5) + 40, 120


def read_env_file(path):
    env = {}
    if Path(path).exists():
        for line in Path(path).read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def setting(name, default=None, env_file=cfg.ENV_FILE):
    return os.environ.get(name) or read_env_file(env_file).get(name) or default


def default_model():
    return setting("LLM_MODEL", cfg.DEFAULT_MODEL)


class Ranker:
    name = "ranker"

    def rank(self, lab, candidates):
        raise NotImplementedError


class LexicalBaseline(Ranker):
    name = "lexical_baseline"

    def rank(self, lab, candidates):
        codes = [c["code"] for c in candidates]
        return Decision(choice=codes[0] if codes else None, top3=codes[:3], confidence=None,
                        reason="highest retrieval score" if codes else "no candidates")


class ClaudeRanker(Ranker):
    """One API call per lab, answered from the cache when the same request was made before.

    use_cache=False (the stability check) neither reads nor writes the cache. dry_run=True never calls the API and
    only counts what a real run would send.
    """

    def __init__(self, model, index, cache=None, budget=None, client=None, use_cache=True, dry_run=False,
                 prompt_version=cfg.PROMPT_VERSION, purpose="eval"):
        self.model, self.index, self.prompt_version, self.purpose = model, index, prompt_version, purpose
        self.name = f"{model} {prompt_version}"
        self.cache = cache or Cache(cfg.CACHE_DIR)
        self.ledger = Ledger(self.cache.root / "_ledger.jsonl")
        self.budget = budget or Budget(cfg.DEFAULT_MAX_CALLS)
        self._client, self.use_cache, self.dry_run = client, use_cache, dry_run
        self.planned_calls, self.planned_tokens = 0, [0, 0]
        self._lock = threading.Lock()
        self._key_locks = {}

    def key_lock(self, key):
        """One lock per request, so two identical requests in flight make one call and the second reads the cache."""
        with self._lock:
            return self._key_locks.setdefault(key, threading.Lock())

    @property
    def client(self):
        with self._lock:
            return self._make_client()

    def _make_client(self):
        if self._client is None:
            import anthropic
            key = setting("ANTHROPIC_API_KEY")
            if not key:
                raise SystemExit("ANTHROPIC_API_KEY is not set (environment or analytics/.env)")
            self._client = anthropic.Anthropic(api_key=key, max_retries=3, timeout=120.0)
        return self._client

    def call(self, request):
        """Send one request; returns the cache entry shape. Counts against the call cap first."""
        self.budget.take()
        start = time.perf_counter()
        response = self.client.messages.create(**request)
        latency_ms = (time.perf_counter() - start) * 1000
        text = next((b.text for b in response.content if getattr(b, "type", "") == "text"), "")
        self.ledger.add(model=self.model, input_tokens=response.usage.input_tokens,
                        output_tokens=response.usage.output_tokens, prompt_version=self.prompt_version,
                        purpose=self.purpose if self.use_cache else "stability")
        return {"text": text, "stop_reason": response.stop_reason, "model": response.model,
                "input_tokens": response.usage.input_tokens, "output_tokens": response.usage.output_tokens,
                "latency_ms": round(latency_ms, 1)}

    def rank(self, lab, candidates):
        request = build_request(self.model, lab, candidates, self.prompt_version)
        key = key_of(dict(request, prompt_version=self.prompt_version))
        if not self.use_cache:
            return self.decide(self.call(request), candidates, cached=False)
        with self.key_lock(key):
            entry = self.cache.get(self.model, key)
            cached = entry is not None
            if entry is None:
                if self.dry_run:
                    tin, tout = estimate_tokens(request)
                    with self._lock:
                        self.planned_calls += 1
                        self.planned_tokens[0] += tin
                        self.planned_tokens[1] += tout
                    return Decision(choice=None, dry_run=True)
                entry = self.call(request)
                self.cache.put(self.model, key, entry)
        return self.decide(entry, candidates, cached)

    def decide(self, entry, candidates, cached):
        usage = {"input_tokens": entry.get("input_tokens", 0), "output_tokens": entry.get("output_tokens", 0),
                 "latency_ms": entry.get("latency_ms"), "cached": cached}
        if entry.get("stop_reason") == "refusal":
            return Decision(choice=None, error="refusal", **usage)
        try:
            raw = parse_response(entry.get("text"))
        except ParseError as e:
            return Decision(choice=None, error=f"parse: {e}", **usage)
        d = validate(raw, candidates, self.index)
        for k, v in usage.items():
            setattr(d, k, v)
        return d
