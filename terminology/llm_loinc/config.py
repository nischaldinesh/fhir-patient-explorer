"""One place for the LLM assisted LOINC mapper's settings: paths, models, prices, request settings.

Prices are USD per million tokens, Claude API first party rates for prompts up to 100,000 tokens (every prompt here
is a few thousand). Checked by hand against the pricing page on the date below; cost is always the real token counts
the API reports times these prices, never an estimate (the dry run is the only estimate, and says so).
"""
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
DATA = ROOT / "data"

LOINC_CSV = DATA / "loinc" / "Loinc.csv"
MAPTO_CSV = DATA / "loinc" / "MapTo.csv"
INDEX_PATH = DATA / "loinc" / "index.pkl"
CACHE_DIR = DATA / "llm_cache"
EVAL_DIR = DATA / "llm_eval"
D_LABITEMS = DATA / "mimic_raw" / "d_labitems.csv.gz"
MIMIC_LABS = DATA / "mimic" / "MimicObservationLabevents.ndjson.gz"
ENV_FILE = ROOT / "analytics" / ".env"

LOCAL_LABS = HERE / "local_labs.csv"
LAB_MAP = ROOT / "terminology" / "lab_loinc_map.csv"
LAB_EXCLUDE = ROOT / "terminology" / "lab_exclude.csv"
LAB_SYNONYMS = ROOT / "hl7v2" / "lab_synonyms.csv"
REVIEW_QUEUE = ROOT / "terminology" / "loinc_review_queue.csv"
PROVENANCE = ROOT / "terminology" / "lab_loinc_provenance.csv"
EVAL_DOC = ROOT / "docs" / "LLM_LOINC_EVAL.md"

DEFAULT_MODEL = "claude-haiku-5-5"
EVAL_MODELS = ["claude-haiku-5-5", "claude-sonnet-5-5"]

PRICES_CHECKED = "2026-10-19"
PRICES_SOURCE = "https://platform.claude.com/docs/en/about-claude/pricing"
PRICES = {  # model: (input, output) USD per million tokens
    "claude-haiku-5-5": (0.10, 0.50),
    "claude-sonnet-5-5": (2.00, 10.00),
}

# Sampling parameters other than the defaults are rejected by both models, so there is no temperature 0. Thinking is
# turned off (each model has its own switch) and effort is low: the task is one short structured answer.
REQUEST = {
    "claude-haiku-5-5": {"thinking": {"type": "disabled"}, "output_config": {"effort": "low"}},
    "claude-sonnet-5-5": {"thinking": {"type": "between_tools"}, "output_config": {"effort": "low"}},
}
MAX_TOKENS = 1024
PROMPT_VERSION = "v2"  # the prompt suggest.py uses; the eval runs every version in EVAL_PROMPTS
EVAL_PROMPTS = ["v1", "v2"]
TOP_K = 20
DEFAULT_MAX_CALLS = 300
SMALL_CELL = 10  # a code with this many results or fewer gets no value range or answers in local_labs.csv

LOINC_NOTICE = (
    "This material contains content from LOINC (http://loinc.org). LOINC is copyright Regenstrief Institute, Inc. "
    "and the Logical Observation Identifiers Names and Codes (LOINC) Committee and is available at no cost under the "
    "license at http://loinc.org/license. LOINC is a registered United States trademark of Regenstrief Institute, Inc."
)


def price(model):
    if model not in PRICES:
        raise SystemExit(f"No price for model {model!r}: add it to PRICES in {Path(__file__).name}")
    return PRICES[model]


def request_settings(model):
    return REQUEST.get(model, {"output_config": {"effort": "low"}})

# Frozen on 2026-10-19, before the holdout was built: prompt v2 and the retrieval code. tests/test_llm_loinc.py fails
# when either changes, so a holdout result always belongs to exactly these.
FROZEN_ON = "2026-10-19"
FROZEN = {
    "prompt_v2_sha256": "3d4c30aeb89b5cbd92cb09c6881f8428ac7ce2a40438c8b2913a4f70b8696ce6",
    "loinc_index_py_sha256": "c4ccd0477e2fbbeeed1b648de1e61d616d8f2aeb8c09175076b0594c09d6d066",
}

# The holdout: MIMIC III Clinical Database Demo D_LABITEMS (PhysioNet, open access, ODbL), whose LOINC_CODE column
# gives a mapping for many of the same lab item ids. Pairs whose item is in the dev set are left out.
MIMIC3_LABITEMS = DATA / "mimic_raw" / "D_LABITEMS_mimic3.csv"
MIMIC3_URL = "https://physionet.org/files/mimiciii-demo/1.4/D_LABITEMS.csv"
HOLDOUT_PROMPT = "v2"
THRESHOLD_TARGET = 0.95   # the confidence threshold is the lowest one whose auto answers on the dev set are this right
THRESHOLD_GRID = [round(0.5 + 0.05 * i, 2) for i in range(10)]   # 0.5 to 0.95
HOLDOUT_SAMPLE_SIZE = 200  # pairs Claude Sonnet 5.5 answers on the holdout (cost cap); the others answer all
