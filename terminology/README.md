# Terminology mapping (Option A)

Real clinical data rarely arrives in standard codes. MIMIC labs use local item codes (498 of them), and about
half of MIMIC diagnoses are still in retired ICD 9 codes. This step maps them to standards.

* `terminology/lab_loinc_map.csv`: curated MIMIC lab code to LOINC table. Judgment calls are documented in the `note` column.
* `terminology/build_maps.py`: checks the table against the real data, measures coverage weighted by result count,
  and maps every ICD 9 diagnosis to ICD 10 with the official CMS/CDC General Equivalence Mappings (GEMs),
  flagging one to many, combination and unmappable codes for human review.

```bash
mkdir -p data/terminology
curl -L -o data/terminology/2018_I9gem.txt https://ftp.cdc.gov/pub/Health_Statistics/nchs/Publications/ICD10CM/2018/2018_I9gem.txt
curl -L -o data/terminology/order.zip https://ftp.cdc.gov/pub/Health_Statistics/nchs/Publications/ICD10CM/2018/2018-ICD-10-Code-Order-Descriptions.zip
unzip -o -j data/terminology/order.zip "*order*.txt" -d data/terminology
python3 terminology/profile_codes.py      # what codes are in the data
python3 terminology/build_maps.py         # map them, writes terminology/out/
```


## Step 3: publish as FHIR ConceptMaps

`terminology/lab_exclude.csv` lists MIMIC codes that are not patient measurements (sample quality indices,
specimen type). They are left out of the coverage numbers instead of being mapped.

`terminology/conceptmaps.py` turns both mapping tables into FHIR R4 ConceptMap resources, loads them into
HAPI, and uses HAPI's built in `$translate` operation:

```bash
python3 terminology/build_maps.py                     # refresh coverage after table edits
python3 terminology/conceptmaps.py load               # PUT both ConceptMaps (safe to rerun)
python3 terminology/conceptmaps.py verify             # 5 known translations must PASS
python3 terminology/conceptmaps.py translate icd9 2765   # one to many: needs review
python3 terminology/conceptmaps.py translate lab 50931
```

Raw HTTP equivalent:

```
GET /fhir/ConceptMap/$translate?url=https://github.com/nischaldinesh/fhir-patient-explorer/ConceptMap/mimic-icd9-to-icd10
    &system=http://mimic.mit.edu/fhir/mimic/CodeSystem/mimic-diagnosis-icd9&code=25000
    &targetsystem=http://hl7.org/fhir/sid/icd-10-cm
```

## Step 4: LLM assisted mapping (demo)

440 MIMIC lab codes, plus the feed's pain score, have no LOINC code yet. `terminology/llm_loinc/` suggests one for
each and measures how good the suggestions are against the human maps above. A person decides every row; nothing is
applied automatically.

| File | Job |
|---|---|
| `local_labs.py`, `local_labs.csv` | What the lab system knows about each test: label, specimen, category, unit, number or text, normal range, value range, short coded answers. No patient data; codes with 10 or fewer results get no range and no answers |
| `loinc_index.py` | A local LOINC index (ACTIVE laboratory terms) and search: BM25 over the names, boosted when the specimen, unit and scale fit. The top 20 are the candidates. No LLM |
| `rankers.py` | The lexical baseline (top search result) and the Claude ranker: lab metadata and the 20 candidates in, a choice or NONE, a top 3, a confidence and a reason out |
| `validate.py` | Drops any code that is not a candidate or not in Loinc.csv, so an invented code can never get through; maps deprecated codes through MapTo.csv |
| `cache.py` | One file per call in `data/llm_cache` (a rerun costs nothing), a call ledger for spend, a hard cap on calls per run |
| `eval.py`, `report.py` | The accuracy eval on the dev set and the holdout: `docs/LLM_LOINC_EVAL.md`, `data/llm_eval/results.json` |
| `holdout.py` | The MIMIC III holdout, its doubt flags and spot check, the near match lens and the confidence threshold |
| `suggest.py` | The review queue `terminology/loinc_review_queue.csv`, and `accept` / `reject` |
| `config.py` | Models, prices (with the date they were checked), request settings, paths |

Setup (once). The LOINC table is licensed separately and never committed: download it from loinc.org and put
`Loinc.csv` and `MapTo.csv` (from the LoincTable folder) in `data/loinc/`.

```bash
python3 -m pip install -r terminology/llm_loinc/requirements.txt    # anthropic, pinned
mkdir -p data/mimic_raw && curl -L -o data/mimic_raw/d_labitems.csv.gz \
  https://physionet.org/files/mimic-iv-demo/2.2/hosp/d_labitems.csv.gz   # MIMIC lab categories, open access, ODbL
python3 terminology/llm_loinc/local_labs.py           # only to rebuild local_labs.csv (needs data/mimic)
python3 terminology/llm_loinc/loinc_index.py build    # data/loinc/index.pkl, a few seconds
```

The API key goes in `analytics/.env` as `ANTHROPIC_API_KEY` (placeholder in `analytics/.env.example`); `LLM_MODEL`
picks the model, default `claude-haiku-5-5`.

```bash
python3 terminology/llm_loinc/eval.py run --dry-run          # calls not cached yet and the estimated cost
curl -L -o data/mimic_raw/D_LABITEMS_mimic3.csv https://physionet.org/files/mimiciii-demo/1.4/D_LABITEMS.csv   # holdout gold
python3 terminology/llm_loinc/eval.py run --max-calls 220    # dev set and holdout; a rerun answers from the cache
python3 terminology/llm_loinc/eval.py spend                  # every API call so far and what it cost
python3 terminology/llm_loinc/suggest.py run --max-calls 500 # the review queue, every row pending
python3 terminology/llm_loinc/suggest.py accept mimic:51007 --reviewer "Your Name"           # take the suggestion
python3 terminology/llm_loinc/suggest.py accept mimic:51007 --reviewer "Your Name" --loinc CODE   # or another code
python3 terminology/llm_loinc/suggest.py reject mimic:51266 --reviewer "Your Name"
python3 terminology/build_maps.py && python3 terminology/conceptmaps.py build   # after accepting
```

`accept` appends the row to `lab_loinc_map.csv` (MIMIC) or `hl7v2/lab_synonyms.csv` (the feed), with a note naming the
model, the reviewer and the date, and records the same in `terminology/lab_loinc_provenance.csv` (source
`llm_reviewed`). The ConceptMap and the dbt seed read the map, so they pick the row up as is.

Results (October 2026, details in `docs/LLM_LOINC_EVAL.md`). The headline is a **holdout** of 316 pairs from the
MIMIC III demo's lab dictionary, which carries a LOINC code per item; prompt v2 and the retrieval code were frozen
before it was built (their hashes are in the doc). On a seeded sample of 200 holdout pairs, top 1 (lenient): lexical
baseline 53%, Claude Haiku 5.5 50%, Claude Sonnet 5.5 66%. Only Sonnet's gain is beyond chance (exact McNemar test,
p = 0.00004). A confidence threshold of 0.9, chosen on the dev set, sends 70% of Sonnet's answers to review, and the
rest are right 75% of the time, against 95% on the dev set, so every suggestion still needs a person. MIMIC III's own
codes are imperfect: they agree with the human map on 36 of 49 shared items. The 82 human mapped pairs are now the
**dev set**: prompt v2 was written on them (93% and 95% there), so those numbers are tuned and not a result to quote.
No suggested code was ever outside the candidates. Everything, both sets, the queue and the stability check, cost
$2.65 in API calls.

This material contains content from LOINC (http://loinc.org). LOINC is copyright Regenstrief Institute, Inc. and the
Logical Observation Identifiers Names and Codes (LOINC) Committee and is available at no cost under the license at
http://loinc.org/license. LOINC is a registered United States trademark of Regenstrief Institute, Inc.
