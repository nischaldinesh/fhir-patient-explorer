# Patient matching (EMPI, demo)

A hospital gets the same person from several systems, each with its own id and its own spelling. An Enterprise Master
Patient Index (EMPI) finds those records and links them. `empi/match.py` (standard library only) does it for every
Patient on the server:

* **Blocking.** Only pairs that share birth year plus the Soundex of the last name, or the exact birth date, are compared.
  MIMIC records are deidentified (no name, shifted dates), so they are skipped, as they should be.
* **Scoring.** A weighted comparison per field gives 0 to 100 and a reason for each field: last name and first name
  (Jaro Winkler, Soundex, a nickname table `empi/nicknames.csv` for first names), middle name, birth date (exact, or two
  digits or day and month swapped), sex, phone, ZIP code and street address. A field missing on one side counts neither
  way. Weights and thresholds are in `empi/config.json`.
* **Deciding.** 90 and above: linked automatically. 70 to 89: a person reviews the pair. Under 70: not the same person.
* **Linking.** Both Patients get `Patient.link` (type `seealso`) pointing at each other, written to HAPI directly on
  localhost (the matcher refuses any other host). Records are never merged or deleted, and a pair already linked is
  left alone, so a second run updates nothing.

The test set (demo): `python3 hl7v2/pipeline.py empi-seed` sends 30 fixed, seeded `ADT^A04` registrations to the
invented Riverside General (MRNs `RV300001` to `RV300030`, tagged `empi-test-set`): 20 copies of Synthea patients with
a nickname, a typo in the last name, no middle name, a new address, two birth date digits swapped, a new phone, or a move
and a new phone together; and 10 hard negatives, other people with the same first and last name (born the same year
elsewhere, the same year in the same ZIP code, or the same day but of the other sex). The ground truth goes to
`data/empi/truth.json` and `Binary/empi-truth`. The daily feed and the original 198 message feed are unchanged.

```bash
python3 hl7v2/pipeline.py empi-seed                            # the test set and its ground truth
python3 empi/match.py run                                      # score, link, publish Binary/empi-review
python3 empi/match.py decide pair-4f0127c541 --accept          # or --reject; an accepted pair is linked
python3 hl7v2/pipeline.py empi-seed --seed 7                   # a held out set: data/empi/truth-seed7.json
python3 empi/match.py run && python3 empi/match.py evaluate --truth data/empi/truth-seed7.json
```

The nightly Airflow run calls `match.py run` after the feed and before the extract, so a link the feed's rewrite of a
Patient dropped is back the same night; a matcher failure fails the run and sends the alert (see
`orchestration/airflow/README.md`).

Measured on this repo's data (448 patients plus the 30 test records, October 2026): 71 candidate pairs, **precision
1.0** (19 of 19 automatic links are true matches), **recall 0.95** (19 of 20 true matches linked automatically),
**0 of 10 hard negatives linked automatically**, 3 pairs in review: the person who moved and changed phone (82.0) and
the two people with the same name and birthday but the other sex (77.1 and 74.0). A second run creates no new links.

| Set | Records | Precision | Recall | Hard negatives auto linked | In review |
|---|---|---|---|---|---|
| Tuning set, seed 42 (the weights were chosen with it in view) | 20 matches, 10 hard negatives | 1.0 (19 of 19) | 0.95 (19 of 20) | 0 of 10 | 3 |
| Held out set, seed 7 (none of the tuning set's people) | 20 matches, 10 hard negatives | 1.0 (19 of 19) | 0.95 (19 of 20) | 0 of 10 | 3 |

The held out set is drawn from the 77 Synthea patients the tuning set does not use, so it tests people the weights
never saw; it comes from the same generator, so it does not test kinds of variation the generator does not make.
In both sets the one true match left for review is the person who moved and changed phone (82.0).
Honest limits: the test set is small and synthetic, and the weights were chosen with it in view; a new address or a new
phone alone scores 90.6 to 91, just above the threshold; blocking compares birth years, so a swapped digit in the year
is never even compared (the test set only swaps month and day digits); the live feed rewrites its Patients on every
update, which drops a link until the next `run` (every night, in the Airflow run) restores it.

## In the warehouse

`gold_patient_crosswalk` gives one master id per person across sources, and `gold_empi_quality` recomputes precision
and recall against the truth (see `analytics/README.md`). Every count in the marts is one person per master id, not
one record. Decisions on pairs in review are made on the command line only (`match.py decide`).
