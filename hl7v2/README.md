# HL7 v2 ingestion (Option B)

Hospitals still move most clinical data as HL7 v2 messages: pipe delimited text from the 1980s.
FHIR usually sits on top as an API layer, so a real pipeline has to translate one into the other.

## Step 1: feed, parser, router

| File | What it does |
|---|---|
| `generate.py` | Writes a realistic, messy HL7 v2.5.1 feed for 25 invented patients at an invented hospital (fixed seed) |
| `hl7.py` | HL7 v2 parser, standard library only: separators read from MSH, components, repetitions, escape sequences |
| `route.py` | Decides what each message is before touching it: process, skip, duplicate, or quarantine, always with a reason |
| `pipeline.py` | Command line: `generate`, `route`, `show`, `load`, `daily`, `backfill`, `reference`, `prune-reports`, `empi-seed` |
| `reference.py` | The invented hospital's master data: organizations, locations and beds, staff, services (step 5) |
| `empi_seed.py` | The EMPI test set: 30 registrations, 20 copies of Synthea patients and 10 hard negatives (step 6) |

The feed is messy on purpose, the way real interfaces are:

* lab tests as free text or local codes (`GLU`, `Blood sugar`, `GLUCOSE, SERUM`), a few already in LOINC
* diagnoses in retired ICD 9, sometimes dotted (`250.00`), sometimes ICD 10, once with no code at all
* values that are not numbers (`<0.01`, `Moderate`)
* acknowledgments, queries and message types this pipeline does not handle
* resends of the same message, and a few broken messages

```bash
python3 hl7v2/pipeline.py generate          # data/hl7v2/feed/*.hl7
python3 hl7v2/pipeline.py route             # summary + data/hl7v2/route_report.json + quarantine/
python3 hl7v2/pipeline.py show data/hl7v2/feed/0001_ADT_A01.hl7
```

Message types handled: `ADT^A01` admit, `ADT^A02` transfer, `ADT^A03` discharge, `ADT^A04` register, `ADT^A08` update,
`ORU^R01` results.

The parser is written out in full to show how the format works. Production systems use a library
(hl7apy, python-hl7) or an interface engine (Mirth Connect, Rhapsody).

## Step 2: map to US Core FHIR and load

`to_fhir.py` replays the routed feed in order into the current state of each patient, visit,
diagnosis and result, then `pipeline.py load` upserts everything into HAPI.

| HL7 v2 | FHIR | Notes |
|---|---|---|
| PID | Patient | MRN as identifier, name, birth date, sex, address, phone. `ADT^A08` replaces demographics |
| PV1 | Encounter | Visit number, class, location, attending, admit time. `ADT^A03` adds the discharge time |
| DG1 | Condition | ICD 9 is upgraded through the server's own `ConceptMap/$translate`. When CMS lists several ICD 10 options, no code is guessed: the Condition keeps ICD 9, gets a note listing the options and a `needs-review` tag |
| OBR/OBX | Observation | Lab names matched to LOINC: LOINC at source, then local code, then a text synonym (`lab_synonyms.csv`). `<0.01` becomes a quantity with a comparator; text stays text; reference range and H/L flags are kept |

Idempotent by design: every id comes from source identifiers (MRN, visit number, order number and
result position), and resources are written with PUT by id. Running `load` twice changes nothing.

```bash
python3 hl7v2/pipeline.py load --dry-run    # map only, write data/hl7v2/fhir_resources.ndjson
python3 hl7v2/pipeline.py load              # map and upsert into http://localhost:8080/fhir
```

The run prints how lab names were matched, which names are still unmapped, what happened to every
diagnosis (upgraded, needs review, no ICD 10 match), and how many values were text or had a comparator.
Details go to `data/hl7v2/load_report.json`.

## Step 3: the run report

`load` also publishes its run report to the FHIR server as `Binary/hl7v2-ingestion-report`: routing by message type,
the quarantine with reasons, LOINC matching, diagnosis outcomes and every diagnosis waiting for review (by visit
number, no patient details). The BI export reads the same counts for the Pipeline health dashboard.

## Step 4: a live feed (demo)

A real interface engine never stops: messages keep arriving. `pipeline.py daily` makes the invented
Riverside General feed do the same, one day at a time on real dates (UTC), next to the static MIMIC and
Synthea archives. All patients are invented; MRNs start at `RV200000` (the original 25 keep `RV100200` to
`RV100368`).

| Piece | What it does |
|---|---|
| `feed_state.py` | The feed's memory, saved as `data/hl7v2/feed_state.json`: the patient roster, who is in hospital, past discharges, control IDs seen in the last 30 days, the last generated day |
| `generate.build_day()` | One day of traffic from that state, seeded with `seed + day`, so the same day always gives the same messages |
| `Router(seen=...)` | Starts with yesterday's control IDs, so a resend is caught as a duplicate across days |

A day holds 2 to 5 admissions (`ADT^A01`, about 30% returning patients, so some are readmissions within 30
days; PV1-4 admission type about 60% emergency), 1 to 3 emergency visits that go home the same day
(`ADT^A04` + `ADT^A03`, PV1-2 `E`), lab results for everyone in hospital (a steady creatinine rise for some,
which feeds the kidney signal), discharges after 2 to 9 days, a few address updates (`ADT^A08`), and the same
noise as the original feed: ACKs, queries, an unsupported type, about 3% resends and now and then a broken
message. Ids carry the date (`RVG<yyyymmdd><seq>`, `V<yyyymmdd><nn>`, `L<yyyymmdd><nnn>`), so they never repeat.

PV1-2 maps to `Encounter.class` (`IMP`, `EMER`) and PV1-4 to `Encounter.priority` (v3 ActPriority `EM`, `UR`, `EL`).

```bash
python3 hl7v2/pipeline.py daily --dry-run          # today: data/hl7v2/daily/<day>/, nothing loaded, state unchanged
python3 hl7v2/pipeline.py daily                    # today into http://localhost:8080/fhir
python3 hl7v2/pipeline.py daily --day yesterday    # the previous UTC day: every event is in the past
python3 hl7v2/pipeline.py daily --day 2026-10-01   # a given day (must come after the last generated day)
python3 hl7v2/pipeline.py backfill --days 56       # every day of the 56 ending yesterday not generated yet, oldest first
```

Each run writes the day's `.hl7` files, `fhir_resources.ndjson` and `load_report.json` to `data/hl7v2/daily/<day>/`,
replaces `Binary/hl7v2-ingestion-report` with the day's report, saves the same report as
`Binary/hl7v2-ingestion-report-<yyyymmdd>`, and adds a record to `Binary/pipeline-run-history`
(JSON list, newest first, last 400 runs): run id, day, start, finish, duration, status (`success`, `partial`,
`failed`), message and routing counts, resources loaded, new and failed, LOINC coverage, admissions, discharges,
ED visits and patients in hospital.

Safe to rerun: the state is saved only after a successful load, so a failed day is generated again with the
same messages and loaded by id (`0 new`). Running a day that is already loaded routes its saved files again
(every clinical message is a duplicate) and loads nothing. The nightly Airflow run loads the previous UTC
day, like a real nightly batch, so every event it loads is in the past (see `orchestration/airflow/README.md`).
A manual `daily` for today also generates events later in the day than the run.

Day reports are kept for the newest 30 days of the run history; the nightly job deletes older ones, talking to HAPI
directly on localhost:

```bash
python3 hl7v2/pipeline.py prune-reports --keep 30 [--base URL]   # a second run deletes nothing
```

The run history backs four service level objectives (feed fresh within 36 hours, at least 95% of runs succeed over 7 days, under 5% of
messages quarantined, at least 90% of lab results coded to LOINC).

## Step 5: who and where (the FHIR Administration module, demo)

A real hospital's systems also know which bed a patient is in, who their doctor is, who else is on their care team,
which lab ran a test and which service follows them up. `reference.py` holds that master data for the invented
Riverside General Hospital, every resource tagged as invented demo data (`riverside-reference`):

| Resource | What |
|---|---|
| Organization (5) | Riverside General Hospital, Riverside Reference Laboratory (runs the lab tests), 3 invented payers |
| Location (112) | Site > units (ED 12 beds, MED 24, CARD 16, ICU 10) > rooms > beds, with `partOf` and `physicalType`; clinics. Bed ids are stable (`riverside-loc-med-214-a`); beds carry `operationalStatus` O or U |
| Practitioner, PractitionerRole (11 each) | Hospitalists, an emergency physician, a cardiologist, an intensivist, nurses and RN care managers, invented names, `DEMO-` staff numbers. Dr. Dana Morgan and Jordan Lee are the demo clinician and care manager |
| HealthcareService (5) | Emergency Department, Inpatient Medicine, Cardiology Clinic, Discharge Follow Up Clinic, Primary Care |

```bash
python3 hl7v2/pipeline.py reference [--base URL]   # PUT by id: the second run reports 0 new
```

`daily`, `backfill` and `load` load it first when the server does not have it yet (they check a few marker resources),
and record a failed run in the history if that fails. Bed status follows the feed: each daily run updates the beds
whose occupancy changed.

The live feed now uses it (the original 25 patient feed is unchanged, byte for byte):

* PV1-7 attending comes from the staff list: hospitalists on the medical unit, the intensivist in the ICU, the
  emergency physician for ED visits.
* Every stay holds a real bed (PV1-3 `unit^room^bed`) and two stays never share a bed (a bed board per day).
* `ADT^A02` transfers, with the old place in PV1-6: about half of the emergency admissions wait in an ED bay and then
  move up to their unit; some stays go to the ICU on a later day and come back. Stays from before beds existed move
  to a real bed with an A02 on their next day.
* ROL segments: the primary nurse on every ADT message, a care manager on the inpatient discharge.
* OBX-23 on lab results names the performing lab (`Riverside Reference Laboratory^^^^^^^^^RRL`).

| HL7 v2 | FHIR |
|---|---|
| PV1-3 | `Encounter.location`: a unit entry and a bed entry, each with `status` and `period`. Only an A02 moves a patient: it completes the current entries and adds new ones. Places that are not beds of the hospital (the original feed) stay display text |
| PV1-7 | `Encounter.participant` type ATND, pointing at the Practitioner |
| PV1-7, ROL | `CareTeam` per visit (attending, primary nurse, care manager from discharge), members are PractitionerRoles, linked with `CareTeam.encounter` (R4 has no `Encounter.careTeam`). The care manager stays on after discharge for the follow up; their role names the Discharge Follow Up Clinic |
| OBX-23 | `Observation.performer`, the lab Organization |

A visit can last several days, and each day's run maps only that day's messages. So before mapping, `daily` reads the
Encounter and CareTeam of every visit still open from the server and merges the new messages into them, the way an
interface engine keeps its own state. MIMIC and the original feed send none of this, so their visits have no beds or care team.

## Step 6: the EMPI test set (demo)

`pipeline.py empi-seed` writes 30 `ADT^A04` registrations (MSH, EVN, PID, no visit) to `data/empi/feed/`, routes and
maps them like any other message, and loads one Patient each (PUT by id, tagged `empi-test-set`; a Patient.link the
matcher added is kept on a rerun). 20 are copies of Synthea patients with one realistic variation each (one has two: a
move and a new phone), 10 are hard negatives with the same first and last name as a Synthea patient. MRNs `RV300001` to
`RV300030` are shuffled, so they do not give away which is which. Fixed seed: the same Synthea patients always give the
same messages. The ground truth goes to `data/empi/truth.json` and `Binary/empi-truth`.

```bash
python3 hl7v2/pipeline.py empi-seed [--base URL] [--dry-run]   # a second run reports 0 new
python3 hl7v2/pipeline.py empi-seed --seed 7                   # a held out set, see below
python3 empi/match.py run                                      # then match (see the main README, Patient matching)
```

Any seed other than 42 writes a held out set for `empi/match.py evaluate --truth FILE`: its own MRNs (from `RV310001`
up, seed 7 gives `RV310701` to `RV310730`), its own control IDs, none of the Synthea patients the tuning set uses, files
in `data/empi/feed-seed7/` and `data/empi/truth-seed7.json`. Its truth is not published: `Binary/empi-truth`, which
the warehouse and `/empi` read, stays the tuning set. Load one held out set at a time; two would share people.

The daily feed and the original 25 patient feed do not change.
