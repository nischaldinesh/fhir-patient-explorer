# Tableau Public dashboards (demo)

Four dashboards to build by hand in Tableau Public on the Google Sheet that `analytics/export_bi.py` fills every
night. The sheet holds only suppressed aggregates and pipeline metadata: no identifiers, no count of people from 1 to
10. The data is demo data: the
MIMIC IV demo (deidentified, dates shifted into the 2100s), Synthea (synthetic) and the invented Riverside General feed.
Every dashboard says so in its subtitle. None of this is a real hospital.

## The sheet

| Tab | One row per | Fields |
|---|---|---|
| `bi_ops_daily` | day and source (live feed) | `period`, `source`, `admissions`, `ed_visits`, `bed_days`, `is_suppressed` |
| `bi_ops_weekly` | ISO week (Monday) and source | the same, plus `is_complete_period` |
| `bi_ops_monthly` | month and source | the same, plus `is_complete_period` |
| `bi_occupancy_monthly` | month and unit | `period`, `unit_name`, `bed_occupancy_rate` (0 to 1), `is_suppressed`, `is_complete_period` |
| `bi_quality` | origin, period, measure and source | `origin` (`quality_mart` or `semantic_layer`), `period_type` (`all_time`, `year`), `period_start`, `measure_id`, `measure_name`, `source` (`all`, `hl7v2`, `mimic`, `synthea`), `unit`, `numerator`, `denominator`, `rate`, `value`, `is_suppressed`, `suppression_reason` |
| `bi_population` | sex, age band and source | `sex`, `age_band`, `source` (`all`, or the sources a person has records in), `people`, `is_suppressed` |
| `bi_suppression` | quality mart grain | `mart`, `grain`, `grain_order`, `pct_suppressed` (0 to 100) |
| `bi_pipeline_runs` | warehouse build (`build.py build`), oldest first | `build_id`, `build_date`, `started_at`, `finished_at`, `status` (`success`, `failed`, `error`), `duration_s`, `tests_pass`, `tests_warn`, `tests_fail`, `tests_error`, `tests_skipped`, `models_built`, `extract_complete`, `landing_age_hours`, `rows_bronze_resources`, `rows_silver_observation`, `rows_fact_encounter`, `rows_fact_lab_result`, `git_commit` |
| `bi_feed_runs` | feed day (the newest run of the day) | `day`, `status` (`success`, `partial`, `failed`), `duration_s`, `messages`, `processed`, `skipped`, `duplicates`, `errors`, `resources_loaded`, `load_failed`, `pct_loinc` (0 to 100) |
| `bi_data_quality` | check, measure, scope and band | `check_group` (`coding`, `empi`, `terminology_queue`, `freshness`, `completeness`), `measure`, `scope` (a source, a resource type, `synthetic test set`, `live feed`, `landing`, `all`), `band` (confidence band of the review queue, else `all`), `value`, `numerator`, `denominator`, `measure_unit` (`percent`, `ratio`, `hours`, `lab codes`, `flag`, `date`), `as_of`, `label` |
| `bi_meta` | item | `item`, `value`: `data_as_of`, `exported_at`, `snapshot_version`, rows per tab, cells hidden by each check, builds in the log, the rule, the demo notice |

A hidden cell is empty, with `is_suppressed` true. Empty never means zero: a zero is written as 0.

The three pipeline tabs describe how the pipeline ran, not who was treated. Their counts are of messages, resources,
table rows, dbt tests, diagnosis records and lab codes, so small values such as 2 errors are shown as they are: the 1 to
10 rule is for counts of people. They never carry a patient event (admissions, discharges, emergency visits, transfers)
or a split by sex, age or unit. In `bi_pipeline_runs` and `bi_feed_runs` an empty cell means the value was not
recorded (for example the test counts of a build that stopped before dbt ran), not hidden.

## Color plan

Colorblind safe (Tableau 10), plus the three status colors of Dashboard 4, which always come with a text label (Success,
Warning, Failed) so color is never the only signal. Keep each meaning on one color in all four dashboards.

| Meaning | Color | Hex |
|---|---|---|
| Live feed, `hl7v2` (Riverside General) | blue | `#4E79A7` |
| MIMIC, `mimic` | orange | `#F28E2B` |
| Synthea, `synthea` | green | `#59A14F` |
| All sources, `all` | dark gray | `#4D4D4D` |
| Hidden cell (any chart) | light gray, label "hidden" | `#C8C8C8` |
| Admissions | blue | `#4E79A7` |
| Emergency visits | teal | `#76B7B2` |
| Occupied bed days | purple | `#B07AA1` |
| Units: Medical, Cardiac, Intensive Care, Emergency Department | `#4E79A7`, `#E15759`, `#EDC948`, `#76B7B2` | |
| Female, male | `#B07AA1`, `#9C755F` | |
| Status: success, pass, met | green | `#2E7D32` |
| Status: warning, partial, warn | amber | `#F9A825` |
| Status: failed, error, fail | red | `#C62828` |
| Status: skipped, not recorded | light gray | `#C8C8C8` |
| Feed routing: processed, skipped, duplicates, errors | `#4E79A7`, `#BAB0AC`, `#EDC948`, `#C62828` | |
| Review queue bands: high, medium, low, no suggestion | `#2E7D32`, `#F9A825`, `#E15759`, `#C8C8C8` | |

Text: titles `#222222`, captions `#555555`, 9 pt. White background, no gridlines except the zero line.

## Calculated fields (create once per data source)

| Name | Formula | Use |
|---|---|---|
| `Hidden mark` | `IF [is_suppressed] THEN 0 END` | a gray dot at zero with the label "hidden" where a value is hidden |
| `Hidden label` | `IF [is_suppressed] THEN "hidden" END` | its label |
| `Rate shown` | `IF NOT [is_suppressed] THEN [rate] END` | rates on charts |
| `Rolling 4 weeks admissions` | see below | the rolling line |

For a tab with several count columns, make the hidden mark per column, for example
`IF ISNULL([admissions]) THEN 0 END`, because `is_suppressed` is true when any column of the row is hidden.

## Why "rolling 4 weeks" and not the 28 day metric

The semantic layer has `admissions_rolling_28d`, but it is not exported. Daily admissions are 1 to 5, so every day is
hidden. A rolling count published for each day would give them back: the value of one day minus the value of the day
before is that day's admissions minus the admissions 28 days earlier, and for the first 28 days of the feed it is that
day's admissions exactly. The dashboard instead adds up four weeks of `bi_ops_weekly` in Tableau. Those weeks are
already published, so the line tells a reader nothing new.

The sum must never be partial. If one week in the window is hidden, a sum of the other three would be too low, and the
four week sum minus the three shown weeks would give back the hidden one. So the line is empty (a gap) whenever any
week in its window is hidden or is the current, unfinished week:

```
// Rolling 4 weeks admissions, on bi_ops_weekly; table calculation along period (Week), compute using period
IF INDEX() >= 4
   AND WINDOW_SUM(IIF(ISNULL(SUM([admissions])) OR NOT MIN([is_complete_period]), 1, 0), -3, 0) = 0
THEN WINDOW_SUM(SUM([admissions]), -3, 0)
END
```

## Dashboard 1: Operations (live feed)

* **Title:** Riverside General operations
* **Subtitle:** Invented hospital, demo data. Live feed only, data as of `<data_as_of>`.
* **Size:** 1200 by 900, fixed.
* **Filters (apply to every worksheet on this dashboard):** `period` as a date range (relative to the latest);
  `is_complete_period` (default: all, the current week and month drawn with a dashed line).

| Worksheet | Tab | Chart | Columns, rows, marks |
|---|---|---|---|
| Admissions and emergency visits per week | `bi_ops_weekly` | line with circle markers | Columns: `period` (exact date, continuous, Week). Rows: Measure Values `admissions`, `ed_visits`; dual axis with `Hidden mark` as a gray circle labeled "hidden". Line pattern: dashed when `is_complete_period` is false |
| Rolling 4 weeks of admissions | `bi_ops_weekly` | line | Columns: `period` (Week). Rows: `Rolling 4 weeks admissions`. Gaps stay gaps (Format, Pane, Special values: Show at default value off) |
| Daily activity | `bi_ops_daily` | highlight table | Columns: `period` (Day). Rows: Measure Names `admissions`, `ed_visits`, `bed_days`. Color: Measure Values, sequential purple; hidden cells gray with the text "hidden" |
| Occupied bed days per day | `bi_ops_daily` | bars | Columns: `period` (Day). Rows: `bed_days`. Hidden days: gray dot at zero |
| Bed occupancy by unit, per month | `bi_occupancy_monthly` | side by side bars | Columns: `period` (Month), `unit_name`. Rows: `Rate shown` as a percentage, axis fixed 0 to 100%. Color: unit |
| Admissions and emergency visits per month | `bi_ops_monthly` | text table | Rows: `period` (Month). Columns: `admissions`, `ed_visits`, `bed_days`; empty cells show "hidden" |

**Captions:**

* Weekly: "Admissions and emergency visits that went home, per week. A gray dot means the week is hidden (fewer than
  11, or it would let a hidden week or day be worked out). The current week is dashed: it is not over."
* Rolling: "Admissions in the four weeks ending each week, added up from the weekly counts. The line has a gap when
  any of the four weeks is hidden or not over yet; it is never a partial sum."
* Daily: "Every day has fewer than 11 admissions and emergency visits, so the daily counts are all hidden. Occupied
  bed days show from the first day with 11 or more."
* Occupancy: "Occupied beds at midnight out of beds available, per unit. The Emergency Department shows 0: nobody
  stays in an emergency bay at midnight in this feed."

## Dashboard 2: Quality

* **Title:** Quality measures by source
* **Subtitle:** Demo data: MIMIC IV demo (deidentified, shifted dates), Synthea (synthetic), Riverside General
  (invented). All time. Not risk adjusted, not certified.
* **Size:** 1200 by 800, fixed.
* **Data source filter:** `period_type` = `all_time`. **Filters:** `origin` (single value, default `quality_mart`),
  `source` (multiple values, all).

| Worksheet | Tab | Chart | Columns, rows, marks |
|---|---|---|---|
| 30 day readmission rate by source | `bi_quality`, `measure_id` = `readmission_30d` | dot plot | Rows: `source` (order: all, hl7v2, mimic, synthea). Columns: `Rate shown` as a percentage. Color: source; hidden: `Hidden mark` gray dot at zero, label "hidden". Tooltip: numerator of denominator stays |
| Average length of stay by source | `bi_quality`, `measure_id` = `los_mean_days` | dot plot | Rows: `source`. Columns: `value` (days). Hidden the same way. Tooltip: `denominator` stays |
| Every measure, all time | `bi_quality` | highlight table | Rows: `measure_name`. Columns: `source`. Text: `value` (percent for proportions, days, hours). Color: gray `#C8C8C8` where `is_suppressed`, else white; text "hidden" in gray cells |
| Why a cell is hidden | `bi_quality` | text table | Rows: `measure_name`, `source`, `suppression_reason`; filter `is_suppressed` true |

**Caption (under the charts):** "A gray dot or cell is hidden, not zero. Under the CMS cell size rule a count from 1
to 10 is not shown, and neither is a rate whose readmissions, stays or stays without a readmission are 1 to 10
(small cell). Some shown looking cells are hidden too, because subtracting the shown cells from the all sources total
would give a small one back (complementary). The semantic layer's numbers are shown only where the quality mart shows
the same cell, and the two must agree."

**Trends:** the quality tab also has `year` rows, but trends exist only for the live feed. The archive spans many
decades (Synthea back to 1943, MIMIC shifted across 2110 to 2201) with a few stays per year, so almost every year is
hidden and MIMIC years do not line up in time. If a year view is added, filter it to `source` = `hl7v2` or `all` and
`period_start` from 2026.

## Dashboard 3: Population and data quality

* **Title:** Who is in the data, and how much is hidden
* **Subtitle:** Demo data. People counted once across linked records. Data as of `<data_as_of>`.
* **Size:** 1200 by 800, fixed.
* **Filters:** `sex` (all), `source` on the by source worksheet only.

| Worksheet | Tab | Chart | Columns, rows, marks |
|---|---|---|---|
| People by age band and sex | `bi_population`, `source` = `all` | grouped bars | Rows: `age_band` (manual order: 0-17, 10s, 20s to 80s, 90+, unknown). Columns: `people`. Color: `sex`. Hidden: gray dot at zero, label "hidden" |
| People by source | `bi_population`, `source` other than `all` | highlight table | Rows: `age_band`. Columns: `source`, `sex`. Text: `people`; gray "hidden" cells |
| Share of quality cells hidden | `bi_suppression` | horizontal bars | Rows: `grain` sorted by `grain_order`. Columns: `pct_suppressed` (0 to 100). One color, dark gray. Label: the percent |
| Data as of | `bi_meta`, `item` in `data_as_of`, `exported_at`, `snapshot_version` | text | Rows: `item`, `value` |
| Demo notice | `bi_meta`, `item` = `demo_notice` | text | `value` |

**Captions:**

* Age band and sex: "One person per row of the patient dimension, records the matcher linked counted once. Unknown
  age: MIMIC shifts dates, so its ages are not shown. Ages 90 and over are one band (HIPAA Safe Harbor)."
* By source: "Hidden cells: fewer than 11 people, or a cell that would let a hidden one be worked out from the all
  sources total or the other columns."
* Share hidden: "Most monthly, quarterly and yearly quality cells are hidden because the demo has few stays per period.
  All time cells are mostly shown."

## Dashboard 4: Pipeline health

For the data team: is the pipeline running, is the data complete, fresh and well coded.

* **Title:** Pipeline health
* **Subtitle:** Demo pipeline on demo data (MIMIC IV demo, Synthea, the invented Riverside General feed). Builds of the
  warehouse and runs of the feed, as of `<as_of>`.
* **Size:** 1200 by 900, fixed. KPI row on top, two charts per row below.
* **Filters:** `build_date` (range, last 30 days by default) on the build charts, `day` (range, last 30 days) on the
  feed chart.

Calculated fields on `bi_pipeline_runs`:

| Name | Formula | Use |
|---|---|---|
| `Is last build` | `[started_at] = {FIXED : MAX([started_at])}` | filter the KPI tiles to the newest build |
| `Status label` | `CASE [status] WHEN "success" THEN "Success" WHEN "failed" THEN "Failed" ELSE "Error" END` | tile text and color |
| `Tests not passed` | `ZN([tests_warn]) + ZN([tests_fail]) + ZN([tests_error])` | tile color |
| `Tests total` | `ZN([tests_pass]) + [Tests not passed] + ZN([tests_skipped])` | tile text |
| `Age status` | `IF ISNULL([landing_age_hours]) THEN "Unknown" ELSEIF [landing_age_hours] <= 36 THEN "Fresh" ELSEIF [landing_age_hours] <= 72 THEN "Warning" ELSE "Stale" END` | tile color |
| `Build minutes` | `[duration_s] / 60` | the duration line |

KPI tiles (one text worksheet each, filtered to `Is last build` true):

| Tile | Field | Color rule | Caption |
|---|---|---|---|
| Last build | `Status label`, with `finished_at` and `git_commit` below in gray | Success green `#2E7D32`, Failed or Error red `#C62828` | "The newest run of the warehouse build (dbt seed and build). Failed: a model or test did not pass. Error: dbt could not run." |
| Tests passed | `tests_pass` of `Tests total` | green when `Tests not passed` is 0, amber `#F9A825` when only warnings, red when any fail or error | "dbt data tests of the newest build. Tests have severity warn, so a failing test shows as a warning and the build still finishes." |
| Data age | `landing_age_hours`, one decimal, "hours" | `Age status`: Fresh green, Warning amber (over 36), Stale red (over 72) | "Hours from the newest record the FHIR server changed (Patient, Encounter, Observation; the stalest of them) to the end of the newest build. The same rule as dbt source freshness and the app's 36 hour warning." |
| Extract complete | `extract_complete`: "Yes" or "No" | Yes green, No red | "Every resource type extracted equals the total the FHIR server reported. No means a page was lost or the server changed during the extract." |

| Worksheet | Tab | Chart | Columns, rows, marks |
|---|---|---|---|
| Build duration | `bi_pipeline_runs` | line with dots | Columns: `started_at` as exact date and time (continuous). Rows: `Build minutes`. Dot color: `Status label` (green, red). Tooltip: `build_id`, `status`, `models_built`, `git_commit` |
| Test results per build | `bi_pipeline_runs` | stacked bars | Columns: `build_id` (discrete, sorted by `started_at`). Rows: Measure Values `tests_pass`, `tests_warn`, `tests_fail`, `tests_error`, `tests_skipped`. Color: pass green, warn amber, fail and error red, skipped gray. Label: totals on top |
| Feed routing per day | `bi_feed_runs` | stacked bars | Columns: `day` (exact date, discrete). Rows: Measure Values `processed`, `skipped`, `duplicates`, `errors`. Color: processed blue `#4E79A7`, skipped gray `#BAB0AC`, duplicates yellow `#EDC948`, errors red `#C62828`. Shape on top: a red triangle where `status` is not `success`. Tooltip: `messages`, `resources_loaded`, `load_failed`, `duration_s`, `pct_loinc` |
| LOINC coverage by source | `bi_data_quality`, `check_group` = `coding`, `measure` = `loinc_coverage` | horizontal bars | Rows: `scope` (hl7v2, mimic, synthea). Columns: `value` (0 to 100). Color: source colors of the color plan. Reference line at 90 ("SLO 90%"). Label: `value` percent and `numerator` of `denominator` |
| Review queue by confidence | `bi_data_quality`, `check_group` = `terminology_queue` | stacked bar | Rows: `measure` (`review_pending`, `review_accepted`, `review_rejected` as present). Columns: `numerator`. Color: `band` (high green, medium amber, low red, no suggestion gray), order high, medium, low, no suggestion. Label: `numerator` |
| Data quality details | `bi_data_quality`, `check_group` in `coding`, `empi`, `completeness` | text table | Rows: `check_group`, `measure`, `scope`. Text: `value` with `measure_unit`, and `numerator` of `denominator` where present. Tooltip: `label` |

**Captions:**

* Build duration: "Minutes from the start to the end of each warehouse build. A jump usually means more data or a
  full refresh of the lab results fact. A red dot is a build that did not pass."
* Test results: "Every dbt data test of each build: unique keys, no orphans, accepted values and the business rules
  of the warehouse. All green is the goal. Gray means the test was skipped because something it depends on failed."
* Feed routing: "Messages of the invented Riverside General feed for each day, by what the router did: processed into
  FHIR, skipped (acknowledgments, queries, types not handled), duplicates (resends) and errors (broken messages, kept
  in quarantine). A red triangle marks a day whose run did not succeed. These are counts of messages, not of people."
* LOINC coverage: "Share of numeric lab results with a LOINC code, per source. The rest wait for a mapping; the
  service level objective is 90 percent."
* Review queue: "Lab codes that the LLM assisted mapper suggested a LOINC code for, by the model's confidence. High
  is 0.9 and above, the threshold chosen in the evaluation; every suggestion still needs a person to accept it."
* Details: "Coding coverage per source, the patient matcher's precision and recall on its synthetic test set (demo,
  rates only), and extracted out of server total per resource type."

**Honest limits:** the build log starts with the first build after this change; earlier builds are not in it. A build
on another machine (for example local Airflow) writes its own log next to its own warehouse; this sheet shows the one
whose export it is. Data age is the one measured at the end of the newest build, so it does not grow between builds:
for the age now, use `NOW()` in Tableau against `finished_at` plus `landing_age_hours`.

## Connect Tableau Public to the sheet, with Keep data in sync

1. Share the sheet with your own Google account as Viewer (the service account is the Editor that writes it). Keep it
   private: do not publish it to the web or share it by link.
2. Run the export once (`python3 analytics/export_bi.py export --target sheets`) so every tab exists.
3. Open Tableau Public (the desktop app). Connect, To a Server, Google Sheets. Sign in with your Google account and
   allow access. Pick the sheet.
4. Make one data source per tab (drag one tab to the canvas; for the next tab, Data, New Data Source, the same
   sheet). The tabs are not joined: each worksheet above names its tab.
5. On each data source page, set the types: `period`, `period_start`, `build_date`, `day` and `as_of` to Date;
   `started_at` and `finished_at` to Date and Time; counts to Number (whole); `rate`, `bed_occupancy_rate`, `value`,
   `duration_s`, `landing_age_hours` and `pct_loinc` to Number (decimal); the `is_` fields and `extract_complete` to
   Boolean. The export writes values
   raw, so Sheets does not change them, but dates arrive as text.
6. Build the worksheets and dashboards above, with the colors and captions.
7. File, Save to Tableau Public. In the save dialog, check **Keep this data in sync with Google Sheets** (Tableau
   Public asks to keep your Google credentials for that). If you missed it, open the viz on your Tableau Public
   profile, Settings, and turn on Keep data in sync.
8. Check the viz page: it shows when the data was last refreshed, and **Request update** fetches the sheet now.

Tableau Public syncs about once a day at a time it picks, and the export runs after the nightly job, so the dashboard
can be a day behind the sheet. The data as of date on Dashboard 3 and the as of date on Dashboard 4 always say which
day they show.

## Limits

* Suppression protects exact sums of hidden cells: inside each tab, between days, weeks and months, between the units
  and the month, between the sources and all sources, and between the quality mart and the semantic layer. It does
  not stop a reader who knows outside facts (that a given person was there on a given day).
* The metrics API (`api/`) and this sheet are separate channels. Each is suppressed on its own, so a reader with both
  could in theory subtract one from the other (for example an API query by a dimension the sheet does not use). The
  API runs only on 127.0.0.1 and needs a key, which is kept to people under a data use agreement.
* Anyone can download the data behind a Tableau Public viz. That is acceptable here only because the sheet holds
  suppressed aggregates and nothing else.
* Rates and means carry no risk adjustment; the diabetes measure is HEDIS style, not certified. See
  `analytics/README.md` for the definitions.
* Capacity is the beds known now; the reference data keeps no history of beds.
