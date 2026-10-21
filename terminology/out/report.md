# Terminology mapping report

## Labs: MIMIC local codes to LOINC
- Excluded 4 codes (4,933 results) that are not patient measurements
- Curated table maps 54 of 494 remaining MIMIC lab codes
- Those cover **82,227 of 102,794 lab results (80.0%)**
- Display mismatches to check: 0

## Diagnoses: ICD 9 to ICD 10 (CMS GEMs 2018)
- 784 distinct ICD 9 codes in 2,442 diagnosis rows
- Mapped automatically (exact or approximate): **1,838 rows (75.3%)**

| Category | Codes | Rows |
|---|---|---|
| exact | 269 | 904 |
| approximate | 333 | 934 |
| one_to_many | 154 | 560 |
| combination | 5 | 11 |
| no_map | 23 | 33 |
| not_in_gem | 0 | 0 |
