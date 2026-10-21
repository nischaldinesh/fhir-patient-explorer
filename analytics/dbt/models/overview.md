{% docs __overview__ %}
# FHIR Patient Explorer: dbt warehouse

The warehouse of an end to end healthcare data pipeline
([repository](https://github.com/nischaldinesh/fhir-patient-explorer)). Every night, data from an HL7 v2 feed, MIMIC IV
and Synthea is copied out of a HAPI FHIR server as NDJSON and built here with dbt on DuckDB.

**Layers**

* **bronze**: the raw FHIR JSON exactly as landed, so any build can be replayed.
* **silver**: typed, coded and deduplicated, one row per thing.
* **gold**: clinical logic: daily census, bed board, weekly metrics, LACE readmission risk, a patient crosswalk.
* **governance**: pseudonymized analyst views (salted keys, no names or identifiers).
* **marts**: a star schema (7 dimensions, 5 facts) and hospital quality measures with small cell suppression.
* **semantic**: MetricFlow semantic models, 7 metrics and 2 saved queries.

Every build runs 406 data tests; every mart has an enforced contract.

**Start here**

* Open the lineage graph with the button at the bottom right, or pick a model on the left.
* Good first models: `fact_encounter`, `dim_patient`, `mart_quality_period`, `gold_readmission_risk`.
* Exposures list the Tableau dashboards and the marts each one depends on.

Column types are not shown: this site is built from the project files only, without connecting to the warehouse.
All data in the project is deidentified (MIMIC IV demo) or synthetic.
{% enddocs %}
