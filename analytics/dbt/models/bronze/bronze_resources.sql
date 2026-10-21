-- Bronze: every FHIR resource exactly as extracted, plus lineage. Nothing is transformed here.
-- The landing folder and the extract time come from the vars landing_path and loaded_at (set by build.py).
{{ bronze_select() }}
