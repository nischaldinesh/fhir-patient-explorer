"""Helpers for the fhir_nightly DAG: the CLI commands each task runs, the summaries pushed to XCom, and the alerts.
Kept apart from the DAG file so tests can import them without Airflow, and listed in .airflowignore."""
