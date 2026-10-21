#!/usr/bin/env bash
# Entry point for the two Airflow containers.
#   start.sh api-server   the UI and REST API
#   start.sh scheduler    migrate the metadata database, then the scheduler (LocalExecutor runs the tasks here),
#                         the DAG processor and the triggerer, in one container
set -euo pipefail

case "${1:-}" in
  api-server)
    exec airflow api-server --port 8080
    ;;
  scheduler)
    airflow db migrate
    # one slot: two runs (a scheduled run and a backfill) never run steps at the same time
    airflow pools set fhir_pipeline 1 "fhir_nightly steps, one at a time"
    airflow dag-processor &
    airflow triggerer &
    airflow scheduler &
    # stop the container when any of the three stops, so Docker restarts it
    wait -n
    exit 1
    ;;
  *)
    exec airflow "$@"
    ;;
esac
