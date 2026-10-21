#!/usr/bin/env bash
# Seed the throwaway HAPI (port 8090) and the Airflow data directory for testing the DAG. Never touches your local
# HAPI on 8080 or your local data/ folder. Run from the repo root.
#
#   orchestration/airflow/throwaway-hapi/seed.sh [--start-day YYYY-MM-DD] [--data-dir DIR]
#       Default, feed only: a new, empty HAPI, the invented hospital's reference data (pipeline.py reference), then the
#       Riverside General feed for the 3 days before the start day (default: 3 days ago, UTC). The DAG then loads from
#       the start day on. Small and fast; MIMIC and Synthea are not there.
#
#   orchestration/airflow/throwaway-hapi/seed.sh --from-local-db [--data-dir DIR]
#       A full copy of your local database (pg_dump of the local fhir-db container, read only) and of your local feed
#       state. Needs the local Postgres running (docker compose up -d db) and about 3.5 GB of disk.
#
# Either way the throwaway HAPI starts over and the data directory's feed state, landing files and warehouse are
# deleted (its airflow-logs are kept). DIR defaults to data/airflow and may not be data/ itself.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$ROOT"
DATA_DIR="data/airflow"
START_DAY=""
FROM_LOCAL_DB=0
while [ $# -gt 0 ]; do
  case "$1" in
    --start-day) START_DAY="$2"; shift 2 ;;
    --data-dir) DATA_DIR="$2"; shift 2 ;;
    --from-local-db) FROM_LOCAL_DB=1; shift ;;
    *) echo "unknown option $1" >&2; exit 2 ;;
  esac
done
case "$(cd "$(dirname "$DATA_DIR")" 2>/dev/null && pwd)/$(basename "$DATA_DIR")" in
  "$ROOT/data"|"$ROOT/data/") echo "Refusing to use $DATA_DIR: that is your local data folder" >&2; exit 2 ;;
esac
BASE="http://localhost:8090/fhir"
COMPOSE=(docker compose -f orchestration/airflow/throwaway-hapi/compose.yaml)

wait_for_hapi() {
  until curl -fsS "$BASE/metadata" >/dev/null 2>&1; do sleep 5; done
}

echo "== reset $DATA_DIR (feed state, landing files, warehouse)"
rm -rf "$DATA_DIR/hl7v2" "$DATA_DIR/empi" "$DATA_DIR/analytics" "$DATA_DIR/dbt-target" "$DATA_DIR/orchestration"
mkdir -p "$DATA_DIR/hl7v2" "$DATA_DIR/airflow-logs"

echo "== throwaway HAPI: start over"
"${COMPOSE[@]}" down -v >/dev/null 2>&1 || true

if [ "$FROM_LOCAL_DB" -eq 1 ]; then
  DUMP="$DATA_DIR/seed/hapi.dump"
  mkdir -p "$DATA_DIR/seed"
  echo "== dump of the local database (read only)"
  docker exec fhir-db pg_dump -U admin -d hapi -Fc -Z 6 > "$DUMP"
  ls -lh "$DUMP"
  "${COMPOSE[@]}" up -d db
  until "${COMPOSE[@]}" exec -T db pg_isready -U admin -d hapi >/dev/null 2>&1; do sleep 2; done
  "${COMPOSE[@]}" exec -T db pg_restore -U admin -d hapi --no-owner --no-privileges < "$DUMP"
  "${COMPOSE[@]}" up -d fhir
  wait_for_hapi
  cp data/hl7v2/feed_state.json "$DATA_DIR/hl7v2/feed_state.json"
else
  START_DAY="${START_DAY:-$(python3 -c 'from datetime import datetime, timedelta, timezone; print((datetime.now(timezone.utc) - timedelta(days=3)).date())')}"
  "${COMPOSE[@]}" up -d
  echo "== waiting for the empty HAPI (its first start creates the schema, 1 to 2 minutes)"
  wait_for_hapi
  STATE="$DATA_DIR/hl7v2/feed_state.json"
  echo "== reference data of the invented hospital"
  python3 hl7v2/pipeline.py reference --base "$BASE" --state "$STATE"
  for k in 3 2 1; do
    DAY="$(python3 -c 'import sys; from datetime import date, timedelta; print(date.fromisoformat(sys.argv[1]) - timedelta(days=int(sys.argv[2])))' "$START_DAY" "$k")"
    echo "== feed for $DAY"
    python3 hl7v2/pipeline.py daily --day "$DAY" --base "$BASE" --state "$STATE" --daily "$DATA_DIR/hl7v2/daily" | tail -3
  done
fi

curl -s -H 'Cache-Control: no-cache' "$BASE/Patient?_summary=count" | python3 -c 'import json, sys; print("Patients on the throwaway HAPI:", json.load(sys.stdin)["total"])'
python3 -c "import json,sys; print('last generated day', json.load(open(sys.argv[1]))['last_day'])" "$DATA_DIR/hl7v2/feed_state.json"
