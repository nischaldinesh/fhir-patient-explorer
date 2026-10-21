#!/usr/bin/env bash
# Start the metrics API on http://127.0.0.1:8000 (docs at /docs). Run from anywhere; reads api/.env when present.
#   api/run.sh                 # the serving snapshot must exist: python3 analytics/build.py snapshot
#   PORT=8001 api/run.sh
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
if [ -f api/.env ]; then
  set -a; . api/.env; set +a
fi
if [ -z "${METRICS_API_KEY:-}" ]; then
  echo "METRICS_API_KEY is not set: copy api/.env.example to api/.env and set a key" >&2
  exit 2
fi
PY="${PYTHON:-.venv/bin/python}"
# no access log: uvicorn's would print query strings; the app logs route, status and time only
exec "$PY" -m uvicorn --factory api.app:create_app --host 127.0.0.1 --port "${PORT:-8000}" --no-access-log --log-level info
