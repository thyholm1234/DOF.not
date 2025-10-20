#!/usr/bin/env bash
set -euo pipefail

# Miljødefaults
TZ="${TZ:-Europe/Copenhagen}"
APP_VERSION="${APP_VERSION:-dev}"
UVICORN_HOST="${UVICORN_HOST:-0.0.0.0}"
UVICORN_PORT="${UVICORN_PORT:-8000}"
UVICORN_LOG_LEVEL="${UVICORN_LOG_LEVEL:-info}"
BIRD_CONF="${BIRD_CONF:-clients.yaml}"
BIRD_INTERVAL="${BIRD_INTERVAL:-60}"
BIRD_EXTRA_ARGS="${BIRD_EXTRA_ARGS:-}"

export TZ

echo "[dofnot] version=${APP_VERSION} tz=${TZ}"
echo "[dofnot] starting uvicorn on ${UVICORN_HOST}:${UVICORN_PORT}"
uvicorn server.server:app --host "${UVICORN_HOST}" --port "${UVICORN_PORT}" --log-level "${UVICORN_LOG_LEVEL}" --access-log &

echo "[dofnot] starting birdnotification.py --config ${BIRD_CONF} --watch -i ${BIRD_INTERVAL} ${BIRD_EXTRA_ARGS}"
python birdnotification.py --config "${BIRD_CONF}" --watch -i "${BIRD_INTERVAL}" ${BIRD_EXTRA_ARGS} &

# Håndter stop pænt
trap 'echo "[dofnot] stopping..."; kill 0; wait' SIGINT SIGTERM

# Hvis én proces dør, stop den anden
wait -n || true
kill 0
wait