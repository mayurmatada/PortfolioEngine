#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/infra"

echo "Tearing down containers and removing volumes…"
docker compose down -v

echo "Building and starting all services…"
docker compose up --build -d

echo "Done. Tailing logs (Ctrl-C to stop)…"
docker compose logs -f
