#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/infra"

if ! command -v docker >/dev/null 2>&1; then
	echo "Docker CLI not found in PATH."
	echo "Install/start Docker Desktop, then retry ./start.sh"
	exit 1
fi

if ! docker version >/dev/null 2>&1; then
	echo "Docker CLI is present but not usable (WSL/Docker Desktop bridge issue)."
	echo "Try this recovery sequence:"
	echo "  1) In Windows PowerShell: wsl --shutdown"
	echo "  2) Restart Docker Desktop"
	echo "  3) Re-open this terminal and run ./start.sh again"
	exit 1
fi

echo "Tearing down containers and removing volumes…"
docker compose down -v

echo "Building and starting all services…"
docker compose up --build -d

echo "Done. Tailing logs (Ctrl-C to stop)…"
docker compose logs -f
