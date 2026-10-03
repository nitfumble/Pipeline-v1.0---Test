#!/usr/bin/env bash
# Runs INSIDE WSL. Don't double-click this directly -- start_stacks.bat
# (Windows side) launches WSL and calls this script for you.
#
# Manages its own private venvs (server + pipeline) via installer/bootstrap_env.sh
# -- doesn't touch or depend on any other Python environment (conda, music_env,
# etc.) you might use elsewhere. First run creates them; every run after that
# just activates and starts.
set -e
cd "$(dirname "$0")"

VENV_DIR="$(bash installer/bootstrap_env.sh)"

# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"

PORT=8000
( for i in $(seq 1 30); do
    curl -s "http://localhost:${PORT}/api/setup" >/dev/null 2>&1 && { explorer.exe "http://localhost:${PORT}" 2>/dev/null; break; }
    sleep 0.5
  done ) &

exec uvicorn app:app --host 0.0.0.0 --port "${PORT}"
