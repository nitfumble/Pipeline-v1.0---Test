#!/usr/bin/env bash
# Thin convenience wrapper over the dj orchestrator: run the whole pipeline.
# Any args are passed straight through, e.g.  ./run_all.sh --from embed
cd "$(dirname "$0")"
exec python3 dj.py run "$@"
