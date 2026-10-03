#!/usr/bin/env bash
# Runs INSIDE WSL. Don't double-click this directly -- start_stacks.bat
# (Windows side) launches WSL and calls this script for you.
#
# Manages its own private venv (.venv) right next to this script -- doesn't
# touch or depend on any other Python environment (conda, music_env, etc.)
# you might use for the pipeline scripts. First run creates it and installs
# fastapi/uvicorn/pydantic (Python 3.11); every run after that just activates and starts.
set -e
cd "$(dirname "$0")"

# Windows drives mounted into WSL (/mnt/e, /mnt/c, ...) use a filesystem
# (DrvFs) that can't create symlinks -- and `python3 -m venv` needs to create
# one (lib64 -> lib), so a venv living on E:\ etc. fails with "Operation not
# permitted" even though the venv module itself is fine. Fix: keep the venv
# in WSL's native filesystem (under $HOME, real ext4, no such restriction)
# instead of next to the project. Keyed by this project's own path so
# multiple installs (e.g. a "- Test" copy) each get their own venv.
PROJECT_DIR="$(pwd)"
VENV_KEY=$(echo -n "$PROJECT_DIR" | md5sum | cut -c1-10)
VENV_DIR="$HOME/.local/share/stacks-venvs/$VENV_KEY"

if [ ! -d "$VENV_DIR" ]; then
    echo "First run: creating server Python environment ..."
    mkdir -p "$(dirname "$VENV_DIR")"
    if ! python3.11 -m venv "$VENV_DIR"; then
        echo
        echo "Couldn't create the venv. On Ubuntu/WSL this usually means the"
        echo "venv module isn't installed -- fix with:"
        echo "    sudo apt update && sudo apt install -y python3.11 python3.11-venv"
        exit 1
    fi
fi

# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"

if ! python -c "import fastapi, uvicorn, pydantic, numpy" 2>/dev/null; then
    echo "Installing server dependencies (first run only) ..."
    pip install --quiet --upgrade pip
    pip install --quiet -r requirements.txt
fi

# ── Pipeline environment ───────────────────────────────────────────────────────
PIPELINE_VENV="$HOME/.local/share/stacks-venvs/pipeline"
if [ ! -d "$PIPELINE_VENV" ]; then
    echo ""
    echo "Creating pipeline Python environment (numpy, essentia, umap — takes a few minutes) ..."
    mkdir -p "$(dirname "$PIPELINE_VENV")"
    if python3.11 -m venv "$PIPELINE_VENV"; then
        "$PIPELINE_VENV/bin/pip" install --quiet --upgrade pip
        if "$PIPELINE_VENV/bin/pip" install --quiet -r requirements-pipeline.txt; then
            echo "  Pipeline environment ready: $PIPELINE_VENV"
        else
            echo "  Warning: some pipeline packages failed to install."
            echo "  Retry manually: source $PIPELINE_VENV/bin/activate && pip install -r requirements-pipeline.txt"
        fi
    else
        echo "  Warning: couldn't create pipeline venv — pipelines will fall back to system Python."
    fi
    echo ""
fi

PORT=8000
( for i in $(seq 1 30); do
    curl -s "http://localhost:${PORT}/api/setup" >/dev/null 2>&1 && { explorer.exe "http://localhost:${PORT}" 2>/dev/null; break; }
    sleep 0.5
  done ) &

exec uvicorn app:app --host 0.0.0.0 --port "${PORT}"
