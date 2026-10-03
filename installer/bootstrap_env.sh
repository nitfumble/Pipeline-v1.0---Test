#!/usr/bin/env bash
# Shared venv + dependency bootstrap. Runs INSIDE WSL. Used by both start.sh
# (daily dev launch) and the Windows installer (first-time provisioning) so
# there's one copy of this logic instead of two that can drift apart.
#
# Idempotent: safe to re-run; only does work on first run / when something's
# missing. Prints progress to stderr so it's visible live even when the
# caller captures stdout; the ONLY thing written to stdout is the final web
# venv path, so callers can do `VENV_DIR="$(bash installer/bootstrap_env.sh)"`.
set -e
cd "$(dirname "$0")/.."

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
PIPELINE_VENV="$HOME/.local/share/stacks-venvs/pipeline"

if [ ! -d "$VENV_DIR" ]; then
    echo "First run: creating server Python environment ..." >&2
    mkdir -p "$(dirname "$VENV_DIR")"
    if ! python3.11 -m venv "$VENV_DIR"; then
        echo >&2
        echo "Couldn't create the venv. On Ubuntu/WSL this usually means the" >&2
        echo "venv module isn't installed -- fix with:" >&2
        echo "    sudo apt update && sudo apt install -y python3.11 python3.11-venv" >&2
        exit 1
    fi
fi

if ! "$VENV_DIR/bin/python" -c "import fastapi, uvicorn, pydantic, numpy" 2>/dev/null; then
    echo "Installing server dependencies (first run only) ..." >&2
    "$VENV_DIR/bin/pip" install --quiet --upgrade pip
    "$VENV_DIR/bin/pip" install --quiet -r requirements.txt
fi

# ── Pipeline environment ───────────────────────────────────────────────────
if [ ! -d "$PIPELINE_VENV" ]; then
    echo "" >&2
    echo "Creating pipeline Python environment (numpy, essentia, umap — takes a few minutes) ..." >&2
    mkdir -p "$(dirname "$PIPELINE_VENV")"
    if python3.11 -m venv "$PIPELINE_VENV"; then
        "$PIPELINE_VENV/bin/pip" install --quiet --upgrade pip
        if "$PIPELINE_VENV/bin/pip" install --quiet -r requirements-pipeline.txt; then
            echo "  Pipeline environment ready: $PIPELINE_VENV" >&2
        else
            echo "  Warning: some pipeline packages failed to install." >&2
            echo "  Retry manually: source $PIPELINE_VENV/bin/activate && pip install -r requirements-pipeline.txt" >&2
        fi
    else
        echo "  Warning: couldn't create pipeline venv — pipelines will fall back to system Python." >&2
    fi
    echo "" >&2
fi

echo "$VENV_DIR"
