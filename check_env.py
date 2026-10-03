#!/usr/bin/env python3
"""check_env.py — verify the Python deps for each part of Stacks are importable.
Usage:  python3 check_env.py [--web | --pipeline | --all]   (default: --all)
Exits non-zero if any *requested* group has missing imports."""
import importlib, sys, shutil

WEB = {"fastapi": "fastapi", "uvicorn": "uvicorn", "pydantic": "pydantic", "numpy": "numpy"}
PIPELINE = {
    "numpy": "numpy", "pandas": "pandas", "scikit-learn": "sklearn",
    "essentia-tensorflow": "essentia.standard", "umap-learn": "umap", "hdbscan": "hdbscan",
    "matplotlib": "matplotlib", "spotipy": "spotipy", "pyrekordbox": "pyrekordbox",
    "yt-dlp": "yt_dlp", "requests": "requests", "mutagen": "mutagen", "rapidfuzz": "rapidfuzz",
}

def check(group_name, mods):
    print(f"\n── {group_name} ──")
    missing = []
    for pkg, mod in mods.items():
        try:
            importlib.import_module(mod)
            print(f"  ✓ {pkg}")
        except Exception as e:
            print(f"  ✗ {pkg:<20} ({type(e).__name__})")
            missing.append(pkg)
    return missing

def main():
    arg = sys.argv[1] if len(sys.argv) > 1 else "--all"
    miss = []
    if arg in ("--web", "--all"):
        miss += check("web app", WEB)
    if arg in ("--pipeline", "--all"):
        miss += check("pipeline", PIPELINE)
        print(f"\n  fpcalc (system binary): {'✓ found' if shutil.which('fpcalc') else '✗ missing — sudo apt install libchromaprint-tools'}")
    if miss:
        print(f"\n✗ missing: {', '.join(sorted(set(miss)))}")
        print("  install with the matching requirements file (web: requirements.txt, pipeline: requirements-pipeline.txt)")
        sys.exit(1)
    print("\n✓ all requested deps importable")

if __name__ == "__main__":
    main()
