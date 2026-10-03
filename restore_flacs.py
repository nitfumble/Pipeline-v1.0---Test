#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
restore_flacs.py
================
Replace corrupted audio-dir FLACs with their original Soulseek downloads.
Primary matching is by audio fingerprint (fpcalc / Chromaprint). Some FLACs
have corrupted audio frames (decode_frame() failed) that fpcalc cannot read
at all — for those there is no audio signal to fingerprint, so they fall
back to matching on embedded tags (ARTIST/TITLE, still intact) + duration.

How it works:
  1. Fingerprint + tag-index all source FLACs (cached — only runs once)
  2. For each audio-dir FLAC:
       a. Try fpcalc fingerprint match (Hamming similarity >= 0.85)
       b. If fpcalc can't decode the file at all → fuzzy tag+duration match
  3. Replace audio-dir file with source file, keeping destination filename

Usage (WSL, inside music_env):
    python3 restore_flacs.py --dry-run            # show matches, no changes
    python3 restore_flacs.py --replace            # overwrite audio-dir files
    python3 restore_flacs.py --replace --target pipeline
    python3 restore_flacs.py --replace --target library
    python3 restore_flacs.py --dry-run --rebuild-cache  # force re-fingerprint sources

Pre-requisite:
    sudo apt install -y libchromaprint-tools       # provides fpcalc
"""

import argparse
import json
import logging
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

from mutagen import File as MutagenFile
from rapidfuzz import fuzz

sys.path.insert(0, str(Path(__file__).resolve().parent))

# ---------------------------------------------------------------------------
# Targets
# ---------------------------------------------------------------------------

JOBS = {
    "pipeline": {
        "audio_dir":   Path("/mnt/e/Pipeline v1.0 - Test/audio"),
        "source_dirs": [
            Path("/mnt/e/Pipeline v1.0 - Test/source/soulseek"),
            Path("/mnt/e/Music Library/slsk"),
            Path("/mnt/e/Music Library/slskd"),
        ],
        "cache_file":  Path("/mnt/e/Pipeline v1.0 - Test/tmp/source_fp_cache_pipeline.json"),
    },
    "library": {
        "audio_dir":   Path("/mnt/e/Music Library/audio"),
        "source_dirs": [
            Path("/mnt/e/Music Library/slsk"),
            Path("/mnt/e/Music Library/slskd"),
            Path("/mnt/e/Pipeline v1.0 - Test/source/soulseek"),
        ],
        "cache_file":  Path("/mnt/e/Music Library/tmp/source_fp_cache_library.json"),
    },
}

# ---------------------------------------------------------------------------
# Tuning
# ---------------------------------------------------------------------------

FP_LENGTH_S   = 120    # seconds to fingerprint (120 s is enough to ID a track)
FP_MIN_SIM    = 0.85   # Hamming similarity threshold for a confident match
FP_AMBIG_BAND = 0.03   # two candidates within this band → AMBIGUOUS

# Tag-fallback (only used when fpcalc can't decode the audio-dir file at all)
TAG_SCORE_CUTOFF = 80      # rapidfuzz token_set_ratio minimum
TAG_DUR_TOL_MS   = 5_000   # ±5 s duration tolerance

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(format="%(levelname)s  %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# fpcalc — Chromaprint fingerprinting
# ---------------------------------------------------------------------------

FPCALC = (
    shutil.which("fpcalc")
    or next((p for p in (
        "/home/bart/.local/bin/fpcalc",
        "/usr/bin/fpcalc",
        "/home/bart/chromaprint/src/cmd/fpcalc",
    ) if Path(p).exists()), "fpcalc")
)

def _fp_raw(path: Path) -> list[int] | None:
    """
    Return raw Chromaprint integer array via fpcalc -json -raw.
    Returns None if fpcalc fails (e.g. corrupted audio data).
    """
    try:
        out = subprocess.run(
            [FPCALC, "-json", "-raw", "-length", str(FP_LENGTH_S), str(path)],
            capture_output=True, text=True, timeout=180,
        )
        if out.returncode != 0:
            return None
        data = json.loads(out.stdout)
        return data.get("fingerprint")  # list[int32]
    except Exception as e:
        log.debug(f"fpcalc failed for {path.name}: {e}")
        return None

def _fp_similarity(fp1: list[int], fp2: list[int]) -> float:
    """Hamming similarity: 1.0 = identical audio, 0.0 = completely different."""
    length = min(len(fp1), len(fp2))
    if length == 0:
        return 0.0
    diff_bits = sum(bin(fp1[i] ^ fp2[i]).count("1") for i in range(length))
    return 1.0 - diff_bits / (length * 32)

# ---------------------------------------------------------------------------
# Tags + duration (fallback signal when fpcalc can't decode a file at all)
# ---------------------------------------------------------------------------

def _read_tags(path: Path) -> dict:
    """Best-effort {artist, title, duration_ms} via mutagen. Never raises."""
    artist = title = ""
    dur_ms = None
    try:
        m = MutagenFile(str(path))
        if m is not None:
            if hasattr(m, "info") and m.info:
                dur_ms = int(m.info.length * 1000)
            if m.tags:
                artist = (m.tags.get("artist") or m.tags.get("ARTIST") or [""])[0]
                title  = (m.tags.get("title")  or m.tags.get("TITLE")  or [""])[0]
    except Exception:
        pass
    return {"artist": str(artist), "title": str(title), "dur_ms": dur_ms}

# ---------------------------------------------------------------------------
# Source fingerprint index (with JSON cache for speed)
# ---------------------------------------------------------------------------

def _load_cache(cache_file: Path) -> dict:
    """Load {posix_path: {fp, artist, title, dur_ms}} from JSON cache."""
    try:
        with open(cache_file, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_cache(cache_file: Path, data: dict) -> None:
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    tmp = cache_file.parent / (cache_file.name + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f)
    os.replace(str(tmp), str(cache_file))

def build_source_index(source_dirs: list[Path], cache_file: Path,
                       rebuild: bool = False) -> list[dict]:
    """
    Walk source dirs; for each .flac record fingerprint + tags + duration.
    Cached so re-runs are instant. Returns list of entry dicts (path, fp,
    artist, title, dur_ms) — fp may be None if fpcalc couldn't read it,
    those entries still serve the tag-fallback path.
    """
    cache = {} if rebuild else _load_cache(cache_file)

    all_flacs = []
    for src_root in source_dirs:
        if not src_root.exists():
            log.warning(f"Source dir not found, skipping: {src_root}")
            continue
        all_flacs.extend(sorted(src_root.rglob("*.flac")))

    new_entries = 0
    for i, p in enumerate(all_flacs, 1):
        key = str(p)
        if key not in cache:
            fp = _fp_raw(p)
            tags = _read_tags(p)
            cache[key] = {"fp": fp, **tags}
            new_entries += 1
            if new_entries % 10 == 0:
                print(f"  indexing source files... {i}/{len(all_flacs)}", flush=True)

    if new_entries > 0:
        _save_cache(cache_file, cache)
        print(f"  indexed {new_entries} new source files, cache saved.")

    index = []
    for k, v in cache.items():
        if not Path(k).exists():
            continue
        index.append({
            "path":   Path(k),
            "fp":     v.get("fp"),
            "artist": v.get("artist", ""),
            "title":  v.get("title", ""),
            "dur_ms": v.get("dur_ms"),
        })
    n_fp = sum(1 for e in index if e["fp"])
    log.info(f"Source index: {len(index)} files ({n_fp} fingerprinted, "
             f"{len(index)-n_fp} tag-only) from {len(all_flacs)} total")
    return index

# ---------------------------------------------------------------------------
# Match audio file against source index
# ---------------------------------------------------------------------------

def _fp_match(audio_fp: list[int], source_index: list[dict]) -> tuple[dict | None, float, float]:
    """Returns (best_entry, best_sim, second_best_sim) among entries with a fingerprint."""
    best_sim = second_sim = 0.0
    best = None
    for entry in source_index:
        if not entry["fp"]:
            continue
        sim = _fp_similarity(audio_fp, entry["fp"])
        if sim > best_sim:
            second_sim = best_sim
            best_sim = sim
            best = entry
        elif sim > second_sim:
            second_sim = sim
    return best, best_sim, second_sim

def _tag_match(audio_path: Path, source_index: list[dict]) -> tuple[dict | None, float]:
    """
    Fallback for files fpcalc can't decode: fuzzy-match embedded ARTIST/TITLE
    tags (or filename if tags are empty) + duration gate against the source
    index. Returns (best_entry, score) or (None, 0).
    """
    tags = _read_tags(audio_path)
    query = f"{tags['artist']} {tags['title']}".strip() or audio_path.stem
    audio_dur = tags["dur_ms"]

    best, best_score = None, 0.0
    for entry in source_index:
        candidate = f"{entry['artist']} {entry['title']}".strip() or entry["path"].stem
        score = fuzz.token_set_ratio(query.lower(), candidate.lower())
        if score < TAG_SCORE_CUTOFF:
            continue
        if audio_dur and entry["dur_ms"] and abs(audio_dur - entry["dur_ms"]) > TAG_DUR_TOL_MS:
            continue
        if score > best_score:
            best_score = score
            best = entry
    return best, best_score

def find_match(audio_path: Path,
               source_index: list[dict]) -> tuple[dict | None, float, str]:
    """
    Returns (best_source_entry | None, score, status).
    status: 'matched' | 'ambiguous' | 'unmatched' | 'tag-matched' | 'tag-unmatched'
    Primary path: fpcalc fingerprint. Fallback: tag+duration fuzzy match,
    only used when fpcalc cannot decode the audio-dir file at all (it has
    corrupted audio frames and there is no audio signal to fingerprint).
    """
    audio_fp = _fp_raw(audio_path)

    if audio_fp is not None:
        best, best_sim, second_sim = _fp_match(audio_fp, source_index)
        if best is not None and best_sim >= FP_MIN_SIM:
            # Only flag ambiguous when the match is imperfect and runner-up is close.
            # If best_sim >= 0.99 both candidates are identical audio — just pick first.
            if best_sim < 0.99 and best_sim - second_sim < FP_AMBIG_BAND:
                return best, best_sim, "ambiguous"
            return best, best_sim, "matched"
        return None, best_sim, "unmatched"

    # fpcalc couldn't decode this file — corrupted audio frames, no signal
    # to fingerprint. Fall back to tags+duration (still intact on disk).
    best, score = _tag_match(audio_path, source_index)
    if best is not None:
        return best, score, "tag-matched"
    return None, score, "tag-unmatched"

# ---------------------------------------------------------------------------
# Atomic file replacement
# ---------------------------------------------------------------------------

def replace_file(source: Path, dest: Path) -> None:
    """Copy source → tmp in same dir → atomic rename over dest."""
    tmp = dest.parent / (dest.stem + ".__restoring__.flac")
    try:
        shutil.copy2(str(source), str(tmp))
        m = MutagenFile(str(tmp))
        if m is None:
            raise RuntimeError("Copy failed mutagen readability check")
        os.replace(str(tmp), str(dest))
    except Exception:
        if tmp.exists():
            tmp.unlink(missing_ok=True)
        raise

# ---------------------------------------------------------------------------
# Job runner
# ---------------------------------------------------------------------------

def _short_path(p: Path, source_dirs: list[Path]) -> str:
    for sd in source_dirs:
        try:
            return str(p.relative_to(sd.parent))
        except ValueError:
            pass
    return p.name

def run_job(job_name: str, job: dict, *, dry_run: bool, rebuild_cache: bool) -> dict:
    audio_dir   = job["audio_dir"]
    source_dirs = job["source_dirs"]
    cache_file  = job["cache_file"]

    if not audio_dir.exists():
        log.warning(f"Audio dir not found: {audio_dir}")
        return {"matched": 0, "ambiguous": 0, "fp_failed": 0,
                "unmatched": 0, "replaced": 0, "issues": []}

    print(f"\n{'='*64}")
    print(f"Job: {job_name}  |  {'DRY RUN' if dry_run else 'REPLACING'}")
    print(f"{'='*64}")
    print("Building source fingerprint index (cached after first run)…")
    t0 = time.time()
    source_index = build_source_index(source_dirs, cache_file, rebuild=rebuild_cache)
    print(f"Index ready in {time.time()-t0:.1f}s\n")

    audio_flacs = sorted(f for f in audio_dir.iterdir() if f.suffix.lower() == ".flac")
    print(f"Fingerprinting {len(audio_flacs)} audio-dir FLACs…\n")

    matched = tag_matched = ambiguous = unmatched = tag_unmatched = replaced = 0
    issues = []   # (label, filename, detail)

    def _do_replace(src_entry, audio_path):
        nonlocal replaced
        if not dry_run:
            try:
                replace_file(src_entry["path"], audio_path)
                print(f"         ✓ replaced")
                replaced += 1
            except Exception as e:
                print(f"         ✗ replace FAILED: {e}")
                issues.append(("replace-error", audio_path.name, str(e)))

    for audio_path in audio_flacs:
        src_entry, sim, status = find_match(audio_path, source_index)

        if status == "matched":
            short = _short_path(src_entry["path"], source_dirs)
            print(f"  OK     {audio_path.name}")
            print(f"         → {short}  (sim={sim:.3f})")
            matched += 1
            _do_replace(src_entry, audio_path)

        elif status == "tag-matched":
            short = _short_path(src_entry["path"], source_dirs)
            print(f"  TAGOK  {audio_path.name}  (fpcalc failed — matched via tags)")
            print(f"         → {short}  (tag_score={sim:.0f})")
            tag_matched += 1
            _do_replace(src_entry, audio_path)

        elif status == "ambiguous":
            short = _short_path(src_entry["path"], source_dirs)
            print(f"  AMBIG  {audio_path.name}")
            print(f"         best match: {short}  (sim={sim:.3f}, runner-up too close)")
            ambiguous += 1
            issues.append(("ambiguous", audio_path.name, f"sim={sim:.3f}"))

        elif status == "tag-unmatched":
            print(f"  NOFP   {audio_path.name}  (fpcalc failed + no tag match — fully corrupt)")
            tag_unmatched += 1
            issues.append(("no-match-corrupt", audio_path.name, "fpcalc failed, tags didn't match any source"))

        else:  # unmatched
            print(f"  MISS   {audio_path.name}  (best sim={sim:.3f} < {FP_MIN_SIM})")
            unmatched += 1
            issues.append(("unmatched", audio_path.name, f"best_sim={sim:.3f}"))

    return {
        "matched":       matched,
        "tag_matched":   tag_matched,
        "ambiguous":     ambiguous,
        "unmatched":     unmatched,
        "tag_unmatched": tag_unmatched,
        "replaced":      replaced,
        "issues":        issues,
    }

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(
        description="Restore audio-dir FLACs from original Soulseek downloads (fingerprint matching)."
    )
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true", help="Show matches, no file changes")
    mode.add_argument("--replace", action="store_true", help="Overwrite audio-dir files")
    ap.add_argument(
        "--target", choices=["pipeline", "library", "all"], default="all",
        help="Which audio dir to process (default: all)",
    )
    ap.add_argument(
        "--rebuild-cache", action="store_true",
        help="Re-fingerprint all source files even if cached",
    )
    args = ap.parse_args()

    if not Path(FPCALC).exists() and not shutil.which(FPCALC):
        sys.exit(
            "ERROR: fpcalc not found.\n"
            "Install with:  sudo apt install -y libchromaprint-tools"
        )
    log.info(f"Using fpcalc: {FPCALC}")

    dry_run = args.dry_run
    targets = list(JOBS.items()) if args.target == "all" else [(args.target, JOBS[args.target])]

    totals = {"matched": 0, "tag_matched": 0, "ambiguous": 0,
              "unmatched": 0, "tag_unmatched": 0, "replaced": 0}
    all_issues = []

    for job_name, job in targets:
        result = run_job(job_name, job, dry_run=dry_run, rebuild_cache=args.rebuild_cache)
        for k in totals:
            totals[k] += result[k]
        all_issues.extend(result["issues"])

    print(f"\n{'='*64}")
    print(f"SUMMARY  ({'dry run' if dry_run else 'replaced'})")
    print(f"  matched:       {totals['matched']}  (fingerprint)")
    print(f"  tag-matched:   {totals['tag_matched']}  (corrupt audio — matched via tags+duration)")
    print(f"  ambiguous:     {totals['ambiguous']}  (two equally-close fingerprint candidates)")
    print(f"  unmatched:     {totals['unmatched']}  (best fingerprint sim < {FP_MIN_SIM})")
    print(f"  no-match-corrupt: {totals['tag_unmatched']}  (fpcalc + tags both failed)")
    if not dry_run:
        print(f"  replaced:      {totals['replaced']}")

    if all_issues:
        print(f"\nFiles needing manual attention ({len(all_issues)}):")
        for label, fname, detail in all_issues:
            print(f"  [{label}]  {fname}  — {detail}")

if __name__ == "__main__":
    main()
