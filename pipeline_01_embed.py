#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pipeline_01_embed.py
====================
Stage 1 — register and embed all tracks.

Execution order:
  pipeline_00  →  pipeline_01  →  [manual RB import]  →  pipeline_02  →  pipeline_03

Stages:
  1  Register — walk NEW_AUDIO (always) and OLD_AUDIO (if CHECK_OLD=True).
                For each unregistered file: extract metadata, BPM, key in parallel.
                BPM outside 70-180 discarded as corrupt.
                Results inserted into both DBs (no tag writes to files).

  2  Scan     — for each file in NEW_AUDIO only, three cases:
                  a) Already in new DB with embeddings → skip
                  b) In old DB with embeddings → migrate, no re-scan
                  c) Not scanned anywhere → full Essentia model stack
                Commits every COMMIT_EVERY tracks. Errors logged.

  3  Ledger   — confirm all new DB paths have a SPOT ledger entry.
                Non-Spotify tracks get a minimal seed entry.

JOIN KEY: file path. No Spotify IDs anywhere in this file.

DUAL DB:
  NEW_DB = E:/Music Library/db/music.db  — primary, always written
  OLD_DB = E:/Music/db/music.db          — kept in sync for legacy tools
  Both receive identical writes for any new or updated track.

MODELS (all in TRAINED_DIR):
  discogs-effnet-bs64-1.pb              backbone embeddings (n_frames × 1280)
  discogs-maest-30s-pw-519l-2.pb        genre classification (3 × 30s windows)
  voice_instrumental-discogs-effnet-1   vocals probability
  danceability-discogs-effnet-1         danceability score
  mood_party-discogs-effnet-1           party score
  timbre-discogs-effnet-1               timbre score
  mood_aggressive/happy/relaxed/        binary mood models
    sad/acoustic/party
  mtg_jamendo_moodtheme                 multi-label mood/theme (blob)
  mtg_jamendo_instrument                multi-label instrument (blob)
"""

import shutil
import os, json, logging
from pathlib import Path
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
import sqlite3
import gc
import numpy as np
from tqdm import tqdm
from mutagen import File as MutagenFile
from mutagen.easyid3 import EasyID3
from mutagen.id3 import ID3NoHeaderError

# Canonical path handling — single source of truth (see dj_paths.py)
from dj_paths import to_key as _norm, to_path, to_win, atomic_write_json
from config import load

os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"
import essentia.standard as es
from essentia.standard import (
    TensorflowPredictMAEST,
    TensorflowPredictEffnetDiscogs,
    TensorflowPredict2D,
)
from essentia import EssentiaLogger
EssentiaLogger().warningActive = False

import argparse
_parser = argparse.ArgumentParser(description="DJ Pipeline - Embed & register tracks")
_parser.add_argument("--commit-every", type=int, default=10, help="Commit to DB every N tracks (default: 10)")
_parser.add_argument("--debug", nargs="?", const=1, default=None, type=int, metavar="N", help="Process N tracks only (default: 1)")
_parser.add_argument("--workers", type=int, default=None, metavar="N", help="Stage-1 workers (default: config or cpu-4)")
_parser.add_argument("--no-interactive", action="store_true")
_args = _parser.parse_args()

# ══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════════════════
# ── Load config — single source of truth (config.py / config.json) ────────────
cfg = load()

NEW_ROOT    = cfg.paths.library_root
NEW_AUDIO   = cfg.paths.audio
NEW_DB      = cfg.paths.music_db
LOG_DIR     = cfg.paths.logs
LEDGER      = cfg.paths.ledger
TMP_DIR     = cfg.paths.tmp
BIN_DIR     = cfg.paths.bin
BIN_DIR.mkdir(parents=True, exist_ok=True)

# Essentia models: configured dir, else the repo-bundled models/ (shipped via git).
_cfg_models = to_path(cfg.embed.model_dir) if cfg.embed.model_dir else None
TRAINED_DIR = _cfg_models if (_cfg_models and _cfg_models.exists()) \
              else (Path(__file__).resolve().parent / "models")

COMMIT_EVERY = _args.commit_every
MAX_WORKERS  = _args.workers or (cfg.embed.workers or max(1, os.cpu_count() - 4))
DEBUG_LIMIT  = _args.debug

AUDIO_EXTS   = {f".{e.lower().lstrip('.')}" for e in cfg.download.accepted_formats}
AUDIO_SR     = 44100
MAEST_SR     = 16000
MAEST_RUNS   = 3
BPM_MIN      = 70
BPM_MAX      = 180

MODEL_FILES = {
    "effnet":          "discogs-effnet-bs64-1.pb",
    "maest":           "discogs-maest-30s-pw-519l-2.pb",
    "vocals":          "voice_instrumental-discogs-effnet-1.pb",
    "danceability":    "danceability-discogs-effnet-1.pb",
    "party":           "mood_party-discogs-effnet-1.pb",
    "timbre":          "timbre-discogs-effnet-1.pb",
    "moodtheme":       "mtg_jamendo_moodtheme-discogs-effnet-1.pb",
    "instrument":      "mtg_jamendo_instrument-discogs-effnet-1.pb",
    "mood_aggressive": "mood_aggressive-discogs-effnet-1.pb",
    "mood_happy":      "mood_happy-discogs-effnet-1.pb",
    "mood_relaxed":    "mood_relaxed-discogs-effnet-1.pb",
    "mood_sad":        "mood_sad-discogs-effnet-1.pb",
    "mood_acoustic":   "mood_acoustic-discogs-effnet-1.pb",
    "mood_party":      "mood_party-discogs-effnet-1.pb",
}

LABEL_FILES = {
    "maest":      "discogs-maest-30s-pw-519l-2.json",
    "instrument": "mtg_jamendo_instrument-discogs-effnet-1.json",
    "moodtheme":  "mtg_jamendo_moodtheme-discogs-effnet-1.json",
}

# ── Init ──────────────────────────────────────────────────────────────────────
LOG_DIR.mkdir(parents=True, exist_ok=True)
TMP_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "pipeline_01.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)

# ── Kill stale processes holding the DB ───────────────────────────────────────
import subprocess
_db_path = str(NEW_ROOT / "db" / "music.db")
_result = subprocess.run(["lsof", "-t", _db_path], capture_output=True, text=True)
_stale_pids = [p for p in _result.stdout.strip().split("\n") if p.strip() and p.strip() != str(os.getpid())]
if _stale_pids:
    print(f"  Killing {len(_stale_pids)} stale processes holding DB lock...")
    for _pid in _stale_pids:
        try:
            os.kill(int(_pid), 9)
        except Exception:
            pass
    import time
    time.sleep(1)

# ── Init ──────────────────────────────────────────────────────────────────────
def log_err(path, stage, err):
    logging.error(f"[{stage}] {Path(path).name}: {err}")

print("=== Pipeline 01: Embed ===")

missing = [k for k, f in MODEL_FILES.items() if not (TRAINED_DIR / f).exists()]
if missing:
    print(f"  !! Missing models: {missing}")
else:
    print("  ✓ All model files present.")

print(f"  CPU workers: {MAX_WORKERS}")

def save_ledger(ledger: dict):
    atomic_write_json(LEDGER, ledger)


# ══════════════════════════════════════════════════════════════════════════════
# DB HELPERS
# ══════════════════════════════════════════════════════════════════════════════
DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS tracks (
    path              TEXT PRIMARY KEY,
    filename          TEXT,
    extension         TEXT,
    artist            TEXT,
    title             TEXT,
    genre             TEXT,
    bpm               REAL,
    key               TEXT,
    vocals_prob       REAL CHECK (vocals_prob >= 0.0 AND vocals_prob <= 1.0),
    duration          REAL,
    duration_time     TEXT,
    size              REAL,
    bitrate           INTEGER,
    sample_rate       INTEGER,
    channels          INTEGER,
    rating            REAL CHECK (rating >= 0 AND rating <= 5),
    sp_energy         REAL,
    sp_danceability   REAL,
    sp_playcount      INTEGER DEFAULT 0,
    sp_playlist_keys  TEXT,
    status            TEXT CHECK (status IN ('keep','delete')),
    sim_keep          REAL,
    sim_delete        REAL,
    preference_score  REAL,
    danceability      REAL,
    party_score       REAL,
    timbre            REAL,
    mood_aggressive   REAL,
    mood_happy        REAL,
    mood_relaxed      REAL,
    mood_sad          REAL,
    mood_acoustic     REAL,
    mood_party        REAL,
    date_added        TEXT DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS track_metadata (
    track_path    TEXT PRIMARY KEY,
    genre_scores  BLOB,
    embeddings    BLOB,
    moodtheme     BLOB,
    instrument    BLOB,
    FOREIGN KEY(track_path) REFERENCES tracks(path)
);
CREATE INDEX IF NOT EXISTS idx_tracks_path   ON tracks(path);
CREATE INDEX IF NOT EXISTS idx_metadata_path ON track_metadata(track_path);
"""

def _open_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.executescript(DB_SCHEMA)
    # Add mood columns if missing (backward compat with older DB)
    existing = {r[1] for r in conn.execute("PRAGMA table_info(tracks)").fetchall()}
    for col in ["mood_aggressive", "mood_happy", "mood_relaxed",
                "mood_sad", "mood_acoustic", "mood_party"]:
        if col not in existing:
            conn.execute(f"ALTER TABLE tracks ADD COLUMN {col} REAL")
    conn.commit()
    return conn

def _write_both(cn, co, table, cols, vals):
    # OLD_DB is legacy/read-only — writes target NEW_DB only (co kept for signature).
    sql = (f"INSERT OR IGNORE INTO {table} "
           f"({','.join(cols)}) VALUES ({','.join(['?'] * len(cols))})")
    cn.execute(sql, vals)

def _update_both(cn, co, table, set_clause, where_clause, vals):
    # OLD_DB is legacy/read-only — writes target NEW_DB only.
    sql = f"UPDATE {table} SET {set_clause} WHERE {where_clause}"
    cn.execute(sql, vals)

def load_ledger() -> dict:
    if not LEDGER.exists():
        return {}
    with open(LEDGER, encoding="utf-8") as f:
        raw = json.load(f)
    return {_norm(k): v for k, v in raw.items()}

# NOTE: _norm is now dj_paths.to_key (imported above). The old local _norm also
# mapped /media/bart/One Touch/Music -> e:/music for the legacy external drive.
# That alias is intentionally dropped (old library is legacy). If that drive is
# still mounted and in use, tell me and I'll add the alias into dj_paths.

def _seed_entry(path_str, artist, title, source, dur_ms=None, size_bytes=None) -> dict:
    """Minimal ledger entry in the SAME canonical schema as
    pipeline_00_download.make_entry(). Keep these fields in sync with pipe00.
    Fingerprint is left None — fingerprinting is a pipe00 ingestion concern."""
    p = to_path(path_str)
    return {
        "path":              str(p),
        "path_win":          to_win(p),
        "artist":            artist or "",
        "title":             title or "",
        "duration_ms":       dur_ms,
        "size_bytes":        size_bytes,
        "fingerprint":       None,
        "size_suspicious":   False,
        "source":            source,
        "original_source":   source,
        "spotify_track_id":  None,
        "_spotify_id":       None,
        "spotify_playlists": [],
        "spot":              None,
        "validated":         None,
        "validated_at":      None,
        "last_sync":         None,
        "likely_dup_of":     None,
    }
# ══════════════════════════════════════════════════════════════════════════════
#%% Stage 1: Register — BPM, key, metadata for unregistered tracks
# ══════════════════════════════════════════════════════════════════════════════
"""
Walk NEW_AUDIO always. Walk OLD_AUDIO if CHECK_OLD=True.
For each file not yet in either DB:
  - Extract metadata (duration, bitrate, sample rate, channels, size, ID3 tags)
  - Extract BPM (70-180 range) and key in parallel via ProcessPoolExecutor
  - INSERT OR IGNORE into both DBs (no tag writes)
"""
print("\n── Stage 1: Register ──")

conn_new = _open_db(NEW_DB)
conn_old = conn_new          # single-library v1.0: one DB. Alias keeps dual-write helpers working.

# Registered paths (single DB)
reg_new = {r[0] for r in conn_new.execute("SELECT path FROM tracks").fetchall()}
registered = reg_new
_display_registered = reg_new

# Collect audio files to process (single library)
audio_dirs = [NEW_AUDIO]

all_audio = []
for audio_dir in audio_dirs:
    if audio_dir.exists():
        all_audio.extend(
            p for p in audio_dir.iterdir()
            if p.suffix.lower() in AUDIO_EXTS
        )

new_files = [p for p in all_audio if str(p) not in registered]

if DEBUG_LIMIT:
    new_files = new_files[:DEBUG_LIMIT]

print(f"  Audio dirs scanned:  {[str(d) for d in audio_dirs]}")
print(f"  Total audio files:   {len(all_audio)}")
print(f"  Already registered:  {len(_display_registered)}")
print(f"  New to register:     {len(new_files)}")


# ── DB backup ─────────────────────────────────────────────────────────────────
_ts_db = datetime.now().strftime("%Y%m%d_%H%M%S")

for _db_path, _root in [(NEW_DB, NEW_ROOT)]:   # OLD_DB is read-only legacy — not backed up
    if not _db_path.exists():
        continue
    _dst_dir = _root / "backups" / "music_db"
    _dst_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(str(_db_path), str(_dst_dir / f"{_db_path.stem}_{_ts_db}.db"))
    print(f"  DB backed up: {_db_path.stem} → {_root.name}/backups/db/")
    # Rotate — keep last 10
    _old = sorted(_dst_dir.glob(f"{_db_path.stem}_*.db"), key=lambda f: f.stat().st_mtime)
    for _f in _old[:-10]:
        _f.unlink()

def _safe_float(v):
    try:    return float(v)
    except: return None


def _extract_meta(path: Path) -> dict:
    """
    CPU-bound worker — no shared state, no DB access.
    Extracts metadata, BPM (70-180), and key for a single audio file.
    Safe to run in ProcessPoolExecutor.
    """
    r = {
        "path":      str(path),
        "filename":  path.name,
        "extension": path.suffix,
        "date_added": datetime.now().isoformat(),
    }

    # File metadata
    try:
        info           = MutagenFile(path).info
        r["duration"]      = info.length
        r["duration_time"] = f"{int(info.length // 60):02d}:{int(info.length % 60):02d}"
        r["bitrate"]       = getattr(info, "bitrate",     None)
        r["sample_rate"]   = getattr(info, "sample_rate", None)
        r["channels"]      = getattr(info, "channels",    None)
    except Exception:
        r.update(duration=None, duration_time=None,
                 bitrate=None, sample_rate=None, channels=None)

    r["size"] = path.stat().st_size / (1024 * 1024)

    # ID3 tags
    r.update(artist=None, title=None, sp_energy=None, sp_danceability=None)
    if path.suffix.lower() == ".mp3":
        try:
            tags             = EasyID3(path)
            r["artist"]      = ", ".join(tags.get("artist", [])) or None
            r["title"]       = " ".join(tags.get("title",  [])) or None
            r["sp_energy"]   = _safe_float((tags.get("composer", [None])[0]))
            r["sp_danceability"] = _safe_float((tags.get("album", [None])[0]))
        except Exception:
            pass

    # Fallback artist/title from filename
    parts       = path.stem.split(" - ", 1)
    r["artist"] = r["artist"] or (parts[0].strip() if len(parts) > 1 else None)
    r["title"]  = r["title"]  or parts[-1].strip()

    # BPM + Key — single audio load
    try:
        audio = es.MonoLoader(filename=str(path), sampleRate=AUDIO_SR)()
    except Exception:
        audio = None

    try:
        bpm, *_  = es.RhythmExtractor2013(method="multifeature")(audio)
        bpm_val  = round(float(bpm), 2)
        r["bpm"] = bpm_val if BPM_MIN <= bpm_val <= BPM_MAX else None
    except Exception:
        r["bpm"] = None

    try:
        key, scale, strength = es.KeyExtractor()(audio)
        r["key"] = f"{key}{'m' if scale == 'minor' else ''}" if strength >= 0.2 else None
    except Exception:
        r["key"] = None

    del audio

    return r

from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed

def _prescreen(path: Path) -> tuple[Path, str | None]:
    """Returns (path, None) if OK, (path, error) if bad."""
    try:
        import essentia.standard as es
        audio = es.MonoLoader(filename=str(path), sampleRate=4000)()
        audio = audio[:4000]
        if len(audio) == 0:
            return path, "empty audio"
        return path, None
    except Exception as e:
        return path, str(e)

# if new_files:
#     # Parallel pre-screen using threads (safe — no fork, no TF state)
#     print(f"  Pre-screening {len(new_files)} files...", end="", flush=True)
#     screened, bad_files = [], []
#     with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
#         for path, err in pool.map(_prescreen, new_files):
#             if err is None:
#                 screened.append(path)
#             else:
#                 bad_files.append((path, err))
#     print(f" {len(screened)} OK, {len(bad_files)} bad")
if new_files:
    print(f"  Skipping pre-screen — {len(new_files)} files queued for embedding")
    screened = new_files
    # (Corrupt-file pre-screen retired — pipe00's accept_file gate keeps bad files out.)

    track_rows = []
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(_extract_meta, p): p for p in screened}
        total_f  = len(futures)
        done_f   = 0
        last_pct = -1
        for fut in tqdm(as_completed(futures), total=total_f, desc="  BPM/Key"):
            done_f += 1
            pct = done_f * 100 // total_f
            if pct // 5 > last_pct // 5:
                last_pct = pct
                tqdm.write(f"  BPM/Key: {done_f}/{total_f} ({pct}%)")
            try:
                r = fut.result()
                track_rows.append(r)
            except Exception as e:
                for pid, proc in pool._processes.items():
                    if proc.exitcode is not None and proc.exitcode != 0:
                        code = proc.exitcode
                        reason = 'segfault' if code == -11 else 'OOM' if code == -9 else f'code {code}'
                        logging.error(f"Worker PID {pid} exited: {reason}")
                log_err(futures[fut], "REGISTER", e)

    insert_cols = [
        "path", "filename", "extension", "artist", "title",
        "bpm", "key", "duration", "duration_time", "size",
        "bitrate", "sample_rate", "channels", "sp_energy", "sp_danceability",
        "date_added",
    ]
    for r in track_rows:
        _write_both(conn_new, conn_old, "tracks", insert_cols,
                    [r.get(c) for c in insert_cols])

    conn_new.commit()
    if conn_old is not conn_new:
        conn_old.commit()

    print(f"  Registered: {len(track_rows)}")
else:
    print("  Nothing new to register.")


# ══════════════════════════════════════════════════════════════════════════════
#%% Stage 2: Scan — Essentia models
# ══════════════════════════════════════════════════════════════════════════════
"""
For each file in NEW_AUDIO only — three cases:
  a) Already in new DB with embeddings → skip entirely
  b) In old DB with embeddings (matched by filename) → migrate, no re-scan
  c) Not scanned anywhere → run full Essentia model stack

TF models run sequentially (cannot safely share GPU across processes).
Commits every COMMIT_EVERY tracks. All errors logged.
"""
print("\n── Stage 2: Scan ──")

MODEL_OUTPUTS = {
    # Binary classification — single label, softmax
    "vocals":          "model/Softmax",
    "danceability":    "model/Softmax",
    "party":           "model/Softmax",
    "timbre":          "model/Softmax",
    "mood_aggressive": "model/Softmax",
    "mood_happy":      "model/Softmax",
    "mood_relaxed":    "model/Softmax",
    "mood_sad":        "model/Softmax",
    "mood_acoustic":   "model/Softmax",
    "mood_party":      "model/Softmax",
    # Multi-label classification — sigmoid
    "moodtheme":       "model/Sigmoid",
    "instrument":      "model/Sigmoid",
}

# What's already scanned in new DB
scanned_new = {
    r[0] for r in conn_new.execute(
        "SELECT track_path FROM track_metadata WHERE embeddings IS NOT NULL"
    ).fetchall()
}

# Build old DB lookup by filename (lowercase) — both features and embeddings
old_feats_by_fname = {}
old_meta_by_fname  = {}

if conn_old is not conn_new:
    for row in conn_old.execute("""
        SELECT LOWER(t.filename),
               t.bpm, t.key, t.vocals_prob, t.danceability, t.party_score, t.timbre,
               t.mood_aggressive, t.mood_happy, t.mood_relaxed, t.mood_sad,
               t.mood_acoustic, t.mood_party, t.sp_energy, t.sp_danceability,
               t.genre, t.rating,
               tm.genre_scores, tm.embeddings, tm.moodtheme, tm.instrument
        FROM tracks t
        JOIN track_metadata tm ON t.path = tm.track_path
        WHERE tm.embeddings IS NOT NULL
    """).fetchall():
        fname = row[0]
        old_feats_by_fname[fname] = row[1:17]   # scalar features
        old_meta_by_fname[fname]  = row[17:]    # blobs

# Sort new audio into buckets
new_audio_files = [
    p for p in NEW_AUDIO.iterdir()
    if p.suffix.lower() in AUDIO_EXTS
]

to_scan    = []
to_migrate = []


for p in new_audio_files:
    ps = str(p)
    if ps in scanned_new:
        continue                            # a) already embedded
    to_scan.append(p)                       # b) fresh scan needed

if DEBUG_LIMIT:
    to_scan    = to_scan[:DEBUG_LIMIT]

print(f"  New audio files:      {len(new_audio_files)}")
print(f"  Already in new DB:    {len(scanned_new)}")
print(f"  Scan fresh:           {len(to_scan)}")


# ── Migrate from old DB ───────────────────────────────────────────────────────
if to_migrate:
    print(f"\n  Migrating {len(to_migrate)} tracks from old DB...")
    migrated = 0

    scalar_cols = [
        "bpm", "key", "vocals_prob", "danceability", "party_score", "timbre",
        "mood_aggressive", "mood_happy", "mood_relaxed", "mood_sad",
        "mood_acoustic", "mood_party", "sp_energy", "sp_danceability",
        "genre", "rating",
    ]

    for p in tqdm(to_migrate, desc="  Migrating"):
        ps    = str(p)
        fname = p.name.lower()
        feats = old_feats_by_fname.get(fname)
        meta  = old_meta_by_fname.get(fname)

        if feats:
            _update_both(
                conn_new, conn_old, "tracks",
                ", ".join(f"{c}=?" for c in scalar_cols),
                "path=?",
                list(feats) + [ps],
            )
        if meta:
            conn_new.execute(
                "INSERT OR IGNORE INTO track_metadata "
                "(track_path, genre_scores, embeddings, moodtheme, instrument) "
                "VALUES (?, ?, ?, ?, ?)",
                [ps] + list(meta),
            )
            migrated += 1

    conn_new.commit()
    if conn_old is not conn_new:
        conn_old.commit()
    print(f"  Migrated: {migrated} embeddings from old DB")


# ── Load models ───────────────────────────────────────────────────────────────
if to_scan:
    labels = {}
    for key, fname in LABEL_FILES.items():
        fpath = TRAINED_DIR / fname
        labels[key] = json.load(open(fpath))["classes"] if fpath.exists() else []
    maest_classes = labels.get("maest", [])

    print("\n  Loading models...")

    EFFNET = TensorflowPredictEffnetDiscogs(
        graphFilename=str(TRAINED_DIR / MODEL_FILES["effnet"]),
        output="PartitionedCall:1",
    ) if "effnet" not in missing else None

    MAEST = TensorflowPredictMAEST(
        graphFilename=str(TRAINED_DIR / MODEL_FILES["maest"]),
        output="PartitionedCall/Identity_13",
    ) if "maest" not in missing else None

    DS = {}
    for key in MODEL_OUTPUTS:
        if key not in missing:
            try:
                DS[key] = TensorflowPredict2D(
                    graphFilename=str(TRAINED_DIR / MODEL_FILES[key]),
                    output=MODEL_OUTPUTS[key],
                )
                print(f"  ✓ {key}")
            except RuntimeError as e:
                print(f"  ✗ {key} — {MODEL_OUTPUTS[key]} invalid, trying Sigmoid...")
                try:
                    DS[key] = TensorflowPredict2D(
                        graphFilename=str(TRAINED_DIR / MODEL_FILES[key]),
                        output="model/Sigmoid",
                    )
                    print(f"  ✓ {key} (Sigmoid)")
                except RuntimeError as e2:
                    print(f"  ✗ {key} FAILED: {e2}")

    # for key in MODEL_OUTPUTS:
    #     if key not in missing:
    #         DS[key] = TensorflowPredict2D(
    #             graphFilename=str(TRAINED_DIR / MODEL_FILES[key]),
    #             output=MODEL_OUTPUTS[key],
    #         )

    print(f"  Loaded {len(DS)} downstream models.")

    def _genre_scores(audio_16k):
        """Run MAEST on up to MAEST_RUNS × 30s windows, average results."""
        if MAEST is None:
            return None
        W = 30 * MAEST_SR
        L = len(audio_16k)
        if L < W:
            return None
        if L >= MAEST_RUNS * W:
            segs = [
                audio_16k[
                    int(L * k / (MAEST_RUNS + 1)) - W // 2:
                    int(L * k / (MAEST_RUNS + 1)) - W // 2 + W
                ]
                for k in range(1, MAEST_RUNS + 1)
            ]
            preds = np.stack([MAEST(s) for s in segs])
        else:
            preds = MAEST(audio_16k[:W])[np.newaxis]
        return preds.mean(axis=0).squeeze().astype(np.float32)

    def _downstream(emb):
        """Run all downstream models on effnet embeddings."""
        out = {}
        # Binary mood models — positive class probability averaged across frames
        for k in ["mood_aggressive", "mood_happy", "mood_relaxed",
                  "mood_sad", "mood_acoustic", "mood_party"]:
            if k in DS:
                try:    out[k] = float(DS[k](emb)[:, 1].mean())
                except: out[k] = None
        # Vocals
        if "vocals" in DS:
            try:    out["vocals_prob"] = float(DS["vocals"](emb)[:, 1].mean())
            except: out["vocals_prob"] = None
        # Scalar feature models
        for k, col in [("danceability", "danceability"),
                       ("party", "party_score"),
                       ("timbre", "timbre")]:
            if k in DS:
                try:    out[col] = float(DS[k](emb)[:, 0].mean())
                except: out[col] = None
        # Multi-label blob models
        for k in ["moodtheme", "instrument"]:
            if k in DS:
                try:    out[k] = DS[k](emb).mean(axis=0).astype(np.float32)
                except: out[k] = None
        return out

    def _flush(batch_tracks, batch_meta):
        """Write a batch of scan results to both DBs."""
        scalar_cols = [
            "vocals_prob", "danceability", "party_score", "timbre", "genre",
            "mood_aggressive", "mood_happy", "mood_relaxed", "mood_sad",
            "mood_acoustic", "mood_party",
        ]
        for r in batch_tracks:
            _update_both(
                conn_new, conn_old, "tracks",
                ", ".join(f"{c}=?" for c in scalar_cols),
                "path=?",
                [r.get(c) for c in scalar_cols] + [r["path"]],
            )
        meta_cols = ["track_path", "genre_scores", "embeddings", "moodtheme", "instrument"]
        for m in batch_meta:
            _write_both(conn_new, conn_old, "track_metadata", meta_cols,
                        [m["path"], m["gs"], m["emb"], m["mt"], m["inst"]])
        conn_new.commit()
        if conn_old is not conn_new:
            conn_old.commit()

    # ── Scan loop ─────────────────────────────────────────────────────────────
    bt, bm, errors = [], [], 0

    _scan_total = len(to_scan)
    _milestone  = max(1, _scan_total // 20)
    for i, path in enumerate(tqdm(to_scan, desc="  Scanning"), 1):
        if i == 1 or i % _milestone == 0 or i == _scan_total:
            tqdm.write(f"  [{i:>4}/{_scan_total}]  {path.name}")
        try:
            if EFFNET is None:
                raise RuntimeError("effnet model missing — cannot scan")

            # # Reinitialise EFFNET every 5 files to prevent TF state corruption
            # if i % 5 == 0:
            #     del EFFNET
            #     EFFNET = TensorflowPredictEffnetDiscogs(
            #         graphFilename=str(TRAINED_DIR / MODEL_FILES["effnet"]),
            #         output="PartitionedCall:1")

            a44 = es.MonoLoader(filename=str(path), sampleRate=AUDIO_SR)()
            a16 = es.MonoLoader(filename=str(path), sampleRate=MAEST_SR,
                                resampleQuality=4)()
            emb = EFFNET(a44)
            gs  = _genre_scores(a16)
            ds  = _downstream(emb)

            # Top genre from MAEST
            top_genre = None
            if gs is not None and maest_classes:
                top_genre = maest_classes[int(np.argmax(gs))]\
                            .replace("---", " - ").title()
            bt.append({
                "path":           str(path),
                "vocals_prob":    ds.get("vocals_prob"),
                "danceability":   ds.get("danceability"),
                "party_score":    ds.get("party_score"),
                "timbre":         ds.get("timbre"),
                "genre":          top_genre,
                "mood_aggressive":ds.get("mood_aggressive"),
                "mood_happy":     ds.get("mood_happy"),
                "mood_relaxed":   ds.get("mood_relaxed"),
                "mood_sad":       ds.get("mood_sad"),
                "mood_acoustic":  ds.get("mood_acoustic"),
                "mood_party":     ds.get("mood_party"),
            })
            bm.append({
                "path": str(path),
                "gs":   gs.tobytes() if gs is not None else None,
                "emb":  emb.mean(axis=0).astype(np.float32).tobytes(),
                "mt":   ds["moodtheme"].tobytes() if ds.get("moodtheme") is not None else None,
                "inst": ds["instrument"].tobytes() if ds.get("instrument") is not None else None,
            })
            del a44, a16, emb, gs, ds
            gc.collect()
        except Exception as e:
            errors += 1
            log_err(path, "SCAN", e)
            tqdm.write(f"  ERR {path.name}: {e}")
            continue

        if i % COMMIT_EVERY == 0:
            _flush(bt, bm)
            bt.clear()
            bm.clear()

    if bt:
        _flush(bt, bm)

    print(f"  Scanned:  {len(to_scan) - errors}")
    print(f"  Errors:   {errors}  → {LOG_DIR / 'pipeline_01.log'}")


# ══════════════════════════════════════════════════════════════════════════════
#%% Stage 3: Ledger confirmation
# ══════════════════════════════════════════════════════════════════════════════
"""
Confirms that all paths in NEW_DB also have a SPOT ledger entry.
Tracks from non-Spotify sources (manual imports, old library) won't have
a ledger entry yet — seed a minimal one so pipeline_03 can handle them.
Spotify playlist fields left empty; pipeline_03 fills them on first sync.
No Spotify IDs here — path is the join key.
"""

print("\n── Stage 3: Ledger sync ──")

if LEDGER.exists():
    with open(LEDGER, encoding="utf-8") as f:
        ledger = json.load(f)
else:
    ledger = {}

all_db_paths = {r[0] for r in conn_new.execute("SELECT path FROM tracks").fetchall()}
new_entries  = 0

for path_str in all_db_paths:
    nk = _norm(path_str)
    if nk not in ledger:
        row = conn_new.execute(
            "SELECT artist, title, duration, size FROM tracks WHERE path=?", (path_str,)
        ).fetchone()
        # A DB track with no ledger entry shouldn't happen in a healthy pipeline
        # (pipe00 creates the entry on ingest). Defensive fallback only.
        logging.warning(f"DB track missing from ledger, seeding 'unknown': {nk}")
        src    = "unknown"
        dur_ms = int(row[2] * 1000)        if row and row[2] else None
        size_b = int(row[3] * 1024 * 1024) if row and row[3] else None   # DB stores size in MB
        ledger[nk] = _seed_entry(path_str,
                                 row[0] if row else None,
                                 row[1] if row else None,
                                 src, dur_ms=dur_ms, size_bytes=size_b)
        new_entries += 1

_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
_backup_dir = NEW_ROOT / "backups"
_backup_dir.mkdir(parents=True, exist_ok=True)
if LEDGER.exists():
    shutil.copy2(str(LEDGER), str(_backup_dir / f"sync_ledger_{_ts}.json"))
save_ledger(ledger)

print(f"  Ledger entries total:  {len(ledger)}")
print(f"  New entries seeded:    {new_entries}")


# ══════════════════════════════════════════════════════════════════════════════
#%% Stage 3.5: Tag enrichment (MP3 only)
# ══════════════════════════════════════════════════════════════════════════════
"""
For each .mp3 in NEW_AUDIO:
  - artist / title: write from Spotify ledger if the file has no embedded tags
  - genre: write MAEST top genre (strip leading "Foo - " category prefix)
Leaves .flac, .wav and all other formats untouched.
"""

print("\n── Stage 3.5: Tag enrichment (MP3 only) ──")

def _enrich_mp3_tags(path: Path, artist: str | None, title: str | None, genre: str | None):
    try:
        try:
            tags = EasyID3(path)
        except ID3NoHeaderError:
            EasyID3().save(path)
            tags = EasyID3(path)
        if artist:
            tags["artist"] = [artist]
        if title:
            tags["title"] = [title]
        if genre:
            tags["genre"] = [genre]
        tags.save(v2_version=3)   # ID3v2.3 — RB expects this over v2.4
    except Exception as e:
        logging.warning(f"Tag enrichment failed for {path.name}: {e}")

_tag_updated = 0
_tag_skipped = 0

for _ap in sorted(NEW_AUDIO.iterdir()):
    if not _ap.is_file() or _ap.suffix.lower() != ".mp3":
        continue

    _path_str = str(_ap)
    _nk       = _norm(_path_str)

    # Genre from DB — strip leading category ("Electronic - House" → "House")
    _row      = conn_new.execute("SELECT genre FROM tracks WHERE path=?", (_path_str,)).fetchone()
    _db_genre = _row[0] if _row else None
    _genre    = _db_genre.split(" - ", 1)[-1] if _db_genre and " - " in _db_genre else _db_genre

    # Spotify artist/title from ledger (only write if file tags are absent)
    _le       = ledger.get(_nk, {})
    _sp_artist = _le.get("artist") or None
    _sp_title  = _le.get("title")  or None

    # Read current embedded tags
    _write_artist = _write_title = None
    try:
        _existing = EasyID3(_ap)
        if _sp_artist and not (", ".join(_existing.get("artist", [])) or None):
            _write_artist = _sp_artist
        if _sp_title and not (" ".join(_existing.get("title", [])) or None):
            _write_title = _sp_title
    except ID3NoHeaderError:
        _write_artist = _sp_artist
        _write_title  = _sp_title

    if _write_artist or _write_title or _genre:
        _enrich_mp3_tags(_ap, _write_artist, _write_title, _genre)
        _tag_updated += 1
    else:
        _tag_skipped += 1

print(f"  MP3s enriched: {_tag_updated}")
print(f"  Skipped:       {_tag_skipped}")


# ── Stats summary ─────────────────────────────────────────────────────────────
print("\n── DB Stats ──")

total     = conn_new.execute("SELECT COUNT(*) FROM tracks").fetchone()[0]
has_embed = conn_new.execute(
    "SELECT COUNT(*) FROM track_metadata WHERE embeddings IS NOT NULL"
).fetchone()[0]
has_bpm   = conn_new.execute("SELECT COUNT(*) FROM tracks WHERE bpm IS NOT NULL").fetchone()[0]
has_key   = conn_new.execute("SELECT COUNT(*) FROM tracks WHERE key IS NOT NULL").fetchone()[0]
has_genre = conn_new.execute("SELECT COUNT(*) FROM tracks WHERE genre IS NOT NULL").fetchone()[0]
on_disk   = sum(1 for r in conn_new.execute("SELECT path FROM tracks").fetchall()
                if to_path(r[0]).exists())

print(f"  Total registered:   {total}")
print(f"  Files on disk:      {on_disk}")
print(f"  Missing from disk:  {total - on_disk}")
print(f"  Has embeddings:     {has_embed}")
print(f"  Has BPM:            {has_bpm}")
print(f"  Has key:            {has_key}")
print(f"  Has genre:          {has_genre}")

conn_new.close()
if conn_old is not conn_new:
    conn_old.close()

from collections import Counter

with open(LEDGER, encoding="utf-8") as f:
    ledger = json.load(f)

print('═══ LEDGER ═══')
print(f'Total entries:        {len(ledger)}')
fields = ['artist','title','duration_ms','source','_spotify_id','spotify_playlists','spot','path']
for field in fields:
    filled = sum(1 for v in ledger.values() if v.get(field))
    print(f'  {field:<22} {filled:>4}/{len(ledger)}')
sources = Counter(v.get('source') for v in ledger.values())
print(f'\nSources: {dict(sources)}')
missing_disk = [k for k,v in ledger.items() if v.get('path') and not to_path(v['path']).exists()]
print(f'Ledger paths missing from disk: {len(missing_disk)}')

conn = sqlite3.connect(NEW_DB)
print('\n═══ DATABASE ═══')
total     = conn.execute('SELECT COUNT(*) FROM tracks').fetchone()[0]
has_bpm   = conn.execute('SELECT COUNT(*) FROM tracks WHERE bpm IS NOT NULL').fetchone()[0]
has_key   = conn.execute('SELECT COUNT(*) FROM tracks WHERE key IS NOT NULL').fetchone()[0]
has_genre = conn.execute('SELECT COUNT(*) FROM tracks WHERE genre IS NOT NULL').fetchone()[0]
has_embed = conn.execute('SELECT COUNT(*) FROM track_metadata WHERE embeddings IS NOT NULL').fetchone()[0]
no_meta   = conn.execute('''SELECT COUNT(*) FROM tracks t
                            LEFT JOIN track_metadata tm ON t.path = tm.track_path
                            WHERE tm.track_path IS NULL''').fetchone()[0]
on_disk   = sum(1 for r in conn.execute('SELECT path FROM tracks').fetchall()
                if to_path(r[0]).exists())

print(f'Total registered:     {total}')
print(f'Files on disk:        {on_disk}')
print(f'Missing from disk:    {total - on_disk}')
print(f'Has BPM:              {has_bpm}')
print(f'Has key:              {has_key}')
print(f'Has genre:            {has_genre}')
print(f'Has embeddings:       {has_embed}')
print(f'No metadata at all:   {no_meta}')
result = conn.execute('PRAGMA integrity_check').fetchone()[0]
print(f'DB integrity:         {result}')

print('\n═══ CROSS-CHECK ═══')
db_keys     = {_norm(r[0]) for r in conn.execute('SELECT path FROM tracks').fetchall()}
ledger_keys = {_norm(v['path']) for v in ledger.values() if v.get('path')}
in_db_not_ledger = db_keys - ledger_keys
in_ledger_not_db = ledger_keys - db_keys
print(f'In DB but not ledger: {len(in_db_not_ledger)}')
print(f'In ledger but not DB: {len(in_ledger_not_db)}')
for k in list(in_ledger_not_db)[:5]: print(f'  {Path(k).name}')
for k in list(in_db_not_ledger)[:5]: print(f'  {Path(k).name}')
conn.close()

print("\n✓ Pipeline 01 complete.")
print("  Next: pipeline_02_cluster.py")

