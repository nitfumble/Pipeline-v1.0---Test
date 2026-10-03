#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pipeline_03_tag.py
==================
Stage 3 — assign tags, sync RB ↔ Spotify, update SPOT ledger.

Execution order:
  pipeline_00  →  pipeline_01  →  [manual RB import]  →  pipeline_02  →  pipeline_03

Cells:
  1  Rename RB taxonomy (no creates / deletes — rename only)
  2  Classify and assign tags:
       a) Load all data (RB, Spotify, ledger, music DB) in correct order
       b) Path diagnostic — confirm RB↔DB↔ledger paths match
       c) Auto tags (Genre, Vibe, Components) from ML features, preserved thereafter unless --overwrite
       d) Manual tags (Floor, Set Position, Flags) — authority-resolved, never lost
       e) Apply to RB DB, update Spotify playlists, save ledger
  3  Verify RB DB integrity

TAG AUTHORITY — ledger["spot"] is the single source of truth:
  spot = what RB and Spotify last agreed on (set by pipeline_03)

  First run (spot=None):
    RB has tags  → RB wins  (preserve hand-curated state)
    RB no tags   → Spotify seeds from live API membership

  Subsequent runs:
    Only RB changed   → RB wins → update Spotify + ledger
    Only SP changed   → SP wins → update RB + ledger
    Both changed      → RB wins → update Spotify + ledger
    Neither changed   → skip entirely (no writes)

  Track NOT in RB (not yet imported / path changed):
    → ledger updated with cur_sp_tags, applied on next run after reimport

  Non-Spotify tracks (no _spotify_id):
    → SP side always empty → RB is always authority
    → Spotify API never touched for these tracks
    → Tags preserved in RB and ledger only

TAG PRESERVATION:
  Auto tags (Genre, Vibe, Comp-*) written from ML, preserved thereafter unless --overwrite.
  Manual tags (Floor, Set Position, Flag-*) saved before clear, restored after,
  then authority-resolved. Manual tags are NEVER lost between runs.

Reads from:  music.db (ML features, read-only), ledger, RB DB, Spotify API
Writes to:   RB DB (tags only), Spotify API (SP tracks only), ledger (spot + last_sync)
             music.db is NEVER written to in this pipeline.

IMPORTANT:
  - _spotify_id is metadata for Spotify API calls only — never a join key.
  - RB must be CLOSED when running Cells 1 and 2.

Args:
  --dry-run          Preview only — no writes to RB, Spotify, or ledger
  --apply-taxonomy   Apply Cell 1 taxonomy renames to RB (default: dry-run Cell 1)
  --no-overwrite     Skip auto-tag clear (additive mode)
  --commit-every N   Commit to RB DB every N tracks (default: 500)
"""

import json, uuid, shutil, time, logging, argparse
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict, Counter

from sqlalchemy import func
import numpy as np

import spotipy as sp
from spotipy.oauth2 import SpotifyOAuth
from spotipy.cache_handler import CacheFileHandler

import pyrekordbox
from pyrekordbox import update_config
from pyrekordbox.db6 import tables

# Canonical path handling + atomic writes — single source of truth (dj_paths.py).
# Aliased so existing _norm()/_wsl_path() call sites keep working.
from config import load
from dj_paths import (
    to_key as _norm,
    to_posix as _wsl_path,
    to_win,
    to_path,
    db_variants,
    atomic_write_json,
)

# ══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════════════════
# ── Load config — single source of truth (config.py / config.json) ────────────
cfg = load()
_rb = cfg.rekordbox

NEW_ROOT  = cfg.paths.library_root
NEW_AUDIO = cfg.paths.audio
MUSIC_DB  = cfg.paths.music_db
LOG_DIR   = cfg.paths.logs
LEDGER    = cfg.paths.ledger
BIN_DIR   = cfg.paths.bin
DB_DIR    = cfg.paths.db
TMP_DIR   = cfg.paths.tmp
BACKUP_DIR = cfg.paths.backups

# Rekordbox (WSL path to the Windows master.db)
RB_ENABLED          = _rb.enabled
RB_DB_PATH          = _rb.master_db
RECENTLY_ADDED_DAYS = _rb.recently_added_days

# Essentia model label files: configured dir, else repo-bundled models/ (git).
_cfg_models = to_path(cfg.embed.model_dir) if cfg.embed.model_dir else None
TRAINED_DIR = _cfg_models if (_cfg_models and _cfg_models.exists()) \
              else (Path(__file__).resolve().parent / "models")

# ── Args ──────────────────────────────────────────────────────────────────────
_parser = argparse.ArgumentParser(description="Pipeline 03 — Tag & Sync")
_parser.add_argument("--dry-run",        action="store_true", help="Preview only — no writes to RB, Spotify, or ledger")
_parser.add_argument("--apply-taxonomy", action="store_true", help="Bootstrap: rename your empty RB MyTags to the config tag names")
_parser.add_argument("--overwrite",      action="store_true", help="Re-classify ALL tracks' auto tags (default: only newly-added tracks)")
_parser.add_argument("--m3u8",           action="store_true", help="Skip Rekordbox; export each tag as playlists/flags/<tag>.m3u8 from the ledger")
_parser.add_argument("--commit-every",   type=int, default=_rb.commit_every, metavar="N", help="Commit to RB every N tracks")
_parser.add_argument("--no-interactive", action="store_true", help="Skip all interactive prompts (used when called from web UI)")
_args = _parser.parse_args()

DRY_RUN       = _args.dry_run
DRY_RUN_CELL1 = not _args.apply_taxonomy
OVERWRITE     = _args.overwrite             # default False -> auto tags only for newly-added tracks
COMMIT_EVERY  = _args.commit_every
M3U8_MODE     = _args.m3u8 or (not RB_ENABLED)   # no-RB fallback: export tags as m3u8
FLAGS_PLAYLIST_DIR = cfg.paths.playlists / "flags"

SPOTIFY_CLIENT_ID     = cfg.secrets["spotify"]["client_id"]
SPOTIFY_CLIENT_SECRET = cfg.secrets["spotify"]["client_secret"]
SPOTIFY_REDIRECT_URI  = cfg.spotify.redirect_uri
SPOTIFY_SCOPES        = cfg.spotify.scopes
SPOTIFY_CACHE         = str(cfg.spotify.token_cache)

# pyrekordbox needs the Pioneer app dir (…/Pioneer) — derive from the master.db path.
RB_APP_DIR = str(Path(RB_DB_PATH).parent.parent) if RB_DB_PATH else ""
RB_INSTALL = _rb.install_dir if "install_dir" in _rb._d else ""

# ── Manual-tag taxonomy (FROM CONFIG) ─────────────────────────────────────────
# Synced manual tags <-> Spotify [RB] playlists.
PLAYLIST_TO_TAG  = cfg.playlist_tag_map()
TAG_TO_PLAYLIST  = cfg.tag_playlist_map()
SYNCED_RB_TAGS   = set(PLAYLIST_TO_TAG.values())
ALL_RB_PLAYLISTS = cfg.tag_playlists()

def _mtags(group):
    g = cfg.tags._d["manual_groups"].get(group, {})
    pre = g.get("prefix", "")
    return [f"{pre}{t['name']}" for t in g.get("tags", [])]

# Every manual tag is NEVER auto-cleared (authority-resolved).
EXTRAS_MANUAL    = set(_mtags("Floor")) | set(_mtags("Flag"))
SET_POSITION_TAGS = _mtags("Set Position")
SET_POSITION_SET  = set(SET_POSITION_TAGS)

# ── Auto-classified taxonomy (model label space — stays in code) ──────────────
GENRE_TAGS = [
    "Techno","House","Trance","Tech House","Hard Techno","Electro","Breakbeat",
    "Hardcore","Hard Trance","Acid","Hard House","Jungle","Deep House",
    "Drum N Bass","Progressive House","Euro House","UK Garage","Tribal",
    "Progressive Trance","Breaks","Dubstep","Bassline","Electro House","Minimal",
    "Tech Trance","Downtempo","Disco","Speed Garage","Garage House","Synth-Pop",
    "Deep Techno","Donk","Experimental","Acid House","Psy-Trance","Dance-Pop",
    "Ambient","Eurodance","Hardstyle","Happy Hardcore","Schranz","Tribal House",
    "Nu-Disco","Grime","Rock","Funk / Soul","Pop","Hip Hop","Reggae","Others",
]
VIBE_TAGS = [
    "Chill","Groovy","Driving","Peak",
    "Dark","Euphoric","Aggressive","Hypnotic",
    "Uplifting","Melancholic","Deep","Dramatic",
]
COMP_TAGS = [
    "Comp - Acid Line","Comp - Arpeggio","Comp - Beat","Comp - Brass",
    "Comp - Instrumental","Comp - Organic","Comp - Percussion Heavy",
    "Comp - Piano / Keys","Comp - Strings","Comp - Sub Bass Heavy",
    "Comp - Synth Lead","Comp - Vocal","Comp - Wobble Bass",
]
# Extras parent group = manual Floor/Flag (from config) + auto Comp + recency flag.
EXTRAS_TAGS = _mtags("Floor") + ["Flag - Recently Added"] + _mtags("Flag") + COMP_TAGS

GROUP_MAP = {
    "Genre":        ("Genre",        GENRE_TAGS),
    "Set Position": ("Set Position", SET_POSITION_TAGS),
    "Vibe":         ("Vibe",         VIBE_TAGS),
    "Extras":       ("Extras",       EXTRAS_TAGS),
}

# ── Classification config (used by Cell 2) ────────────────────────────────────
ELECTRONIC_GENRES = [
    "Techno","House","Trance","Tech House","Hard Techno","Electro","Breakbeat",
    "Hardcore","Hard Trance","Acid","Hard House","Jungle","Deep House",
    "Drum N Bass","Progressive House","Euro House","UK Garage","Tribal",
    "Progressive Trance","Breaks","Dubstep","Bassline","Electro House","Minimal",
    "Tech Trance","Downtempo","Disco","Speed Garage","Garage House","Synth-Pop",
    "Deep Techno","Donk","Experimental","Acid House","Psy-Trance","Dance-Pop",
    "Ambient","Eurodance","Hardstyle","Happy Hardcore","Schranz","Tribal House",
    "Nu-Disco","Grime",
]
NON_ELECTRONIC_PARENTS = ["Rock","Funk / Soul","Pop","Hip Hop","Reggae"]
ELEC_THRESH    = 0.15
NONELEC_THRESH = 0.05
VIBE_DANCE_T   = {"chill": 0.55, "groovy": 0.68, "driving": 0.78}
MOOD_T = {
    "party_p25": 0.108, "party_p50": 0.228, "party_p75": 0.387,
    "sad_p50":   0.101, "sad_p75":   0.132,
    "aggressive_p50": 0.773, "aggressive_p75": 0.869, "aggressive_p90": 0.929,
    "relaxed_p75": 0.768,
}
INST = {
    "vocal": 0.50, "instrumental": 0.20, "sub_bass": 0.25, "percussion": 0.15,
    "beat":  0.12, "organic": 0.12, "strings": 0.03, "brass": 0.02,
    "arpeggio": 0.32, "acid_synth": 0.50, "acid_bass": 0.20, "acid_genre": 0.08,
    "wobble_bass": 0.25, "wobble_genre": 0.03,
}
INST_MAP = {
    "Comp - Sub Bass Heavy":   (["bass"],                              INST["sub_bass"]),
    "Comp - Percussion Heavy": (["drums","drummachine","percussion"],  INST["percussion"]),
    "Comp - Beat":             (["beat","drummachine"],                INST["beat"]),
    "Comp - Strings":          (["strings","violin","cello","viola"],  INST["strings"]),
    "Comp - Brass":            (["brass","trumpet","trombone"],        INST["brass"]),
    "Comp - Organic":          (["acousticguitar","guitar"],           INST["organic"]),
}

# ── Init ──────────────────────────────────────────────────────────────────────
RB_BACKUP_DIR = NEW_ROOT / "backups" / "rb_db"
RB_BACKUP_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "pipeline_03.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
print("=== Pipeline 03: Tag & Sync ===")
print(f"  MUSIC_DB:   {MUSIC_DB}")
print(f"  RB_DB:      {RB_DB_PATH}")
print(f"  LEDGER:     {LEDGER}")
print(f"  DRY_RUN={DRY_RUN}  OVERWRITE={OVERWRITE}  COMMIT_EVERY={COMMIT_EVERY}")


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

# Path helpers (_norm = dj_paths.to_key, _wsl_path = dj_paths.to_posix) are
# imported above. The One Touch external-drive alias is intentionally dropped.

def _open_rb_db():
    """Open master.db, self-healing from the newest backup if it's corrupt/unreadable."""
    try:
        return pyrekordbox.Rekordbox6Database(path=RB_DB_PATH)
    except Exception as e:
        backups = sorted(RB_BACKUP_DIR.glob("master_*.db"),
                         key=lambda p: p.stat().st_mtime, reverse=True)
        if not backups:
            raise SystemExit(f"✗ master.db unreadable and no backup to restore: {e}")
        print(f"  !! master.db unreadable ({e}) — restoring backup {backups[0].name}")
        logging.error(f"master.db corrupt; restored {backups[0]}")
        shutil.copy2(str(backups[0]), str(RB_DB_PATH))
        return pyrekordbox.Rekordbox6Database(path=RB_DB_PATH)

def load_ledger() -> dict:
    if LEDGER.exists():
        try:
            with open(LEDGER, encoding="utf-8") as f:
                raw = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            # Self-heal: ledger unreadable/corrupt → restore the newest good backup.
            backups = sorted(BACKUP_DIR.glob("sync_ledger_*.json"),
                             key=lambda p: p.stat().st_mtime, reverse=True)
            if not backups:
                raise SystemExit(f"✗ Ledger corrupt and no backup to restore: {e}")
            print(f"  !! Ledger unreadable ({e}) — restoring backup {backups[0].name}")
            logging.error(f"Ledger corrupt; restored {backups[0]}")
            shutil.copy2(str(backups[0]), str(LEDGER))
            with open(LEDGER, encoding="utf-8") as f:
                raw = json.load(f)
        # Normalize keys to e:/… form (legacy entries may use /mnt/e/…).
        return {_norm(k): v for k, v in raw.items()}
    return {}

TMP_DIR = NEW_ROOT / "tmp"
TMP_DIR.mkdir(parents=True, exist_ok=True)

def save_ledger(ledger: dict):
    atomic_write_json(LEDGER, ledger)

def _canon_entry(path: str, source: str) -> dict:
    """Canonical ledger entry (mirrors pipe00.make_entry / pipe01._seed_entry).
    Used only when a DB+RB track somehow has no ledger entry — source defaults
    to ytdlp so the track stays eligible for HQ re-download."""
    p = Path(path)
    return {
        "path": str(p), "path_win": to_win(p),
        "artist": "", "title": p.stem, "duration_ms": None, "size_bytes": None,
        "fingerprint": None, "size_suspicious": False,
        "source": source, "original_source": source,
        "spotify_track_id": None, "_spotify_id": None,
        "spotify_playlists": [], "spot": None,
        "validated": None, "validated_at": None, "last_sync": None, "likely_dup_of": None,
    }


# ── Spotify helpers ───────────────────────────────────────────────────────────
def _sp_client() -> sp.Spotify:
    return sp.Spotify(auth_manager=SpotifyOAuth(
        client_id=SPOTIFY_CLIENT_ID,
        client_secret=SPOTIFY_CLIENT_SECRET,
        scope=["user-library-read", "playlist-read-private",
               "playlist-modify-public", "playlist-modify-private"],
        redirect_uri="https://example.com/callback",
        cache_handler=CacheFileHandler(cache_path=SPOTIFY_CACHE),
    ))

def _sp_call(fn, *args, retries: int = 5, **kwargs):
    """Retry Spotify API calls on 429 rate-limit, respecting Retry-After."""
    for attempt in range(retries):
        try:
            return fn(*args, **kwargs)
        except sp.exceptions.SpotifyException as e:
            if e.http_status == 429:
                wait = int(getattr(e, "headers", {}).get("Retry-After", 5))
                logging.warning(f"Spotify rate limit — waiting {wait}s (attempt {attempt+1})")
                time.sleep(wait)
            else:
                raise
    raise RuntimeError("Spotify API retries exhausted")

def _sp_all_playlists(client) -> dict:
    out, offset = {}, 0
    while True:
        page = _sp_call(client.current_user_playlists, limit=50, offset=offset)
        for p in page["items"]:
            out[p["name"]] = p["id"]
        if not page["next"]:
            break
        offset += 50
    return out

def _sp_all_items(client, playlist_id: str) -> list:
    results = _sp_call(
        client.playlist_items, playlist_id,
        fields="items(track(id,name,artists,duration_ms)),next",
        limit=100,
    )
    items = []
    while results:
        items.extend(results["items"])
        results = _sp_call(client.next, results) if results["next"] else None
    return items

def _sp_add(client, pl_id: str, ids: list):
    for i in range(0, len(ids), 100):
        _sp_call(client.playlist_add_items, pl_id, ids[i:i+100])

def _sp_remove(client, pl_id: str, ids: list):
    for i in range(0, len(ids), 100):
        _sp_call(client.playlist_remove_all_occurrences_of_items, pl_id, ids[i:i+100])


# ══════════════════════════════════════════════════════════════════════════════
#%% m3u8 export mode (no Rekordbox) + RB-open safety
# ══════════════════════════════════════════════════════════════════════════════
def _export_tags_to_m3u8():
    """No-RB fallback: write playlists/flags/<tag>.m3u8 from the ledger's tags so
    flags stay usable without Rekordbox."""
    led = load_ledger()
    by_tag: dict = {}
    for e in led.values():
        for tag in (e.get("spot") or []):
            by_tag.setdefault(tag, []).append(e)
    FLAGS_PLAYLIST_DIR.mkdir(parents=True, exist_ok=True)
    written = 0
    for tag, entries in sorted(by_tag.items()):
        safe = tag.replace("/", "-").replace("\\", "-").strip()
        dst  = FLAGS_PLAYLIST_DIR / f"{safe}.m3u8"
        out  = ["#EXTM3U"]
        for e in entries:
            p = e.get("path_win") or to_win(e.get("path", ""))
            if not p:
                continue
            label = f"{e.get('artist','')} - {e.get('title','')}".strip(" -")
            out.append(f"#EXTINF:-1,{label}")
            out.append(p)
        tmp = dst.with_name(dst.name + ".tmp")
        tmp.write_text("\n".join(out) + "\n", encoding="utf-8")
        tmp.replace(dst)                       # same-dir atomic swap
        written += 1
    return written, len(by_tag)

def _rekordbox_running() -> bool:
    """Best-effort: detect a running Rekordbox (Windows) from WSL via tasklist.exe."""
    try:
        import subprocess
        out = subprocess.run(["tasklist.exe"], capture_output=True, text=True, timeout=10).stdout.lower()
        return "rekordbox.exe" in out
    except Exception:
        return False        # can't tell → don't block

if M3U8_MODE:
    print("\n── m3u8 export mode (Rekordbox skipped) ──")
    _n, _t = _export_tags_to_m3u8()
    print(f"  ✓ Wrote {_n} playlists ({_t} tags) → {FLAGS_PLAYLIST_DIR}")
    print("  (Rekordbox disabled or --m3u8 passed — no RB/Spotify/ledger writes.)")
    raise SystemExit(0)

# RB-open safety: writing master.db while Rekordbox is open corrupts it.
if not DRY_RUN and _rekordbox_running():
    raise SystemExit(
        "✗ Rekordbox appears to be running — it locks master.db and writing now would "
        "corrupt it. Close Rekordbox and re-run (or use --dry-run to preview)."
    )


# ══════════════════════════════════════════════════════════════════════════════
#%% Cell 1: Rename RB taxonomy
# ══════════════════════════════════════════════════════════════════════════════
"""
Renames tag slots in RB to match our taxonomy by position.
Never creates or deletes DjmdMyTag rows — only UPDATE statements.
Preserves RB-native IDs and UUIDs required for Track Filter panel.
Only needs to run when the taxonomy changes. Use --apply-taxonomy to apply.
"""

try:
    update_config(pioneer_install_dir=RB_INSTALL or None, pioneer_app_dir=RB_APP_DIR or None)
except Exception as _e:
    logging.warning(f"pyrekordbox update_config best-effort failed: {_e}")
rb_db = _open_rb_db()

def _gate_tag_slots():
    """Hard precondition: each RB parent group must have >= the required number of
    MyTag slots. If not, instruct the user, DISCONNECT (release the lock so they can
    edit RB), and prompt to retry. The pipeline does not proceed until satisfied."""
    global rb_db
    while True:
        missing = []
        for rb_name, (new_name, target_tags) in GROUP_MAP.items():
            grp = (rb_db.session.query(tables.DjmdMyTag).filter_by(Name=rb_name, Attribute=1).first()
                   or rb_db.session.query(tables.DjmdMyTag).filter_by(Name=new_name, Attribute=1).first())
            need = len(target_tags)
            have = rb_db.session.query(tables.DjmdMyTag).filter_by(ParentID=grp.ID).count() if grp else 0
            if not grp:
                missing.append((rb_name, "no group", need, need))
            elif have < need:
                missing.append((rb_name, have, need, need - have))
        if not missing:
            return
        print("\n" + "═" * 64)
        print("  ✗ Rekordbox is missing MyTag slots. Create these EMPTY tags in")
        print("    Rekordbox (right-click a parent group → add tag), then continue:")
        for name, have, need, add in missing:
            print(f"      • {name:<14} add {add:>2} empty tag(s)   (have {have}, need {need})")
        print("═" * 64)
        rb_db.close()        # release the master.db lock so the user can edit RB
        if _args.no_interactive:
            raise SystemExit(
                "✗ Missing Rekordbox MyTag slots — cannot continue in UI mode.\n"
                "  Add the missing empty tags in Rekordbox, close it, then re-run pipeline_03."
            )
        ans = input("\n  Added the tags and CLOSED Rekordbox? (Y = retry / N = abort): ").strip().lower()
        if ans != "y":
            raise SystemExit("Aborted — add the required empty MyTags and re-run.")
        rb_db = _open_rb_db()    # reconnect & re-check

if not DRY_RUN:
    _gate_tag_slots()

total_changes, errors = 0, []
print("\n── Cell 1: Rename taxonomy ──")

for rb_name, (new_name, target_tags) in GROUP_MAP.items():
    group = rb_db.session.query(tables.DjmdMyTag)\
                 .filter_by(Name=rb_name, Attribute=1).first()
    if not group:
        errors.append(f"GROUP NOT FOUND: '{rb_name}'")
        continue

    children = rb_db.session.query(tables.DjmdMyTag)\
                    .filter_by(ParentID=group.ID)\
                    .order_by(tables.DjmdMyTag.Seq).all()

    if len(target_tags) > len(children):
        errors.append(
            f"NOT ENOUGH SLOTS in '{rb_name}': "
            f"need {len(target_tags)}, have {len(children)}"
        )

    if rb_name != new_name:
        if not DRY_RUN_CELL1:
            group.Name = new_name
            group.updated_at = datetime.now()
        total_changes += 1

    for i, child in enumerate(children):
        if i < len(target_tags):
            nn = target_tags[i]
            if child.Name != nn:
                prefix = "  [DRY" if DRY_RUN_CELL1 else "  ["
                print(f"{prefix}{i+1:>2}] '{child.Name}' → '{nn}'")
                if not DRY_RUN_CELL1:
                    child.Name = nn
                    child.Seq  = i + 1
                    child.updated_at = datetime.now()
                total_changes += 1
        else:
            spare = f"_spare_{i+1}"
            if not child.Name.startswith("_spare") and not DRY_RUN_CELL1:
                child.Name = spare
                child.Seq  = i + 1
                child.updated_at = datetime.now()

if not DRY_RUN_CELL1:
    rb_db.session.commit()
    print(f"  ✓ {total_changes} renames committed.")
else:
    print(f"  [DRY RUN] {total_changes} renames pending. Set DRY_RUN_CELL1=False to apply.")

for e in errors:
    print(f"  !! {e}")


# ══════════════════════════════════════════════════════════════════════════════
#%% Cell 2: Classify, assign tags, sync Spotify, update ledger
# ══════════════════════════════════════════════════════════════════════════════
"""
Execution order within Cell 2:
  1. Flags (DRY_RUN, OVERWRITE) + backup
  2. Load class labels + build genre indices + classification functions
  3. Load RB maps: tag map, content map (path→ID), synced tags by path
  4. Load Spotify: client, playlist IDs, live track membership
  5. Load ledger
  6. Load music DB (rows with ML features)
  7. Path diagnostic — verify RB↔DB↔ledger paths all normalise to same key
  8. USN counter
  9. Selective clear — save manual tags, wipe auto tags, restore manual
 10. Build existing_assignments set (skip duplicate writes)
 11. Main loop: auto tags + manual authority resolution per track
 12. Commit RB changes
 13. Apply Spotify playlist changes (batch)
 14. Save ledger
 15. Report
"""

# ── 1. Flags + backup ─────────────────────────────────────────────────────────

if not DRY_RUN:
    rb_path = Path(RB_DB_PATH)
    _ts     = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup  = RB_BACKUP_DIR / f"master_{_ts}.db"
    shutil.copy2(str(rb_path), str(backup))
    if backup.stat().st_size != rb_path.stat().st_size:
        raise RuntimeError(f"Backup size mismatch — aborting. Check {backup}")
    # Rotate — keep last 10
    _old = sorted(RB_BACKUP_DIR.glob("master_*.db"), key=lambda f: f.stat().st_mtime)
    for _f in _old[:-10]:
        try: _f.unlink()
        except Exception: pass
    print(f"\n  ✓ Backup: {backup.name}  ({backup.stat().st_size // 1024} KB)")

    # Ledger backup — cheap insurance before any live ledger write.
    if LEDGER.exists():
        _led_bak = NEW_ROOT / "backups" / f"sync_ledger_{_ts}.json"
        _led_bak.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(str(LEDGER), str(_led_bak))
        print(f"  ✓ Ledger backup: {_led_bak.name}")

print(f"\n── Cell 2: {'[DRY RUN] ' if DRY_RUN else ''}Classify & sync ──")

# ── 2. Class labels + genre indices + classification functions ─────────────────
with open(TRAINED_DIR / "discogs-maest-30s-pw-519l-2.json") as f:
    maest_classes = json.load(f)["classes"]
with open(TRAINED_DIR / "mtg_jamendo_instrument-discogs-effnet-1.json") as f:
    instrument_classes = json.load(f)["classes"]

elec_genre_idx = {}
for genre in ELECTRONIC_GENRES:
    for i, cls in enumerate(maest_classes):
        if cls.lower() == f"electronic---{genre}".lower():
            elec_genre_idx[genre] = i; break
    if genre not in elec_genre_idx:
        norm_g = genre.lower().replace(" ", "").replace("-", "")
        for i, cls in enumerate(maest_classes):
            if cls.startswith("Electronic---"):
                cn = cls.replace("Electronic---","").lower().replace(" ","").replace("-","")
                if cn == norm_g:
                    elec_genre_idx[genre] = i; break

nonelec_genre_idx = {
    p: [i for i, cls in enumerate(maest_classes)
        if cls.split("---")[0].strip().lower() == p.lower()]
    for p in NON_ELECTRONIC_PARENTS
}
print(f"  Genres mapped: {len(elec_genre_idx)}/{len(ELECTRONIC_GENRES)}")


def decode(blob):
    return np.frombuffer(bytes(blob), dtype=np.float32) if blob else None

def classify_genre(gs):
    tags = []
    for g, idx in elec_genre_idx.items():
        if float(gs[idx]) >= ELEC_THRESH:
            tags.append(g)
    ne = [p for p, idxs in nonelec_genre_idx.items()
          if idxs and max(gs[i] for i in idxs) >= NONELEC_THRESH]
    tags.extend(ne)
    if not tags:
        tags.append("Others")
    return tags

def classify_vibe(party_score, sp_dance, bpm, mood_agg, mood_rel, mood_sad, mood_party):
    tags, scores, weights = [], [], []
    if party_score is not None: scores.append(party_score);                          weights.append(0.50)
    if sp_dance    is not None: scores.append(sp_dance);                             weights.append(0.35)
    if bpm and bpm > 0:         scores.append(float(np.clip((bpm-80)/100, 0, 1)));  weights.append(0.15)
    if scores:
        v = float(np.average(scores, weights=weights[:len(scores)]))
        if   v < VIBE_DANCE_T["chill"]:   tags.append("Chill")
        elif v < VIBE_DANCE_T["groovy"]:  tags.append("Groovy")
        elif v < VIBE_DANCE_T["driving"]: tags.append("Driving")
        else:                              tags.append("Peak")

    p = mood_party or 0.0; s = mood_sad or 0.0
    a = mood_agg   or 0.0; r = mood_rel or 0.0
    bv = bpm       or 0.0; mt = []

    if p >= MOOD_T["party_p75"]:                                       mt.append("Euphoric")
    if s >= MOOD_T["sad_p75"]:                                         mt.append("Melancholic")
    if a >= MOOD_T["aggressive_p90"]:                                  mt.append("Aggressive")
    if a >= MOOD_T["aggressive_p75"] and p < MOOD_T["party_p25"]:     mt.append("Dramatic")
    if r >= MOOD_T["relaxed_p75"]    and p < MOOD_T["party_p50"]:     mt.append("Hypnotic")
    if a >= MOOD_T["aggressive_p50"] and p < MOOD_T["party_p25"] \
                                      and s >= MOOD_T["sad_p50"]:     mt.append("Dark")
    if p >= MOOD_T["party_p75"]      and bv > 128:                    mt.append("Uplifting")
    if r >= MOOD_T["relaxed_p75"]    and s >= MOOD_T["sad_p50"]:      mt.append("Deep")

    if not mt:
        cands = {
            "Euphoric":    p / 0.387,
            "Melancholic": s / 0.132,
            "Aggressive":  a / 0.929,
            "Dramatic":    (a / 0.869) * (1 - p / 0.387),
            "Hypnotic":    (r / 0.768) * (1 - p / 0.228),
            "Dark":        (a / 0.773) * (1 - p / 0.108),
            "Uplifting":   (p / 0.387) * (1.2 if bv > 128 else 0.6),
            "Deep":        (r / 0.768) * (s / 0.101),
        }
        mt.append(max(cands, key=cands.get))

    tags.extend(mt)
    return tags

def classify_components(vp, inst_scores, gs):
    tags = []
    v = float(vp) if vp is not None else 0.5
    if v >= INST["vocal"]:        tags.append("Comp - Vocal")
    if v <  INST["instrumental"]: tags.append("Comp - Instrumental")

    if inst_scores is not None:
        cl = [c.lower() for c in instrument_classes]
        def gi(n):
            try:    return cl.index(n.lower())
            except: return None
        def mi(names):
            idxs = [gi(n) for n in names if gi(n) is not None]
            return float(np.mean([inst_scores[i] for i in idxs])) if idxs else 0.0

        for tag, (src, thr) in INST_MAP.items():
            if mi(src) >= thr:
                tags.append(tag)

        syn  = mi(["synthesizer"]); comp = mi(["computer","sampler"])
        if syn + comp * 0.3 >= 0.67:
            tags.append("Comp - Synth Lead")

        kb = mi(["keyboard","electricpiano"])
        if (syn + kb) / 2 >= INST["arpeggio"]:
            tags.append("Comp - Arpeggio")

        pi = [j for j in (gi(n) for n in ["piano","keyboard","electricpiano","rhodes"]) if j is not None]
        if pi and max(inst_scores[i] for i in pi) >= 0.15:
            tags.append("Comp - Piano / Keys")

        if gs is not None and len(gs) == len(maest_classes):
            bass = mi(["bass"])
            acid = max(
                float(gs[elec_genre_idx["Acid"]])       if "Acid"       in elec_genre_idx else 0.0,
                float(gs[elec_genre_idx["Acid House"]]) if "Acid House" in elec_genre_idx else 0.0,
            )
            if syn >= INST["acid_synth"] and bass >= INST["acid_bass"] and acid >= INST["acid_genre"]:
                tags.append("Comp - Acid Line")

            wob = max(
                float(gs[elec_genre_idx["Dubstep"]])  if "Dubstep"  in elec_genre_idx else 0.0,
                float(gs[elec_genre_idx["Bassline"]]) if "Bassline" in elec_genre_idx else 0.0,
            )
            if bass >= INST["wobble_bass"] and wob >= INST["wobble_genre"]:
                tags.append("Comp - Wobble Bass")

    return tags


# ── 3. Load RB maps ────────────────────────────────────────────────────────────
rb_tag_map      = {r.Name: r.ID   for r in rb_db.get_my_tag().all()}
rb_tag_uuid_map = {r.Name: r.UUID for r in rb_db.get_my_tag().all()}

# norm_path → content_id
rb_content_map = {}
for song in rb_db.get_content().all():
    if song.FolderPath:
        rb_content_map[_norm(song.FolderPath)] = song.ID

print(f"  RB tags: {len(rb_tag_map)}  |  RB tracks: {len(rb_content_map)}")

# Current synced (manual) tags in RB: norm_path → set of tag names
rb_synced_by_path = defaultdict(set)
tag_id_to_name    = {r.ID: r.Name for r in rb_db.get_my_tag().all()}
synced_ids        = {tid for tid, n in tag_id_to_name.items() if n in SYNCED_RB_TAGS}

for row in rb_db.session.query(tables.DjmdSongMyTag).all():
    if row.MyTagID not in synced_ids:
        continue
    content = rb_db.session.query(tables.DjmdContent).filter_by(ID=row.ContentID).first()
    if content and content.FolderPath:
        rb_synced_by_path[_norm(content.FolderPath)].add(tag_id_to_name[row.MyTagID])

# ── 4. Load Spotify ────────────────────────────────────────────────────────────
if not SYNC_OLD:
    print("  Fetching Spotify playlist membership...")
    sp_cli = _sp_client()
    sp_pls = _sp_all_playlists(sp_cli)

    sp_id_to_pls: dict = defaultdict(set)
    for pl_name in ALL_RB_PLAYLISTS:
        pl_id = sp_pls.get(pl_name)
        if not pl_id:
            continue
        for item in _sp_all_items(sp_cli, pl_id):
            t = item.get("track")
            if t and t.get("id"):
                sp_id_to_pls[t["id"]].add(pl_name)
    print(f"  Spotify tracks found: {len(sp_id_to_pls)}")
else:
    sp_cli       = None
    sp_pls       = {}
    sp_id_to_pls = defaultdict(set)
    print("  [sync-old] Skipping Spotify fetch.")

def _sp_pls_for_path(norm_path: str) -> set:
    """Get current Spotify playlists for a track via ledger's _spotify_id.
    Returns empty set for non-Spotify tracks (no _spotify_id)."""
    sp_id = ledger.get(norm_path, {}).get("_spotify_id")
    return sp_id_to_pls.get(sp_id, set()) if sp_id else set()

# ── 5. Load ledger ─────────────────────────────────────────────────────────────
ledger = load_ledger()
print(f"  Ledger entries: {len(ledger)}")

# ── 6. Load music DB ───────────────────────────────────────────────────────────
_active_db = MUSIC_DB
conn = sqlite3.connect(_active_db)
rows = conn.execute("""
    SELECT t.path, t.bpm, t.vocals_prob, t.sp_danceability, t.party_score,
           t.mood_aggressive, t.mood_happy, t.mood_relaxed,
           t.mood_sad, t.mood_acoustic, t.mood_party,
           t.date_added, tm.genre_scores, tm.instrument
    FROM tracks t
    LEFT JOIN track_metadata tm ON tm.track_path = t.path
    WHERE tm.genre_scores IS NOT NULL
""").fetchall()
conn.close()

print(f"  Music DB: {_active_db.name}  ({len(rows)} tracks)")

# ── 7. Path diagnostic ────────────────────────────────────────────────────────
# All three path sources must normalise to the same e:/... format.
# If DB→RB matches is 0, path normalisation is broken and nothing will be tagged.
print(f"\n  Path format check (first non-sampler examples):")

rb_music_sample = next(
    (k for k in rb_content_map if ("music/audio" in k or "music library" in k) and "pioneerdj" not in k),
    list(rb_content_map.keys())[0] if rb_content_map else "none"
)

db_sample     = _norm(rows[0][0])     if rows   else "none"
ledger_sample = next(iter(ledger), "none")

print(f"  RB:     {rb_music_sample}")
print(f"  DB:     {db_sample}")
print(f"  Ledger: {ledger_sample}")

matched = sum(1 for r in rows if rb_content_map.get(_norm(r[0])))
print(f"  DB→RB matches: {matched}/{len(rows)}")

if matched == 0 and len(rows) > 0:
    print("  !! WARNING: 0 matches — path normalisation mismatch. Tags will NOT be applied.")
    print("              Compare RB, DB, and Ledger paths above and fix _norm().")

# ── Flag actions — processed BEFORE main tag loop ─────────────────────────────
# Flag - Remove:     move file to bin, remove from both DBs + ledger, clear RB tags
# Flag - Redownload: delete file from disk, reset spot=None, pipeline_00 re-queues
# Both run in dry-run mode safely — changes only applied when DRY_RUN=False

REMOVE_TAG     = "Flag - Remove"
REDOWNLOAD_TAG = "Flag - Redownload"
remove_id      = rb_tag_map.get(REMOVE_TAG)
redownload_id  = rb_tag_map.get(REDOWNLOAD_TAG)

flagged_remove     = []
flagged_redownload = []

if (remove_id or redownload_id):
    content_id_to_norm = {v: k for k, v in rb_content_map.items()}
    for row in rb_db.session.query(tables.DjmdSongMyTag).all():
        norm = content_id_to_norm.get(row.ContentID)
        if not norm:
            continue
        content = rb_db.session.query(tables.DjmdContent)\
                       .filter_by(ID=row.ContentID).first()
        if not content or not content.FolderPath:
            continue
        if row.MyTagID == remove_id:
            flagged_remove.append((row.ContentID, norm, content.FolderPath))
        elif row.MyTagID == redownload_id:
            flagged_redownload.append((row.ContentID, norm, content.FolderPath))

if not SYNC_OLD:
    print(f"\n  ── Flag actions ──")
    print(f"  Flag - Remove:      {len(flagged_remove)} tracks")
    print(f"  Flag - Redownload:  {len(flagged_redownload)} tracks")

removed_ok = redownload_ok = 0

for content_id, norm_path, rb_path in flagged_remove:
    file_path = Path(_wsl_path(rb_path))
    fn        = file_path.name
    bin_dst   = BIN_DIR / fn
    if DRY_RUN:
        print(f"  [DRY RUN] Would remove: {fn}")
    else:
        if file_path.exists():
            shutil.move(str(file_path), str(bin_dst))
            logging.info(f"Moved to bin: {file_path}")
        if MUSIC_DB.exists():
            c = sqlite3.connect(MUSIC_DB)
            variants = db_variants(file_path)
            q = ",".join("?" * len(variants))
            c.execute(f"DELETE FROM track_metadata WHERE track_path IN ({q})", variants)
            c.execute(f"DELETE FROM tracks WHERE path IN ({q})", variants)
            c.commit(); c.close()
        if norm_path in ledger:
            del ledger[norm_path]
        rb_db.session.query(tables.DjmdSongMyTag)\
             .filter_by(ContentID=content_id)\
             .delete(synchronize_session=False)
        removed_ok += 1
        print(f"  ✓ Removed: {fn}")

for content_id, norm_path, rb_path in flagged_redownload:
    file_path = Path(_wsl_path(rb_path))
    fn        = file_path.name
    if DRY_RUN:
        print(f"  [DRY RUN] Would queue redownload: {fn}")
    else:
        if file_path.exists():
            file_path.unlink()
            logging.info(f"Deleted for redownload: {file_path}")
        if norm_path in ledger:
            ledger[norm_path]["spot"]      = None
            ledger[norm_path]["last_sync"] = None
        rb_db.session.query(tables.DjmdSongMyTag)\
             .filter_by(ContentID=content_id, MyTagID=redownload_id)\
             .delete(synchronize_session=False)
        redownload_ok += 1
        print(f"  ✓ Queued redownload: {fn}")

if not DRY_RUN and (removed_ok or redownload_ok):
    rb_db.session.commit()
    save_ledger(ledger)
    print(f"  Done — Removed: {removed_ok}  |  Redownload queued: {redownload_ok}")

# ── 8. USN counter ─────────────────────────────────────────────────────────────
max_usn = max(
    int(rb_db.session.query(func.max(tables.DjmdSongMyTag.rb_local_usn)).scalar() or 0),
    int(rb_db.session.query(func.max(tables.DjmdMyTag.rb_local_usn)).scalar()    or 0),
) + 1

def next_usn():
    global max_usn
    v = max_usn; max_usn += 1; return v

# ── 9. Selective clear — save manual tags, clear auto, restore manual ──────────
manual_tag_ids = {tid for n, tid in rb_tag_map.items()
                  if n in EXTRAS_MANUAL or n in SET_POSITION_SET}

if not DRY_RUN and OVERWRITE and not SYNC_OLD:
    print("\n  Preserving manual tags, clearing auto tags...")
    all_song_tags = rb_db.session.query(tables.DjmdSongMyTag).all()
    manual_saved  = [
        {
            "content_id": r.ContentID, "tag_id": r.MyTagID,
            "uuid":       r.UUID,      "usn":    r.rb_local_usn,
            "created_at": r.created_at,
        }
        for r in all_song_tags if r.MyTagID in manual_tag_ids
    ]
    print(f"  Saving {len(manual_saved)} manual assignments...")
    rb_db.session.query(tables.DjmdSongMyTag).delete(synchronize_session=False)
    rb_db.session.flush()
    for d in manual_saved:
        row = tables.DjmdSongMyTag()
        row.ID           = str(uuid.uuid4()); row.UUID = d["uuid"]
        row.ContentID    = d["content_id"];   row.MyTagID = d["tag_id"]
        row.TrackNo      = None
        row.rb_data_status = row.rb_local_data_status = 0
        row.rb_local_deleted = row.rb_local_synced = 0
        row.usn          = None; row.rb_local_usn = d["usn"]
        row.created_at   = d["created_at"]; row.updated_at = datetime.now()
        rb_db.session.add(row)
    rb_db.session.commit()
    print(f"  ✓ {len(manual_saved)} manual assignments restored")
elif DRY_RUN and not SYNC_OLD:
    all_song_tags = rb_db.session.query(tables.DjmdSongMyTag).all()
    manual_n      = sum(1 for r in all_song_tags if r.MyTagID in manual_tag_ids)
    print(f"\n  [DRY RUN] Would clear {len(all_song_tags) - manual_n} auto tags, "
          f"preserve {manual_n} manual assignments.")

elif SYNC_OLD:
    print("\n  [sync-old] Auto tags will be added additively — no clear performed.")

# ── 10. Existing assignments (skip duplicate writes) ──────────────────────────
existing_assignments = set()
for row in rb_db.session.query(tables.DjmdSongMyTag).all():
    if row.ContentID and row.MyTagID:
        existing_assignments.add((row.ContentID, row.MyTagID))

# ── 10b. Safety snapshot: write RB synced tags into ledger before any changes ──
# If a track has RB tags but no ledger entry (or spot=None), snapshot them now.
# This ensures tags are NEVER lost — even if RB is wiped between runs.
if not SYNC_OLD:
    _snapshot_count = 0
    for norm_path, rb_tags in rb_synced_by_path.items():
        if not rb_tags:
            continue
        entry = ledger.setdefault(norm_path, {})
        if entry.get("spot") is None:
            entry["spot"] = sorted(rb_tags)
            _snapshot_count += 1
    if _snapshot_count:
        print(f"  Snapshotted {_snapshot_count} unrecorded RB tag states → ledger")

def assign_tag(content_id, tag_name, stats):
    tag_id   = rb_tag_map.get(tag_name)
    tag_uuid = rb_tag_uuid_map.get(tag_name)
    if tag_id is None:
        stats["missing"].add(tag_name); return
    if (content_id, tag_id) in existing_assignments:
        stats["skipped"] += 1; return
    if not DRY_RUN:
        r = tables.DjmdSongMyTag()
        r.ID  = str(uuid.uuid4()); r.UUID = tag_uuid
        r.ContentID = content_id;  r.MyTagID = tag_id
        r.TrackNo   = None
        r.rb_data_status = r.rb_local_data_status = 0
        r.rb_local_deleted = r.rb_local_synced = 0
        r.usn = None; r.rb_local_usn = next_usn()
        r.created_at = r.updated_at = datetime.now()
        rb_db.session.add(r)
        existing_assignments.add((content_id, tag_id))
    stats["assigned"] += 1
    # Track manual tags separately for reporting
    if tag_name in SYNCED_RB_TAGS or tag_name in EXTRAS_MANUAL:
        stats["manual_dist"][tag_name] = stats["manual_dist"].get(tag_name, 0) + 1

def remove_tag(content_id, tag_name, stats):
    """Remove a synced tag from RB when authority resolution says it should be gone."""
    tag_id = rb_tag_map.get(tag_name)
    if tag_id is None:
        return
    if (content_id, tag_id) not in existing_assignments:
        return
    if not DRY_RUN:
        rb_db.session.query(tables.DjmdSongMyTag)\
            .filter_by(ContentID=content_id, MyTagID=tag_id)\
            .delete(synchronize_session=False)
        existing_assignments.discard((content_id, tag_id))
    stats["removed"] = stats.get("removed", 0) + 1

# ── 11. Main loop ─────────────────────────────────────────────────────────────
recently_cutoff = datetime.now() - timedelta(days=RECENTLY_ADDED_DAYS)
now_str         = datetime.now().isoformat()

stats = {
    "processed": 0, "no_rb": 0, "assigned": 0, "skipped": 0,
    "missing": set(), "genre_dist": {}, "vibe_dist": {}, "comp_dist": {},
    "manual_dist": {},
    "recently": 0, "sp_wins": 0, "rb_wins": 0, "no_rb_sp_authority": 0,
    "first_run_rb": 0, "first_run_sp": 0, "no_change": 0, "reapplied": 0,
}

sp_pl_add    = {pl: [] for pl in ALL_RB_PLAYLISTS}
sp_pl_remove = {pl: [] for pl in ALL_RB_PLAYLISTS}

# ── Pre-run manual tag audit ──────────────────────────────────────────────────
# Show current state across all three systems before making any changes.
if not SYNC_OLD:
    print(f"\n  Manual tag audit (pre-run):")
    sp_tag_counts     = Counter()
    ledger_tag_counts = Counter()
    rb_tag_counts     = Counter()

    for norm_path, entry in ledger.items():
        sp_id = entry.get("_spotify_id")
        if sp_id:
            for pl in sp_id_to_pls.get(sp_id, set()):
                if pl in PLAYLIST_TO_TAG:
                    sp_tag_counts[PLAYLIST_TO_TAG[pl]] += 1
        for tag in (entry.get("spot") or []):
            if tag in SYNCED_RB_TAGS:
                ledger_tag_counts[tag] += 1
        for tag in rb_synced_by_path.get(norm_path, set()):
            rb_tag_counts[tag] += 1

    all_manual_tags = sorted(SYNCED_RB_TAGS | EXTRAS_MANUAL)
    print(f"  {'Tag':<28}  {'Spotify':>7}  {'Ledger':>7}  {'RB':>7}")
    print(f"  {'-'*28}  {'-'*7}  {'-'*7}  {'-'*7}")
    for tag in all_manual_tags:
        sp_n  = sp_tag_counts.get(tag, 0)
        ld_n  = ledger_tag_counts.get(tag, 0)
        rb_n  = rb_tag_counts.get(tag, 0)
        if sp_n or ld_n or rb_n:
            print(f"  {tag:<28}  {sp_n:>7}  {ld_n:>7}  {rb_n:>7}")
    print(f"  {'TOTAL':<28}  {sum(sp_tag_counts.values()):>7}  "
          f"{sum(ledger_tag_counts.values()):>7}  {sum(rb_tag_counts.values()):>7}")

# Tracks that already carry an auto tag (genre/vibe/components). Unless --overwrite,
# these are left untouched so hand-adjusted auto tags survive (only NEW tracks get
# auto-classified). Recency is time-based and always re-evaluated.
AUTO_TAG_NAMES = set(GENRE_TAGS) | set(VIBE_TAGS) | set(COMP_TAGS)
_auto_tag_ids  = {tid for tid, n in tag_id_to_name.items() if n in AUTO_TAG_NAMES}
_tracks_with_auto = {cid for (cid, tid) in existing_assignments if tid in _auto_tag_ids}

for row in rows:
    (path, bpm, vp, sp_dance, party_score,
     mood_agg, mood_happy, mood_rel, mood_sad, mood_ac, mood_party,
     date_added, genre_blob, inst_blob) = row

    norm_path  = _norm(path)
    content_id = rb_content_map.get(norm_path)
    gs         = decode(genre_blob)
    inst       = decode(inst_blob)

    # ── Auto tags — Genre, Vibe, Components, Recently Added ───────────────────
    # Always derived from ML features. Only applied when track is in RB.
    if content_id:
        stats["processed"] += 1

        # Only classify NEW tracks (no existing auto tag) unless --overwrite.
        if OVERWRITE or content_id not in _tracks_with_auto:
            if gs is not None and len(gs) == len(maest_classes):
                for tag in classify_genre(gs):
                    assign_tag(content_id, tag, stats)
                    stats["genre_dist"][tag] = stats["genre_dist"].get(tag, 0) + 1

            for tag in classify_vibe(party_score, sp_dance, bpm,
                                      mood_agg, mood_rel, mood_sad, mood_party):
                assign_tag(content_id, tag, stats)
                stats["vibe_dist"][tag] = stats["vibe_dist"].get(tag, 0) + 1

            for tag in classify_components(vp, inst, gs):
                assign_tag(content_id, tag, stats)
                stats["comp_dist"][tag] = stats["comp_dist"].get(tag, 0) + 1
        else:
            stats["skipped_existing_auto"] = stats.get("skipped_existing_auto", 0) + 1

        # Recently-Added: time-based (entry date + config window) — always evaluated.
        if date_added:
            try:
                added_dt = datetime.fromisoformat(str(date_added).split(".")[0])
                if added_dt >= recently_cutoff:
                    assign_tag(content_id, "Flag - Recently Added", stats)
                    stats["recently"] += 1
            except (ValueError, TypeError):
                pass

# ── Manual tags — skip entirely in sync-old mode ──────────────────────────
    if not SYNC_OLD:
        entry       = ledger.get(norm_path, {})
        sp_id       = entry.get("_spotify_id")
        spot        = entry.get("spot")
        cur_sp_pls  = _sp_pls_for_path(norm_path)
        cur_rb_tags = rb_synced_by_path.get(norm_path, set())
        cur_sp_tags = {PLAYLIST_TO_TAG[pl] for pl in cur_sp_pls if pl in PLAYLIST_TO_TAG}

        if content_id is None:
            stats["no_rb_sp_authority"] += 1
            stats["no_rb"] += 1
            logging.info(f"Not in RB (pending reimport): {norm_path}")
            continue

        if spot is None:
            if cur_rb_tags:
                new_truth_tags = cur_rb_tags
                stats["first_run_rb"] += 1
            else:
                new_truth_tags = cur_sp_tags
                stats["first_run_sp"] += 1
        else:
            spot_tags  = set(spot)
            rb_changed = cur_rb_tags != spot_tags
            # SP is only an authority for tracks that actually have a Spotify ID.
            # Without one, cur_sp_tags is always empty and must NOT be read as
            # "Spotify cleared the tags" — that would wipe manual tags on non-SP
            # tracks every run. For these, RB is the sole authority.
            sp_changed = (sp_id is not None) and (cur_sp_tags != spot_tags)
            if rb_changed and not sp_changed:
                if not cur_rb_tags and spot_tags:
                    new_truth_tags = spot_tags
                    stats["reapplied"] += 1
                else:
                    new_truth_tags = cur_rb_tags
                    stats["rb_wins"] += 1
            elif sp_changed and not rb_changed:
                new_truth_tags = cur_sp_tags
                stats["sp_wins"] += 1
            elif rb_changed and sp_changed:
                new_truth_tags = cur_rb_tags
                stats["rb_wins"] += 1
            else:
                stats["no_change"] += 1
                continue

        for tag_name in (cur_rb_tags - new_truth_tags):
            if tag_name in SYNCED_RB_TAGS:
                remove_tag(content_id, tag_name, stats)
        for tag_name in new_truth_tags:
            assign_tag(content_id, tag_name, stats)
        if new_truth_tags and content_id:
            stats["manual_tracks"] = stats.get("manual_tracks", 0) + 1

        if sp_id:
            for pl_name, rb_tag in PLAYLIST_TO_TAG.items():
                should_be_in = rb_tag in new_truth_tags
                is_in        = pl_name in cur_sp_pls
                if should_be_in and not is_in:
                    sp_pl_add[pl_name].append(sp_id)
                elif not should_be_in and is_in:
                    sp_pl_remove[pl_name].append(sp_id)

        if norm_path in ledger or new_truth_tags:
            if norm_path not in ledger:
                # A DB+RB track with no ledger entry shouldn't normally happen
                # (pipe01 seeds every DB track). If it does, write a canonical
                # entry; unknown provenance → ytdlp (eligible for HQ re-download).
                logging.warning(f"DB+RB track missing from ledger, seeding: {norm_path}")
                ledger[norm_path] = _canon_entry(path, "ytdlp")
            ledger[norm_path]["spot"]      = sorted(new_truth_tags)
            ledger[norm_path]["last_sync"] = now_str
    if content_id is None:
        stats["no_rb"] += 1

    if not DRY_RUN and stats["processed"] % COMMIT_EVERY == 0:
        rb_db.session.commit()
        print(f"  Committed {stats['processed']} tracks...")

# ── 12. Commit ────────────────────────────────────────────────────────────────
if not DRY_RUN:
    rb_db.session.commit()

if not SYNC_OLD:
    # (v1.0 single-library: removed the OLD_AUDIO/sp_metadata migration block)

    # ── 13. Apply Spotify playlist changes (incl. old-dir pushes) ─────────────────
    if not DRY_RUN:
        for pl_name in ALL_RB_PLAYLISTS:
            pl_id = sp_pls.get(pl_name)
            if not pl_id:
                continue
            add_ids = list(dict.fromkeys(sp_pl_add[pl_name]))      # dedup, keep order
            rem_ids = list(dict.fromkeys(sp_pl_remove[pl_name]))
            if add_ids:
                _sp_add(sp_cli, pl_id, add_ids)
                print(f"  SP + {len(add_ids)} → {pl_name}")
            if rem_ids:
                _sp_remove(sp_cli, pl_id, rem_ids)
                print(f"  SP - {len(rem_ids)} ← {pl_name}")
    else:
        sp_add_total    = sum(len(set(v)) for v in sp_pl_add.values())
        sp_remove_total = sum(len(set(v)) for v in sp_pl_remove.values())
        print(f"\n  [DRY RUN] Spotify changes pending: +{sp_add_total} / -{sp_remove_total}")

    # ── 14. Save ledger ────────────────────────────────────────────────────────────
    if not DRY_RUN:
        save_ledger(ledger)
    else:
        print(f"  [DRY RUN] Ledger NOT saved.")

# ── 15. Report ────────────────────────────────────────────────────────────────
total = max(stats["processed"], 1)
print(f"\n{'='*60}")
print(f"  {'[DRY RUN] ' if DRY_RUN else ''}COMPLETE")
if SYNC_OLD:
    print(f"  MODE: sync-old (OLD_DB auto tags → RB, no SP/ledger writes)")
print(f"{'='*60}")
print(f"  Music DB tracks:       {len(rows)}")
print(f"  Processed (in RB):     {stats['processed']}")
print(f"  Not in RB:             {stats['no_rb']}")
print(f"  No change (skipped):   {stats['no_change']}")
print(f"  Tags assigned:         {stats['assigned']}")
print(f"  Tags skipped (dupes):  {stats['skipped']}")
print(f"  Recently Added:        {stats['recently']}")
print(f"\n  Authority resolution:")
print(f"    First run — RB kept:   {stats['first_run_rb']}")
print(f"    First run — SP seed:   {stats['first_run_sp']}")
print(f"    RB wins:               {stats['rb_wins']}")
print(f"    Spotify wins:          {stats['sp_wins']}")
print(f"    Safety reapplied:      {stats['reapplied']}  ← RB empty but spot had tags")
print(f"    No change (skipped):   {stats['no_change']}")
print(f"    No RB / SP auth:       {stats['no_rb_sp_authority']}")
print(f"    Synced tags removed:   {stats.get('removed', 0)}  ← stale RB tags dropped")

if stats["missing"]:
    print(f"\n  !! Missing RB tag names: {sorted(stats['missing'])}")

dance_t = {"Chill","Groovy","Driving","Peak"}
mood_t  = {"Dark","Euphoric","Aggressive","Hypnotic","Uplifting","Melancholic","Deep","Dramatic"}

# Manual tags — Floor, Set Position, Flags
for label, dist in [
    ("Genre (top 15)",              sorted(stats["genre_dist"].items(),  key=lambda x: x[1], reverse=True)[:15]),
    ("Danceability",                sorted({k:v for k,v in stats["vibe_dist"].items() if k in dance_t}.items(), key=lambda x: x[1], reverse=True)),
    ("Mood",                        sorted({k:v for k,v in stats["vibe_dist"].items() if k in mood_t}.items(),  key=lambda x: x[1], reverse=True)),
    ("Components",                  sorted(stats["comp_dist"].items(),   key=lambda x: x[1], reverse=True)),
    ("Manual (Floor/Set Position/Flag)", sorted(stats["manual_dist"].items(), key=lambda x: x[1], reverse=True)),
]:
    print(f"\n  {label}:")
    if dist:
        for tag, cnt in dist:
            print(f"    {cnt:>5} ({100*cnt/total:>5.1f}%)  {tag}")
    else:
        print(f"    (none)")

print(f"\n  Tracks with manual tags: {stats.get('manual_tracks', 0)}")


# ══════════════════════════════════════════════════════════════════════════════
#%% Cell 3: Verify RB DB integrity
# ══════════════════════════════════════════════════════════════════════════════
"""
Run after a live Cell 2 to verify no orphaned rows, UUID mismatches,
USN zeros, or missing mood coverage. Safe to run in dry-run too.
"""
print(f"\n── Cell 3: Verify ──")

all_a    = rb_db.session.query(tables.DjmdSongMyTag).all()
with_t   = [r for r in all_a if r.MyTagID is not None]
null_t   = [r for r in all_a if r.MyTagID is None]
tag_map  = {r.ID: r.Name for r in rb_db.get_my_tag().all()}
valid_ids = set(tag_map.keys())
orphaned  = [r for r in with_t if r.MyTagID not in valid_ids]

print(f"  Total SongMyTag rows:    {len(all_a)}")
print(f"  Valid:                   {len(with_t) - len(orphaned)}  ← main")
print(f"  Orphaned:                {len(orphaned)}  ← should be 0")
print(f"  NULL MyTagID:            {len(null_t)}  ← should be 0")

tag_uuid = {r.ID: r.UUID for r in rb_db.get_my_tag().all()}
uuid_ok  = sum(1 for r in with_t if r.MyTagID in tag_uuid and r.UUID == tag_uuid[r.MyTagID])
uuid_bad = sum(1 for r in with_t if r.MyTagID in tag_uuid and r.UUID != tag_uuid[r.MyTagID])
print(f"\n  UUID matches:            {uuid_ok}  ← should equal valid rows")
print(f"  UUID mismatches:         {uuid_bad}  ← should be 0")

usns  = [r.rb_local_usn for r in with_t]
zeros = sum(1 for u in usns if not u or u == 0)
print(f"\n  USN zeros:               {zeros}  ← should be 0")
print(f"  USN range:               {min(usns) if usns else 'N/A'} – {max(usns) if usns else 'N/A'}")

print(f"\n  Top 25 assigned tags:")
for name, cnt in Counter(tag_map.get(r.MyTagID, "ORPHAN") for r in with_t).most_common(25):
    print(f"    {cnt:>6}  {name}")

manual_names = EXTRAS_MANUAL | SET_POSITION_SET
manual_cnt   = sum(1 for r in with_t if tag_map.get(r.MyTagID, "") in manual_names)
print(f"\n  Manual tags preserved:   {manual_cnt}")

mood_ids  = {rid for rid, n in tag_map.items() if n in mood_t}
with_mood = {r.ContentID for r in with_t if r.MyTagID in mood_ids}
all_cont  = {r.ContentID for r in with_t}
print(f"  Mood coverage:           {len(with_mood)}/{len(all_cont)} "
      f"({100 * len(with_mood) / max(len(all_cont), 1):.1f}%)")

ra_id  = next((rid for rid, n in tag_map.items() if n == "Flag - Recently Added"), None)
ra_cnt = sum(1 for r in with_t if r.MyTagID == ra_id)
print(f"  Flag - Recently Added:   {ra_cnt} tracks  (last {RECENTLY_ADDED_DAYS} days)")

ledger   = load_ledger()
synced   = sum(1 for e in ledger.values() if e.get("spot") is not None)
print(f"\n  Ledger entries:          {len(ledger)}")
print(f"  Ledger synced (spot≠∅):  {synced}")
print(f"  Ledger pending sync:     {len(ledger) - synced}")
