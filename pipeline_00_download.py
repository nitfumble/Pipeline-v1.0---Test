#%%
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pipeline_00_download.py
=======================
Stage 0 — identify and pull new songs into the library.

Execution order:
  pipeline_00  →  pipeline_01  →  [manual RB import]  →  pipeline_02  →  pipeline_03

Cells:
  1   Ensure all [RB] Spotify playlists exist (create missing, never delete)
  2   Delta comparison — which tracks are in [RB] playlists but not yet on disk?
  2b  Optional — extra playlist selector (default off)
  3   Acquire tracks:
        1. Soulseek downloads folder (small, scanned per track)
        2. Manual dir              (small, scanned per track)
        3. Old library             (direct filename lookup — no scanning)
             duration OK  → move to new library, clean up old ledger entry
             duration BAD → delete old file + old ledger entry, fall through to yt-dlp
        4. yt-dlp                  (top-10 YouTube search, >=80% title match)
      Checkpointed per track — safe to restart after a crash.
  4   Validate, copy to new library, write SPOT ledger entries.
      Non-ytdlp duration failures → retry yt-dlp → bin only if yt-dlp also fails.
  5   HALT — open RB, import new files, close RB, press Y

JOIN KEY: file path throughout. Spotify IDs are stored as ledger metadata only.

SPOT LEDGER:
  Keyed by normalised file path. Created here, extended by pipeline_03.
  "spot" field is null until pipeline_03 runs its first sync.
"""

import json, shutil, logging, time, uuid, requests, re, glob, subprocess
from pathlib import Path
from datetime import datetime
from collections import defaultdict

import spotipy as sp
from spotipy.oauth2 import SpotifyOAuth
from spotipy.cache_handler import CacheFileHandler

from mutagen import File as MutagenFile
from yt_dlp import YoutubeDL
from rapidfuzz import fuzz

# v1.0: single source of truth for settings + canonical path/IO helpers.
from config import load
from dj_paths import to_path, to_path as _root, atomic_write_json

import argparse
_parser = argparse.ArgumentParser(description="DJ Pipeline - Download & sync tracks")
_parser.add_argument("--extra-playlists", action="store_true", help="Enable manual playlist selection")
_parser.add_argument("--retry-failed",    action="store_true", help="Retry previously failed downloads")
_parser.add_argument("--debug",           action="store_true", help="Debug mode — process 1 track only")
_parser.add_argument("--slsk-only",       action="store_true", help="Only try Soulseek, skip yt-dlp")
_parser.add_argument("--ytdlp-only",      action="store_true", help="Only try yt-dlp, skip Soulseek")
_parser.add_argument("--reconcile-dry",   action="store_true", help="Reconcile reports only — do not delete ghost entries")
_parser.add_argument("--sweep", nargs="?", const="all", default=None, metavar="SOURCE",
                     help="Import-only: sweep source folders, no Spotify/download. "
                          "Bare --sweep = all; or one of: manual, bandcamp, beatport, soulseek.")
_parser.add_argument("--list-playlists",  action="store_true",
                     help="Print all Spotify playlists as JSON and exit (used by web UI)")
_parser.add_argument("--playlist-names",  nargs="*", default=None,
                     help="Pre-selected playlist names from UI (skips interactive input())")
_parser.add_argument("--no-interactive",  action="store_true",
                     help="Skip all interactive prompts (used when called from web UI)")
_args = _parser.parse_args()

# %%
# ══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════════════════
class RateLimitError(Exception):
    pass

# ── Load config — single source of truth (config.py / config.json) ────────────
cfg = load()
_dl, _sk = cfg.download, cfg.slskd

ROOT            = cfg.paths.library_root
NEW_ROOT        = ROOT                       # back-compat alias for existing code
NEW_AUDIO       = cfg.paths.audio
BIN_DIR         = cfg.paths.bin
TMP_DIR         = cfg.paths.tmp
LOG_DIR         = cfg.paths.logs
DB_DIR          = cfg.paths.db
LEDGER          = cfg.paths.ledger
CHECKPOINT      = DB_DIR / "pipeline_00_checkpoint.json"
DL_QUEUE_FILE   = DB_DIR / "download_queue.json"
FAILED_LOG      = DB_DIR / "failed_downloads.txt"
SLSK_BLACKLIST  = DB_DIR / "slsk_blacklist.json"
BLACKLIST_FILE  = DB_DIR / "download_blacklist.json"
SLSK_DIR        = cfg.paths.soulseek
SLSK_QUEUE_FILE = DB_DIR / "slsk_queue.json"
SOULSEEK_DIR    = to_path(_sk.download_dir) if _sk.download_dir else SLSK_DIR
COOKIE_FILE     = _dl.cookie_file or None

# Source registry: name -> (absolute folder, upgradeable). 'ytdlp' lives in audio/,
# 'soulseek' in slsk/; the rest (manual/bandcamp/beatport) are drop folders.
SOURCE_DIRS     = {name: (ROOT / s["folder"], s["upgradeable"]) for name, s in cfg.sources.items()}
ACQUIRE_SOURCES = {n: d for n, (d, _u) in SOURCE_DIRS.items() if n != "ytdlp"}
# --sweep: import-only mode. soulseek's download folder is sweepable too.
SWEEP_ONLY = _args.sweep is not None
_sweepable = {**ACQUIRE_SOURCES, "soulseek": SLSK_DIR}
if _args.sweep in (None, "all"):
    SWEEP_SOURCES = _sweepable
elif _args.sweep in _sweepable:
    SWEEP_SOURCES = {_args.sweep: _sweepable[_args.sweep]}
else:
    raise SystemExit(f"--sweep: unknown '{_args.sweep}'. Choices: all, {', '.join(_sweepable)}")
AUDIO_EXTS  = {f".{e.lower().lstrip('.')}" for e in _dl.accepted_formats}

DEBUG_LIMIT              = 1 if _args.debug else None
RETRY_FAILED             = _args.retry_failed
SLSK_ONLY                = _args.slsk_only
YTDLP_ONLY               = _args.ytdlp_only
DOWNLOAD_EXTRA_PLAYLISTS = _args.extra_playlists

YTDLP_SLEEP       = _dl.ytdlp_sleep

def _wsl_remap_url(url: str) -> str:
    # In WSL, 127.0.0.1/localhost routes to WSL loopback, not Windows host.
    # Windows host IP = default gateway (from `ip route show default`).
    try:
        if "microsoft" not in Path("/proc/sys/kernel/osrelease").read_text().lower():
            return url
        import subprocess as _sp
        out = _sp.run(["ip", "route", "show", "default"], capture_output=True, text=True, timeout=5).stdout
        for line in out.splitlines():
            if line.startswith("default via"):
                host_ip = line.split()[2]
                return url.replace("127.0.0.1", host_ip).replace("localhost", host_ip)
    except Exception:
        pass
    return url

SLSKD_URL         = _wsl_remap_url(_sk.url)
SLSKD_USER        = cfg.secrets["slskd"].get("api_username") or cfg.secrets["slskd"]["username"]
SLSKD_PASS        = cfg.secrets["slskd"].get("api_password") or cfg.secrets["slskd"]["password"]
SLSK_SEARCH_WAIT  = _sk.search_wait
SLSK_DL_TIMEOUT   = _sk.dl_timeout
SLSK_TITLE_THRESH = _sk.title_thresh
SLSK_EXT_THRESH   = _sk.ext_thresh
SLSK_QUEUE_TTL    = _sk.queue_ttl_days * 86400
SLSK_QUEUE_RETRY  = _sk.queue_retry_hours * 3600
SLSK_MAX_ATTEMPTS = _sk.max_attempts
SLSK_MAX_QUEUE    = _sk.max_queue
SLSK_QUEUE_POS_MAX = _sk.get("queue_pos_max", 100)
FREE_SLOT_ONLY    = _sk.free_slot_only

DUR_TOL_ABS        = _dl.dur_tol_abs_ms
DUR_TOL_PCT        = _dl.dur_tol_pct
MIN_DURATION_S     = _dl.min_duration_s
MAX_DURATION_S     = getattr(_dl, "max_duration_s", 900)
SIZE_LIMITS_MB     = {k: tuple(v) for k, v in _dl.size_limits_mb.items()}
FINGERPRINT_LEN    = _dl.fingerprint_len
FUZZY_DUR_TOL      = _dl.fuzzy_dur_tol_ms
FUZZY_MATCH_THRESH = _dl.fuzzy_match_thresh
TITLE_ONLY_FLOOR   = _dl.title_only_floor
MANUAL_DUR_TOL     = _dl.manual_dur_tol

SPOTIFY_CLIENT_ID     = cfg.secrets["spotify"]["client_id"]
SPOTIFY_CLIENT_SECRET = cfg.secrets["spotify"]["client_secret"]
SPOTIFY_REDIRECT_URI  = cfg.spotify.redirect_uri
SPOTIFY_SCOPES        = cfg.spotify.scopes
SPOTIFY_CACHE         = str(cfg.spotify.token_cache)

# [RB] Spotify playlists (download sources + tag-sync targets) — from config.
ALL_RB_PLAYLISTS = cfg.tag_playlists()
PLAYLIST_TO_TAG  = cfg.playlist_tag_map()

# ── Init dirs ─────────────────────────────────────────────────────────────────
for _d in [NEW_AUDIO, DB_DIR, BIN_DIR, TMP_DIR, LOG_DIR, SLSK_DIR] + list(ACQUIRE_SOURCES.values()):
    _d.mkdir(parents=True, exist_ok=True)

import sys as _sys
if hasattr(_sys.stdout, 'reconfigure'):
    _sys.stdout.reconfigure(encoding='utf-8', errors='replace')

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "pipeline_00.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
print("=== Pipeline 00: Download ===")

YTDLP_UPDATE_CHECK    = DB_DIR / "ytdlp_update_check.json"
YTDLP_UPDATE_INTERVAL = 24 * 3600

def _maybe_update_ytdlp():
    """Best-effort, throttled to once/24h — YouTube breaks stale yt-dlp builds often."""
    try:
        last_check = 0.0
        if YTDLP_UPDATE_CHECK.exists():
            last_check = json.loads(YTDLP_UPDATE_CHECK.read_text(encoding="utf-8")).get("last_check", 0.0)
        if time.time() - last_check < YTDLP_UPDATE_INTERVAL:
            return

        result = subprocess.run(
            [_sys.executable, "-m", "pip", "install", "-U", "yt-dlp"],
            capture_output=True, text=True, timeout=30,
        )
        atomic_write_json(YTDLP_UPDATE_CHECK, {"last_check": time.time()})
        if result.returncode != 0:
            logging.warning(f"yt-dlp update check failed: {result.stderr.strip()[:300]}")
            return

        m = re.search(r"Successfully installed .*?yt-dlp-(\S+)", result.stdout)
        if m:
            logging.info(f"yt-dlp updated -> {m.group(1)}")
    except Exception as e:
        logging.warning(f"yt-dlp update check skipped: {e}")

if not _args.list_playlists and not _args.slsk_only:
    _maybe_update_ytdlp()

# %%
# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _norm(p) -> str:
    s = str(p).replace("\\", "/")
    if s.startswith("/mnt/") and len(s) > 6:
        parts = s.split("/", 3)
        s = parts[2] + ":/" + (parts[3] if len(parts) > 3 else "")
    return s.lower()

def _dur_ok(file_ms: float, ref_ms: int) -> bool:
    return abs(file_ms - ref_ms) <= max(DUR_TOL_ABS, ref_ms * DUR_TOL_PCT)

# ── Audio validation + blacklist ──────────────────────────────────────────────
def _load_blacklist() -> dict:
    if not BLACKLIST_FILE.exists():
        return {}
    with open(BLACKLIST_FILE, encoding="utf-8") as f:
        return json.load(f)

def _add_to_blacklist(track: dict, reason: str):
    bl = _load_blacklist()
    sp_id = track.get("_spotify_id") or track.get("id")
    key   = sp_id or f"{track.get('artist','')} - {track.get('title','')}"
    bl[key] = {
        "artist":     track.get("artist", ""),
        "title":      track.get("title", ""),
        "_spotify_id": sp_id,
        "reason":     reason,
        "date":       datetime.now().isoformat(),
    }
    atomic_write_json(BLACKLIST_FILE, bl)
    logging.warning(f"Blacklisted: {key} — {reason}")

def _is_blacklisted(track: dict) -> bool:
    bl = _load_blacklist()
    sp_id = track.get("_spotify_id") or track.get("id")
    if sp_id and sp_id in bl:
        return True
    key = f"{track.get('artist','')} - {track.get('title','')}"
    return key in bl

def _get_dur_ms(path: Path) -> float | None:
    try:
        info = MutagenFile(path)
        if info is not None:
            return info.info.length * 1000
    except Exception:
        pass
    # Fallback for non-standard WAV/AIFF (BWF, RF64, etc.) that mutagen can't parse
    try:
        import soundfile as sf
        with sf.SoundFile(str(path)) as s:
            return (s.frames / s.samplerate) * 1000
    except Exception:
        return None

def to_win(p) -> str:
    """Canonical Windows path string (for Rekordbox / M3U). Inverse of _root().
    /mnt/e/Music Library/... -> E:\\Music Library\\...  (idempotent on Win paths)."""
    s = str(p).replace("\\", "/")
    if s.startswith("/mnt/") and len(s) > 6:
        s = s[5].upper() + ":" + s[6:]
    return s.replace("/", "\\")

def _fingerprint(path: Path) -> str | None:
    """Chromaprint content fingerprint via fpcalc. Identity for exact-dup detection.
    Same upload twice -> identical string. Different encode of same song -> different
    (that near-dup case is the embedding pass's job in find_duplicates.py)."""
    try:
        out = subprocess.run(
            ["fpcalc", "-length", str(FINGERPRINT_LEN), str(path)],
            capture_output=True, text=True, timeout=60,
        )
        for line in out.stdout.splitlines():
            if line.startswith("FINGERPRINT="):
                return line.split("=", 1)[1].strip() or None
    except Exception as e:
        logging.warning(f"fpcalc failed for {path.name}: {e}")
    return None

def accept_file(path: Path, expected_dur_ms: int | None) -> tuple[bool, str, dict]:
    """THE validation gate — every acquired file passes this, regardless of source.
    Returns (ok, reason, facts). facts carries duration/size/fingerprint for the ledger.
    Problem 2 (corruption): mutagen-readable + >= MIN_DURATION_S.
    Problem 3 (wrong size): per-format reject ceiling; warn ceiling flags in ledger."""
    if not path.exists():
        return False, "missing", {}

    size_bytes = path.stat().st_size
    size_mb    = size_bytes / (1024 * 1024)
    ext        = path.suffix.lower().lstrip(".")

    warn_mb, reject_mb = SIZE_LIMITS_MB.get(ext, (None, None))
    if reject_mb and size_mb > reject_mb:
        return False, (f"size {size_mb:.0f}MB > {reject_mb}MB for .{ext} "
                       f"(likely mislabeled FLAC or compilation)"), {}

    dur_ms = _get_dur_ms(path)
    if dur_ms is None:
        return False, "unreadable by mutagen", {}
    if dur_ms / 1000 < MIN_DURATION_S:
        if expected_dur_ms and abs(dur_ms - expected_dur_ms) / max(expected_dur_ms, 1) <= 0.20:
            pass  # genuinely short track — expected duration confirms it
        else:
            return False, f"too short ({dur_ms/1000:.0f}s < {MIN_DURATION_S}s)", {}
    if dur_ms / 1000 > MAX_DURATION_S:
        return False, f"too long ({dur_ms/1000:.0f}s > {MAX_DURATION_S}s — likely mix/compilation)", {}
    if expected_dur_ms and not _dur_ok(dur_ms, expected_dur_ms):
        return False, (f"duration {int(dur_ms//1000)}s vs "
                       f"expected {expected_dur_ms//1000}s"), {}

    if ext == "mp3":
        with open(path, "rb") as _fh:
            _magic = _fh.read(4)
        if _magic[:4] in (b"fLaC", b"RIFF"):
            return False, f"codec mismatch: .mp3 file has {_magic[:4]!r} header", {}

    facts = {
        "size_bytes":      size_bytes,
        "duration_ms":     int(dur_ms),
        "fingerprint":     _fingerprint(path),
        "size_suspicious": bool(warn_mb and size_mb > warn_mb),
    }
    return True, "", facts

def _load_slsk_blacklist() -> set:
    if not SLSK_BLACKLIST.exists():
        return set()
    with open(SLSK_BLACKLIST, encoding="utf-8") as f:
        return set(json.load(f))

def _add_slsk_blacklist(username: str, filename: str, reason: str):
    bl = _load_slsk_blacklist()
    key = f"{username}/{filename}"
    bl.add(key)
    atomic_write_json(SLSK_BLACKLIST, sorted(bl))
    logging.warning(f"SLSK blacklisted: {key} — {reason}")

def _clean_fn(artist: str, title: str, ext: str = ".mp3") -> str:
    """
    Build a Windows-safe filename.
    Strips illegal chars, trailing dots/spaces, truncates to 251 UTF-8 bytes.
    """
    name = f"{artist} - {title}"
    for ch in ['?', '<', '>', '*', '/', '\\', ':', '|', '"']:
        name = name.replace(ch, '')
    name    = name.strip('. ')
    encoded = name.encode('utf-8')
    if len(encoded) > 251:
        name = encoded[:251].decode('utf-8', errors='ignore').strip('. ')
    return name + ext

def _match_folder(folder: Path, artist: str, title: str, dur_ms: int) -> Path | None:
    """Word-based filename match with duration validation. Used for small folders only."""
    if not folder.exists():
        return None
    raw    = f"{artist} - {title}".lower().replace('-', ' ')
    words  = [w for w in raw.split() if len(w) > 2]
    needed = max(1, int(len(words) * 0.6))
    best, best_score = None, 0
    for f in folder.rglob("*"):
        if f.suffix.lower() not in AUDIO_EXTS:
            continue
        fname = f.stem.lower().replace('-', ' ')
        score = sum(1 for w in words if w in fname)
        if score >= needed and score > best_score:
            fdur = _get_dur_ms(f)
            if fdur is not None and _dur_ok(fdur, dur_ms):
                best, best_score = f, score
    return best


# ── Ledger ────────────────────────────────────────────────────────────────────
def load_ledger() -> dict:
    if not LEDGER.exists():
        return {}
    with open(LEDGER, encoding="utf-8-sig") as f:
        return json.load(f)

def save_ledger(ledger: dict):
    atomic_write_json(LEDGER, ledger)


# ── Canonical ledger entry (one shape, all write sites) ───────────────────────
def make_entry(dst: Path, meta: dict, source: str, original_source: str,
               facts: dict | None = None) -> dict:
    """Build a ledger entry in the canonical schema. facts comes from accept_file().
    spotify_track_id is the first-class field; _spotify_id is mirrored for the
    other scripts (pipeline_01/02/03, find_duplicates, etc.) that still read it."""
    facts   = facts or {}
    sp_id   = meta.get("_spotify_id") or meta.get("spotify_track_id")
    dur_ms  = facts.get("duration_ms") or meta.get("duration_ms", 0)
    return {
        "path":              str(dst),
        "path_win":          to_win(dst),
        "artist":            meta.get("artist", ""),
        "title":             meta.get("title", ""),
        "duration_ms":       dur_ms,
        "size_bytes":        facts.get("size_bytes"),
        "fingerprint":       facts.get("fingerprint"),
        "size_suspicious":   facts.get("size_suspicious", False),
        "source":            source,
        "original_source":   original_source,
        "spotify_track_id":  sp_id,
        "_spotify_id":       sp_id,           # mirror — back-compat with other scripts
        "spotify_playlists": meta.get("spotify_playlists") or meta.get("playlists") or [],
        "spot":              meta.get("spot") or None,
        "validated":         bool(facts),
        "validated_at":      datetime.now().isoformat() if facts else None,
        "last_sync":         meta.get("last_sync"),
        "likely_dup_of":     None,
    }


# ── Dedup gates (Problem 1) ───────────────────────────────────────────────────
def build_dedup_index(ledger: dict) -> dict:
    """Prebuilt lookups so already_have() is O(1)/O(n_fuzzy) per query, not O(ledger)."""
    return {
        "by_spotid": {v["_spotify_id"]: k for k, v in ledger.items() if v.get("_spotify_id")},
        "by_key":    {_norm(k) for k in ledger},
        "by_fp":     {v["fingerprint"]: (k, v.get("duration_ms", 0))
                      for k, v in ledger.items() if v.get("fingerprint")},
        "fuzzy":     [(_key_words(f"{v.get('artist','')} {v.get('title','')}"),
                       v.get("artist", ""), v.get("title", ""), k)
                      for k, v in ledger.items()],
    }

def already_have(meta: dict, ledger: dict, idx: dict) -> str | None:
    """Pre-acquisition dedup. Returns the matching ledger key, or None.
    Layers: spotify id -> canonical filename/path -> fuzzy title+duration."""
    sp = meta.get("_spotify_id") or meta.get("spotify_track_id")
    if sp and sp in idx["by_spotid"]:
        return idx["by_spotid"][sp]

    key = _norm(NEW_AUDIO / _clean_fn(meta["artist"], meta["title"]))
    if key in idx["by_key"]:
        return key
    key_flac = _norm(NEW_AUDIO / _clean_fn(meta["artist"], meta["title"], ".flac"))
    if key_flac in idx["by_key"]:
        return key_flac

    words = _key_words(f"{meta['artist']} {meta['title']}")
    for lw, la, lt, lk in idx["fuzzy"]:
        if not words & lw:
            continue
        if _title_score(meta["artist"], meta["title"], f"{la} - {lt}") >= FUZZY_MATCH_THRESH:
            qy, ly = _extract_year(meta.get("title", "")), _extract_year(lt)
            if qy and ly and qy != ly:
                continue
            qr, lr2 = _remix_tag(meta.get("title", "")), _remix_tag(lt)
            if (qr or lr2) and not (qr and lr2 and fuzz.ratio(qr, lr2) >= 60):
                continue
            ld = ledger.get(lk, {}).get("duration_ms", 0)
            md = meta.get("duration_ms", 0)
            if ld and md and abs(ld - md) > FUZZY_DUR_TOL:
                continue
            return lk
    return None

def is_content_dup(fingerprint: str | None, duration_ms: int | None, idx: dict) -> str | None:
    """Post-download, pre-commit. Catches the same audio under an unrelated filename.
    Requires BOTH fingerprint match AND duration within tolerance — a shared intro
    (Original vs Extended mix) must NOT count as a duplicate."""
    if not fingerprint:
        return None
    hit = idx["by_fp"].get(fingerprint)
    if not hit:
        return None
    key, ex_dur = hit
    if duration_ms and ex_dur and not _dur_ok(duration_ms, ex_dur):
        return None
    return key


# ── Reconciler — enforces the four ledger invariants (Problem 4) ──────────────
def reconcile_ledger(ledger: dict, sp_reference: dict | None = None,
                     quarantine_ghosts: bool = True) -> dict:
    """Make the ledger canonical. Idempotent: a clean ledger comes out unchanged.
      1. every audio file on disk has exactly one entry  (adopt orphans)
      2. every entry resolves to a real file              (quarantine ghosts)
      3. key == _norm(entry['path'])                       (rekey drift)
      4. spotify-sourced entries carry spotify_track_id    (retro-assign)
    sp_reference: {spotify_id: {...}} + fuzzy list, used for retro Spotify-ID match."""
    stats = {"adopted": 0, "ghosts": 0, "rekeyed": 0, "sp_linked": 0, "migrated": 0}

    # --- migrate old entries to canonical shape (additive, in place) ---
    for k, v in list(ledger.items()):
        if "path_win" not in v or "spotify_track_id" not in v:
            sp_id = v.get("_spotify_id") or v.get("spotify_track_id")
            v.setdefault("path_win", to_win(v.get("path", k)))
            v.setdefault("spotify_track_id", sp_id)
            v["_spotify_id"] = sp_id
            v.setdefault("fingerprint", None)
            v.setdefault("size_bytes", None)
            v.setdefault("validated", None)
            v.setdefault("likely_dup_of", None)
            stats["migrated"] += 1

    # --- invariant 3: key must equal _norm(path) ---
    for k in list(ledger.keys()):
        if k not in ledger:
            continue
        v = ledger[k]
        want = _norm(v.get("path", k))
        if want != k:
            if want in ledger and want != k:
                # collision — keep the one whose file exists, drop the other
                ledger.pop(k, None)
            else:
                ledger[want] = ledger.pop(k)
            stats["rekeyed"] += 1

    # --- invariant 2: ghosts (entry whose file is gone — the file is already
    #     absent, so there is nothing to bin; just drop the stale entry) ---
    for k in list(ledger.keys()):
        v = ledger[k]
        p = _root(v["path"]) if "\\" in v.get("path", "") else Path(v.get("path", ""))
        if not p.exists():
            stats["ghosts"] += 1
            logging.warning(f"Ghost ledger entry (no file): {k}")
            if quarantine_ghosts:
                del ledger[k]

    # --- invariant 1: orphans (file on disk, no entry) ---
    existing_paths = {_norm(k) for k in ledger}
    if NEW_AUDIO.exists():
        for f in NEW_AUDIO.rglob("*"):
            if f.suffix.lower() not in AUDIO_EXTS:
                continue
            nk = _norm(f)
            if nk in existing_paths:
                continue
            dur = _get_dur_ms(f) or 0
            artist, title = "", f.stem
            parts = re.sub(r"^\d+[\s\-_.]+", "", f.stem).split(" - ", 1)
            if len(parts) == 2:
                artist, title = parts[0].strip(), parts[1].strip()
            ledger[nk] = make_entry(
                f, {"artist": artist, "title": title, "duration_ms": dur},
                source="ytdlp", original_source="ytdlp",   # unknown provenance → ytdlp (upgradeable)
                facts={"duration_ms": dur, "size_bytes": f.stat().st_size,
                       "fingerprint": _fingerprint(f)},
            )
            existing_paths.add(nk)
            stats["adopted"] += 1
            logging.info(f"Adopted orphan file: {f.name}")

    # --- invariant 4: retro-assign Spotify IDs to manual/unmatched entries ---
    if sp_reference:
        fuzzy  = sp_reference.get("fuzzy", [])
        for k, v in ledger.items():
            if v.get("_spotify_id"):
                continue
            words = _key_words(f"{v.get('artist','')} {v.get('title','')}")
            for lw, sa, st, sid, sdur in fuzzy:
                if not words & lw:
                    continue
                if _title_score(v.get("artist",""), v.get("title",""), f"{sa} - {st}") >= FUZZY_MATCH_THRESH:
                    vy, sy2 = _extract_year(v.get("title", "")), _extract_year(st)
                    if vy and sy2 and vy != sy2:
                        continue
                    vr, sr = _remix_tag(v.get("title", "")), _remix_tag(st)
                    if (vr or sr) and not (vr and sr and fuzz.ratio(vr, sr) >= 60):
                        continue
                    vd = v.get("duration_ms", 0)
                    if vd and sdur and abs(vd - sdur) > FUZZY_DUR_TOL:
                        continue
                    v["_spotify_id"]      = sid
                    v["spotify_track_id"] = sid
                    # NOTE: a Spotify match sets the ID only — it NEVER changes source.
                    stats["sp_linked"] += 1
                    logging.info(f"Retro-linked Spotify ID for '{v.get('title')}'")
                    break

    logging.info(f"Reconcile: {stats}")
    return stats


# ── Checkpoint ────────────────────────────────────────────────────────────────
def load_checkpoint() -> dict:
    if not CHECKPOINT.exists():
        return {}
    with open(CHECKPOINT, encoding="utf-8") as f:
        return json.load(f)

def load_slsk_queue() -> dict:
    if not SLSK_QUEUE_FILE.exists():
        return {}
    with open(SLSK_QUEUE_FILE, encoding="utf-8") as f:
        return json.load(f)

def save_slsk_queue(data: dict):
    atomic_write_json(SLSK_QUEUE_FILE, data)

def save_checkpoint(data: dict):
    atomic_write_json(CHECKPOINT, data)

def clear_checkpoint():
    if CHECKPOINT.exists():
        CHECKPOINT.unlink()


# ── Spotify ───────────────────────────────────────────────────────────────────
def _sp_client() -> sp.Spotify:
    return sp.Spotify(auth_manager=SpotifyOAuth(
        client_id=SPOTIFY_CLIENT_ID,
        client_secret=SPOTIFY_CLIENT_SECRET,
        scope=SPOTIFY_SCOPES,
        redirect_uri=SPOTIFY_REDIRECT_URI,
        cache_handler=CacheFileHandler(cache_path=SPOTIFY_CACHE),
    ))

def _sp_call(fn, *args, retries: int = 5, **kwargs):
    """Retry any Spotify API call on 429 rate-limit, respecting Retry-After."""
    for attempt in range(retries):
        try:
            return fn(*args, **kwargs)
        except sp.exceptions.SpotifyException as e:
            if e.http_status == 429:
                wait = int(getattr(e, "headers", {}).get("Retry-After", 5))
                logging.warning(f"Spotify rate limit — waiting {wait}s (attempt {attempt + 1})")
                time.sleep(wait)
            else:
                raise
    raise RuntimeError("Spotify API retries exhausted")

def _sp_all_playlists(client: sp.Spotify) -> dict:
    out, offset = {}, 0
    while True:
        page = _sp_call(client.current_user_playlists, limit=50, offset=offset)
        for p in page["items"]:
            out[p["name"]] = p["id"]
        if not page["next"]:
            break
        offset += 50
    return out

def _sp_all_items(client: sp.Spotify, playlist_id: str) -> list:
    results = _sp_call(
        client.playlist_items, playlist_id,
        fields="items(track(id,name,artists,duration_ms,album(name))),next",
        limit=100,
    )
    items = []
    while results:
        items.extend(results["items"])
        results = _sp_call(client.next, results) if results["next"] else None
    return items

def _norm_title(s: str) -> str:
    s = s.lower()
    s = re.sub(r'\s*&\s*', ' and ', s)
    s = re.sub(r'[(\[]', ' ', s)
    s = re.sub(r'[)\]]', '', s)
    return re.sub(r'\s+', ' ', s).strip()

def _extract_year(s: str):
    m = re.search(r'\b(19|20)\d{2}\b', s)
    return m.group() if m else None

def _remix_tag(s: str):
    """Return the remix/mix/edit suffix if it looks like a named variant."""
    parts = s.rsplit(' - ', 1)
    if len(parts) < 2:
        return None
    t = _norm_title(parts[1])
    if any(w in t for w in ('remix', 'mix', 'edit', 'dub', 'version', 'rework', 'vip', 'extended', 'club', 'bootleg')):
        return t
    return None

def _title_score(artist: str, title: str, fn_stem: str) -> float:
    fn_lower = _norm_title(fn_stem)
    full_query = _norm_title(f"{artist} {title}")
    title_n = _norm_title(title)
    score1 = fuzz.partial_ratio(full_query, fn_lower)
    score2 = fuzz.partial_ratio(title_n, fn_lower)
    base   = _norm_title(title.split(" - ")[0].strip())
    score3 = fuzz.partial_ratio(base, fn_lower)
    track_score = max(score1, score2, score3)
    primary_artist = _norm_title(artist.split(",")[0].strip())
    artist_score   = fuzz.partial_ratio(primary_artist, fn_lower)
    return 0.75 * track_score + 0.25 * artist_score


# ══════════════════════════════════════════════════════════════════════════════
#%% Cell 1: Ensure [RB] Spotify playlists exist
# ══════════════════════════════════════════════════════════════════════════════
# ── Ledger backup ─────────────────────────────────────────────────────────────
_ledger_backup_dir = NEW_ROOT / "backups"
_ledger_backup_dir.mkdir(parents=True, exist_ok=True)
if LEDGER.exists():
    _ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    shutil.copy2(str(LEDGER), str(_ledger_backup_dir / f"sync_ledger_{_ts}.json"))
    logging.info(f"Ledger backed up ({LEDGER.stat().st_size} bytes)")
    # Keep only the 10 most recent backups
    _all_backups = sorted(_ledger_backup_dir.glob("sync_ledger_*.json"), key=lambda f: f.stat().st_mtime)
    for _old in _all_backups[:-10]:
        _old.unlink()
        logging.info(f"Pruned old backup: {_old.name}")

print("── Cell 1: Ensure [RB] playlists ──")

client   = _sp_client()

if _args.list_playlists:
    import json as _json, sys as _sys
    _all_pl = _sp_all_playlists(client)
    print(_json.dumps([{"name": n, "id": i} for n, i in _all_pl.items()]))
    _sys.exit(0)

_sp_user_id = _sp_call(client.current_user)["id"]   # authenticated user (was hardcoded)
existing = _sp_all_playlists(client)
created  = []

for pl_name in ALL_RB_PLAYLISTS:
    if pl_name not in existing:
        new_pl = _sp_call(
            client.user_playlist_create,
            _sp_user_id, pl_name, public=False,
            description="Auto-managed by DJ pipeline. Do not rename.",
        )
        existing[pl_name] = new_pl["id"]
        created.append(pl_name)
        print(f"  [+] Created: {pl_name}")

print(f"  Existing: {len(ALL_RB_PLAYLISTS) - len(created)}  Created: {len(created)}")


# ══════════════════════════════════════════════════════════════════════════════
#%% Cell 2: Delta comparison — what's in Spotify but not yet on disk?
# ══════════════════════════════════════════════════════════════════════════════
print("── Cell 2: Delta ──")

ledger          = load_ledger()
ledger_paths    = {_norm(k) for k in ledger}
# Spotify ID lookup: spotify_id → ledger_key
ledger_by_spotid = {v["_spotify_id"]: k for k, v in ledger.items() if v.get("_spotify_id")}

# SLSK_DIR stem index — tracks already downloaded but not yet moved to NEW_AUDIO
def _norm_stem(s: str) -> str:
    import unicodedata
    s = re.sub(r'^\d{1,2}[\s\-_.]+', '', s)
    s = unicodedata.normalize('NFKD', s).encode('ascii', 'ignore').decode()
    return re.sub(r'[^a-z0-9]+', '', s.lower())

slsk_stems = set()
if SLSK_DIR.exists():
    slsk_stems = {
        _norm_stem(f.stem)
        for f in SLSK_DIR.rglob("*")
        if f.suffix.lower() in AUDIO_EXTS
    }

# Artist+title lookup for fuzzy matching
def _key_words(text: str) -> set:
    return {w.lower() for w in re.split(r'[\s\-_\.,]+', text) if len(w) > 3}

ledger_at_list = [
    (_key_words(f"{v.get('artist','')} {v.get('title','')}"),
     v.get("artist",""), v.get("title",""), k)
    for k, v in ledger.items()
]

# print(f"  DEBUG: ledger loaded {len(ledger)} entries")
# print(f"  DEBUG: first ledger key: {next(iter(ledger_paths), 'EMPTY')}")
# print(f"  DEBUG: ledger loaded {len(ledger)} entries from {LEDGER}")
# print(f"  DEBUG: file size: {LEDGER.stat().st_size}")
sp_tracks    = {}

for pl_name in ALL_RB_PLAYLISTS:
    pl_id = existing.get(pl_name)
    if not pl_id:
        print(f"  !! Playlist not found: {pl_name} — run Cell 1 first")
        continue
    for item in _sp_all_items(client, pl_id):
        t = item.get("track")
        if not t or not t.get("name"):
            continue
        artist = ", ".join(a["name"] for a in t["artists"])
        title  = t["name"]
        fn     = _clean_fn(artist, title)

        if fn in sp_tracks:
            existing_id = sp_tracks[fn].get("_spotify_id")
            new_id      = t.get("id")
            if existing_id and new_id and existing_id != new_id:
                logging.warning(
                    f"Filename collision: '{fn}' maps to Spotify IDs "
                    f"'{existing_id}' and '{new_id}' — second track skipped"
                )
                print(f"  !! Collision skipped: {fn}")
            else:
                if pl_name not in sp_tracks[fn]["playlists"]:
                    sp_tracks[fn]["playlists"].append(pl_name)
            continue

        sp_tracks[fn] = {
            "artist":      artist,
            "title":       title,
            "duration_ms": t["duration_ms"],
            "album":       t.get("album", {}).get("name", ""),
            "_spotify_id": t.get("id"),
            "playlists":   [pl_name],
        }

# ── Spotify reference for the reconciler (retro Spotify-ID assignment) ─────────
sp_reference = {
    "by_id": {m["_spotify_id"]: m for m in sp_tracks.values() if m.get("_spotify_id")},
    "fuzzy": [(_key_words(f"{m['artist']} {m['title']}"),
               m["artist"], m["title"], m["_spotify_id"], m["duration_ms"])
              for m in sp_tracks.values() if m.get("_spotify_id")],
}

# ── Start-of-run reconcile: migrate schema, assign IDs, quarantine ghosts ─────
# Idempotent. Runs before acquisition so the dedup index sees a clean ledger.
_recon0 = reconcile_ledger(ledger, sp_reference=sp_reference, quarantine_ghosts=not _args.reconcile_dry)
if any(_recon0.values()):
    save_ledger(ledger)
    ledger_paths     = {_norm(k) for k in ledger}
    ledger_by_spotid = {v["_spotify_id"]: k for k, v in ledger.items() if v.get("_spotify_id")}
    ledger_at_list = [
        (_key_words(f"{v.get('artist','')} {v.get('title','')}"),
         v.get("artist",""), v.get("title",""), k)
        for k, v in ledger.items()
    ]
    print(f"  Reconcile (start): migrated {_recon0['migrated']}  adopted {_recon0['adopted']}  "
          f"ghosts {_recon0['ghosts']}  rekeyed {_recon0['rekeyed']}  sp-linked {_recon0['sp_linked']}")

download_queue  = []
already_present = 0
ledger_updated  = 0
sp_id_linked    = 0
_blacklist = _load_blacklist()

for fn, meta in sp_tracks.items():
    dst      = NEW_AUDIO / fn
    norm_dst = _norm(dst)

    # Layer 1: exact filename match or already in SLSK_DIR
    fn_stem      = _norm_stem(Path(fn).stem)
    exact_match  = dst.exists() or norm_dst in ledger_paths or fn_stem in slsk_stems

    # Layer 2: Spotify ID match
    spot_id      = meta.get("_spotify_id")
    spotid_match = bool(spot_id and spot_id in ledger_by_spotid)

    # Layer 3: fuzzy title match against ledger
    fuzzy_match  = False
    fuzzy_key    = None
    if not exact_match and not spotid_match:
        search_words = _key_words(f"{meta['artist']} {meta['title']}")
        for lw, la, lt, lk in ledger_at_list:
            if not search_words & lw:
                continue
            if _title_score(meta["artist"], meta["title"], f"{la} - {lt}") >= FUZZY_MATCH_THRESH:
                qy, ly = _extract_year(meta.get("title","")), _extract_year(lt)
                if qy and ly and qy != ly:
                    continue
                qr, lr2 = _remix_tag(meta.get("title","")), _remix_tag(lt)
                if (qr or lr2) and not (qr and lr2 and fuzz.ratio(qr, lr2) >= 60):
                    continue
                ledger_dur = ledger.get(lk, {}).get("duration_ms", 0)
                meta_dur   = meta.get("duration_ms", 0)
                if ledger_dur and meta_dur and abs(ledger_dur - meta_dur) > FUZZY_DUR_TOL:
                    continue
                fuzzy_match = True
                fuzzy_key   = lk
                logging.info(f"Fuzzy match found for '{fn}': {lk}")
                break

    if exact_match or spotid_match or fuzzy_match:
        already_present += 1

        # Find the right ledger key
        match_key = norm_dst if exact_match else \
                    ledger_by_spotid.get(spot_id) if spotid_match else \
                    fuzzy_key
        entry = ledger.get(match_key, {})

        # Check if file was flagged for redownload by pipeline_03
        match_path = _root(entry["path"]) if entry.get("path") and "\\" in entry.get("path","") else Path(entry.get("path", str(dst)))
        if match_key in ledger_paths and not match_path.exists():
            logging.info(f"Redownload queued (file missing from disk): {fn}")
            # Remove the ghost entry — file is gone, so the entry is stale (Problem 4)
            if match_key in ledger:
                del ledger[match_key]
                ledger_paths.discard(match_key)
                ledger_by_spotid.pop(spot_id, None)
                ledger_updated += 1
            download_queue.append({**meta, "clean_fn": fn})
            already_present -= 1
            continue

        # Update existing ledger entry with current Spotify state
        if match_key in ledger:
            old_playlists = set(entry.get("spotify_playlists") or [])
            new_playlists = set(meta["playlists"])
            changed = False
            if old_playlists != new_playlists:
                entry["spotify_playlists"] = sorted(new_playlists)
                logging.info(f"Updated playlists for '{fn}': {old_playlists} → {new_playlists}")
                changed = True
            if not entry.get("_spotify_id") and meta.get("_spotify_id"):
                entry["_spotify_id"]      = meta["_spotify_id"]
                entry["spotify_track_id"] = meta["_spotify_id"]   # mirror
                # ID only — source is never changed by a Spotify match.
                sp_id_linked += 1
                changed = True
                logging.info(f"Linked Spotify ID for '{fn}'")
            if changed:
                ledger[match_key] = entry
                ledger_updated   += 1
        continue

    download_queue.append({**meta, "clean_fn": fn})

if ledger_updated:
    save_ledger(ledger)
    print(f"  Ledger updated:                   {ledger_updated} entries")
    if sp_id_linked:
        print(f"  Spotify ID linked:                {sp_id_linked} tracks")

print(f"  Tracks in [RB] Spotify playlists: {len(sp_tracks)}")
print(f"  Already present:                  {already_present}")
print(f"  Queued for download:              {len(download_queue)}")

# ── download_queue.json — old RB tracks queued by pipeline_03 for HQ upgrade ──
if DL_QUEUE_FILE.exists():
    with open(DL_QUEUE_FILE, encoding="utf-8") as f:
        _dl_queue = json.load(f)
    fn_set   = {t["clean_fn"] for t in download_queue}
    dq_added = 0
    for norm_path, p in _dl_queue.items():
        artist = p.get("artist", "")
        title  = p.get("title", "")
        if not artist or not title:
            continue
        fn    = _clean_fn(artist, title)
        dst   = NEW_AUDIO / fn
        sp_id = p.get("_spotify_id")
        # Skip if already upgraded to new library
        if (dst.exists()
                or _norm(dst) in ledger_paths
                or fn in fn_set
                or (sp_id and sp_id in ledger_by_spotid)):
            continue
        download_queue.append({
            "artist":       artist,
            "title":        title,
            "album":        "",
            "duration_ms":  p.get("duration_ms", 0),
            "playlists":    [],
            "_spotify_id":  sp_id,       # set → Cell 3 can use for yt-dlp search
            "spot":         p.get("spot") or p.get("rb_tags") or [],
            "old_path":     p.get("old_path"),   # set → Cell 3 tries direct copy first
            "clean_fn":     fn,
            "source_hint":  "rb_upgrade",
        })
        fn_set.add(fn)
        dq_added += 1
    if dq_added:
        print(f"  RB upgrade queue:                 {dq_added} old tagged tracks")

# ══════════════════════════════════════════════════════════════════════════════
#%% Cell 2b: Optional — extra playlist selector (default off)
# ══════════════════════════════════════════════════════════════════════════════
if DOWNLOAD_EXTRA_PLAYLISTS:
    print("── Cell 2b: Extra Playlist Selector ──\n")

    all_playlists = _sp_all_playlists(client)
    all_pl_list = [
        (name, pid) for name, pid in all_playlists.items()
        if name not in ALL_RB_PLAYLISTS
    ]

    if _args.playlist_names is not None:
        # Pre-selected from UI — skip interactive input()
        selected_playlists = [(name, pid) for name, pid in all_pl_list
                              if name in _args.playlist_names]
        print(f"  Using {len(selected_playlists)} pre-selected playlist(s) from UI")
    else:
        # Interactive CLI selection (terminal fallback)
        page = 0
        page_size = 20
        selected_playlists = []

        while True:
            start = page * page_size
            end   = start + page_size
            playlist_page = all_pl_list[start:end]

            print(f"  Available playlists (page {page+1}, {start+1}-{min(end, len(all_pl_list))} of {len(all_pl_list)}):\n")
            for i, (name, _) in enumerate(playlist_page, start+1):
                mark = " ✓" if any(n == name for n, _ in selected_playlists) else ""
                print(f"  {i:>3}.  {name}{mark}")

            print(f"\n  Numbers to toggle, 'n' next, 'p' prev, Enter to confirm:")
            raw = input("  > ").strip()

            if raw.lower() == 'n':
                if end < len(all_pl_list):
                    page += 1
                else:
                    print("  Already on last page.")
                continue
            elif raw.lower() == 'p':
                if page > 0:
                    page -= 1
                else:
                    print("  Already on first page.")
                continue
            elif not raw:
                break
            else:
                tokens = [t.strip() for t in raw.split(",") if t.strip().isdigit()]
                for tok in tokens:
                    idx = int(tok) - 1
                    if 0 <= idx < len(all_pl_list):
                        item = all_pl_list[idx]
                        if item not in selected_playlists:
                            selected_playlists.append(item)
                            print(f"  + {item[0]}")
                        else:
                            selected_playlists.remove(item)
                            print(f"  - {item[0]} (deselected)")

    if _args.playlist_names is None and selected_playlists:
        all_pl_names = list(all_playlists.keys())
        indices = ", ".join(str(all_pl_names.index(name) + 1)
                           for name, _ in selected_playlists
                           if name in all_playlists)
        print(f"\n  Selected: {indices}")

    fn_set = {t["clean_fn"] for t in download_queue}

    for pl_name, pl_id in selected_playlists:
        for item in _sp_all_items(client, pl_id):
            track = item.get("track")
            if not track or not track.get("id"):
                continue
            artist = ", ".join(a["name"] for a in track["artists"])
            title  = track["name"]
            fn     = _clean_fn(artist, title)
            dst    = NEW_AUDIO / fn
            spot_id = track.get("id")
            if (dst.exists() 
                or _norm(dst) in ledger_paths 
                or fn in fn_set
                or (spot_id and spot_id in ledger_by_spotid)):
                continue
            download_queue.append({
                "artist":      artist,
                "title":       title,
                "album":       track["album"]["name"],
                "duration_ms": track["duration_ms"],
                "playlists":   [pl_name],
                "_spotify_id": track["id"],
                "clean_fn":    fn,
            })
            fn_set.add(fn)
        print(f"  ✓  Queued tracks from '{pl_name}'")

    print(f"\n  Download queue now: {len(download_queue)} tracks")


# ══════════════════════════════════════════════════════════════════════════════
#%% Cell 3: Acquire tracks
# ══════════════════════════════════════════════════════════════════════════════

# ── yt-dlp helpers ────────────────────────────────────────────────────────────
def _combined_score(artist: str, title: str, yt_entry: dict, dur_ms: int) -> float:
    yt_dur_ms   = (yt_entry.get("duration") or 0) * 1000
    dur_score   = max(0.0, 1 - abs(yt_dur_ms - dur_ms) / dur_ms) if dur_ms else 0.0
    title_score = _title_score(artist, title, yt_entry.get("title", "")) / 100
    return 0.6 * title_score + 0.4 * dur_score

def _dur_plausible(yt_entry: dict, dur_ms: int) -> bool:
    yt_ms = (yt_entry.get("duration") or 0) * 1000
    if dur_ms == 0:
        return True
    return 0.5 <= yt_ms / dur_ms <= 2.0

class YTDLPLogger:
    def debug(self, msg):
        if msg.startswith('[debug]'):
            return  # skip verbose debug noise
        logging.debug(f"yt-dlp: {msg}")
    def info(self, msg):
        logging.info(f"yt-dlp: {msg}")
    def warning(self, msg):
        if "No supported JavaScript runtime" in msg:
            return
        logging.warning(f"yt-dlp: {msg}")
    def error(self, msg):
        logging.error(f"yt-dlp: {msg}")

def _try_ytdlp(artist: str, title: str, dur_ms: int) -> Path | None:
    """
    Search top-10 YouTube results. Requires >=80% title match.
    Tries ranked candidates in order — skips age-gated, region-blocked, etc.
    UUID tmp filename prevents stale-file pickup.
    """
    query  = f"{artist} - {title}"
    tmp_id = uuid.uuid4().hex[:12]

    info_opts = {
        "js_runtimes": {"node": {"path": r"C:\Program Files\nodejs\node.EXE"}},
        # "quiet": True,  
        "logger": YTDLPLogger(),
        "quiet": False,   # ← change to False
        "noplaylist": True, "skip_download": True,
        "no_warnings": True, "default_search": "auto",
        "geo_bypass": True, 
        # "ignoreerrors": True,
        "cookiefile": COOKIE_FILE if COOKIE_FILE and Path(COOKIE_FILE).exists() else None,
    }
    dl_opts = {
        "js_runtimes": {"node": {"path": r"C:\Program Files\nodejs\node.EXE"}},
        "format": "bestaudio/best",
        "outtmpl": str(TMP_DIR / f"tmp_{tmp_id}.%(ext)s"),
        # "quiet": True, 
        "logger": YTDLPLogger(),
        "quiet": False,   # ← change to False
        "noplaylist": True,
        "no_warnings": True, 
        "default_search": "auto",
        "geo_bypass": True,
        "cookiefile": COOKIE_FILE if COOKIE_FILE and Path(COOKIE_FILE).exists() else None,
        "postprocessors": [{"key": "FFmpegExtractAudio",
                            "preferredcodec": "mp3", "preferredquality": "320"}],
    }

    try:
        with YoutubeDL(info_opts) as ydl:
            info    = ydl.extract_info(f"ytsearch10:{query}", download=False)
            entries = [e for e in (info.get("entries") or []) if e and e.get("duration")]
    except Exception as e:
        if "rate-limited" in str(e).lower() or "rate limited" in str(e).lower():
            raise RateLimitError()
        logging.warning(f"yt-dlp info [{query}]: {e}")
        return None

    if not entries:
        return None

    entries    = [e for e in entries if _dur_plausible(e, dur_ms)]
    candidates = [e for e in entries if _title_score(artist, title, e.get("title", "")) >= 75]

    if not candidates:
        best_title = (
            max(entries, key=lambda e: _title_score(artist, title, e.get("title", ""))).get("title")
            if entries else "none"
        )
        logging.warning(
            f"yt-dlp no title match >=75% for [{query}] — skipping. Best: '{best_title}'"
        )
        return None

    ranked = sorted(candidates, key=lambda e: _combined_score(artist, title, e, dur_ms), reverse=True)

    for candidate in ranked:
        url   = candidate["webpage_url"]
        ts    = _title_score(artist, title, candidate.get("title", ""))
        score = _combined_score(artist, title, candidate, dur_ms)
        logging.info(
            f"yt-dlp trying '{candidate.get('title')}' "
            f"(title={ts:.0f}% combined={score:.2f}) for [{query}]"
        )
        try:
            with YoutubeDL(dl_opts) as ydl:
                ydl.download([url])
            files = [f for f in TMP_DIR.iterdir() if f.name.startswith(f"tmp_{tmp_id}.")]
            if files:
                return files[0]
        except Exception as e:
            err = str(e).lower()
            if "rate-limited" in err or "rate limited" in err:
                raise RateLimitError()
            if any(k in err for k in ("sign in", "cookies", "age")):
                print(f"\n  ⚠  YouTube auth error — cookies.txt may need refreshing")
                logging.warning(f"yt-dlp auth error for '{candidate.get('title')}': {e}")
            else:
                logging.warning(f"yt-dlp skipping '{candidate.get('title')}': {e}")
            continue

    logging.warning(f"yt-dlp all candidates exhausted for [{query}]")
    return None

def _slskd_token() -> str | None:
    """Get a fresh slskd API token."""
    try:
        r = requests.post(f"{SLSKD_URL}/api/v0/session",
                          json={"username": SLSKD_USER, "password": SLSKD_PASS},
                          timeout=10)
        return r.json().get("token")
    except Exception as e:
        logging.warning(f"slskd auth failed: {e}")
        return None


def _slskd_search(token: str, query: str) -> list:
    """Run a slskd search and return all file candidates with metadata."""
    try:
        r = requests.post(f"{SLSKD_URL}/api/v0/searches",
                          json={"searchText": query},
                          headers={"Authorization": f"Bearer {token}"},
                          timeout=10)
        search_id = r.json()["id"]
    except Exception as e:
        logging.warning(f"slskd search failed for [{query}]: {e}")
        return []

    # Poll until complete
    for _ in range(SLSK_SEARCH_WAIT // 5 + 2):
        time.sleep(5)
        try:
            r = requests.get(f"{SLSKD_URL}/api/v0/searches/{search_id}",
                             headers={"Authorization": f"Bearer {token}"},
                             timeout=10)
            if r.json().get("state", "").startswith("Completed"):
                time.sleep(1)  # wait for responses to settle
                break
        except Exception:
            pass

    # Fetch with responses
    try:
        r = requests.get(f"{SLSKD_URL}/api/v0/searches/{search_id}?includeResponses=true",
                         headers={"Authorization": f"Bearer {token}"},
                         timeout=10)
        responses = r.json().get("responses", [])
    except Exception as e:
        logging.warning(f"slskd fetch results failed for [{query}]: {e}")
        return []

    # Flatten all files — audio only
    candidates = []
    for resp in responses:
        free_slot  = 1 if resp.get("hasFreeUploadSlot", False) else 0
        queue_len  = resp.get("queueLength", 999)
        for f in resp.get("files", []):
            ext = f["filename"].split(".")[-1].lower()
            if ext not in {"mp3", "flac", "wav"}:
                continue
            br = f.get("bitRate", 0) or 0
            if ext == "mp3" and 0 < br < 320:
                continue
            # Problem 3: reject implausibly large files (mislabeled FLAC / compilations)
            size_mb = (f.get("size", 0) or 0) / (1024 * 1024)
            _, reject_mb = SIZE_LIMITS_MB.get(ext, (None, None))
            if reject_mb and size_mb > reject_mb:
                logging.info(f"slskd size reject: {f['filename']} ({size_mb:.0f}MB .{ext})")
                continue
            candidates.append({
                "username":  resp["username"],
                "filename":  f["filename"],
                "size":      f.get("size", 0),
                "bitrate":   f.get("bitRate", 0) or 0,
                "length":    f.get("length", 0) or 0,
                "ext":       ext,
                "free_slot": free_slot,
                "queue_len": queue_len,
            })

    if FREE_SLOT_ONLY:
        candidates = [c for c in candidates if c["free_slot"] > 0]
    return candidates

def _try_soulseek(artist: str, title: str, dur_ms: int, skip_users: set = None) -> Path | tuple | None:
    """
    Try to download via slskd. Two-pass search:
    1. Extended search — strict title match + 'extended' in filename
    2. Regular search — normal title match, prefer non-radio-edit
    Returns Path to downloaded file in SLSK_DIR, or None.
    """
    token = _slskd_token()
    if not token:
        return None

    dur_s = dur_ms / 1000

    def _is_extended(fn: str) -> bool:
        return "extended" in fn.lower()
    
    _slsk_bl = _load_slsk_blacklist()

    def _rank_and_filter(candidates, base_title, threshold, extended_only=False):
        import re
        BOOST = {'extended': 40, 'ext': 40, 'club': 10, 'remaster': 10, 'remastered': 10}
        PENALTY = {'radio': -20, 'bootleg': -15, 'pn': -20}
        results = []
        for c in candidates:
            if c["queue_len"] > SLSK_MAX_QUEUE and not c["free_slot"]:
                continue
            if skip_users and c["username"] in skip_users:
                continue
            # Check SLSK blacklist
            if f"{c['username']}/{c['filename']}" in _slsk_bl:
                logging.info(f"Skipping blacklisted SLSK file: {c['filename']} from {c['username']}")
                continue
            fn_stem = c["filename"].split("\\")[-1].rsplit(".", 1)[0]
            if extended_only and not _is_extended(fn_stem):
                continue
            title_s = _title_score(artist, base_title, fn_stem)
            if title_s < threshold:
                continue
            total = title_s
            # quality — MP3 320 preferred (CDJ-safe); FLAC fallback; WAV/AIFF last (huge)
            br, ext = c["bitrate"], c["ext"]
            if br == 320 and ext == "mp3":   total += 80
            elif ext == "flac":              total += 10
            elif ext in ("wav", "aiff"):     total += 5
            # extra word scoring
            fn_clean = fn_stem.lower().replace("_", " ")
            known = set(re.split(r'[\s\-\.]+', (artist + ' ' + base_title).lower()))
            all_words = set(re.split(r'[\s\-\.\(\)\[\]]+', fn_clean))
            extra = {w for w in all_words - known if len(w) > 2 and not w.isdigit()}
            for word in extra:
                if word in BOOST:      total += BOOST[word]
                elif word in PENALTY:  total += PENALTY[word]
                else:                  total -= 15
            # availability
            if c["free_slot"]:   total += 30
            else:                total -= 20
            total -= (c["queue_len"] // 100) * 3
            results.append((total, c))
        results.sort(key=lambda x: x[0], reverse=True)
        return results

    def _download(token, candidate) -> Path | str | None:
        username = candidate["username"]
        filename = candidate["filename"]
        size     = candidate["size"]
        fn_clean = filename.split("\\")[-1]
        dst      = SLSK_DIR / fn_clean

        existing = [f for f in SLSK_DIR.rglob(f"{glob.escape(Path(fn_clean).stem)}*") if f.is_file()]
        if existing:
            logging.info(f"slskd already exists: {existing[0].name}")
            return existing[0]
        # Normalised check — "03 - Title" and "05 - Title" are the same track
        _norm_stem = re.sub(r'^\d{1,2}[\s\-_.]+', '', Path(fn_clean).stem).lower().strip()
        existing_norm = [f for f in SLSK_DIR.rglob("*") if f.is_file()
                         and re.sub(r'^\d{1,2}[\s\-_.]+', '', f.stem).lower().strip() == _norm_stem]
        if existing_norm:
            logging.info(f"slskd already exists (norm): {existing_norm[0].name}")
            return existing_norm[0]
        try:
            r = requests.post(
                f"{SLSKD_URL}/api/v0/transfers/downloads/{username}",
                json=[{"filename": filename, "size": size}],
                headers={"Authorization": f"Bearer {token}"},
                timeout=10
            )
            resp  = r.json()
            items = (resp.get("enqueued") or []) + (resp.get("existing") or [])
            if not items:
                logging.warning(f"slskd enqueue empty response for {fn_clean} from {username}")
                return None
            dl_id = items[0]["id"]
        except Exception as e:
            logging.warning(f"slskd enqueue failed for {fn_clean} from {username}: {e}")
            return None

        # Poll up to SLSK_DL_TIMEOUT seconds
        _last_place = None

        for _ in range(SLSK_DL_TIMEOUT // 10):
            time.sleep(5)
            try:
                r = requests.get(f"{SLSKD_URL}/api/v0/transfers/downloads",
                                headers={"Authorization": f"Bearer {token}"},
                                timeout=10)
                for u in r.json():
                    for d in u.get("directories", []):
                        for f in d.get("files", []):
                            if f["id"] == dl_id:
                                state = f["state"]
                                if "Succeeded" in state:
                                    # recursive search in SLSK_DIR
                                    matches = [mf for mf in SLSK_DIR.rglob(f"{glob.escape(Path(fn_clean).stem)}*") if mf.is_file()]
                                    if not matches:
                                        logging.warning(f"slskd Succeeded but file not found: {fn_clean}")
                                        return None
                                    got = matches[0]
                                    ok, reason, _facts = accept_file(got, dur_ms)
                                    if not ok:
                                        logging.warning(f"slskd rejected: {got.name} — {reason}")
                                        _add_slsk_blacklist(username, filename, reason)
                                        try: got.unlink()
                                        except Exception: pass
                                        return None
                                    logging.info(f"slskd downloaded + validated: {got.name} from {username}")
                                    return got
                                elif any(s in state for s in ("Aborted", "Errored", "Rejected")):
                                    logging.warning(f"slskd {state}: {fn_clean} from {username}")
                                    return None
                                elif "Queued, Remotely" in state:
                                    place = f.get("placeInQueue", 0) or 0
                                    if place > SLSK_QUEUE_POS_MAX:
                                        logging.info(f"slskd high queue ({place}): {fn_clean} from {username} — saving to queue")
                                        return ("QUEUED", dl_id, candidate)
                                    if place != _last_place:
                                        print(f"  slskd queued (pos {place}): {fn_clean} — waiting", flush=True)
                                        _last_place = place
            except Exception as e:
                logging.warning(f"slskd poll error: {e}")

        logging.warning(f"slskd timeout: {fn_clean} from {username} — cancelling")
        try:
            requests.delete(
                f"{SLSKD_URL}/api/v0/transfers/downloads/{username}/{dl_id}",
                headers={"Authorization": f"Bearer {token}"},
                timeout=5,
            )
        except Exception as _ce:
            logging.warning(f"slskd cancel failed: {_ce}")
        return None

    # ── Pass 1: Extended search ────────────────────────────────────────────────
    queued_entry = None

    # ── Single search ──────────────────────────────────────────────────────────
    query = f"{artist} {title}"
    logging.info(f"slskd search: [{query}]")
    candidates = _slskd_search(token, query)
    ranked = _rank_and_filter(candidates, title, SLSK_TITLE_THRESH)
    for score, c in ranked:
        logging.info(f"slskd trying '{c['filename'].split(chr(92))[-1]}' "
                     f"(score={score:.0f}) from {c['username']}")
        result = _download(token, c)
        if result is None:
            continue
        if isinstance(result, tuple) and result[0] == "QUEUED":
            queued_entry = queued_entry or result
            break
        return result  # Path — success

    if queued_entry:
        return queued_entry

    logging.warning(f"slskd no results for [{query}]")
    return None

# ── Pre-run TMP cleanup ───────────────────────────────────────────────────────
cleaned = sum(1 for f in TMP_DIR.glob("tmp_*") if not f.unlink())  # unlink returns None
for f in TMP_DIR.glob("tmp_*"):                                      # handle failures silently
    try: f.unlink()
    except Exception as e: logging.warning(f"Could not remove tmp file {f}: {e}")

# ── Load checkpoint ───────────────────────────────────────────────────────────
checkpoint = load_checkpoint()

if RETRY_FAILED:
    # Remove failed entries so they get retried
    checkpoint = {k: v for k, v in checkpoint.items()
              if v["status"] not in ("acquired",) or 
              (v["status"] == "acquired" and Path(v["src_path"]).exists())}
    save_checkpoint(checkpoint)

# Merge playlists for duplicate Spotify IDs, then deduplicate
_id_to_playlists = defaultdict(set)
_fn_to_playlists = defaultdict(set)

for t in download_queue:
    sp_id = t.get("_spotify_id")
    fn    = t["clean_fn"]
    pls   = set(t.get("playlists") or [])
    if sp_id:
        _id_to_playlists[sp_id].update(pls)
    _fn_to_playlists[fn].update(pls)

seen = set()
deduped = []
for t in download_queue:
    sp_id = t.get("_spotify_id")
    fn    = t["clean_fn"]
    key   = sp_id or fn
    if key in seen:
        continue
    seen.add(key)
    if sp_id:
        t["playlists"] = sorted(_id_to_playlists[sp_id])
    else:
        t["playlists"] = sorted(_fn_to_playlists[fn])
    deduped.append(t)

print(f"  Download queue after dedup: {len(download_queue)} → {len(deduped)}")
download_queue = deduped

done_fns        = set(checkpoint.keys())
_stale = [k for k, v in checkpoint.items()
          if v.get("status") == "acquired"
          and v.get("src_path")
          and not Path(v["src_path"]).exists()]
if _stale:
    for _k in _stale:
        del checkpoint[_k]
    save_checkpoint(checkpoint)
    logging.info(f"Checkpoint: purged {len(_stale)} stale src paths — will re-download")
acquired        = [v for v in checkpoint.values() if v["status"] == "acquired"]
failed          = [v["meta"] for v in checkpoint.values() if v["status"] == "failed"]
slsk_queue_fns  = set(load_slsk_queue().keys())
remaining_queue = [t for t in download_queue 
                   if t["clean_fn"] not in done_fns 
                   and t["clean_fn"] not in slsk_queue_fns]

if done_fns:
    print(f"  Resuming — {len(done_fns)} already processed, {len(remaining_queue)} remaining")

print(f"\n── Cell 3: Acquire {len(remaining_queue)} tracks ──\n")
total = len(remaining_queue) + len(done_fns)
_ledger_norm_keys = {_norm(k) for k in ledger}
_dedup_idx = build_dedup_index(ledger)

for i, track in enumerate(remaining_queue[:DEBUG_LIMIT] if DEBUG_LIMIT else remaining_queue, len(done_fns) + 1):
    artist, title = track["artist"], track["title"]
    dur_ms        = track["duration_ms"]
    fn            = track["clean_fn"]
    pfx           = f"  [{i:>3}/{total}]  {artist} – {title}"

    src_path        = None
    source          = None
    old_path_purge  = None   # legacy checkpoint field — always None in single-library v1.0
    # Pre-flight: abort if track already exists anywhere
    _preflight_norm = _norm(NEW_AUDIO / fn)
    if (NEW_AUDIO / fn).exists():
        logging.info(f"Pre-flight skip (file on disk): {fn}")
        already_present += 1
        continue
    if _preflight_norm in _ledger_norm_keys:
        logging.info(f"Pre-flight skip (in ledger): {fn}")
        already_present += 1
        continue
    _fn_stem = _norm_stem(Path(fn).stem)
    if _fn_stem in slsk_stems or any(
        fuzz.token_set_ratio(_fn_stem, s) >= 88 for s in slsk_stems
    ):
        logging.info(f"Pre-flight skip (in SLSK_DIR): {fn}")
        already_present += 1
        continue
    # Spotify-ID / fuzzy-title+duration dedup against the ledger
    _have = already_have(track, ledger, _dedup_idx)
    if _have:
        logging.info(f"Pre-flight skip (already_have → {_have}): {fn}")
        already_present += 1
        continue

    # 1. Soulseek (slskd API)
    if not YTDLP_ONLY:
        print(f"{pfx}  →  slskd...", end="", flush=True)
        slsk_result = _try_soulseek(artist, title, dur_ms)
        if isinstance(slsk_result, Path):
            src_path = slsk_result
            source = "soulseek"
            print("  ✓")
        elif isinstance(slsk_result, tuple) and slsk_result[0] == "QUEUED":
            _, dl_id, candidate = slsk_result
            # Save to slsk_queue.json and skip yt-dlp
            slsk_queue      = load_slsk_queue()
            _ledger_31      = load_ledger()
            _ledger_31_keys = {_norm(k) for k in _ledger_31}
            slsk_queue[fn]  = {
                "dl_id":            dl_id,
                "queued_at":        datetime.now().isoformat(),
                "username":         candidate["username"],
                "filename":         candidate["filename"],
                "size":             candidate["size"],
                "candidates_tried": [candidate["username"]],
                "attempts":         1,
                "meta":             track,
            }
            save_slsk_queue(slsk_queue)
            print(f"  queued (saved to slsk_queue.json)")
            logging.info(f"slsk queued: {artist} - {title} from {candidate['username']}")
            # Skip yt-dlp for now — mark as slsk_queued in checkpoint
            checkpoint[fn] = {"status": "slsk_queued", "fn": fn, "meta": track}
            save_checkpoint(checkpoint)
            continue  # skip to next track
        else:
            print("  no result")

    # 2. Manual / purchased source folders (manual, bandcamp, beatport, …)
    if not src_path:
        for _sname, _sfolder in ACQUIRE_SOURCES.items():
            cand = _match_folder(_sfolder, artist, title, dur_ms)
            if cand:
                src_path = cand
                source   = _sname
                print(f"{pfx}  →  {_sname} ✓")
                break

    # 3. yt-dlp
    if not src_path and not SLSK_ONLY:
        print(f"{pfx}  →  yt-dlp...", end="", flush=True)
        try:
            src_path = _try_ytdlp(artist, title, dur_ms)
        except RateLimitError:
            print(f"\n  ⚠  YouTube rate limit hit — stopping early. Wait 1 hour then rerun.")
            logging.warning("YouTube rate limit — pipeline stopped early")
            break
        if src_path:
            source = "ytdlp"
            print("  ✓")
        else:
            print("  FAILED")

    # Checkpoint
    if src_path:
        entry = {
            "status":          "acquired",
            "fn":              fn,
            "source":          source,
            "src_path":        str(src_path),
            "old_path_purge":  str(old_path_purge) if old_path_purge else None,
            "meta":            track,
        }
        acquired.append(entry)
    else:
        entry = {"status": "failed", "fn": fn, "meta": track}
        failed.append(track)
        logging.error(f"Failed: {artist} - {title}")

    checkpoint[fn] = entry
    save_checkpoint(checkpoint)
    time.sleep(YTDLP_SLEEP)

print(f"\n  Acquired: {len(acquired)}  |  Failed: {len(failed)}")
# ══════════════════════════════════════════════════════════════════════════════
#%% Cell 3.1: Resolve pending slsk_queue entries
# ══════════════════════════════════════════════════════════════════════════════
print("\n── Cell 3.1: Resolve slsk queue ──\n")
slsk_queue = load_slsk_queue()
_ledger_31      = load_ledger()
_ledger_31_keys = {_norm(k) for k in _ledger_31}
token = _slskd_token()
resolved = []
still_queued = []

for fn, entry in list(slsk_queue.items()):
    artist  = entry["meta"]["artist"]
    title   = entry["meta"]["title"]
    dl_id   = entry["dl_id"]
    queued_at = datetime.fromisoformat(entry["queued_at"])
    age     = (datetime.now() - queued_at).total_seconds()
    username = entry["username"]
    fn_clean = entry["filename"].split("\\")[-1]
    dst      = SLSK_DIR / fn_clean

    # Check if file already downloaded
    if dst.exists():
        ok, reason, _facts = accept_file(dst, entry["meta"].get("duration_ms"))
        if not ok:
            print(f"  ✗  {artist} - {title}  (rejected: {reason})")
            _add_slsk_blacklist(username, entry["filename"], reason)
            try: dst.unlink()
            except Exception: pass
            resolved.append(fn)   # give up on this candidate; reconcile/next run can retry source
            continue
        print(f"  ✓  {artist} - {title}  (file found, validated)")
        acquired.append({
            "status":         "acquired",
            "fn":             fn,
            "source":         "soulseek",
            "src_path":       str(dst),
            "old_path_purge": None,
            "meta":           entry["meta"],
        })
        resolved.append(fn)
        continue
    # Check if already in NEW_AUDIO (copied by previous run or sp2slsk)
    audio_dst      = NEW_AUDIO / _clean_fn(artist, title)
    audio_dst_flac = NEW_AUDIO / _clean_fn(artist, title, ".flac")
    if audio_dst.exists() or audio_dst_flac.exists() or _norm(audio_dst) in _ledger_31_keys or _norm(audio_dst_flac) in _ledger_31_keys:
        print(f"  ✓  {artist} - {title}  (already in audio dir)")
        slsk_queue[fn]["resolved_at"] = datetime.now().isoformat()
        resolved.append(fn)
        continue

    # Check transfer status — first by dl_id, then by filename fallback
    status    = None
    found_in_slskd = False

    if token:
        try:
            r = requests.get(f"{SLSKD_URL}/api/v0/transfers/downloads",
                             headers={"Authorization": f"Bearer {token}"},
                             timeout=10)
            all_transfers = r.json()

            fn_stem_lower = Path(fn_clean).stem.lower()

            for u in all_transfers:
                for d in u.get("directories", []):
                    for f in d.get("files", []):
                        # Layer 1: exact dl_id match
                        if f["id"] == dl_id:
                            status = f["state"]
                            found_in_slskd = True
                            break
                        # Layer 2: filename stem match (catches ID-pruned transfers)
                        f_stem = Path(f.get("filename","").split("\\")[-1]).stem.lower()
                        if f_stem and f_stem == fn_stem_lower:
                            status = f["state"]
                            found_in_slskd = True
                            # Update dl_id in case it changed
                            entry["dl_id"] = f["id"]
                            break
                    if found_in_slskd:
                        break
                if found_in_slskd:
                    break

        except Exception as e:
            logging.warning(f"slsk queue poll error: {e}")

    if status and "Succeeded" in status:
        matches = [mf for mf in SLSK_DIR.rglob(f"{glob.escape(Path(fn_clean).stem)}*") if mf.is_file()]
        if matches:
            ok, reason, _facts = accept_file(matches[0], entry["meta"].get("duration_ms"))
            if not ok:
                print(f"  ✗  {artist} - {title}  (rejected: {reason})")
                _add_slsk_blacklist(username, entry["filename"], reason)
                try: matches[0].unlink()
                except Exception: pass
                resolved.append(fn)
            else:
                print(f"  ✓  {artist} - {title}  (download succeeded, validated)")
                acquired.append({
                    "status":         "acquired",
                    "fn":             fn,
                    "source":         "soulseek",
                    "src_path":       str(matches[0]),
                    "old_path_purge": None,
                    "meta":           entry["meta"],
                })
                resolved.append(fn)
        else:
            logging.warning(f"slsk Succeeded but file missing: {fn_clean}")
            resolved.append(fn)

    elif found_in_slskd:
        # File is in slskd in some active state — keep waiting regardless of age
        state_clean = status or "unknown"
        print(f"  ⏳  {artist} - {title}  (slskd: {state_clean}, {int(age/3600)}h old)")
        still_queued.append(fn)

    elif age > SLSK_QUEUE_TTL or entry.get("attempts", 1) >= SLSK_MAX_ATTEMPTS:
        # Truly expired — not in slskd, TTL exceeded
        print(f"  ✗  {artist} - {title}  (expired after {int(age/3600)}h, not in slskd)")
        logging.warning(f"slsk queue expired: {artist} - {title}")
        resolved.append(fn)

    else:
        # Not found in slskd at all — try a new candidate, keep in queue if none found
        print(f"  ↻  {artist} - {title}  (not in slskd — searching new candidate)")
        tried = set(entry.get("candidates_tried", []))
        new_result = _try_soulseek(
            artist, entry["meta"]["title"], entry["meta"]["duration_ms"],
            skip_users=tried
        )
        if isinstance(new_result, Path):
            print(f"    ✓  downloaded from new candidate")
            acquired.append({
                "status":         "acquired",
                "fn":             fn,
                "source":         "soulseek",
                "src_path":       str(new_result),
                "old_path_purge": None,
                "meta":           entry["meta"],
            })
            resolved.append(fn)
        elif isinstance(new_result, tuple) and new_result[0] == "QUEUED":
            _, new_dl_id, new_candidate = new_result
            entry["dl_id"]      = new_dl_id
            entry["username"]   = new_candidate["username"]
            entry["filename"]   = new_candidate["filename"]
            entry["queued_at"]  = datetime.now().isoformat()
            entry["candidates_tried"].append(new_candidate["username"])
            entry["attempts"]   = entry.get("attempts", 1) + 1
            still_queued.append(fn)
            print(f"    ⏳  re-queued from {new_candidate['username']}")
        else:
            # No new candidates found right now
            attempts = entry.get("attempts", 1) + 1
            entry["attempts"]  = attempts
            entry["queued_at"] = datetime.now().isoformat()
            if attempts >= SLSK_MAX_ATTEMPTS:
                print(f"    ✗  no new candidates — max attempts reached, dropping")
                logging.warning(f"slsk queue max attempts: {artist} - {title}")
                resolved.append(fn)
            else:
                still_queued.append(fn)
                print(f"    ⏳  no new candidates — staying in queue (attempt {attempts}/{SLSK_MAX_ATTEMPTS})")

# Persist updated entries (dl_id refreshes, attempt counts) then remove resolved
for fn in still_queued:
    if fn in slsk_queue:
        slsk_queue[fn].update({
            k: v for k, v in (slsk_queue.get(fn) or {}).items()
        })

# Mark resolved entries — never delete, used by sp2slsk as manifest
_now = datetime.now().isoformat()
for fn in resolved:
    if fn in slsk_queue:
        slsk_queue[fn]["resolved_at"] = _now
for fn in still_queued:
    if fn in slsk_queue:
        slsk_queue[fn].pop("resolved_at", None)  # clear if re-queued
save_slsk_queue(slsk_queue)

print(f"  Resolved: {len(resolved)}  |  Still queued: {len(still_queued)}")

# ══════════════════════════════════════════════════════════════════════════════
#%% Cell 3.5: Direct copy of unmatched manual tracks
# ══════════════════════════════════════════════════════════════════════════════
print("\n── Cell 3.5: Source-folder imports (manual / bandcamp / beatport …) ──\n")
src_copied = src_binned = 0
_fp_index = {v["fingerprint"]: k for k, v in ledger.items() if v.get("fingerprint")}
_audio_norm_prefix = _norm(str(NEW_AUDIO)) + "/"
_audio_stem_idx = {
    Path(k).stem.lower(): k
    for k in ledger
    if _norm(k).startswith(_audio_norm_prefix)
}

# Flatten (source_name, file) across every non-download source folder.
_sweep_map = SWEEP_SOURCES if SWEEP_ONLY else ACQUIRE_SOURCES
_src_files = [(_sname, f)
              for _sname, _sfolder in _sweep_map.items()
              for f in _sfolder.rglob("*") if f.suffix.lower() in AUDIO_EXTS]

for _sname, f in _src_files:
    # Match against sp_tracks first so we can use the canonical filename
    best_meta, best_score = None, 0
    for track in sp_tracks.values():
        score = _title_score(track["artist"], track["title"], f.stem)
        if score > best_score:
            best_score, best_meta = score, track

    if best_meta and best_score >= 75:
        artist, title = best_meta["artist"], best_meta["title"]
        meta = {"artist": artist, "title": title,
                "playlists": best_meta.get("playlists", []),
                "_spotify_id": best_meta.get("_spotify_id")}
    else:
        parts = re.sub(r"^\d+[\s\-_.]+", "", f.stem).split(" - ", 1)
        artist, title = (parts[0].strip(), parts[1].strip()) if len(parts) == 2 else ("Unknown", f.stem)
        meta = {"artist": artist, "title": title, "playlists": [], "_spotify_id": None}

    # Spotify ID / fuzzy-title dedup against ledger (same gate as Cell 3)
    _ah = already_have(meta, ledger, _dedup_idx)
    if _ah:
        logging.info(f"[{_sname}] {f.name} → skipped (already_have → {_ah})")
        continue

    # SAME filename convention as Cell 4 — this is what kills the dupe path
    src_ext  = f.suffix.lower()
    cfn      = _clean_fn(artist, title, src_ext if src_ext in {".flac", ".wav", ".aiff"} else ".mp3")
    dst      = NEW_AUDIO / cfn
    norm_dst = _norm(dst)

    # Cross-format dup guard: same stem already in audio/ under a different extension
    _stem_conflict = _audio_stem_idx.get(dst.stem.lower())
    if _stem_conflict and _stem_conflict != norm_dst:
        _cf_src = ledger.get(_stem_conflict, {}).get("source", "")
        if not SOURCE_DIRS.get(_cf_src, (None, False))[1]:
            logging.info(f"[{_sname}] {f.name} → skipped (same title in audio/ as {Path(ledger.get(_stem_conflict, {}).get('path', '')).suffix})")
            continue

    # Idempotent quality logic: skip if we already hold a final (non-upgradeable)
    # source; replace only an upgradeable source (ytdlp) with this HQ import.
    existing_source = ledger.get(norm_dst, {}).get("source", "")
    existing_upg    = SOURCE_DIRS.get(existing_source, (None, False))[1]
    if existing_source and not existing_upg:
        continue
    if not existing_upg and dst.exists():
        continue

    # ── Validation gate FIRST — duration is a guideline, not a gate here ─────
    # Pass None so accept_file only checks MIN/MAX absolute duration, not Spotify expected.
    ok, reason, facts = accept_file(f, None)
    if not ok:
        print(f"  ✗  [{_sname}] {f.name}  →  bin ({reason})")
        _bin_local = BIN_DIR / f.name
        BIN_DIR.mkdir(parents=True, exist_ok=True)
        if not _bin_local.exists():
            shutil.copy2(str(f), str(_bin_local))
        src_binned += 1
        continue

    # ── Swap old file AFTER validation — prevents ghost entries on failure ────
    if existing_source and existing_upg:
        if dst.exists():
            BIN_DIR.mkdir(parents=True, exist_ok=True)
            shutil.move(str(dst), str(BIN_DIR / dst.name))
            logging.info(f"{_sname} replacing {existing_source}: moved {dst.name} to bin")
        ledger_paths.discard(norm_dst)

    # ── Content dedup (fingerprint + duration; shared intro is not a dup) ──────
    fp = facts.get("fingerprint")
    _dup_key = _fp_index.get(fp) if fp else None
    if _dup_key and _dup_key != norm_dst:
        _new_dur = facts.get("duration_ms")
        _ex_dur  = ledger.get(_dup_key, {}).get("duration_ms", 0)
        if not (_new_dur and _ex_dur) or _dur_ok(_new_dur, _ex_dur):
            print(f"  ⧉  [{_sname}] {f.name}  →  dup of '{Path(ledger[_dup_key]['path']).name}', skipped")
            continue
        else:
            logging.info(f"fp matches {_dup_key} but duration differs — keeping '{f.name}'")

    shutil.copy2(str(f), str(dst))
    ledger[norm_dst] = make_entry(dst, meta, _sname, _sname, facts)
    if fp:
        _fp_index[fp] = norm_dst
    _audio_stem_idx[dst.stem.lower()] = norm_dst
    ledger_paths.add(norm_dst)
    src_copied += 1
    tag = f"matched '{artist} - {title}' ({best_score:.0f}%)" if (best_meta and best_score >= 75) else f"no Spotify match ({best_score:.0f}%)"
    print(f"  ✓  [{_sname}] {cfn}  →  {tag}")

if src_copied or src_binned:
    save_ledger(ledger)
print(f"  Source imports copied: {src_copied}  |  binned: {src_binned}", flush=True)

# ══════════════════════════════════════════════════════════════════════════════
#%% Cell 4: Validate, copy to new library, write ledger entries
# ══════════════════════════════════════════════════════════════════════════════
"""
ONE gate for every source (Problems 2 & 3). No source is exempt.
  accept_file()  → duration >= MIN_DURATION_S, mutagen-readable, size sane.
  FAIL  → retry yt-dlp (also gated) → bin if that fails too.
  PASS  → fingerprint dedup (Problem 1): exact content match already in library → bin.
          else copy + write canonical ledger entry via make_entry().
"""
print("── Cell 4: Validate & copy ──\n")

ledger   = load_ledger()
moved_ok = moved_bin = dup_binned = 0
seen_fp  = {v["fingerprint"]: k for k, v in ledger.items() if v.get("fingerprint")}

def _bin(src: Path, fn: str, why: str):
    global moved_bin
    BIN_DIR.mkdir(parents=True, exist_ok=True)
    if src.exists():
        dst_bin = BIN_DIR / fn
        if dst_bin.exists():
            dst_bin = BIN_DIR / f"{Path(fn).stem}__{datetime.now():%H%M%S}{Path(fn).suffix}"
        shutil.copy2(str(src), str(dst_bin))
    logging.warning(f"Binned '{fn}': {why}")
    moved_bin += 1

for item in acquired:
    fn              = item["fn"]
    source          = item["source"]
    src_path        = Path(item["src_path"])
    old_path_purge  = Path(item["old_path_purge"]) if item.get("old_path_purge") else None
    meta            = item["meta"]
    dur_ms          = meta.get("duration_ms", 0)
    dst             = NEW_AUDIO / fn
    original_source = source

    # Use actual source extension — slskd may deliver FLAC even though fn was queued as .mp3
    _actual_ext = src_path.suffix.lower()
    if _actual_ext in {".flac", ".wav", ".aiff"} and dst.suffix.lower() == ".mp3":
        fn  = Path(fn).stem + _actual_ext
        dst = NEW_AUDIO / fn

    if not src_path.exists():
        print(f"  !! {fn}  →  source file missing, skipped")
        logging.error(f"Source missing for '{fn}': {src_path}")
        continue

    # ── Universal validation gate ─────────────────────────────────────────────
    ok, reason, facts = accept_file(src_path, dur_ms)
    if not ok:
        print(f"  !! {fn}  ({reason})  → retrying yt-dlp...", end="", flush=True)
        logging.warning(f"Gate fail '{fn}' from '{source}': {reason}")
        ytdlp_path = _try_ytdlp(meta["artist"], meta["title"], dur_ms)
        if ytdlp_path:
            ok2, reason2, facts2 = accept_file(ytdlp_path, dur_ms)
            if ok2:
                src_path, source, facts = ytdlp_path, "ytdlp", facts2
                print("  ✓")
            else:
                print(f"  FAILED  →  bin (yt-dlp also bad: {reason2})")
                _bin(src_path, fn, reason)
                try: ytdlp_path.unlink()
                except Exception: pass
                continue
        else:
            print("  FAILED  →  bin")
            _bin(src_path, fn, reason)
            continue

    # ── Content dedup (Problem 1): exact same audio already in library ─────────
    # Requires fingerprint match AND matching duration — a shared intro (Original
    # vs Extended mix) is NOT a duplicate and must be kept.
    fp = facts.get("fingerprint")
    _dup_key = seen_fp.get(fp) if fp else None
    if _dup_key and _dup_key != _norm(dst):
        _new_dur = facts.get("duration_ms") or dur_ms
        _ex_dur  = ledger.get(_dup_key, {}).get("duration_ms", 0)
        if not (_new_dur and _ex_dur) or _dur_ok(_new_dur, _ex_dur):
            print(f"  ⧉  {fn}  →  duplicate of '{Path(ledger[_dup_key]['path']).name}', binned")
            logging.info(f"Content dup of {_dup_key} — binned '{fn}'")
            _bin(src_path, fn, f"content-dup of {_dup_key}")
            dup_binned += 1
            if source == "ytdlp":
                try: src_path.unlink()
                except Exception: pass
            continue
        else:
            logging.info(f"fp matches {_dup_key} but duration differs "
                         f"({_new_dur}ms vs {_ex_dur}ms) — keeping '{fn}' (likely different mix)")

    # ── Copy to new library ───────────────────────────────────────────────────
    if not dst.exists():
        shutil.copy2(str(src_path), str(dst))

    if source == "ytdlp":
        try: src_path.unlink()
        except Exception: pass

    norm_dst         = _norm(dst)
    ledger[norm_dst] = make_entry(dst, meta, source, original_source, facts)
    if fp:
        seen_fp[fp] = norm_dst
    print(f"  ✓  {fn}  ({source})")
    moved_ok += 1

# ── Failed log ────────────────────────────────────────────────────────────────
if failed:
    with open(FAILED_LOG, "a", encoding="utf-8") as f:
        _manual_dir = ACQUIRE_SOURCES.get("manual", NEW_ROOT / "source" / "manual")
        f.write(f"\n# Run: {datetime.now().isoformat()}\n  → Place manually in: {_manual_dir}\n")
        for t in failed:
            f.write(f"{t['artist']} - {t['title']}\n")

save_ledger(ledger)
clear_checkpoint()

# ── End-of-run reconcile: enforce the four invariants (Problem 4) ─────────────
# Idempotent — on an already-clean library this is a no-op.
_recon = reconcile_ledger(ledger, sp_reference=sp_reference, quarantine_ghosts=not _args.reconcile_dry)
save_ledger(ledger)
print(f"  Reconcile:    adopted {_recon['adopted']}  ghosts {_recon['ghosts']}  "
      f"rekeyed {_recon['rekeyed']}  sp-linked {_recon['sp_linked']}")

# ── Remove completed entries from download_queue.json ─────────────────────────
if DL_QUEUE_FILE.exists():
    with open(DL_QUEUE_FILE, encoding="utf-8") as f:
        _dl_q = json.load(f)
    _done_norms = set()
    for item in acquired:
        if item["meta"].get("source_hint") == "rb_upgrade":
            _done_norms.add(_norm(NEW_AUDIO / item["fn"]))
    _before = len(_dl_q)
    _dl_q   = {k: v for k, v in _dl_q.items()
               if _norm(NEW_AUDIO / _clean_fn(v.get("artist",""), v.get("title",""))) not in _done_norms}
    if len(_dl_q) < _before:
        atomic_write_json(DL_QUEUE_FILE, _dl_q)
        print(f"  Download queue: {_before - len(_dl_q)} completed entries removed")

print(f"\n  Copied:       {moved_ok}")
print(f"  Sent to bin:  {moved_bin}  (incl. {dup_binned} content-dupes)" if dup_binned else f"  Sent to bin:  {moved_bin}")
print(f"  Failed:       {len(failed)}  → {FAILED_LOG}")
print(f"  Ledger:       {len(ledger)} entries  → {LEDGER}")


# ══════════════════════════════════════════════════════════════════════════════
#%% Stats: DB & Ledger summary
# ══════════════════════════════════════════════════════════════════════════════
print("\n── DB Stats ──")

total_entries  = len(ledger)
by_source      = {}
no_spot        = 0
has_spot       = 0
never_synced   = 0

for entry in ledger.values():
    src = entry.get("source", "unknown")
    by_source[src] = by_source.get(src, 0) + 1

    if entry.get("spot") is None:
        no_spot += 1
    else:
        has_spot += 1

    if entry.get("last_sync") is None:
        never_synced += 1

# File existence check
def _path_exists(path_str):
    if not path_str:
        return False
    p = Path(path_str)
    if p.exists():
        return True
    # Try WSL conversion for Windows paths
    if len(path_str) > 1 and path_str[1] == ":":
        wsl = "/mnt/" + path_str[0].lower() + path_str[2:].replace("\\", "/")
        return Path(wsl).exists()
    return False

on_disk = sum(1 for e in ledger.values() if _path_exists(e.get("path","")))
missing   = total_entries - on_disk

print(f"  Ledger entries:     {total_entries}")
print(f"  Files on disk:      {on_disk}")
print(f"  Missing from disk:  {missing}")
print("\n  By source:")
for src, count in sorted(by_source.items(), key=lambda x: -x[1]):
    print(f"    {src:<12}  {count}")
print(f"\n  Spot synced:        {has_spot}")
print(f"  Pending spot sync:  {no_spot}")
print(f"  Never synced:       {never_synced}")

if FAILED_LOG.exists():
    with open(FAILED_LOG, encoding="utf-8") as f:
        fail_lines = [l for l in f.readlines() if l.strip() and not l.startswith("#") and not l.startswith("  →")]
    print(f"\n  Failed downloads:   {len(fail_lines)}  → {FAILED_LOG}")

# ── Generate M3U8 for manually selected playlists ────────────────────────────
if DOWNLOAD_EXTRA_PLAYLISTS and 'selected_playlists' in dir() and selected_playlists:
    PLAYLIST_DIR = NEW_ROOT / "playlists" / "Spotify"
    PLAYLIST_DIR.mkdir(parents=True, exist_ok=True)
    PLAYLIST_DB  = NEW_ROOT / "db" / "playlist_stats.json"
    pl_stats     = json.load(open(PLAYLIST_DB)) if PLAYLIST_DB.exists() else {}
    print("\n── Generating playlists ──\n")

    # Build lookups
    ledger_by_spotid_m3u = {v["_spotify_id"]: v for v in ledger.values() if v.get("_spotify_id")}
    ledger_at_m3u = [
        (_key_words(f"{v.get('artist','')} {v.get('title','')}"),
         v.get("artist",""), v.get("title",""), v)
        for v in ledger.values()
    ]

    for pl_name, pl_id in selected_playlists:
        safe_name     = re.sub(r'[<>:"/\\|?*]', '', pl_name).strip()
        out_path      = PLAYLIST_DIR / f"{safe_name}.m3u8"
        lines         = ["#EXTM3U", f"# Generated: {datetime.now().isoformat()}", ""]
        found         = 0
        missing       = 0
        missing_tracks = []

        # Fetch live from Spotify
        for item in _sp_all_items(client, pl_id):
            t = item.get("track")
            if not t or not t.get("name"):
                continue
            sp_artist = ", ".join(a["name"] for a in t["artists"])
            sp_title  = t["name"]
            sp_id     = t.get("id")
            sp_dur    = t.get("duration_ms", 0)

            entry = None

            # Layer 1: Spotify ID match
            if sp_id and sp_id in ledger_by_spotid_m3u:
                entry = ledger_by_spotid_m3u[sp_id]

            # Layer 2: exact filename match
            if not entry:
                fn   = _clean_fn(sp_artist, sp_title)
                dst  = NEW_AUDIO / fn
                norm = _norm(dst)
                if norm in ledger:
                    entry = ledger[norm]

            # Layer 3: fuzzy match
            if not entry:
                search_words = _key_words(f"{sp_artist} {sp_title}")
                for lw, la, lt, lv in ledger_at_m3u:
                    if not search_words & lw:
                        continue
                    if _title_score(sp_artist, sp_title, f"{la} - {lt}") >= FUZZY_MATCH_THRESH:
                        dur_ok = True
                        if sp_dur and lv.get("duration_ms"):
                            dur_ok = abs(sp_dur - lv["duration_ms"]) <= FUZZY_DUR_TOL
                        if dur_ok:
                            entry = lv
                            break

            if not entry:
                missing += 1
                missing_tracks.append(f"{sp_artist} - {sp_title}")
                logging.info(f"M3U8 missing: {sp_artist} - {sp_title}")
                continue

            # Resolve file path
            p = _root(entry["path"]) if "\\" in entry.get("path","") else Path(entry.get("path",""))
            if not p.exists():
                missing += 1
                missing_tracks.append(f"{sp_artist} - {sp_title} (file missing)")
                continue

            dur_s = entry.get("duration_ms", 0) // 1000
            lines.append(f"#EXTINF:{dur_s},{entry.get('artist','Unknown')} - {entry.get('title','Unknown')}")
            # Convert to Windows path for Rekordbox
            p_str = str(entry["path"])
            if p_str.startswith("/mnt/"):
                p_str = p_str[5].upper() + ":" + p_str[6:].replace("/", "\\")
            lines.append(p_str)
            found += 1

        # Write M3U8
        with open(out_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")

        # Store stats
        total = found + missing
        pct   = round(found / total * 100, 1) if total > 0 else 0
        pl_stats[pl_name] = {
            "generated_at":   datetime.now().isoformat(),
            "total_in_sp":    total,
            "found_on_disk":  found,
            "missing":        missing,
            "pct_complete":   pct,
            "missing_tracks": missing_tracks,
        }
        json.dump(pl_stats, open(PLAYLIST_DB, "w", encoding="utf-8"), indent=2, ensure_ascii=False)

        print(f"  ✓  {pl_name:<40} {found}/{total} ({pct}%)  →  {out_path.name}")
        if missing_tracks:
            print(f"     Missing: {missing_tracks[:3]}{'...' if len(missing_tracks) > 3 else ''}")


# ══════════════════════════════════════════════════════════════════════════════
#%% Cell 5: HALT — import to Rekordbox collection (skipped when RB disabled)
# ══════════════════════════════════════════════════════════════════════════════
if cfg.rekordbox.enabled:
    print("\n" + "=" * 60)
    print("HALT — ACTION REQUIRED")
    print("=" * 60)
    print(f"""
New tracks are ready in:
  {NEW_AUDIO}

Steps:
  1. Open Rekordbox
  2. File → Add folder to collection → {NEW_AUDIO}
  3. Wait for analysis to complete
  4. Close Rekordbox completely (verify in Task Manager)

Then continue with pipeline_01_embed.py
""")
    if not _args.no_interactive:
        input("Rekordbox closed and ready? Press Enter to confirm: ")
    else:
        print("  [UI mode] Skipping Rekordbox confirmation — proceed manually then run pipeline_01.")
    print("✓ Continue with pipeline_01_embed.py")
else:
    print(f"\n✓ Download complete. New tracks in: {NEW_AUDIO}")
    print("  (Rekordbox disabled — skipping import step. Continue with pipeline_01_embed.py)")
