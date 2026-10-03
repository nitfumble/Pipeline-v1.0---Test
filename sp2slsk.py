#!/usr/bin/env python3
"""
sp2slsk.py — Upgrade yt-dlp-sourced tracks to Soulseek quality.

For each ytdlp entry in the ledger:
  1. Search slskd for the track
  2. If found & downloaded → replace in audio/ (same filename, DB paths stay valid)
  3. Move old file to bin/
  4. Update ledger: source → soulseek, original_source preserved

Usage:
  python3 sp2slsk.py
  python3 sp2slsk.py --dry-run
  python3 sp2slsk.py --limit 10
"""

import glob, json, logging, re, shutil, time
import requests
from datetime import datetime
from pathlib import Path
from mutagen import File as MutagenFile
from rapidfuzz import fuzz

import argparse
_parser = argparse.ArgumentParser(description="Upgrade yt-dlp tracks to Soulseek quality")
_parser.add_argument("--dry-run",        action="store_true", help="Preview only, no changes")
_parser.add_argument("--limit",          type=int, default=None, help="Max tracks to attempt")
_parser.add_argument("--no-interactive", action="store_true")  # GUI flag, silently accepted
_args = _parser.parse_args()

import sys
sys.path.insert(0, str(Path(__file__).parent))
from config import load
from dj_paths import to_path, to_key, to_win, atomic_write_json

cfg = load()
_sk = cfg.slskd

# ── Paths ─────────────────────────────────────────────────────────────────────
DB_DIR              = cfg.paths.db
NEW_AUDIO           = cfg.paths.audio
BIN_DIR             = cfg.paths.bin
LEDGER              = cfg.paths.ledger
SLSK_DIR            = cfg.paths.soulseek
LOG_DIR             = cfg.paths.logs
TMP_DIR             = cfg.paths.tmp
BACKUP_DIR          = cfg.paths.backups
SLSK_QUEUE_FILE     = DB_DIR / "slsk_queue.json"
SLSK_HOLD_FILE      = DB_DIR / "slsk_hold.json"
SLSK_PROCESSED_FILE = DB_DIR / "slsk_processed.json"

for _d in [NEW_AUDIO, BIN_DIR, DB_DIR, TMP_DIR, LOG_DIR, SLSK_DIR, BACKUP_DIR]:
    _d.mkdir(parents=True, exist_ok=True)

# ── slskd connection ──────────────────────────────────────────────────────────
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

SLSKD_URL           = _wsl_remap_url(_sk.url)
SLSKD_USER          = cfg.secrets["slskd"].get("api_username") or cfg.secrets["slskd"]["username"]
SLSKD_PASS          = cfg.secrets["slskd"].get("api_password") or cfg.secrets["slskd"]["password"]
SLSK_SEARCH_WAIT    = _sk.search_wait
SLSK_DL_TIMEOUT     = _sk.dl_timeout
SLSK_TITLE_THRESH   = _sk.title_thresh
SLSK_HOLD_TTL       = _sk.queue_ttl_days * 86400
SLSK_QUEUE_POS_MAX  = _sk.get("queue_pos_max", 100)
SLSK_USER_QUEUE_MAX = 7500   # skip peers with more than this many queued uploads
AUDIO_EXTS          = {".mp3", ".wav", ".flac", ".aiff", ".m4a", ".ogg"}
TITLE_ONLY_FLOOR    = 55
_dl            = cfg.download
MIN_DUR_S      = getattr(_dl, "min_duration_s", 60)
MAX_DUR_S      = getattr(_dl, "max_duration_s", 900)

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "sp2slsk.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)

# ── IO helpers ────────────────────────────────────────────────────────────────
def load_ledger() -> dict:
    if not LEDGER.exists():
        return {}
    with open(LEDGER, encoding="utf-8-sig") as f:
        return json.load(f)

def save_ledger(ledger: dict):
    atomic_write_json(LEDGER, ledger)

def load_slsk_queue() -> dict:
    if not SLSK_QUEUE_FILE.exists():
        return {}
    with open(SLSK_QUEUE_FILE, encoding="utf-8") as f:
        return json.load(f)

def load_hold() -> dict:
    if not SLSK_HOLD_FILE.exists():
        return {}
    with open(SLSK_HOLD_FILE, encoding="utf-8") as f:
        return json.load(f)

def save_hold(data: dict):
    atomic_write_json(SLSK_HOLD_FILE, data)

def load_processed() -> set:
    if not SLSK_PROCESSED_FILE.exists():
        return set()
    with open(SLSK_PROCESSED_FILE, encoding="utf-8") as f:
        return set(json.load(f))

def save_processed(data: set):
    atomic_write_json(SLSK_PROCESSED_FILE, sorted(data))

# ── Audio / filename helpers ──────────────────────────────────────────────────
def _get_dur_s(path: Path) -> float | None:
    try:
        info = MutagenFile(path)
        if info and info.info:
            return info.info.length
    except Exception:
        pass
    try:
        import soundfile as sf
        with sf.SoundFile(str(path)) as s:
            return s.frames / s.samplerate
    except Exception:
        return None

def _clean_filename(stem: str) -> str:
    s = re.sub(r'^\[?\d+\]?\s*[-.]?\s*', '', stem)
    s = re.sub(r'^\d+[\s\-\.]+', '', s)
    s = re.sub(r'-[0-9a-f]{8}$', '', s)
    s = re.sub(r'^\d+bpm[-\s]', '', s, flags=re.IGNORECASE)
    return s.replace("_", " ").strip()

# ── Matching helpers ──────────────────────────────────────────────────────────
def _title_score(artist: str, title: str, fn_stem: str) -> float:
    fn_lower    = fn_stem.lower()
    full_query  = f"{artist} {title}".lower()
    score1      = fuzz.partial_ratio(full_query, fn_lower)
    score2      = fuzz.partial_ratio(title.lower(), fn_lower)
    base        = title.split(" - ")[0].strip().lower()
    score3      = fuzz.partial_ratio(base, fn_lower)
    track_score = max(score1, score2, score3)
    primary_art = artist.split(",")[0].strip().lower()
    art_score   = fuzz.partial_ratio(primary_art, fn_lower)
    return 0.75 * track_score + 0.25 * art_score

def _already_in_library(slsk_stem: str, ledger: dict) -> bool:
    clean = re.sub(r'[-_]([0-9a-f]{8})$', '', slsk_stem)
    clean = re.sub(r'^\[?\d+\]?\s*[-.]?\s*', '', clean)
    clean = re.sub(r'^\d+[\s\-_.]+', '', clean)
    clean = clean.replace("_", " ").lower().strip()
    for v in ledger.values():
        ledger_stem  = to_path(v.get("path", "") or "").stem
        ledger_clean = re.sub(r'^\d+[\s\-_.]+', '', ledger_stem).lower().strip()
        if fuzz.token_set_ratio(clean, ledger_clean) >= 88:
            return True
    return False

def _find_library_match(slsk_stem: str, ledger: dict) -> tuple:
    clean = re.sub(r'[-_]([0-9a-f]{8})$', '', slsk_stem)
    clean = re.sub(r'^\[?\d+\]?\s*[-.]?\s*', '', clean)
    clean = re.sub(r'^\d+[\s\-_.]+', '', clean)
    clean = clean.replace("_", " ").lower().strip()
    for k, v in ledger.items():
        ledger_stem  = to_path(v.get("path", "") or "").stem
        ledger_clean = re.sub(r'^\d+[\s\-_.]+', '', ledger_stem).lower().strip()
        if fuzz.token_set_ratio(clean, ledger_clean) >= 88:
            return k, v
    return None, None

# ── Hold harvest ──────────────────────────────────────────────────────────────
def harvest_hold(ledger: dict) -> int:
    hold = load_hold()
    if not hold:
        return 0
    harvested, to_remove = 0, []
    for hold_key, entry in hold.items():
        age = (datetime.now() - datetime.fromisoformat(entry["queued_at"])).total_seconds()
        if age > SLSK_HOLD_TTL:
            logging.info(f"slsk_hold expired: {hold_key}")
            to_remove.append(hold_key)
            continue
        matches = list(SLSK_DIR.rglob(f"{glob.escape(Path(entry['slsk_filename']).stem)}*"))
        if not matches:
            continue
        slsk_file  = matches[0]
        ledger_key = entry["ledger_key"]
        old_path   = to_path(ledger[ledger_key]["path"]) if ledger_key in ledger else None
        if old_path and old_path.exists():
            BIN_DIR.mkdir(parents=True, exist_ok=True)
            shutil.move(str(old_path), str(BIN_DIR / old_path.name))
        new_name = (old_path.stem + slsk_file.suffix) if old_path else slsk_file.name
        dst      = NEW_AUDIO / new_name
        shutil.copy2(str(slsk_file), str(dst))
        if ledger_key in ledger:
            new_key    = to_key(str(dst))
            entry_data = ledger.pop(ledger_key)
            entry_data["source"]          = "soulseek"
            entry_data["original_source"] = "ytdlp"
            entry_data["slsk_file"]       = str(slsk_file)
            entry_data["upgraded_at"]     = datetime.now().isoformat()
            entry_data["path"]            = str(dst)
            entry_data["path_win"]        = to_win(dst)
            ledger[new_key] = entry_data
        save_ledger(ledger)
        print(f"  ✓  harvested: {slsk_file.name}")
        to_remove.append(hold_key)
        harvested += 1
    for k in to_remove:
        hold.pop(k, None)
    save_hold(hold)
    return harvested

# ── SLSK_DIR harvest ──────────────────────────────────────────────────────────
def harvest_slsk_dir(ledger: dict) -> int:
    slsk_queue    = load_slsk_queue()
    processed     = load_processed()
    queue_by_stem = {}
    for fn, entry in slsk_queue.items():
        stem = Path(entry.get("filename", "").split("\\")[-1]).stem.lower()
        if stem:
            queue_by_stem[stem] = (fn, entry)

    imported = switched = skipped_hq = manual_imp = stale_skip = 0

    # Pre-build cleaned stem → path map of all existing audio files (catches ledger orphans)
    _audio_stems: dict = {}
    if NEW_AUDIO.exists():
        for _f in NEW_AUDIO.iterdir():
            if _f.is_file() and _f.suffix.lower() in AUDIO_EXTS:
                _s = re.sub(r'^\d+[\s\-_.]+', '', _f.stem).replace("_", " ").lower().strip()
                _audio_stems[_s] = _f

    for slsk_file in SLSK_DIR.rglob("*"):
        if slsk_file.suffix.lower() not in AUDIO_EXTS:
            continue
        if str(slsk_file) in processed:
            continue

        slsk_dur = _get_dur_s(slsk_file)
        q_match  = queue_by_stem.get(slsk_file.stem.lower())

        if q_match:
            queue_fn, q_entry = q_match
            queued_at   = datetime.fromisoformat(q_entry["queued_at"])
            age         = (datetime.now() - queued_at).total_seconds()
            resolved_at = q_entry.get("resolved_at")
            if resolved_at:
                resolved_age = (datetime.now() - datetime.fromisoformat(resolved_at)).total_seconds()
                if resolved_age > SLSK_HOLD_TTL:
                    print(f"  ✗  stale (resolved {int(resolved_age/86400)}d ago): {slsk_file.name}")
                    processed.add(str(slsk_file)); stale_skip += 1; continue
            elif age > SLSK_HOLD_TTL:
                print(f"  ✗  stale (queued {int(age/86400)}d ago): {slsk_file.name}")
                processed.add(str(slsk_file)); stale_skip += 1; continue

            meta       = q_entry.get("meta", {})
            artist     = meta.get("artist", "")
            title      = meta.get("title", "")
            duration   = meta.get("duration_ms", 0)
            playlists  = meta.get("playlists", [])
            sp_id      = meta.get("_spotify_id")
            spot       = meta.get("spot") or None
            dst_fn     = meta.get("clean_fn") or queue_fn
            dst        = (NEW_AUDIO / dst_fn).with_suffix(slsk_file.suffix)
            ledger_key = to_key(str(dst))

            if ledger_key in ledger and ledger[ledger_key].get("source") in ("soulseek", "manual"):
                processed.add(str(slsk_file)); skipped_hq += 1; continue
            if _already_in_library(slsk_file.stem, ledger):
                processed.add(str(slsk_file)); skipped_hq += 1; continue
            _slsk_c = re.sub(r'^\d+[\s\-_.]+', '', slsk_file.stem.replace("_", " ")).lower().strip()
            if any(fuzz.token_set_ratio(_slsk_c, _s) >= 88 for _s in _audio_stems):
                processed.add(str(slsk_file)); skipped_hq += 1; continue

            old_path = to_path(ledger[ledger_key]["path"]) if ledger_key in ledger else None
            if old_path and old_path.exists():
                BIN_DIR.mkdir(parents=True, exist_ok=True)
                shutil.move(str(old_path), str(BIN_DIR / old_path.name))
                switched += 1
            else:
                imported += 1
            shutil.copy2(str(slsk_file), str(dst))
            ledger[ledger_key] = {
                "path":              str(dst),
                "path_win":          to_win(dst),
                "artist":            artist,
                "title":             title,
                "duration_ms":       duration or int((slsk_dur or 0) * 1000),
                "source":            "soulseek",
                "original_source":   ledger.get(ledger_key, {}).get("source", "soulseek"),
                "spotify_playlists": playlists,
                "_spotify_id":       sp_id,
                "spot":              spot or ledger.get(ledger_key, {}).get("spot"),
                "last_sync":         ledger.get(ledger_key, {}).get("last_sync"),
                "slsk_file":         str(slsk_file),
                "upgraded_at":       datetime.now().isoformat(),
            }
            save_ledger(ledger)
            print(f"  ✓  {'switched' if old_path else 'imported'}: {dst_fn}")
            processed.add(str(slsk_file))
        else:
            parts = _clean_filename(slsk_file.stem).split(" - ", 1)
            if len(parts) == 2:
                parsed_artist, parsed_title = parts[0].strip(), parts[1].strip()
                clean_stem = f"{parsed_artist} - {parsed_title}"
            else:
                parsed_artist = "Unknown"
                parsed_title  = _clean_filename(slsk_file.stem)
                clean_stem    = parsed_title

            dst = NEW_AUDIO / f"{clean_stem}{slsk_file.suffix}"
            counter = 1
            while dst.exists():
                dst = NEW_AUDIO / f"{clean_stem} ({counter}){slsk_file.suffix}"
                counter += 1

            norm_dst = to_key(str(dst))
            lib_key, lib_entry = _find_library_match(slsk_file.stem, ledger)

            if lib_entry:
                if lib_entry.get("source") in ("soulseek", "manual"):
                    processed.add(str(slsk_file)); skipped_hq += 1; continue
                # ytdlp match — swap file, update path only, leave all other ledger fields as-is
                old_path = to_path(lib_entry["path"])
                new_name = old_path.stem + slsk_file.suffix
                new_dst  = NEW_AUDIO / new_name
                if old_path.exists():
                    BIN_DIR.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(old_path), str(BIN_DIR / old_path.name))
                shutil.copy2(str(slsk_file), str(new_dst))
                new_key = to_key(str(new_dst))
                if new_key != lib_key:
                    ledger[new_key] = ledger.pop(lib_key)
                ledger[new_key]["path"]            = str(new_dst)
                ledger[new_key]["path_win"]        = to_win(new_dst)
                ledger[new_key]["source"]          = "soulseek"
                ledger[new_key]["original_source"] = "ytdlp"
                ledger[new_key]["slsk_file"]       = str(slsk_file)
                ledger[new_key]["upgraded_at"]     = datetime.now().isoformat()
                save_ledger(ledger)
                print(f"  ✓  replaced (manual slsk): {new_dst.name}")
                switched += 1
                processed.add(str(slsk_file))
                continue

            _slsk_c = re.sub(r'^\d+[\s\-_.]+', '', slsk_file.stem.replace("_", " ")).lower().strip()
            _disk_match = next(
                (_ex for _st, _ex in _audio_stems.items() if fuzz.token_set_ratio(_slsk_c, _st) >= 88),
                None
            )
            if _disk_match:
                logging.info(f"slsk already on disk (unledgered): {slsk_file.name} ~ {_disk_match.name}")
                processed.add(str(slsk_file)); skipped_hq += 1; continue

            if norm_dst in ledger:
                processed.add(str(slsk_file)); skipped_hq += 1; continue

            shutil.copy2(str(slsk_file), str(dst))
            ledger[norm_dst] = {
                "path":              str(dst),
                "path_win":          to_win(dst),
                "artist":            parsed_artist,
                "title":             parsed_title,
                "duration_ms":       int((slsk_dur or 0) * 1000),
                "source":            "manual",
                "original_source":   "slsk_manual",
                "spotify_playlists": [],
                "_spotify_id":       None,
                "spot":              None,
                "last_sync":         None,
                "slsk_file":         str(slsk_file),
                "flag":              "needs_review",
            }
            save_ledger(ledger)
            print(f"  ✓  manual import: {dst.name}  [needs review]")
            manual_imp += 1
            processed.add(str(slsk_file))

    save_processed(processed)
    print(f"  Switched (ytdlp→slsk): {switched}")
    print(f"  Imported (manifest):   {imported}")
    print(f"  Imported (manual):     {manual_imp}")
    print(f"  Skipped (already hq):  {skipped_hq}")
    print(f"  Skipped (stale):       {stale_skip}")
    return imported + switched + manual_imp

# ── slskd API ─────────────────────────────────────────────────────────────────
def _slskd_token() -> str | None:
    try:
        r = requests.post(f"{SLSKD_URL}/api/v0/session",
                          json={"username": SLSKD_USER, "password": SLSKD_PASS},
                          timeout=10)
        return r.json().get("token")
    except Exception as e:
        logging.warning(f"slskd auth failed: {e}")
        return None

def _slskd_search(token: str, query: str) -> list:
    try:
        r = requests.post(f"{SLSKD_URL}/api/v0/searches",
                          json={"searchText": query},
                          headers={"Authorization": f"Bearer {token}"},
                          timeout=10)
        search_id = r.json()["id"]
    except Exception as e:
        logging.warning(f"slskd search failed [{query}]: {e}")
        return []

    for _ in range(SLSK_SEARCH_WAIT // 5 + 2):
        time.sleep(5)
        try:
            r = requests.get(f"{SLSKD_URL}/api/v0/searches/{search_id}",
                             headers={"Authorization": f"Bearer {token}"},
                             timeout=10)
            if r.json().get("state", "").startswith("Completed"):
                break
        except Exception:
            pass

    try:
        r = requests.get(f"{SLSKD_URL}/api/v0/searches/{search_id}?includeResponses=true",
                         headers={"Authorization": f"Bearer {token}"},
                         timeout=10)
        responses = r.json().get("responses", [])
    except Exception as e:
        logging.warning(f"slskd fetch failed [{query}]: {e}")
        return []

    candidates = []
    for resp in responses:
        free_slot = 1 if resp.get("hasFreeUploadSlot", False) else 0
        queue_len = resp.get("queueLength", 999)
        for f in resp.get("files", []):
            ext = f["filename"].split(".")[-1].lower()
            if ext not in {"mp3", "flac", "wav", "aiff", "m4a", "ogg"}:
                continue
            br = f.get("bitRate", 0) or 0
            if ext == "mp3" and 0 < br < 320:
                continue
            if ext == "m4a" and br < 256:
                continue
            candidates.append({
                "username":  resp["username"],
                "filename":  f["filename"],
                "size":      f.get("size", 0),
                "bitrate":   br,
                "length":    f.get("length", 0) or 0,
                "ext":       ext,
                "free_slot": free_slot,
                "queue_len": queue_len,
            })
    return candidates

def _rank_candidates(candidates: list, artist: str, title: str) -> list:
    BOOST   = {"extended": 40, "ext": 40, "club": 10, "remaster": 10, "remastered": 10}
    PENALTY = {"radio": -20, "bootleg": -15, "pn": -20}
    results = []
    for c in candidates:
        fn_stem = c["filename"].split("\\")[-1].rsplit(".", 1)[0]
        title_s = _title_score(artist, title, fn_stem)
        if title_s < SLSK_TITLE_THRESH:
            continue
        if c["queue_len"] > SLSK_USER_QUEUE_MAX and not c["free_slot"]:
            continue
        total    = title_s
        br, ext  = c["bitrate"], c["ext"]
        if br == 320 and ext == "mp3":   total += 80
        elif ext == "flac":              total += 10
        elif ext in ("wav", "aiff"):     total += 5
        fn_clean  = fn_stem.lower().replace("_", " ")
        known     = set(re.split(r"[\s\-\.]+", (artist + " " + title).lower()))
        all_words = set(re.split(r"[\s\-\.\(\)\[\]]+", fn_clean))
        extra     = {w for w in all_words - known if len(w) > 2 and not w.isdigit()}
        for word in extra:
            total += BOOST.get(word, PENALTY.get(word, -15))
        total += 30 if c["free_slot"] else -20
        total -= (c["queue_len"] // 100) * 3
        results.append((total, c))
    results.sort(key=lambda x: x[0], reverse=True)
    return results

def _download(token: str, candidate: dict, expected_dur_s: float = 0) -> "Path | tuple | None":
    username = candidate["username"]
    filename = candidate["filename"]
    size     = candidate["size"]
    fn_clean = filename.split("\\")[-1]
    existing = [f for f in SLSK_DIR.rglob(f"{glob.escape(Path(fn_clean).stem)}*") if f.is_file()]
    if existing:
        return existing[0]
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
            timeout=10,
        )
        resp  = r.json()
        items = (resp.get("enqueued") or []) + (resp.get("existing") or [])
        if not items:
            logging.warning(f"slskd enqueue empty response for {fn_clean} from {username}")
            return None
        dl_id = items[0]["id"]
    except Exception as e:
        logging.warning(f"slskd enqueue failed {fn_clean} from {username}: {e}")
        return None
    for _ in range(SLSK_DL_TIMEOUT // 5):
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
                                matches = [mf for mf in SLSK_DIR.rglob(f"{glob.escape(Path(fn_clean).stem)}*") if mf.is_file()]
                                if not matches:
                                    return None
                                dur = _get_dur_s(matches[0])
                                if dur is not None and dur < MIN_DUR_S:
                                    if expected_dur_s > 0 and abs(dur - expected_dur_s) / max(expected_dur_s, 1) <= 0.20:
                                        pass  # genuinely short track confirmed by expected duration
                                    else:
                                        logging.warning(f"slskd rejected (too short {dur:.0f}s): {fn_clean}")
                                        matches[0].unlink(missing_ok=True)
                                        return None
                                if dur is not None and dur > MAX_DUR_S:
                                    logging.warning(f"slskd rejected (too long {dur:.0f}s): {fn_clean}")
                                    matches[0].unlink(missing_ok=True)
                                    return None
                                return matches[0]
                            elif any(s in state for s in ("Aborted", "Errored", "Rejected")):
                                logging.warning(f"slskd {state}: {fn_clean} from {username}")
                                return None
                            elif "Queued, Remotely" in state:
                                place = f.get("placeInQueue", 0) or 0
                                if place > SLSK_QUEUE_POS_MAX:
                                    logging.warning(f"slskd high queue ({place}): {fn_clean} from {username} — holding")
                                    return ("HOLD", fn_clean, username)
        except Exception as e:
            logging.warning(f"slskd poll error: {e}")
    logging.warning(f"slskd timeout: {fn_clean} from {username}")
    return ("HOLD", fn_clean, username)

def _try_soulseek(artist: str, title: str, expected_dur_s: float = 0) -> "Path | tuple | None":
    token = _slskd_token()
    if not token:
        return None
    candidates  = _slskd_search(token, f"{artist} {title}")
    ranked      = _rank_candidates(candidates, artist, title)
    hold_result = None
    for _score, c in ranked:
        result = _download(token, c, expected_dur_s)
        if result is None:
            continue
        if isinstance(result, tuple) and result[0] == "HOLD":
            hold_result = result
            break  # one queued item per track — harvest will pick it up
        return result
    return hold_result

# ── Main ──────────────────────────────────────────────────────────────────────
print(f"\n=== sp2slsk: Upgrade yt-dlp → Soulseek ===")

ledger = load_ledger()

if LEDGER.exists():
    _ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    shutil.copy2(str(LEDGER), str(BACKUP_DIR / f"sync_ledger_{_ts}.json"))
    _all_bk = sorted(BACKUP_DIR.glob("sync_ledger_*.json"), key=lambda f: f.stat().st_mtime)
    for _old in _all_bk[:-cfg.backup.retention]:
        _old.unlink()

print("\n── Harvesting completed holds ──")
print(f"  Harvested: {harvest_hold(ledger)}")

print("\n── Harvesting new slsk files ──")
harvest_slsk_dir(ledger)
ledger = load_ledger()  # reload after harvest may have saved new entries

ytdlp_entries = {
    k: v for k, v in ledger.items()
    if v.get("source") == "ytdlp" and v.get("artist") and v.get("title")
    and to_path(v.get("path", "")).parent == NEW_AUDIO
}

print(f"\n── Upgrading ytdlp tracks ──")
print(f"  ytdlp entries: {len(ytdlp_entries)}")
print(f"  dry run:       {_args.dry_run}")
if _args.limit:
    print(f"  limit:         {_args.limit}")

upgraded = failed = skipped = 0
entries = list(ytdlp_entries.items())
if _args.limit:
    entries = entries[:_args.limit]

for ledger_key, entry in entries:
    artist   = entry["artist"]
    title    = entry["title"]
    old_path = to_path(entry["path"])
    n        = upgraded + failed + skipped + 1

    print(f"  [{n}/{len(entries)}]  {artist} – {title}", end="", flush=True)

    if not old_path.exists():
        print(f"  → skipped (file missing)")
        skipped += 1; continue
    if entry.get("source") == "soulseek":
        print(f"  → already soulseek")
        skipped += 1; continue
    slsk_match = next((
        f for f in SLSK_DIR.rglob("*")
        if f.suffix.lower() in AUDIO_EXTS
        and re.sub(r'^\d{1,2}[\s\-_.]+', '', f.stem).lower().strip()
           == re.sub(r'^\d{1,2}[\s\-_.]+', '', old_path.stem).lower().strip()
    ), None) if SLSK_DIR.exists() else None
    if slsk_match:
        print(f"  → already in slsk dir, harvest will pick up")
        skipped += 1; continue
    if _args.dry_run:
        print(f"  → dry run")
        skipped += 1; continue

    slsk_file = _try_soulseek(artist, title, entry.get("duration_ms", 0) / 1000)

    if not slsk_file:
        print(f"  → no result")
        failed += 1; continue

    if isinstance(slsk_file, tuple) and slsk_file[0] == "HOLD":
        _, fn_clean, username = slsk_file
        hold = load_hold()
        hold[f"{artist} - {title}"] = {
            "slsk_filename": fn_clean,
            "username":      username,
            "ledger_key":    ledger_key,
            "queued_at":     datetime.now().isoformat(),
        }
        save_hold(hold)
        print(f"  → queued ({fn_clean})")
        continue

    new_name   = old_path.stem + slsk_file.suffix
    new_dst    = NEW_AUDIO / new_name
    BIN_DIR.mkdir(parents=True, exist_ok=True)
    shutil.move(str(old_path), str(BIN_DIR / old_path.name))
    shutil.copy2(str(slsk_file), str(new_dst))
    new_key    = to_key(str(new_dst))
    entry_data = ledger.pop(ledger_key)
    entry_data["source"]          = "soulseek"
    entry_data["original_source"] = "ytdlp"
    entry_data["slsk_file"]       = str(slsk_file)
    entry_data["upgraded_at"]     = datetime.now().isoformat()
    entry_data["path"]            = str(new_dst)
    entry_data["path_win"]        = to_win(new_dst)
    ledger[new_key] = entry_data
    save_ledger(ledger)
    print(f"  ✓  ({slsk_file.name})")
    upgraded += 1

print(f"\n── Summary ──")
print(f"  Upgraded:  {upgraded}")
print(f"  No result: {failed}")
print(f"  Skipped:   {skipped}")
