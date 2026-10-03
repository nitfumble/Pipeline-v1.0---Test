#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
config.py — single source of truth for all pipeline settings (v1.0)
===================================================================
The SCHEMA (defaults, structure) lives here in Python; `config.json` is the
generated, hand-editable instance every stage loads. Secrets (Spotify/slskd
credentials) live separately in `secrets.json` so `config.json` can be shared.

Files (in CONFIG_DIR — the repo dir by default, or $DJ_CONFIG_DIR):
  config.json            ← your live, editable settings           (gitignored)
  secrets.json           ← your credentials                       (gitignored)
  config.template.json   ← committed sample (defaults, no secrets)
  secrets.template.json  ← committed sample (placeholder creds)

Usage:
  python3 config.py --init       # first-run wizard (prompts essentials)
  python3 config.py --show       # print the resolved config
  python3 config.py --validate   # check config.json against the schema
  python3 config.py --reset      # back up current, regenerate factory defaults
  python3 config.py --edit       # open config.json in $EDITOR
  python3 config.py --template    # (re)write the committed template files

From other stages:
  from config import load
  cfg = load()                   # validated Config object (raises if not set up)
  cfg.paths.audio                # derived Path
  cfg.tag_playlists()            # list of "<tag> [RB]" Spotify playlist names
"""
import os, sys, json, shutil, argparse
from pathlib import Path
from datetime import datetime

from dj_paths import to_path, atomic_write_json

CONFIG_VERSION = 1

CONFIG_DIR    = Path(os.environ.get("DJ_CONFIG_DIR", Path(__file__).resolve().parent))
CONFIG_PATH   = CONFIG_DIR / "config.json"
SECRETS_PATH  = CONFIG_DIR / "secrets.json"
CONFIG_TMPL   = CONFIG_DIR / "config.template.json"
SECRETS_TMPL  = CONFIG_DIR / "secrets.template.json"

# ══════════════════════════════════════════════════════════════════════════════
# SCHEMA — factory defaults. Everything tunable lives here; stages reference it.
# ══════════════════════════════════════════════════════════════════════════════

DEFAULTS = {
    # ── meta ──────────────────────────────────────────────────────────────────
    "config_version": CONFIG_VERSION,
    "setup_complete": False,            # pipeline refuses to run until True

    # ── paths ─────────────────────────────────────────────────────────────────
    # Only `library_root` is essential; the rest derive from it unless overridden.
    "paths": {
        "library_root": "",             # PROMPTED — e.g. "E:/Music Library"
        "audio":     "{root}/audio",
        "db":        "{root}/db",
        "bin":       "{root}/bin",
        "soulseek":  "{root}/source/soulseek",
        "slskd":     "{root}/slskd",
        "playlists": "{root}/playlists",
        "logs":      "{root}/logs",
        "tmp":       "{root}/tmp",
        "backups":   "{root}/backups",
        "ledger":    "{root}/db/sync_ledger.json",
        "music_db":  "{root}/db/music.db",
    },

    # ── rekordbox ───────────────────────────────────────────────────────────────
    "rekordbox": {
        "enabled":   True,              # PROMPTED — off = download/embed/cluster only
        # PROMPTED — tip: C:/Users/<name>/AppData/Roaming/Pioneer/rekordbox/master.db
        "master_db": "",
        "playlist_suffix": " [RB]",     # marker appended to managed Spotify playlists
        "recently_added_days": 30,
        "overwrite": True,              # clear+reapply auto tags each run
        "commit_every": 500,
    },

    # ── sources registry ────────────────────────────────────────────────────────
    # name → {folder under library_root, upgradeable}. Only ytdlp is upgradeable
    # (low quality → slsk HQ). Purchased/HQ sources are final.
    "sources": {
        "ytdlp":    {"folder": "audio",          "upgradeable": True},
        "soulseek": {"folder": "source/soulseek", "upgradeable": False},
        "manual":   {"folder": "source/manual",  "upgradeable": False},
        "bandcamp": {"folder": "source/bandcamp", "upgradeable": False},
        "beatport": {"folder": "source/beatport", "upgradeable": False},
    },

    # ── spotify (settings only — creds in secrets.json) ──────────────────────────
    "spotify": {
        "redirect_uri": "http://localhost:8888/callback",
        "scopes": "playlist-read-private playlist-modify-private playlist-modify-public",
        "token_cache": "{db}/.spotify_cache",
        # The [RB] tag playlists double as the download-source playlists.
        # This list is auto-materialized from the synced manual tags on save;
        # shown here for cross-file reference and user visibility.
        "tag_playlists": [],
    },

    # ── slskd (settings only — creds in secrets.json) ────────────────────────────
    "slskd": {
        "url": "http://127.0.0.1:5030",
        "download_dir": "",             # slskd completed-download folder (prompted, Phase D)
        "search_wait": 120,
        "dl_timeout": 600,
        "title_thresh": 85,
        "ext_thresh": 85,
        "queue_ttl_days": 7,
        "queue_retry_hours": 24,
        "max_attempts": 10,
        "max_queue": 10,
        "queue_pos_max": 100,           # if PlaceInQueue > this when Queued Remotely, don't wait
        "free_slot_only": True,
        "config_file": "",              # Phase D: templated path to slskd.yml
        "share_dirs": [],               # PROMPTED in Phase D
    },

    # ── download / quality gates (pipe00) ────────────────────────────────────────
    "download": {
        "enabled": True,
        "accepted_formats": ["mp3", "wav", "flac"],   # active ingest extensions
        "min_bitrate_mp3": 320,
        "min_bitrate_m4a": 256,
        "min_duration_s": 60,
        # ext -> [warn_MB, reject_MB]: warn flags size_suspicious, reject bins.
        "size_limits_mb": {"mp3": [20, 50], "m4a": [20, 50], "aac": [20, 50],
                           "flac": [None, 150], "wav": [None, 250], "aiff": [None, 250]},
        "fingerprint_len": 0,        # 0 = whole file (windowed false-matches Original/Extended)
        "ytdlp_sleep": 5,            # seconds between yt-dlp downloads
        "cookie_file": "",          # optional yt-dlp cookies.txt path
        "dur_tol_abs_ms": 10000,
        "dur_tol_pct": 0.35,
        "fuzzy_dur_tol_ms": 15000,
        "fuzzy_match_thresh": 92,
        "title_only_floor": 55,
        "manual_dur_tol": 8,
    },

    # ── embed (pipe01) ────────────────────────────────────────────────────────────
    "embed": {
        "model_dir": "{root}/models",
        "workers": 0,                   # 0 = auto (cpu_count - 1)
        "emb_dim": 1280,
        "maest_n": 44,                  # genre-score vector length
    },

    # ── cluster (pipe02) ──────────────────────────────────────────────────────────
    "cluster": {
        # feature weights (≈ sum 1.0)
        "w_embed": 0.60, "w_genre": 0.30, "w_bpm": 0.10,
        # UMAP — clustering space
        "umap_n_neighbors": 15, "umap_min_dist": 0.0, "umap_n_components": 20,
        "umap_metric": "cosine", "umap_random_state": 42,
        # HDBSCAN parameter sweep
        "sweep_cluster_selection": ["leaf"],
        "sweep_min_cluster_size": [10, 15, 20, 30, 40, 50, 75, 100],
        "sweep_min_samples": [2, 3, 4, 5, 10, 15],
        # manual params (used with --manual-params / --skip-sweep)
        "manual_min_cluster_size": 75, "manual_min_samples": 20,
        "manual_cluster_selection": "leaf",
        # quality-score weights (sum 1.0)
        "score_w": {"noise": 0.30, "target_range": 0.25, "genre_purity": 0.25,
                    "bpm_cohesion": 0.10, "silhouette": 0.10},
        # playlist sizing, genre gate, round-2 re-cluster trigger
        "target_min": 50, "target_max": 200,
        "genre_threshold": 0.15,
        "round2_noise_threshold": 50,    # re-cluster noise if >= this many
        "playlist_name_pattern": "Cluster {id} - {genre}",
    },

    # ── tags ────────────────────────────────────────────────────────────────────
    # auto_groups  = ML-derived, fixed (from the model's label space).
    # manual_groups = user-curated; defaults shown, fully customizable in --init.
    #   each manual tag: {"name", "sync"} — sync=True gets an [RB] Spotify playlist.
    "tags": {
        "use_defaults": True,           # PROMPTED — N = define your own manual tags
        "auto_groups": {
            "genre":        {"enabled": True, "prefix": ""},
            "mood":         {"enabled": True, "prefix": ""},
            "danceability": {"enabled": True, "prefix": ""},
            "components":   {"enabled": True, "prefix": "Comp - "},
        },
        # Canonical ML tag name lists (what the bootstrap creates / expects).
        "auto_tags": {
            "mood": ["Melancholic", "Euphoric", "Uplifting", "Deep", "Hypnotic",
                     "Dark", "Aggressive", "Dramatic"],
            "danceability": ["Groovy", "Driving", "Chill", "Peak"],
            "components": ["Instrumental", "Arpeggio", "Piano / Keys", "Sub Bass Heavy",
                           "Beat", "Vocal", "Percussion Heavy", "Acid Line",
                           "Synth Lead", "Wobble Bass", "Strings", "Organic", "Brass"],
            # NOTE: the full 44-entry genre label→tag map is carried from the model
            # config (embed). Representative tags shown; populated at bootstrap.
            "genre": ["House", "Techno", "Tech House", "Deep House", "Trance",
                      "Breakbeat", "Electro", "Progressive House", "Drum N Bass",
                      "UK Garage", "Acid", "Progressive Trance", "Jungle", "Breaks",
                      "Bassline"],
        },
        # User-curated groups. prefix is prepended to each tag's RB name.
        # sync=True → mirrored to a "<full name><suffix>" Spotify playlist.
        "manual_groups": {
            "Floor": {
                "prefix": "Floor - ",
                "tags": [
                    {"name": "Main Floor",   "sync": True},
                    {"name": "Second Floor", "sync": True},
                    {"name": "Lounge",       "sync": True},
                    {"name": "Midnight Set", "sync": True},
                    {"name": "Morning Set",  "sync": True},
                ],
            },
            "Set Position": {
                "prefix": "",
                "tags": [
                    {"name": "Opener",        "sync": True},
                    {"name": "Warm Up",       "sync": True},
                    {"name": "Build Up",      "sync": True},
                    {"name": "Plateau",       "sync": True},
                    {"name": "Riser",         "sync": True},
                    {"name": "Peak Time",     "sync": True},
                    {"name": "Bridge - Break","sync": True},
                    {"name": "Build Down",    "sync": True},
                    {"name": "Closer",        "sync": True},
                    {"name": "After Hours",   "sync": True},
                ],
            },
            "Flag": {
                "prefix": "Flag - ",
                "tags": [
                    {"name": "Favorite",       "sync": True},
                    {"name": "Sharing",        "sync": True},
                    {"name": "Recently Added", "sync": False},  # workflow / RB-only
                    {"name": "Needs Review",   "sync": False},
                    {"name": "Sampler",        "sync": False},
                    {"name": "Remove",         "sync": False},
                    {"name": "Redownload",     "sync": False},
                ],
            },
        },
    },

    # ── backup / logging ──────────────────────────────────────────────────────────
    "backup":  {"retention": 10},
    "logging": {"level": "INFO"},

    # Path to the Python interpreter for pipeline subprocesses (pipe00–03).
    # The web server runs in a lightweight venv (fastapi only); pipeline scripts
    # need essentia, requests, spotipy, etc. Leave empty to auto-detect:
    # tries ~/music_env, then ~/.local/share/stacks-venvs/pipeline.
    "pipeline_python": "",
}

# Credentials — never written to config.json.
SECRETS_DEFAULTS = {
    "spotify": {"client_id": "", "client_secret": ""},
    "slskd":   {"username": "", "password": ""},
}

# Which leaf keys the --init wizard prompts for (everything else = edit the file).
WIZARD_STEPS = [
    "paths.library_root", "rekordbox.enabled", "rekordbox.master_db",
    "tags.use_defaults",
    "secrets.spotify.client_id", "secrets.spotify.client_secret",
    "secrets.slskd.username", "secrets.slskd.password",
]


# ══════════════════════════════════════════════════════════════════════════════
# Helpers: deep merge, dotted get/set
# ══════════════════════════════════════════════════════════════════════════════

# Dict subtrees the user fully OWNS — replaced wholesale, not merged (so custom
# manual tags / source registries don't get factory entries re-added under them).
REPLACE_PATHS = {"tags.manual_groups", "sources"}

def _deep_merge(base: dict, over: dict, _path: str = "") -> dict:
    """Recursively overlay `over` onto a copy of `base` (fills missing defaults).
    Subtrees listed in REPLACE_PATHS are taken verbatim from `over` when present."""
    out = dict(base)
    for k, v in (over or {}).items():
        path = f"{_path}.{k}" if _path else k
        if path in REPLACE_PATHS:
            out[k] = v
        elif isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v, path)
        else:
            out[k] = v
    return out

def _dget(d: dict, dotted: str, default=None):
    cur = d
    for part in dotted.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur

def _dset(d: dict, dotted: str, value):
    parts = dotted.split("."); cur = d
    for p in parts[:-1]:
        cur = cur.setdefault(p, {})
    cur[parts[-1]] = value


# ══════════════════════════════════════════════════════════════════════════════
# Config object — typed-ish accessors over the resolved dict
# ══════════════════════════════════════════════════════════════════════════════

class _Paths:
    def __init__(self, raw: dict):
        root = raw["library_root"]
        self._resolved = {}
        for k, v in raw.items():
            if k == "library_root":
                self._resolved[k] = to_path(root) if root else None
            elif isinstance(v, str) and "{root}" in v:
                self._resolved[k] = to_path(v.replace("{root}", root)) if root else None
            else:
                self._resolved[k] = v
    def __getattr__(self, name):
        try:
            return self._resolved[name]
        except KeyError:
            raise AttributeError(name)

class Config:
    def __init__(self, data: dict, secrets: dict):
        self._d = data
        self._s = secrets
        root = data["paths"]["library_root"]
        # token expansion → POSIX (these are filesystem paths used on WSL)
        db_posix = str(to_path(data["paths"]["db"].replace("{root}", root))) if root else ""
        tc = data["spotify"].get("token_cache", "")
        if "{db}" in tc and root:
            data["spotify"]["token_cache"] = str(to_path(tc.replace("{db}", db_posix)))
        for sec in ("embed",):
            for kk, vv in data[sec].items():
                if isinstance(vv, str) and "{root}" in vv and root:
                    data[sec][kk] = str(to_path(vv.replace("{root}", root)))
        self.paths = _Paths(data["paths"])

    def __getattr__(self, name):
        if name in self._d:
            v = self._d[name]
            return _Section(v) if isinstance(v, dict) else v
        raise AttributeError(name)

    # convenience -------------------------------------------------------------
    @property
    def raw(self) -> dict: return self._d
    @property
    def secrets(self) -> dict: return self._s

    def rb_master_posix(self):
        m = self._d["rekordbox"]["master_db"]
        return to_path(m) if m else None

    def ensure_dirs(self):
        """Create the full library directory structure. Authoritative — called by
        --init. (Each pipeline stage also mkdir's its own subset defensively.)
        Creates the DATA dirs under library_root + model dir + source drop folders.
        NOTE: 'scripts' is the cloned repo (this file's dir) and 'models' ship with
        it via git — neither is created under library_root here."""
        made = []
        for k in ("audio", "db", "bin", "soulseek", "slskd", "playlists", "logs", "tmp", "backups"):
            d = getattr(self.paths, k)
            d.mkdir(parents=True, exist_ok=True); made.append(d)
        md = to_path(self._d["embed"]["model_dir"])
        md.mkdir(parents=True, exist_ok=True); made.append(md)
        root = self.paths.library_root
        for s in self._d["sources"].values():
            d = root / s["folder"]
            d.mkdir(parents=True, exist_ok=True); made.append(d)
        return made

    def tag_playlists(self) -> list:
        """[RB] Spotify playlist names from synced manual tags (download + sync).
        Playlist name = the full RB tag name + suffix (e.g. 'Floor - Main Floor [RB]')."""
        return list(self.playlist_tag_map().keys())

    def playlist_tag_map(self) -> dict:
        """Spotify playlist name -> RB tag name. The playlist name is just the RB
        tag name with the suffix appended — one name, suffix added for Spotify."""
        suffix = self._d["rekordbox"]["playlist_suffix"]
        out = {}
        for grp in self._d["tags"]["manual_groups"].values():
            prefix = grp.get("prefix", "")
            for t in grp["tags"]:
                if t.get("sync"):
                    rb_tag = f"{prefix}{t['name']}"
                    out[f"{_sp_safe(rb_tag)}{suffix}"] = rb_tag
        return out

    def tag_playlist_map(self) -> dict:
        """RB tag name -> Spotify playlist name (inverse of playlist_tag_map)."""
        return {v: k for k, v in self.playlist_tag_map().items()}

    def all_rb_tag_names(self) -> list:
        """Every RB tag the bootstrap must create (auto + manual, with prefixes)."""
        names = []
        ag = self._d["tags"]["auto_groups"]; at = self._d["tags"]["auto_tags"]
        for grp, meta in ag.items():
            if meta.get("enabled"):
                names += [f"{meta.get('prefix','')}{n}" for n in at.get(grp, [])]
        for grp in self._d["tags"]["manual_groups"].values():
            names += [f"{grp.get('prefix','')}{t['name']}" for t in grp["tags"]]
        return names

class _Section:
    def __init__(self, d): self._d = d
    def __getattr__(self, name):
        if name in self._d:
            v = self._d[name]
            return _Section(v) if isinstance(v, dict) else v
        raise AttributeError(name)
    def __getitem__(self, k): return self._d[k]
    def get(self, k, default=None): return self._d.get(k, default)
    def __iter__(self): return iter(self._d)
    def items(self): return self._d.items()


# ══════════════════════════════════════════════════════════════════════════════
# Validation
# ══════════════════════════════════════════════════════════════════════════════

def validate(data: dict, secrets: dict, *, require_setup=True) -> list:
    """Return a list of human-readable problems ([] = valid)."""
    errs = []
    v = data.get("config_version")
    if v != CONFIG_VERSION:
        errs.append(f"config_version {v} != expected {CONFIG_VERSION} (migration needed)")
    if require_setup and not data.get("setup_complete"):
        errs.append("setup_complete is False — run: python3 config.py --init")
    root = _dget(data, "paths.library_root")
    if not root:
        errs.append("paths.library_root is empty")
    if data.get("rekordbox", {}).get("enabled") and not data["rekordbox"].get("master_db"):
        errs.append("rekordbox.enabled but rekordbox.master_db is empty")
    # source upgradeable sanity
    for name, s in data.get("sources", {}).items():
        if "folder" not in s or "upgradeable" not in s:
            errs.append(f"source '{name}' missing folder/upgradeable")
    # secrets presence (only meaningful once set up)
    if require_setup:
        if not _dget(secrets, "spotify.client_id"):
            errs.append("secrets.spotify.client_id is empty")
        if data.get("rekordbox", {}).get("enabled") is not None and \
           not _dget(secrets, "slskd.username"):
            errs.append("secrets.slskd.username is empty")
    return errs


# ══════════════════════════════════════════════════════════════════════════════
# Load / save / template / reset
# ══════════════════════════════════════════════════════════════════════════════

def _read_json(path: Path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)

def _sp_safe(name: str) -> str:
    """Spotify playlist display name from an RB tag name: '/' -> '-'."""
    return name.replace(" / ", " - ").replace("/", "-")

def _materialize(data: dict):
    """Fill auto-derived fields (tag_playlists) before save/use.
    Playlist name = the full RB tag name (prefix+name) + suffix — one name."""
    suffix = data["rekordbox"]["playlist_suffix"]
    pls = []
    for grp in data["tags"]["manual_groups"].values():
        prefix = grp.get("prefix", "")
        for t in grp["tags"]:
            if t.get("sync"):
                pls.append(f"{_sp_safe(prefix + t['name'])}{suffix}")
    data["spotify"]["tag_playlists"] = pls
    return data

def load(*, require_setup=True) -> Config:
    """Load + validate config.json (+ secrets.json), filling defaults. Self-heals
    a corrupt config.json by falling back to the newest backup, then defaults."""
    raw = {}
    if CONFIG_PATH.exists():
        try:
            raw = _read_json(CONFIG_PATH)
        except (json.JSONDecodeError, OSError) as e:
            print(f"  !! config.json unreadable ({e}); attempting recovery", file=sys.stderr)
            raw = _recover_config()
    data = _materialize(_deep_merge(DEFAULTS, raw))

    secrets = {}
    if SECRETS_PATH.exists():
        try:
            secrets = _read_json(SECRETS_PATH)
        except (json.JSONDecodeError, OSError):
            secrets = {}
    secrets = _deep_merge(SECRETS_DEFAULTS, secrets)

    problems = validate(data, secrets, require_setup=require_setup)
    if problems and require_setup:
        msg = "Config not ready:\n  - " + "\n  - ".join(problems)
        raise SystemExit(msg)
    return Config(data, secrets)

def _recover_config() -> dict:
    """Best-effort: newest backups/config_*.json, else {} (defaults)."""
    try:
        bdir = CONFIG_DIR / "backups"
        cands = sorted(bdir.glob("config_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
        for c in cands:
            try:
                return _read_json(c)
            except Exception:
                continue
    except Exception:
        pass
    return {}

def save(data: dict):
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    # back up the existing config first
    if CONFIG_PATH.exists():
        bdir = CONFIG_DIR / "backups"; bdir.mkdir(exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        shutil.copy2(str(CONFIG_PATH), str(bdir / f"config_{ts}.json"))
        for old in sorted(bdir.glob("config_*.json"), key=lambda p: p.stat().st_mtime)[:-10]:
            old.unlink()
    atomic_write_json(CONFIG_PATH, _materialize(data))

def save_secrets(secrets: dict):
    atomic_write_json(SECRETS_PATH, secrets)
    try:
        os.chmod(SECRETS_PATH, 0o600)   # tighten perms on the credential file
    except OSError:
        pass

def write_templates():
    """(Re)write the committed sample files — defaults, no real secrets/paths."""
    tmpl = _materialize(_deep_merge(DEFAULTS, {}))
    atomic_write_json(CONFIG_TMPL, tmpl)
    atomic_write_json(SECRETS_TMPL, SECRETS_DEFAULTS)
    print(f"  Wrote {CONFIG_TMPL.name} and {SECRETS_TMPL.name}")

def reset():
    if CONFIG_PATH.exists():
        bdir = CONFIG_DIR / "backups"; bdir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        shutil.copy2(str(CONFIG_PATH), str(bdir / f"config_{ts}.json"))
        print(f"  Backed up current config → backups/config_{ts}.json")
    atomic_write_json(CONFIG_PATH, _materialize(_deep_merge(DEFAULTS, {})))
    print(f"  Reset {CONFIG_PATH.name} to factory defaults (setup_complete=False).")


# ══════════════════════════════════════════════════════════════════════════════
# Wizard (--init)
# ══════════════════════════════════════════════════════════════════════════════

def run_init(input_fn=input, print_fn=print):
    """Staged first-run setup. Prompts essentials; everything else stays default
    (edit config.json afterward). Writes config.json + secrets.json."""
    data    = _read_json(CONFIG_PATH) if CONFIG_PATH.exists() else _deep_merge(DEFAULTS, {})
    data    = _deep_merge(DEFAULTS, data)
    secrets = _deep_merge(SECRETS_DEFAULTS,
                          _read_json(SECRETS_PATH) if SECRETS_PATH.exists() else {})

    def ask(prompt, default=""):
        suffix = f" [{default}]" if default else ""
        ans = input_fn(f"{prompt}{suffix}: ").strip()
        return ans or default

    def ask_yn(prompt, default=True):
        d = "Y/n" if default else "y/N"
        ans = input_fn(f"{prompt} [{d}]: ").strip().lower()
        return default if not ans else ans.startswith("y")

    print_fn("\n=== DJ Pipeline setup ===\n(Press Enter to accept the [default]. Edit config.json later for advanced settings.)\n")

    # Stage 1 — library location
    print_fn("── 1/4 · Library ──")
    data["paths"]["library_root"] = ask("Library root directory", data["paths"]["library_root"] or "E:/Music Library")

    # Stage 2 — Rekordbox
    print_fn("\n── 2/4 · Rekordbox ──")
    data["rekordbox"]["enabled"] = ask_yn("Use Rekordbox tag sync?", data["rekordbox"]["enabled"])
    if data["rekordbox"]["enabled"]:
        print_fn("  tip: C:/Users/<name>/AppData/Roaming/Pioneer/rekordbox/master.db")
        data["rekordbox"]["master_db"] = ask("Rekordbox master.db path", data["rekordbox"]["master_db"])

    # Stage 3 — tags
    print_fn("\n── 3/4 · Tags ──")
    data["tags"]["use_defaults"] = ask_yn("Use the default manual tag set (Floor / Set Position / Flag)?",
                                          data["tags"]["use_defaults"])
    if not data["tags"]["use_defaults"]:
        print_fn("  Define your manual tag groups. Leave group name blank to finish.")
        groups = {}
        while True:
            gname = ask("  Group name (blank = done)")
            if not gname:
                break
            prefix = ask(f"    Prefix for '{gname}' tags (blank = none)")
            tags = []
            print_fn(f"    Enter tags for '{gname}', blank to finish the group:")
            while True:
                tname = ask("      tag")
                if not tname:
                    break
                sync = ask_yn(f"      sync '{tname}' to a Spotify {data['rekordbox']['playlist_suffix'].strip()} playlist?", True)
                tags.append({"name": tname, "sync": sync})
            if tags:
                groups[gname] = {"prefix": prefix, "tags": tags}
        if groups:
            data["tags"]["manual_groups"] = groups

    # Stage 4 — credentials
    print_fn("\n── 4/4 · Credentials (stored in secrets.json) ──")
    print_fn("  Spotify: create an app at https://developer.spotify.com → Dashboard")
    secrets["spotify"]["client_id"]     = ask("  Spotify client_id", secrets["spotify"]["client_id"])
    secrets["spotify"]["client_secret"] = ask("  Spotify client_secret", secrets["spotify"]["client_secret"])
    secrets["slskd"]["username"]        = ask("  slskd username", secrets["slskd"]["username"])
    secrets["slskd"]["password"]        = ask("  slskd password", secrets["slskd"]["password"])

    # Finalize
    data = _materialize(data)
    problems = validate(data, secrets, require_setup=False)
    blocking = [p for p in problems if "library_root" in p or "master_db" in p]
    if blocking:
        print_fn("\n  ⚠ Still missing:\n   - " + "\n   - ".join(blocking))
        print_fn("  Re-run --init or edit config.json. Not marking setup complete.")
        data["setup_complete"] = False
    else:
        print_fn("\n  Review:")
        print_fn(f"    library_root : {data['paths']['library_root']}")
        print_fn(f"    rekordbox    : {'on' if data['rekordbox']['enabled'] else 'off'}  {data['rekordbox']['master_db']}")
        print_fn(f"    tag playlists: {len(data['spotify']['tag_playlists'])}  (download sources + sync targets)")
        print_fn(f"    RB tags total: {len(Config(data, secrets).all_rb_tag_names())}")
        data["setup_complete"] = ask_yn("\n  Looks right — mark setup complete?", True)

    save(data)
    save_secrets(secrets)
    print_fn(f"\n  Saved {CONFIG_PATH.name} + {SECRETS_PATH.name}  (setup_complete={data['setup_complete']})")
    if data["setup_complete"]:
        made = Config(data, secrets).ensure_dirs()
        print_fn(f"  Created {len(made)} directories under {data['paths']['library_root']}")
    return data


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def _main(argv=None):
    ap = argparse.ArgumentParser(description="DJ pipeline configuration")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--init",     action="store_true", help="first-run setup wizard")
    g.add_argument("--show",     action="store_true", help="print resolved config")
    g.add_argument("--validate", action="store_true", help="validate config.json")
    g.add_argument("--reset",    action="store_true", help="back up + restore factory defaults")
    g.add_argument("--edit",     action="store_true", help="open config.json in $EDITOR")
    g.add_argument("--template", action="store_true", help="(re)write committed template files")
    args = ap.parse_args(argv)

    if args.template:
        write_templates()
    elif args.reset:
        reset()
    elif args.init:
        run_init()
    elif args.edit:
        if not CONFIG_PATH.exists():
            reset()
        os.system(f'{os.environ.get("EDITOR", "nano")} "{CONFIG_PATH}"')
    elif args.validate:
        data = _materialize(_deep_merge(DEFAULTS, _read_json(CONFIG_PATH))) if CONFIG_PATH.exists() else _deep_merge(DEFAULTS, {})
        secrets = _deep_merge(SECRETS_DEFAULTS, _read_json(SECRETS_PATH)) if SECRETS_PATH.exists() else {}
        probs = validate(data, secrets)
        if probs:
            print("INVALID:\n  - " + "\n  - ".join(probs)); sys.exit(1)
        print("config.json is valid.")
    elif args.show:
        cfg = load(require_setup=False)
        shown = dict(cfg.raw)
        print(json.dumps(shown, indent=2, ensure_ascii=False))
        print(f"\n  tag playlists ({len(cfg.tag_playlists())}): {cfg.tag_playlists()}")
        print(f"  total RB tags: {len(cfg.all_rb_tag_names())}")
    else:
        ap.print_help()

if __name__ == "__main__":
    _main()
