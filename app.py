#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
app.py — Stacks web GUI backend
=================================
Part 1 (Tracks, read-only) + Part 1.5 (setup wizard).

The server now ALWAYS starts, even on a totally fresh install with no
config.json. /api/setup tells the frontend whether it needs to show the
wizard; every other /api/* route checks the same thing and returns a clear
409 (not a crash) if the library isn't configured yet.

Run from the repo root (same dir as config.py / dj.py) — start.sh now manages
its own venv automatically (creates it on first run, installs requirements.txt,
reuses it after that). You shouldn't need to pip install anything by hand:
    ./start.sh
or just double-click start_stacks.bat on Windows, which calls this for you.

KNOWN GAPS (unchanged from Part 1 — see chat, not re-explaining here):
  - no "energy" metric (no backing column)
  - tags (Genre/Set Position/Vibe/Extras) are stubbed to genre-only
  - preference_score/sim_keep/sim_delete are unpopulated columns

WIZARD SCOPE — deliberately smaller than config.py's CLI `--init`:
  - covers: library_root, rekordbox enabled+master_db, spotify creds, slskd
    creds — the same fields run_init() calls "essentials".
  - does NOT cover the custom tag-group editor (Stage 3 of run_init). New
    installs get tags.use_defaults=True (the Floor/Situation/Flag set from
    config.template.json). Anyone who wants custom groups still edits
    config.json by hand for now — flagging this as a deliberate cut, not an
    oversight.
"""
import asyncio
import json
import os
import shutil
import sqlite3
import sys
import subprocess as _subproc
import threading
import time
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import List, Optional

try:
    import numpy as np
    _NUMPY_OK = True
except ImportError:
    _NUMPY_OK = False

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, PlainTextResponse, StreamingResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import config as cfgmod
from config import load, save, save_secrets, validate, Config, reset as cfg_reset
from dj_paths import to_win, to_path, to_key

WEB_DIR = Path(__file__).resolve().parent / "web"
app = FastAPI(title="Stacks API")

CLUSTER_PALETTE = [
    "#4DD9C9", "#5B9FE8", "#E8B84D", "#D98C3D", "#9D7FE8",
    "#6FCB9F", "#E87FA0", "#E8E04D", "#7FD4E8", "#C97FE8",
    "#8FD45A", "#E8A0D4",
]
NOISE_COLOR = "#5A5D63"

# ── Module-level caches (mtime-keyed, invalidate when file changes) ───────────
_clusters_cache   = {"mtime": None, "by_path": {}, "clusters": []}
_ledger_cache     = {"mtime": None, "data": {}}
_tags_cache       = {"mtime": None, "data": {}}
_embeddings_cache = {"loaded": False, "matrix": None, "paths": [], "path_idx": {}}


def _load_ledger(cfg: Config) -> dict:
    """Load sync_ledger.json with mtime caching. Returns {key_path: entry}."""
    ledger_path = cfg.paths.ledger
    if not ledger_path or not ledger_path.exists():
        return {}
    mtime = ledger_path.stat().st_mtime
    if _ledger_cache["mtime"] == mtime:
        return _ledger_cache["data"]
    try:
        data = json.loads(ledger_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    _ledger_cache.update({"mtime": mtime, "data": data})
    return data


def _load_tags_json(cfg: Config) -> dict:
    """Load tags.json with mtime caching. Returns {posix_path: {genre, mood, danceability, components}}."""
    if not cfg.paths.playlists:
        return {}
    tags_path = cfg.paths.playlists / "tags.json"
    if not tags_path.exists():
        return {}
    mtime = tags_path.stat().st_mtime
    if _tags_cache["mtime"] == mtime:
        return _tags_cache["data"]
    try:
        data = json.loads(tags_path.read_text(encoding="utf-8")).get("tracks", {})
    except Exception:
        return {}
    _tags_cache.update({"mtime": mtime, "data": data})
    return data


def _build_tag_classifier(cfg_raw: dict) -> dict:
    """Map a full RB spot-tag string → frontend group key (Genre|Set Position|Vibe|Extras)."""
    tags_cfg      = cfg_raw.get("tags", {})
    auto_groups   = tags_cfg.get("auto_groups", {})
    auto_tags     = tags_cfg.get("auto_tags", {})
    manual_groups = tags_cfg.get("manual_groups", {})
    lookup = {}
    group_map = {"genre": "Genre", "mood": "Vibe", "danceability": "Vibe", "components": "Extras"}
    for g_key, dest in group_map.items():
        prefix = auto_groups.get(g_key, {}).get("prefix", "")
        for tag in auto_tags.get(g_key, []):
            lookup[f"{prefix}{tag}"] = dest
    for grp_name, grp in manual_groups.items():
        dest   = "Set Position" if grp_name.lower() in ("situation", "set position") else "Extras"
        prefix = grp.get("prefix", "")
        for t in grp.get("tags", []):
            lookup[f"{prefix}{t['name']}"] = dest
    return lookup


def _spot_to_tags(spot: list, classifier: dict) -> dict:
    result = {"Genre": [], "Set Position": [], "Vibe": [], "Extras": []}
    for tag in (spot or []):
        group = classifier.get(tag)
        if group in result:
            result[group].append(tag)
        else:
            result["Extras"].append(tag)
    return result


def _cfg() -> Config:
    """Fresh load on every call (cheap — it's a small JSON read) so a wizard
    submission is visible immediately, no server restart needed."""
    return load(require_setup=False)


@app.on_event("startup")
def _ensure_dirs_on_boot() -> None:
    """Make sure the library's directory structure exists on every server
    boot, not just when the setup wizard is submitted. Covers the case where
    library_root was changed by hand-editing config.json (e.g. pointing at a
    test path) without re-running the wizard -- ensure_dirs() never ran for
    the new root otherwise."""
    try:
        cfg = _cfg()
        if cfg.raw.get("setup_complete"):
            cfg.ensure_dirs()
    except Exception as e:
        print(f"[startup] couldn't ensure library directories: {e}")
    # Pre-populate Spotify playlist cache in background if no cache exists
    threading.Thread(target=_startup_precompute_spotify, daemon=True).start()


def _startup_precompute_spotify():
    """Silently pre-fetch Spotify playlists on startup if cache is missing and token exists."""
    try:
        cache = _REPO / ".cache" / "spotify_playlists.json"
        if cache.exists():
            return
        cfg = _cfg()
        if not cfg.secrets.get("spotify", {}).get("client_id"):
            return
        if _sp_read_token(cfg):
            _spotify_fetch_live_bg()
    except Exception:
        pass


def _require_configured(cfg: Config) -> None:
    problems = validate(cfg.raw, cfg.secrets, require_setup=False)
    blocking = [p for p in problems if "library_root" in p or "master_db" in p]
    if not cfg.raw.get("setup_complete") or blocking:
        raise HTTPException(409, "Library isn't configured yet — finish setup first.")


# ══════════════════════════════════════════════════════════════════════════
# Setup wizard
# ══════════════════════════════════════════════════════════════════════════
class SetupPayload(BaseModel):
    library_root: str = ""
    rekordbox_enabled: bool = True
    rekordbox_master_db: str = ""
    spotify_client_id: str = ""
    spotify_client_secret: str = ""
    slskd_username: str = ""
    slskd_password: str = ""


@app.get("/api/setup")
def get_setup():
    cfg = _cfg()
    problems = validate(cfg.raw, cfg.secrets, require_setup=False)
    blocking = [p for p in problems if "library_root" in p or "master_db" in p]
    return {
        "setup_complete": bool(cfg.raw.get("setup_complete")) and not blocking,
        "problems": problems,
        "values": {
            "library_root": cfg.raw["paths"]["library_root"],
            "rekordbox_enabled": cfg.raw["rekordbox"]["enabled"],
            "download_enabled": cfg.raw.get("download", {}).get("enabled", True),
            "rekordbox_master_db": cfg.raw["rekordbox"]["master_db"],
            # secrets are never echoed back — just whether they're already set,
            # so the form can show "already set, leave blank to keep" instead
            # of a round-tripped plaintext credential.
            "spotify_client_id_set": bool(cfg.secrets.get("spotify", {}).get("client_id")),
            "spotify_client_secret_set": bool(cfg.secrets.get("spotify", {}).get("client_secret")),
            "slskd_username_set": bool(cfg.secrets.get("slskd", {}).get("username")),
            "slskd_password_set": bool(cfg.secrets.get("slskd", {}).get("password")),
        },
    }


@app.post("/api/setup")
def post_setup(payload: SetupPayload):
    cfg = _cfg()
    data = cfg.raw
    secrets = cfg.secrets

    data["paths"]["library_root"] = payload.library_root.strip()
    data["rekordbox"]["enabled"] = payload.rekordbox_enabled
    if payload.rekordbox_enabled:
        data["rekordbox"]["master_db"] = payload.rekordbox_master_db.strip()

    # blank field = "leave the existing secret alone", not "erase it"
    if payload.spotify_client_id.strip():
        secrets["spotify"]["client_id"] = payload.spotify_client_id.strip()
    if payload.spotify_client_secret.strip():
        secrets["spotify"]["client_secret"] = payload.spotify_client_secret.strip()
    if payload.slskd_username.strip():
        secrets["slskd"]["username"] = payload.slskd_username.strip()
    if payload.slskd_password.strip():
        secrets["slskd"]["password"] = payload.slskd_password.strip()

    problems = validate(data, secrets, require_setup=False)
    blocking = [p for p in problems if "library_root" in p or "master_db" in p]
    data["setup_complete"] = not blocking

    save(data)
    save_secrets(secrets)
    if data["setup_complete"]:
        Config(data, secrets).ensure_dirs()

    return get_setup()


class ResetPayload(BaseModel):
    also_clear_secrets: bool = False


@app.post("/api/config/reset")
def post_config_reset(payload: ResetPayload):
    cfg_reset()
    if payload.also_clear_secrets:
        save_secrets({"spotify": {"client_id": "", "client_secret": ""}, "slskd": {"username": "", "password": ""}})
    return get_setup()


@app.get("/api/browse")
def browse(path: Optional[str] = None, files: bool = False):
    """Lists real directories (and optionally files) on disk, for the wizard's
    Browse buttons. A native <input type=file> picker can't give us an actual
    filesystem path (browsers hide it deliberately) -- but since this server
    already runs locally with full filesystem access, it can just tell the
    frontend what's there directly."""
    if not path:
        roots = []
        mnt = Path("/mnt")
        if mnt.exists():
            for d in sorted(mnt.iterdir()):
                if d.is_dir() and len(d.name) == 1:
                    roots.append({"name": d.name.upper() + ":", "display_path": d.name.upper() + ":\\",
                                  "path": str(d), "is_dir": True})
        home = Path.home()
        roots.append({"name": f"Home ({home.name})", "display_path": str(home),
                      "path": str(home), "is_dir": True})
        return {"path": None, "display_path": None, "parent": None, "entries": roots}

    p = to_path(path)
    try:
        p = p.resolve()
    except Exception:
        raise HTTPException(400, f"invalid path: {path}")
    if not p.exists() or not p.is_dir():
        raise HTTPException(404, f"not a directory: {p}")

    entries = []
    try:
        for child in sorted(p.iterdir(), key=lambda x: (not x.is_dir(), x.name.lower())):
            if child.name.startswith("."):
                continue
            if child.is_dir():
                entries.append({"name": child.name, "display_path": to_win(str(child)),
                                "path": str(child), "is_dir": True})
            elif files:
                entries.append({"name": child.name, "display_path": to_win(str(child)),
                                "path": str(child), "is_dir": False})
    except PermissionError:
        pass

    parent = None if str(p) == p.anchor else str(p.parent)
    return {"path": str(p), "display_path": to_win(str(p)), "parent": parent, "entries": entries}


# ══════════════════════════════════════════════════════════════════════════
# Tracks / Clusters (Part 1)
# ══════════════════════════════════════════════════════════════════════════
def _load_clusters_json(cfg: Config):
    clusters_json = cfg.paths.playlists / "clusters.json" if cfg.paths.playlists else None
    if not clusters_json or not clusters_json.exists():
        return {}, []
    mtime = clusters_json.stat().st_mtime
    if _clusters_cache["mtime"] == mtime:
        return _clusters_cache["by_path"], _clusters_cache["clusters"]
    data = json.loads(clusters_json.read_text(encoding="utf-8"))
    rows = data.get("tracks", [])
    by_path = {r["path"]: r for r in rows}

    ids = sorted({r["cluster"] for r in rows if r["cluster"] != -1})
    clusters = []
    for idx, cid in enumerate(ids):
        members = [r for r in rows if r["cluster"] == cid]
        top_genre = members[0]["genre"] if members else None
        clusters.append({
            "id": cid,
            "name": top_genre or f"Cluster {cid}",
            "color": CLUSTER_PALETTE[idx % len(CLUSTER_PALETTE)],
            "size": len(members),
        })
    noise_n = sum(1 for r in rows if r["cluster"] == -1)
    clusters.append({"id": -1, "name": "Unclassified", "color": NOISE_COLOR, "size": noise_n})
    _clusters_cache.update({"mtime": mtime, "by_path": by_path, "clusters": clusters})
    return by_path, clusters


def _strip_genre(g: str) -> str:
    if not g:
        return g
    if "---" in g:
        return g.split("---", 1)[-1].strip()
    if " - " in g:
        return g.split(" - ", 1)[-1].strip()
    return g


def _row_to_track(row: sqlite3.Row, cluster_lookup: dict,
                  tags_lookup: dict, ledger: dict, classifier: dict,
                  overrides: dict = None) -> dict:
    d    = dict(row)
    path = d["path"]
    c    = cluster_lookup.get(path)
    genre = _strip_genre(d.get("genre"))

    # Auto tags from tags.json (keyed by POSIX path, same format as DB)
    auto       = tags_lookup.get(path, {})
    raw_auto_genre = auto.get("genre") or d.get("genre") or ""
    if isinstance(raw_auto_genre, list):
        auto_genres = [_strip_genre(g) for g in raw_auto_genre if _strip_genre(g)]
    else:
        auto_genres = [_strip_genre(raw_auto_genre)] if _strip_genre(raw_auto_genre) else []
    auto_mood  = list(auto.get("mood", []))
    auto_dance = list(auto.get("danceability", []))
    auto_comp  = list(auto.get("components", []))

    # Manual tags from sync_ledger.json spot[] (look up by KEY-form path)
    entry  = ledger.get(to_key(path), {})
    manual = _spot_to_tags(entry.get("spot", []), classifier)

    def _merge(*lists):
        seen, out = set(), []
        for lst in lists:
            for item in (lst or []):
                if item not in seen:
                    seen.add(item); out.append(item)
        return out

    track = {
        "id": path,
        "title": d.get("title") or d.get("filename") or "Untitled",
        "artist": d.get("artist") or "Unknown",
        "bpm": d.get("bpm"),
        "key": d.get("key"),
        "key_alpha": d.get("key_alpha"),
        "genre": genre,
        "cluster_id": c["cluster"] if c else -1,
        "x": c["x"] if c else 0.0,
        "y": c["y"] if c else 0.0,
        "danceability": round(d["danceability"] * 100, 1) if d.get("danceability") is not None else None,
        "party_score": round(d["party_score"] * 100, 1) if d.get("party_score") is not None else None,
        "sp_energy": d.get("sp_energy"),
        "sp_danceability": d.get("sp_danceability"),
        "sp_playcount": d.get("sp_playcount"),
        "vocals_prob": d.get("vocals_prob"),
        "rating": d.get("rating") or 0,
        "preference_score": round(d["preference_score"] * 100, 1) if d.get("preference_score") is not None else None,
        "mood_aggressive": d.get("mood_aggressive"),
        "mood_happy": d.get("mood_happy"),
        "mood_relaxed": d.get("mood_relaxed"),
        "mood_party": d.get("mood_party"),
        "duration": d.get("duration"),
        "date_added": d.get("date_added"),
        "tags": {
            "Genre":        _merge(auto_genres, manual["Genre"]),
            "Set Position": _merge(manual["Set Position"]),
            "Vibe":         _merge(auto_mood, auto_dance, manual["Vibe"]),
            "Extras":       _merge(auto_comp, manual["Extras"]),
        },
        "spot": entry.get("spot") or [],
    }
    # Snapshot tags before overrides — used by the frontend as the RB baseline
    # (what the pipeline actually wrote to RB last time it ran)
    track["tags_base"] = {k: list(v) for k, v in track["tags"].items()}
    # Apply user tag overrides (from Tag Authority edits, persisted via /api/tags/save)
    if overrides:
        ov = overrides.get(path, {})
        for g in ("Genre", "Set Position", "Vibe", "Extras"):
            if g in ov:
                track["tags"][g] = list(ov[g])
    return track


def _fetch_all_tracks(cfg: Config) -> list:
    music_db = cfg.paths.music_db
    if not music_db or not music_db.exists():
        raise HTTPException(503, f"music.db not found at {music_db} — run pipeline_01 first.")
    cluster_lookup, _ = _load_clusters_json(cfg)
    tags_lookup = _load_tags_json(cfg)
    ledger = _load_ledger(cfg)
    classifier = _build_tag_classifier(cfg.raw)
    overrides = _load_tag_overrides(cfg)
    conn = sqlite3.connect(str(music_db))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute("SELECT * FROM tracks").fetchall()
    finally:
        conn.close()
    return [_row_to_track(r, cluster_lookup, tags_lookup, ledger, classifier, overrides) for r in rows]


@app.get("/api/tracks")
def get_tracks(
    search: Optional[str] = None,
    cluster: Optional[int] = None,
    genre: Optional[str] = None,
    bpm_min: Optional[float] = None,
    bpm_max: Optional[float] = None,
    sort: Optional[str] = None,
    dir: str = "asc",
    page: Optional[int] = None,
    page_size: int = 500,
):
    cfg = _cfg()
    _require_configured(cfg)
    tracks = _fetch_all_tracks(cfg)

    if search:
        s = search.lower()
        tracks = [t for t in tracks if s in t["title"].lower() or s in t["artist"].lower()]
    if cluster is not None:
        tracks = [t for t in tracks if t["cluster_id"] == cluster]
    if genre:
        tracks = [t for t in tracks if (t["genre"] or "").lower() == genre.lower()]
    if bpm_min is not None:
        tracks = [t for t in tracks if (t["bpm"] or 0) >= bpm_min]
    if bpm_max is not None:
        tracks = [t for t in tracks if (t["bpm"] or 0) <= bpm_max]
    if sort:
        reverse = dir == "desc"
        tracks.sort(key=lambda t: (t.get(sort) is None, t.get(sort)), reverse=reverse)

    total = len(tracks)
    if page is not None:
        start = (page - 1) * page_size
        tracks = tracks[start:start + page_size]

    return {"tracks": tracks, "total": total}


_overrides_cache: dict = {}  # {path: {group: [tags]}}
_overrides_mtime: Optional[float] = None


def _load_tag_overrides(cfg: Config) -> dict:
    global _overrides_cache, _overrides_mtime
    p = cfg.paths.db / "tag_overrides.json"
    if not p.exists():
        return {}
    mtime = p.stat().st_mtime
    if mtime == _overrides_mtime:
        return _overrides_cache
    try:
        _overrides_cache = json.loads(p.read_text(encoding="utf-8"))
        _overrides_mtime = mtime
    except Exception:
        _overrides_cache = {}
    return _overrides_cache


class TagSavePayload(BaseModel):
    path: str
    tags: dict  # {group: [tag, ...]}


@app.post("/api/tags/save")
def save_tags(payload: TagSavePayload):
    cfg = _cfg()
    p = cfg.paths.db / "tag_overrides.json"
    overrides: dict = {}
    if p.exists():
        try:
            overrides = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            pass
    overrides[payload.path] = payload.tags
    p.write_text(json.dumps(overrides, ensure_ascii=False), encoding="utf-8")
    global _overrides_mtime
    _overrides_mtime = None  # invalidate cache
    return {"ok": True}


@app.get("/api/tracks/genres")
def get_track_genres():
    cfg = _cfg()
    db_path = cfg.paths.music_db
    if not db_path or not db_path.exists():
        return {"genres": []}
    con = sqlite3.connect(str(db_path))
    try:
        rows = con.execute(
            "SELECT DISTINCT genre FROM tracks WHERE genre IS NOT NULL AND genre != ''"
        ).fetchall()
    finally:
        con.close()
    genres = sorted({_strip_genre(r[0]) for r in rows if r[0]})
    return {"genres": genres}


@app.get("/api/config/sync-tags")
def get_sync_tags():
    cfg = _cfg()
    # Sync-eligible name suffixes from config (the `name` field, without prefix)
    sync_suffixes = set()
    for grp in cfg._d["tags"]["manual_groups"].values():
        for tag in grp.get("tags", []):
            if tag.get("sync"):
                sync_suffixes.add(tag["name"])
    # Scan ledger for actual tag names used in spot — handles old prefix configs
    # where ledger has e.g. "Flag - Favorite" but current config says prefix=""
    led = _load_ledger(cfg)
    actual: set = set()
    for entry in led.values():
        for tag_name in (entry.get("spot") or []):
            for suffix in sync_suffixes:
                if tag_name == suffix or tag_name.endswith(f"- {suffix}"):
                    actual.add(tag_name)
                    break
    # Also include current config-derived names (covers tags not yet written to ledger)
    for grp in cfg._d["tags"]["manual_groups"].values():
        prefix = grp.get("prefix", "")
        for tag in grp.get("tags", []):
            if tag.get("sync"):
                actual.add(f"{prefix}{tag['name']}")
    return {"sync_tags": sorted(actual)}


class _RatingPayload(BaseModel):
    track_id: str
    rating: float


@app.post("/api/tracks/rating")
def set_track_rating(payload: _RatingPayload):
    if not (0 <= payload.rating <= 5):
        raise HTTPException(400, "rating must be 0–5")
    cfg = _cfg()
    _require_configured(cfg)
    music_db = cfg.paths.music_db
    if not music_db or not music_db.exists():
        raise HTTPException(503, "music.db not found")
    with sqlite3.connect(str(music_db)) as con:
        con.execute("UPDATE tracks SET rating=? WHERE path=?", (payload.rating, payload.track_id))
    return {"ok": True}


@app.get("/api/tracks/{track_id:path}")
def get_track(track_id: str):
    cfg = _cfg()
    _require_configured(cfg)
    music_db = cfg.paths.music_db
    if not music_db or not music_db.exists():
        raise HTTPException(503, f"music.db not found at {music_db}")
    cluster_lookup, _ = _load_clusters_json(cfg)
    tags_lookup = _load_tags_json(cfg)
    ledger = _load_ledger(cfg)
    classifier = _build_tag_classifier(cfg.raw)
    conn = sqlite3.connect(str(music_db))
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute("SELECT * FROM tracks WHERE path = ?", (track_id,)).fetchone()
    finally:
        conn.close()
    if row is None:
        raise HTTPException(404, f"no track at path: {track_id}")
    return _row_to_track(row, cluster_lookup, tags_lookup, ledger, classifier)


@app.get("/api/clusters")
def get_clusters():
    cfg = _cfg()
    _require_configured(cfg)
    _, clusters = _load_clusters_json(cfg)
    return {"clusters": clusters}


@app.get("/api/umap/projections")
def get_umap_projections():
    cfg = _cfg()
    _require_configured(cfg)
    proj_path = Path(cfg.paths.music_db).parent / "umap_projections.json"
    if not proj_path.exists():
        return {"available": False, "projections": {}}
    import json as _json
    data = _json.loads(proj_path.read_text(encoding="utf-8"))
    return {"available": True, **data}


def _load_embeddings(cfg: Config) -> dict:
    """Lazy-load all 1280-d track embeddings from track_metadata, L2-normalised for cosine sim."""
    if _embeddings_cache["loaded"]:
        return _embeddings_cache
    if not _NUMPY_OK:
        return _embeddings_cache
    music_db = cfg.paths.music_db
    if not music_db or not music_db.exists():
        return _embeddings_cache
    conn = sqlite3.connect(str(music_db))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT track_path, embeddings FROM track_metadata WHERE embeddings IS NOT NULL"
        ).fetchall()
    finally:
        conn.close()
    paths, vecs = [], []
    for r in rows:
        blob = r["embeddings"]
        if not blob:
            continue
        emb = np.frombuffer(bytes(blob), dtype=np.float32).copy()
        if emb.shape[0] != 1280:
            continue
        paths.append(r["track_path"])
        vecs.append(emb)
    if not vecs:
        return _embeddings_cache
    matrix = np.stack(vecs)
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    norms[norms == 0] = 1
    matrix = matrix / norms
    path_idx = {p: i for i, p in enumerate(paths)}
    _embeddings_cache.update({"loaded": True, "matrix": matrix, "paths": paths, "path_idx": path_idx})
    return _embeddings_cache


@app.get("/api/audio")
def get_audio(track_id: str):
    """Stream an audio file by its DB path (POSIX form). Starlette FileResponse handles Range."""
    cfg = _cfg()
    _require_configured(cfg)
    music_db = cfg.paths.music_db
    if not music_db or not music_db.exists():
        raise HTTPException(503, "music.db not found")
    conn = sqlite3.connect(str(music_db))
    try:
        row = conn.execute("SELECT path FROM tracks WHERE path = ?", (track_id,)).fetchone()
    finally:
        conn.close()
    if not row:
        raise HTTPException(404, f"track not in library: {track_id}")
    audio_path = to_path(row[0])
    if not audio_path.exists():
        raise HTTPException(404, f"audio file not found on disk: {audio_path}")
    import mimetypes as _mt
    mime = _mt.guess_type(str(audio_path))[0] or "application/octet-stream"
    return FileResponse(str(audio_path), media_type=mime)


@app.get("/api/audio/waveform")
async def get_waveform(track_id: str):
    """Compute and cache a 400-point RMS waveform for the player bar."""
    if not _NUMPY_OK or not shutil.which("ffmpeg"):
        return {"waveform": []}
    cfg = _cfg()
    _require_configured(cfg)
    music_db = cfg.paths.music_db
    if not music_db or not music_db.exists():
        raise HTTPException(503, "music.db not found")
    conn = sqlite3.connect(str(music_db))
    try:
        row = conn.execute("SELECT path FROM tracks WHERE path = ?", (track_id,)).fetchone()
    finally:
        conn.close()
    if not row:
        raise HTTPException(404, "track not found")
    audio_path = to_path(row[0])
    if not audio_path.exists():
        raise HTTPException(404, "audio file not found")
    import hashlib
    cache_dir = cfg.paths.db / "waveforms"
    cache_dir.mkdir(exist_ok=True)
    cache_file = cache_dir / (hashlib.md5(track_id.encode()).hexdigest() + "_400.json")
    if cache_file.exists():
        return json.loads(cache_file.read_text(encoding="utf-8"))

    def _compute():
        try:
            proc = _subproc.run(
                ["ffmpeg", "-i", str(audio_path), "-ac", "1", "-ar", "8000", "-f", "f32le", "-"],
                capture_output=True, timeout=30,
            )
            if proc.returncode != 0 or not proc.stdout:
                return {"waveform": []}
            samples = np.frombuffer(proc.stdout, dtype=np.float32)
            if len(samples) < 400:
                return {"waveform": []}
            chunks = np.array_split(samples, 400)
            rms = [float(np.sqrt(np.mean(c ** 2))) for c in chunks]
            peak = max(rms) or 1.0
            waveform = [round(v / peak, 4) for v in rms]
            return {"waveform": waveform}
        except Exception:
            return {"waveform": []}

    result = await asyncio.to_thread(_compute)
    if result["waveform"]:
        cache_file.write_text(json.dumps(result), encoding="utf-8")
    return result


@app.get("/api/neighbors")
def get_neighbors(track_id: str, k: int = 100):
    """Return the top-k nearest neighbours in 1280-d embedding space (cosine similarity)."""
    cfg = _cfg()
    _require_configured(cfg)
    emb = _load_embeddings(cfg)
    if emb["matrix"] is None:
        return {"neighbors": [], "note": "embeddings not loaded — run pipeline_01 + pipeline_02 first"}
    path_idx = emb["path_idx"]
    if track_id not in path_idx:
        return {"neighbors": [], "note": "track not in embedding index"}
    idx  = path_idx[track_id]
    sims = emb["matrix"] @ emb["matrix"][idx]
    sims[idx] = -1.0
    top_k   = int(min(k, len(sims) - 1))
    top_idx = np.argsort(sims)[::-1][:top_k]

    music_db = cfg.paths.music_db
    cluster_lookup, _ = _load_clusters_json(cfg)
    tags_lookup = _load_tags_json(cfg)
    ledger = _load_ledger(cfg)
    classifier = _build_tag_classifier(cfg.raw)

    top_paths = [emb["paths"][i] for i in top_idx]
    conn = sqlite3.connect(str(music_db))
    conn.row_factory = sqlite3.Row
    try:
        placeholders = ",".join("?" * len(top_paths))
        rows = conn.execute(
            f"SELECT * FROM tracks WHERE path IN ({placeholders})", top_paths
        ).fetchall()
    finally:
        conn.close()

    row_by_path = {dict(r)["path"]: r for r in rows}
    neighbors = []
    for i, path in zip(top_idx, top_paths):
        if path in row_by_path:
            t = _row_to_track(row_by_path[path], cluster_lookup, tags_lookup, ledger, classifier)
            t["similarity"] = round(float(sims[i]), 4)
            neighbors.append(t)
    return {"neighbors": neighbors}


class _SetSimilarPayload(BaseModel):
    track_ids: List[str] = []
    n: int = 25


@app.post("/api/set/similar")
def set_similar(payload: _SetSimilarPayload):
    """Return top-N tracks most similar to the centroid of a set, using 1280-d embeddings."""
    if not payload.track_ids:
        return {"tracks": []}
    cfg = _cfg()
    _require_configured(cfg)
    emb = _load_embeddings(cfg)
    if not _NUMPY_OK or emb["matrix"] is None:
        return {"tracks": [], "note": "embeddings not loaded — run pipeline_01 first"}
    path_idx = emb["path_idx"]
    set_vecs = [emb["matrix"][path_idx[tid]] for tid in payload.track_ids if tid in path_idx]
    if not set_vecs:
        return {"tracks": [], "note": "no embeddings found for set tracks"}
    centroid = np.mean(set_vecs, axis=0)
    norm = float(np.linalg.norm(centroid))
    if norm > 1e-9:
        centroid = centroid / norm
    sims = emb["matrix"] @ centroid
    exclude = set(payload.track_ids)
    top_paths, top_sims = [], []
    for i in np.argsort(sims)[::-1]:
        path = emb["paths"][i]
        if path not in exclude:
            top_paths.append(path)
            top_sims.append(float(sims[i]))
        if len(top_paths) >= payload.n:
            break
    if not top_paths:
        return {"tracks": []}
    music_db = cfg.paths.music_db
    cluster_lookup, _ = _load_clusters_json(cfg)
    tags_lookup = _load_tags_json(cfg)
    ledger = _load_ledger(cfg)
    classifier = _build_tag_classifier(cfg.raw)
    conn = sqlite3.connect(str(music_db))
    conn.row_factory = sqlite3.Row
    try:
        placeholders = ",".join("?" * len(top_paths))
        rows = conn.execute(
            f"SELECT * FROM tracks WHERE path IN ({placeholders})", top_paths
        ).fetchall()
    finally:
        conn.close()
    row_by_path = {dict(r)["path"]: r for r in rows}
    sim_by_path = dict(zip(top_paths, top_sims))
    result = []
    for path in top_paths:
        if path in row_by_path:
            t = _row_to_track(row_by_path[path], cluster_lookup, tags_lookup, ledger, classifier)
            t["similarity"] = round(sim_by_path[path], 4)
            result.append(t)
    return {"tracks": result}


# ── slskd management ─────────────────────────────────────────────────────────

def _slskd_running(url: str) -> bool:
    import urllib.request, urllib.error
    try:
        urllib.request.urlopen(url + "/api/v0/application", timeout=5)
        return True
    except urllib.error.HTTPError:
        return True   # server responded (e.g. 401 auth required) → it's running
    except Exception:
        return False


def _find_slskd_zip(slskd_dir: Path) -> Optional[Path]:
    matches = sorted(slskd_dir.glob("slskd-*-win-x64.zip"))
    return matches[-1] if matches else None


def _extract_slskd_zip(zip_path: Path, dest_dir: Path):
    import zipfile
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(dest_dir)


def _find_slskd_config_path(cfg) -> Optional[Path]:
    """Locate slskd.yml via explicit config, then %LOCALAPPDATA%, then glob."""
    explicit = cfg.raw.get("slskd", {}).get("config_file", "")
    if explicit:
        p = to_path(explicit)
        if p and p.exists():
            return p

    # Auto-detect via cmd.exe %LOCALAPPDATA%
    try:
        r = _subproc.run(["cmd.exe", "/c", "echo %LOCALAPPDATA%"],
                         capture_output=True, text=True, timeout=5)
        appdata = r.stdout.strip()
        if appdata and not appdata.startswith("%"):
            r2 = _subproc.run(["wslpath", appdata],
                              capture_output=True, text=True, timeout=5)
            wsl = r2.stdout.strip()
            if wsl:
                p = Path(wsl) / "slskd" / "slskd.yml"
                p.parent.mkdir(parents=True, exist_ok=True)
                return p
    except Exception:
        pass

    # Glob fallback
    try:
        matches = list(Path("/mnt/c/Users").glob("*/AppData/Local/slskd/slskd.yml"))
        if matches:
            return matches[0]
    except Exception:
        pass

    return None


def _write_slskd_yml(config_path: Path, cfg):
    """Write slskd.yml with pipeline-managed settings. Backs up any existing file."""
    port        = int(cfg.raw.get("slskd", {}).get("port", 5030) or 5030)
    username    = cfg.secrets.get("slskd", {}).get("username", "")
    password    = cfg.secrets.get("slskd", {}).get("password", "")
    downloads_w = to_win(cfg.paths.soulseek)
    share_dirs  = cfg.raw.get("slskd", {}).get("share_dirs", [])

    # incomplete dir — slskd's own staging area under its AppData folder
    incomplete_w = ""
    try:
        r = _subproc.run(["cmd.exe", "/c", "echo %LOCALAPPDATA%"],
                         capture_output=True, text=True, timeout=5)
        appdata = r.stdout.strip()
        if appdata and not appdata.startswith("%"):
            incomplete_w = appdata.rstrip("\\") + "\\slskd\\incomplete"
    except Exception:
        pass

    lines = ["---", "directories:"]
    lines.append(f"  downloads: '{downloads_w}'")
    if incomplete_w:
        lines.append(f"  incomplete: '{incomplete_w}'")

    if share_dirs:
        lines += ["shares:", "  directories:"]
        for d in share_dirs:
            lines.append(f"    - '{d}'")

    lines += [
        "web:",
        f"  port: {port}",
        "  authentication:",
        "    disabled: true",
        "logger:",
        "  disk: true",
    ]

    if username or password:
        lines.append("soulseek:")
        if username:
            lines.append(f"  username: {username}")
        if password:
            lines.append(f"  password: {password}")

    content = "\n".join(lines) + "\n"

    if config_path.exists():
        shutil.copy2(str(config_path), str(config_path.with_suffix(".yml.bak")))

    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(content, encoding="utf-8")


@app.get("/api/open-slskd-config")
def open_slskd_config():
    cfg = _cfg()
    _require_configured(cfg)
    p = _find_slskd_config_path(cfg)
    if not p or not p.exists():
        raise HTTPException(404, "slskd.yml not found")
    _subproc.Popen(["notepad.exe", str(p)])
    return {"opened": str(p)}


@app.get("/api/slskd/status")
def slskd_status():
    cfg = _cfg()
    url = cfg.raw.get("slskd", {}).get("url", "http://127.0.0.1:5030")
    exe = cfg.paths.slskd / "slskd.exe"
    return {
        "running":     _slskd_running(url),
        "url":         url,
        "browser_url": url.replace("127.0.0.1", "localhost"),
        "exe_present": exe.exists(),
    }


@app.post("/api/slskd/launch")
def slskd_launch():
    cfg  = _cfg()
    url  = cfg.raw.get("slskd", {}).get("url", "http://127.0.0.1:5030")
    exe  = cfg.paths.slskd / "slskd.exe"

    if not exe.exists():
        zip_path = _find_slskd_zip(cfg.paths.slskd)
        if zip_path:
            _extract_slskd_zip(zip_path, cfg.paths.slskd)
        if not exe.exists():
            raise HTTPException(404,
                f"slskd.exe not found at {to_win(exe)}. "
                "Place slskd.exe in the slskd/ folder inside your library root.")

    if _slskd_running(url):
        return {"ok": True, "already_running": True, "url": url}

    # Write / update slskd.yml
    cfg_path = _find_slskd_config_path(cfg)
    if cfg_path is None:
        raise HTTPException(500,
            "Could not locate slskd config directory. "
            "Set slskd.config_file in config.json to the full path of slskd.yml.")
    _write_slskd_yml(cfg_path, cfg)

    # Launch via PowerShell Start-Process so the exe runs as a proper detached
    # Windows process (direct Popen of a .exe from WSL doesn't go through the
    # Windows process tree correctly).
    slskd_win = to_win(exe)
    _subproc.Popen(
        ["powershell.exe", "-Command", f"Start-Process -FilePath '{slskd_win}'"],
        stdout=_subproc.DEVNULL, stderr=_subproc.DEVNULL,
        stdin=_subproc.DEVNULL,
    )

    browser_url = url.replace("127.0.0.1", "localhost")
    return {"ok": True, "already_running": False, "url": url,
            "browser_url": browser_url, "config": str(cfg_path)}


@app.post("/api/slskd/stop")
def slskd_stop():
    try:
        _subproc.run(
            ["taskkill.exe", "/IM", "slskd.exe", "/F"],
            stdout=_subproc.DEVNULL, stderr=_subproc.DEVNULL, timeout=10,
        )
    except Exception:
        pass
    return {"ok": True}


def _sp_token_cache_path(cfg) -> Path:
    return Path(cfg.spotify.token_cache)

def _sp_read_token(cfg):
    """Read the cached spotipy token JSON, or None if missing/invalid."""
    import json as _json
    p = _sp_token_cache_path(cfg)
    try:
        return _json.loads(p.read_text()) if p.exists() else None
    except Exception:
        return None

def _sp_write_token(cfg, token_info: dict):
    import json as _json
    p = _sp_token_cache_path(cfg)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(_json.dumps(token_info))

def _spotify_fetch_live():
    import json as _json
    cfg    = _cfg()
    py     = _pipeline_python(cfg)
    script = _REPO / "_sp_list_playlists.py"
    cache  = _REPO / ".cache" / "spotify_playlists.json"
    if not script.exists():
        raise HTTPException(404, "_sp_list_playlists.py not found")
    try:
        r = _subproc.run([py, str(script)], capture_output=True, text=True,
                         timeout=30, cwd=str(_REPO))
        if r.returncode != 0:
            # Script exits 1 with JSON error on stderr when no token
            try:
                err = _json.loads(r.stderr)
                if err.get("error") == "no_token":
                    raise HTTPException(401, "no_token")
            except (ValueError, KeyError):
                pass
            raise HTTPException(500, f"Spotify fetch failed: {r.stderr[:300]}")
        data = _json.loads(r.stdout)
        cache.parent.mkdir(exist_ok=True)
        cache.write_text(_json.dumps(data))
        return {"playlists": data, "cached": False}
    except _subproc.TimeoutExpired:
        raise HTTPException(504, "Spotify fetch timed out (30s)")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/spotify/playlists")
def spotify_playlists():
    import json as _json
    cache = _REPO / ".cache" / "spotify_playlists.json"
    if cache.exists():
        return {"playlists": _json.loads(cache.read_text()), "cached": True}
    return _spotify_fetch_live()


@app.post("/api/spotify/playlists/refresh")
def spotify_playlists_refresh():
    return _spotify_fetch_live()


@app.get("/api/spotify/tag-playlists")
def spotify_tag_playlists():
    """Return the playlist names that pipeline_00 always downloads (from config tag_playlists)."""
    try:
        return {"names": _cfg().tag_playlists()}
    except Exception:
        return {"names": []}


@app.get("/api/spotify/auth-url")
def spotify_auth_url():
    """Return the Spotify authorization URL — pure stdlib, no spotipy import needed."""
    from urllib.parse import urlencode
    cfg = _cfg()
    try:
        secrets      = cfg.secrets.get("spotify", {})
        client_id    = secrets.get("client_id", "")
        redirect_uri = cfg.spotify.redirect_uri
        scope        = cfg.spotify.scopes
        if not client_id:
            raise HTTPException(400, "Spotify client_id not configured — open Settings first")
        url = "https://accounts.spotify.com/authorize?" + urlencode({
            "client_id":     client_id,
            "response_type": "code",
            "redirect_uri":  redirect_uri,
            "scope":         scope,
        })
        return {"url": url, "redirect_uri": redirect_uri}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))


class _SpExchangePayload(BaseModel):
    url: str  # the full redirect URL the user pastes from their browser

@app.post("/api/spotify/exchange-code")
def spotify_exchange_code(payload: _SpExchangePayload):
    """Extract the OAuth code from a pasted redirect URL and exchange for a token.
    Pure stdlib — spotipy not imported in the server process."""
    import base64, time as _time, json as _json
    import urllib.request, urllib.parse, urllib.error
    from urllib.parse import urlparse, parse_qs

    qs    = parse_qs(urlparse(payload.url).query)
    codes = qs.get("code")
    if not codes:
        raise HTTPException(400, "No 'code' in URL — paste the full redirect URL including ?code=…")

    cfg           = _cfg()
    secrets       = cfg.secrets.get("spotify", {})
    client_id     = secrets.get("client_id", "")
    client_secret = secrets.get("client_secret", "")
    redirect_uri  = cfg.spotify.redirect_uri

    auth_header = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    body = urllib.parse.urlencode({
        "grant_type":   "authorization_code",
        "code":         codes[0],
        "redirect_uri": redirect_uri,
    }).encode()
    req = urllib.request.Request(
        "https://accounts.spotify.com/api/token",
        data=body,
        headers={
            "Authorization": f"Basic {auth_header}",
            "Content-Type":  "application/x-www-form-urlencoded",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            td = _json.loads(resp.read())
    except urllib.error.HTTPError as e:
        raise HTTPException(400, f"Spotify rejected the code: {e.read().decode()[:200]}")
    except Exception as e:
        raise HTTPException(500, f"Token exchange error: {e}")

    # Write token in spotipy's cache format so _sp_list_playlists.py can read it
    token_info = {
        "access_token":  td["access_token"],
        "token_type":    td.get("token_type", "Bearer"),
        "expires_in":    td.get("expires_in", 3600),
        "refresh_token": td.get("refresh_token", ""),
        "scope":         td.get("scope", ""),
        "expires_at":    int(_time.time()) + td.get("expires_in", 3600),
    }
    try:
        _sp_write_token(cfg, token_info)
    except Exception as e:
        raise HTTPException(500, f"Could not save token: {e}")

    # Kick off background playlist cache build
    cache = _REPO / ".cache" / "spotify_playlists.json"
    if not cache.exists():
        threading.Thread(target=_spotify_fetch_live_bg, daemon=True).start()
    return {"ok": True}


def _spotify_fetch_live_bg():
    """Background precompute — called after OAuth completes and on startup."""
    try:
        _spotify_fetch_live()
    except Exception:
        pass


@app.get("/api/spotify/status")
def spotify_status():
    """Check if a Spotify token is cached — reads the token file directly, no spotipy needed."""
    try:
        token = _sp_read_token(_cfg())
        return {"authenticated": bool(token and (token.get("refresh_token") or token.get("access_token")))}
    except Exception:
        return {"authenticated": False}


# ── Pipeline run ─────────────────────────────────────────────────────────────
_REPO = Path(__file__).resolve().parent


_pipeline_python_cache: Optional[str] = None

def _pipeline_python(cfg) -> str:
    """Find the Python that has all pipeline deps (numpy, essentia, etc.).
    The server runs in a lightweight venv (fastapi only); pipeline scripts need
    the heavier pipeline venv. Checks explicit config first, then well-known
    locations. Tests each candidate for numpy before accepting it so we never
    silently use the server venv. Result is cached for the server lifetime."""
    global _pipeline_python_cache
    import subprocess as _sp

    def _has_numpy(exe: str) -> bool:
        try:
            r = _sp.run([exe, "-c", "import numpy"], capture_output=True, timeout=5)
            return r.returncode == 0
        except Exception:
            return False

    explicit = cfg.raw.get("pipeline_python", "")
    if explicit:
        p = Path(explicit)
        if p.exists():
            return str(p)

    if _pipeline_python_cache:
        return _pipeline_python_cache

    candidates = [
        Path.home() / ".local" / "share" / "stacks-venvs" / "pipeline" / "bin" / "python3",
        Path.home() / "music_env" / "bin" / "python3",
        Path.home() / "music_env" / "bin" / "python",
        Path.home() / "miniconda3" / "bin" / "python3",
        Path.home() / "anaconda3" / "bin" / "python3",
        Path.home() / ".pyenv" / "shims" / "python3",
        Path("/usr/bin/python3"),
        Path("/usr/local/bin/python3"),
    ]
    for candidate in candidates:
        if candidate.exists() and _has_numpy(str(candidate)):
            _pipeline_python_cache = str(candidate)
            return _pipeline_python_cache
    return sys.executable

_STAGE_SCRIPTS = {
    "download": "pipeline_00_download.py",
    "embed":    "pipeline_01_embed.py",
    "cluster":  "pipeline_02_cluster.py",
    "tag":      "pipeline_03_tag.py",
    "sp2slsk":  "sp2slsk.py",
}
_STAGE_ID_MAP = {
    "p00": "download", "p01": "embed", "p02": "cluster", "p03": "tag",
    "sp2slsk": "sp2slsk",
}

_pipeline_lock = threading.Lock()
_pipeline_state = {
    "running":   False,
    "stage":     None,
    "log":       [],        # list of str, grows per run, cleared on new run start
    "exit_code": None,
    "log_file":  "",
}


class _RunPayload(BaseModel):
    stage: str
    args: List[str] = []


def _append_run_record(stage: str, exit_code: int, log_file: str = ""):
    try:
        cfg = _cfg()
        p = cfg.paths.db / "runs.jsonl"
        rec = {"ts": datetime.now().isoformat(timespec="seconds"),
               "stage": stage, "exit_code": exit_code, "ok": exit_code == 0,
               "log_file": log_file}
        with open(p, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec) + "\n")
    except Exception:
        pass


@app.post("/api/pipeline/run")
def pipeline_run(payload: _RunPayload):
    cfg = _cfg()
    stage_name = _STAGE_ID_MAP.get(payload.stage, payload.stage)
    script = _STAGE_SCRIPTS.get(stage_name)
    if not script:
        raise HTTPException(400, f"unknown stage '{payload.stage}'")
    script_path = _REPO / script
    if not script_path.exists():
        raise HTTPException(404, f"script not found: {script}")
    with _pipeline_lock:
        if _pipeline_state["running"]:
            raise HTTPException(409, "a stage is already running")
        _pipeline_state["running"]   = True
        _pipeline_state["stage"]     = stage_name
        _pipeline_state["log"]       = []
        _pipeline_state["exit_code"] = None
        _pipeline_state["log_file"]  = ""

    py  = _pipeline_python(cfg)
    cmd = [py, str(script_path), "--no-interactive"] + payload.args

    def _reader():
        try:
            proc = _subproc.Popen(
                cmd, cwd=str(_REPO),
                stdout=_subproc.PIPE, stderr=_subproc.STDOUT,
                stdin=_subproc.DEVNULL, text=True, bufsize=1,
            )
            for line in proc.stdout:
                with _pipeline_lock:
                    _pipeline_state["log"].append(line.rstrip())
            proc.wait()
            rc = proc.returncode
        except Exception as e:
            with _pipeline_lock:
                _pipeline_state["log"].append(f"[server] error launching stage: {e}")
            rc = -1
        log_fname = ""
        try:
            _log_dir = _cfg().paths.logs
            _log_dir.mkdir(parents=True, exist_ok=True)
            safe_ts = datetime.now().strftime("%Y%m%dT%H%M%S")
            log_fname = f"{safe_ts}_{stage_name}.log"
            with _pipeline_lock:
                _lines = list(_pipeline_state["log"])
            (_log_dir / log_fname).write_text("\n".join(_lines), encoding="utf-8")
            retention = getattr(_cfg().logging, "log_retention_sessions", 10)
            existing = sorted(_log_dir.glob(f"*_{stage_name}.log"), key=lambda p: p.stat().st_mtime)
            for old in existing[:-retention]:
                try: old.unlink()
                except Exception: pass
        except Exception:
            pass
        _append_run_record(stage_name, rc, log_fname)
        # Signal done AFTER log is persisted so SSE carries the correct log_file
        with _pipeline_lock:
            _pipeline_state["log_file"]  = log_fname
            _pipeline_state["running"]   = False
            _pipeline_state["exit_code"] = rc

    threading.Thread(target=_reader, daemon=True).start()
    return {"ok": True, "stage": stage_name}


@app.post("/api/pipeline/stop")
def pipeline_stop():
    with _pipeline_lock:
        if not _pipeline_state["running"]:
            return {"ok": True, "note": "nothing running"}
    return {"ok": False, "note": "stop not yet implemented — kill the server to abort"}


@app.get("/api/pipeline/status")
def pipeline_status():
    cfg = _cfg()
    with _pipeline_lock:
        return {
            "running":         _pipeline_state["running"],
            "stage":           _pipeline_state["stage"],
            "exit_code":       _pipeline_state["exit_code"],
            "log_lines":       len(_pipeline_state["log"]),
            "pipeline_python": _pipeline_python(cfg),
        }


@app.get("/api/pipeline/events")
def pipeline_events():
    def _generate():
        seen = 0
        while True:
            with _pipeline_lock:
                log      = _pipeline_state["log"]
                running  = _pipeline_state["running"]
                rc       = _pipeline_state["exit_code"]
                log_file = _pipeline_state.get("log_file", "")
            while seen < len(log):
                yield f"data: {json.dumps({'line': log[seen]})}\n\n"
                seen += 1
            if not running:
                yield f"data: {json.dumps({'done': True, 'exit_code': rc, 'log_file': log_file})}\n\n"
                return
            time.sleep(0.15)

    return StreamingResponse(
        _generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/api/slskd/downloads")
def get_slskd_downloads():
    """Proxy slskd's transfer list. Returns empty list when slskd is offline."""
    cfg = _cfg()
    url = (cfg.raw.get("slskd") or {}).get("url", "http://127.0.0.1:5030")
    import urllib.request as _ur
    import urllib.error as _ue
    try:
        with _ur.urlopen(url + "/api/v0/transfers/downloads", timeout=3) as r:
            data = json.loads(r.read())
        transfers = []
        for user in data:
            for d in user.get("directories", []):
                for f in d.get("files", []):
                    fname = f.get("filename", "")
                    transfers.append({
                        "filename": fname.split("/")[-1].split("\\")[-1],
                        "state": f.get("state", ""),
                        "bytes_transferred": f.get("bytesTransferred", 0),
                        "size": f.get("size", 0),
                        "username": user.get("username", ""),
                    })
        return {"transfers": transfers, "available": True}
    except _ue.HTTPError as e:
        return {"transfers": [], "available": True, "note": f"HTTP {e.code}"}
    except Exception as e:
        return {"transfers": [], "available": False, "note": str(e)}


@app.get("/api/open-playlists-folder")
def open_playlists_folder():
    cfg = _cfg()
    path = cfg.paths.playlists
    if not path or not path.exists():
        return {"ok": False, "error": "playlists directory does not exist"}
    try:
        r = _subproc.run(["wslpath", "-w", str(path)], capture_output=True, text=True, timeout=5)
        win = r.stdout.strip() if r.returncode == 0 else str(path)
        _subproc.Popen(
            ["powershell.exe", "-Command", f"Start-Process -FilePath 'explorer.exe' -ArgumentList '{win}'"],
            stdout=_subproc.DEVNULL, stderr=_subproc.DEVNULL, stdin=_subproc.DEVNULL,
        )
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


class _ExportM3u8Payload(BaseModel):
    name: str
    track_ids: list

@app.post("/api/export-m3u8")
def export_m3u8(payload: _ExportM3u8Payload):
    cfg = _cfg()
    _require_configured(cfg)
    playlist_dir = cfg.paths.playlists / "exports" if cfg.paths.playlists else None
    if not playlist_dir:
        return {"ok": False, "error": "playlists path not configured"}
    playlist_dir.mkdir(parents=True, exist_ok=True)
    music_db = cfg.paths.music_db
    if not music_db or not music_db.exists():
        return {"ok": False, "error": "music.db not found"}
    conn = sqlite3.connect(str(music_db))
    conn.row_factory = sqlite3.Row
    try:
        rows = {r["path"]: r for r in conn.execute("SELECT path, artist, title FROM tracks").fetchall()}
    finally:
        conn.close()
    lines = ["#EXTM3U"]
    for tid in payload.track_ids:
        row = rows.get(tid)
        if row:
            label = f"{row['artist'] or ''} - {row['title'] or ''}".strip(" -")
        else:
            label = str(tid)
        lines.append(f"#EXTINF:-1,{label}")
        lines.append(to_win(tid))
    safe = payload.name.replace("/", "-").replace("\\", "-").replace(":", "-").strip()
    dst = playlist_dir / f"{safe}.m3u8"
    tmp = dst.with_suffix(".tmp")
    tmp.write_text("\n".join(lines) + "\n", encoding="utf-8")
    tmp.replace(dst)
    return {"ok": True, "path": to_win(str(dst))}


class _LedgerUpdatePayload(BaseModel):
    path: str
    tags: list  # flat list of tag names to store as spot[]

@app.post("/api/ledger/update-spot")
def update_ledger_spot(payload: _LedgerUpdatePayload):
    import datetime as _dt
    cfg = _cfg()
    _require_configured(cfg)
    ledger_path = cfg.paths.ledger
    if not ledger_path:
        return {"ok": False, "error": "ledger path not configured"}
    led = _load_ledger(cfg)
    key = to_key(payload.path)
    if key not in led:
        led[key] = {"path": payload.path}
    led[key]["spot"] = sorted(payload.tags)
    led[key]["last_sync"] = _dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    ledger_path.write_text(json.dumps(led, ensure_ascii=False, indent=2), encoding="utf-8")
    _ledger_cache["mtime"] = None  # invalidate cache
    return {"ok": True}


@app.get("/api/exports")
def list_exports():
    cfg = _cfg()
    exports_dir = (cfg.paths.playlists / "exports") if cfg.paths.playlists else None
    if not exports_dir or not exports_dir.exists():
        return {"files": []}
    files = sorted(exports_dir.glob("*.m3u8"), key=lambda p: p.stat().st_mtime, reverse=True)
    return {"files": [{"name": p.stem, "filename": p.name, "mtime": p.stat().st_mtime} for p in files]}

@app.get("/api/exports/{filename}")
def get_export(filename: str):
    cfg = _cfg()
    exports_dir = (cfg.paths.playlists / "exports") if cfg.paths.playlists else None
    if not exports_dir:
        raise HTTPException(404, "exports dir not configured")
    p = (exports_dir / filename).resolve()
    if exports_dir.resolve() not in p.parents or p.suffix != ".m3u8" or not p.exists():
        raise HTTPException(404, "file not found")
    return PlainTextResponse(p.read_text(encoding="utf-8"))


@app.get("/api/cluster-methods")
def get_cluster_methods():
    cfg = _cfg()
    if not cfg.paths.playlists:
        return {"methods": {}}
    p = cfg.paths.playlists.parent / "db" / "cluster_methods.json"
    if not p.exists():
        return {"methods": {}}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {"methods": {}}


@app.get("/api/pipeline/runs")
def get_pipeline_runs(limit: int = 30):
    """Return recent pipeline run records from runs.jsonl."""
    cfg = _cfg()
    if not cfg.paths.db:
        return {"runs": []}
    p = cfg.paths.db / "runs.jsonl"
    if not p.exists():
        return {"runs": []}
    runs = []
    try:
        for line in p.read_text(encoding="utf-8").splitlines():
            if line.strip():
                runs.append(json.loads(line))
    except Exception:
        return {"runs": []}
    return {"runs": runs[-limit:]}


@app.get("/api/pipeline/logs/{log_file:path}")
def get_pipeline_log(log_file: str):
    cfg = _cfg()
    p = cfg.paths.logs / log_file
    if not p.exists() or not p.is_file():
        raise HTTPException(404, "log not found")
    return {"content": p.read_text(encoding="utf-8")}


class _CombinePayload(BaseModel):
    log_files: List[str] = []


@app.post("/api/pipeline/combine-logs")
def combine_pipeline_logs(payload: _CombinePayload):
    """Merge per-stage log files into a single full-pipeline log after a complete run."""
    cfg = _cfg()
    log_dir = cfg.paths.logs
    parts = []
    for lf in payload.log_files:
        p = log_dir / lf
        if not p.exists():
            continue
        # e.g. "20260618T123456_download.log" → "DOWNLOAD"
        label = lf.split("_", 1)[-1].replace(".log", "").upper() if "_" in lf else lf.upper()
        parts.append(f"=== {label} ===\n" + p.read_text(encoding="utf-8", errors="replace").rstrip())
    if not parts:
        raise HTTPException(400, "no valid log files found")
    combined = "\n\n".join(parts)
    safe_ts = datetime.now().strftime("%Y%m%dT%H%M%S")
    out_file = f"{safe_ts}_full_pipeline.log"
    (log_dir / out_file).write_text(combined, encoding="utf-8")
    _append_run_record("Full Pipeline", 0, out_file)
    return {"log_file": out_file}


@app.get("/api/library/stats")
def get_library_stats():
    """Aggregate counts from the music DB."""
    cfg = _cfg()
    _require_configured(cfg)
    music_db = cfg.paths.music_db
    if not music_db or not music_db.exists():
        return {}
    con = sqlite3.connect(str(music_db))
    try:
        total = con.execute("SELECT COUNT(*) FROM tracks").fetchone()[0]
        recent = con.execute(
            "SELECT COUNT(*) FROM tracks WHERE date_added >= datetime('now','-7 days')"
        ).fetchone()[0]
        ext_rows = con.execute(
            "SELECT extension, COUNT(*) FROM tracks GROUP BY extension ORDER BY COUNT(*) DESC"
        ).fetchall()
        rated = con.execute(
            "SELECT COUNT(*) FROM tracks WHERE rating > 0"
        ).fetchone()[0]
    finally:
        con.close()
    _, clusters = _load_clusters_json(cfg)
    return {
        "total": total,
        "recent_7d": recent,
        "rated": rated,
        "by_extension": {r[0]: r[1] for r in ext_rows if r[0]},
        "cluster_count": sum(1 for c in clusters if c.get("id") != -1),
    }


# Mounted LAST so it never shadows /api/* routes above. If web/ is missing
# (e.g. a copy of the project that didn't include it), don't let that take
# the whole server down -- /api/* should still work for debugging.
if WEB_DIR.exists() and any(WEB_DIR.iterdir()):
    app.mount("/", StaticFiles(directory=str(WEB_DIR), html=True), name="web")
else:
    @app.get("/")
    def _missing_web():
        return PlainTextResponse(
            f"No frontend found at {WEB_DIR}.\n"
            "Copy index.html into a 'web' folder right next to app.py, then restart.",
            status_code=500,
        )
