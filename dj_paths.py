#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
dj_paths.py
===========
THE single source of truth for path handling across every pipeline script.

Three physical forms exist and can't be collapsed into one string:
  - POSIX  /mnt/e/Music Library/audio/x.mp3   ← WSL needs this to open files
  - WIN    E:\\Music Library\\audio\\x.mp3      ← Rekordbox / M3U need this
  - KEY    e:/music library/audio/x.mp3       ← the ledger's canonical identity

Every other script used to re-implement these conversions slightly differently
(_norm, _root, _norm_path, inline win-conversion) — and they disagreed, which is
what caused ghosts, orphans, and dedup misses. From now on:

    from dj_paths import to_key, to_posix, to_path, to_win, db_variants

and never write a path conversion by hand again. All functions are idempotent and
accept any of the three forms as input.

  to_key(p)   -> canonical ledger KEY (lowercased, drive-letter, forward slashes)
  to_posix(p) -> POSIX string for the filesystem (case preserved)
  to_path(p)  -> pathlib.Path that resolves on this filesystem (== Path(to_posix(p)))
  to_win(p)   -> Windows string for Rekordbox / M3U (case preserved)
  db_variants(p) -> every string a DB `path` column might hold, for matching
                    during the migration period (deletes/queries try all of them)
"""
from pathlib import Path
import json, os


def to_key(p) -> str:
    """Canonical ledger key. Lowercased so lookups are case-insensitive.
       /mnt/e/Music Library/x.mp3 -> e:/music library/x.mp3
       E:\\Music Library\\x.mp3    -> e:/music library/x.mp3  (idempotent)"""
    s = str(p).replace("\\", "/")
    if s.startswith("/mnt/") and len(s) > 6:
        parts = s.split("/", 3)
        s = parts[2] + ":/" + (parts[3] if len(parts) > 3 else "")
    return s.lower()


def to_posix(p) -> str:
    """Filesystem form for WSL. Case PRESERVED (/mnt is case-sensitive).
       e:/x -> /mnt/e/x ;  E:\\x -> /mnt/e/x ;  /mnt/e/x -> unchanged"""
    s = str(p).replace("\\", "/")
    if len(s) > 1 and s[1] == ":":
        s = "/mnt/" + s[0].lower() + s[2:]
    return s


def to_path(p) -> Path:
    """A Path that actually resolves on this (WSL) filesystem."""
    return Path(to_posix(p))


def to_win(p) -> str:
    """Windows form for Rekordbox / M3U. Case PRESERVED.
       /mnt/e/x -> E:\\x ;  e:/x -> E:\\x ;  E:\\x -> unchanged"""
    s = str(p).replace("\\", "/")
    if s.startswith("/mnt/") and len(s) > 6:
        s = s[5].upper() + ":" + s[6:]
    elif len(s) > 1 and s[1] == ":":
        s = s[0].upper() + s[1:]
    return s.replace("/", "\\")


def db_variants(p) -> list:
    """Every string form a DB row's `path` column might use, de-duped, ordered.
       Use during the transition while the DB may hold mixed conventions."""
    posix = to_posix(p)
    win_b = to_win(p)
    win_f = win_b.replace("\\", "/")
    out, seen = [], set()
    for v in (posix, win_b, win_f, str(p)):
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def atomic_write_json(path, obj) -> None:
    """Crash-safe ledger/JSON write. Writes a temp file IN THE SAME DIRECTORY as
    the target, fsyncs, then os.replace()s it over the target — an atomic,
    same-filesystem rename. No cross-directory shutil.move, no copy fallback.
    A kill mid-write leaves the previous file intact."""
    p = to_path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.parent / (p.name + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, p)


# Quick self-check: python3 dj_paths.py
if __name__ == "__main__":
    samples = [
        r"/mnt/e/Music Library/audio/Artist - Title.mp3",
        r"E:\Music Library\audio\Artist - Title.mp3",
        r"e:/music library/audio/artist - title.mp3",
    ]
    for s in samples:
        print(f"\nINPUT  {s}")
        print(f"  key   {to_key(s)}")
        print(f"  posix {to_posix(s)}")
        print(f"  win   {to_win(s)}")
    # idempotency
    k = to_key(samples[0])
    assert to_key(to_posix(k)) == k == to_key(to_win(k)), "NOT idempotent!"
    print("\n✓ idempotent across all forms")
