#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
rename_back.py
==============
Restore original audio-dir filenames after restore_flacs.py renamed files to
their Soulseek source names.

Parses the replace log produced by restore_flacs.py --replace to extract the
complete source_name → original_name mapping, then renames each current file
back to its original name.

Usage:
    python3 rename_back.py --dry-run   # show what would be renamed, no changes
    python3 rename_back.py --apply     # execute the renames
"""

import argparse
import re
from pathlib import Path

AUDIO_DIR    = Path("/mnt/e/Pipeline v1.0 - Test/audio")
REPLACE_LOG  = Path(
    "/mnt/c/Users/bart_/.claude/projects/e--Pipeline-v1-0---Test"
    "/4869477d-5ec0-4b4a-a390-98fc8b36c8cb/tool-results/bvcgcog0d.txt"
)

# Patterns
RE_OK    = re.compile(r"^\s{2}(?:OK|TAGOK)\s+(.+\.flac?)(?:\s+\(.*\))?$")
RE_RENAME = re.compile(r"^\s+↪ rename → (.+\.flac?)$")


def parse_replace_log(log_path: Path) -> dict[str, str]:
    """Return {source_name: original_name} for every renamed file in the log."""
    mapping: dict[str, str] = {}
    current_original: str | None = None
    for raw in log_path.read_text(encoding="utf-8").splitlines():
        m_ok = RE_OK.match(raw)
        if m_ok:
            current_original = m_ok.group(1).strip()
            continue
        m_rename = RE_RENAME.match(raw)
        if m_rename and current_original:
            source_name = m_rename.group(1).strip()
            mapping[source_name] = current_original
            current_original = None
    return mapping


def main():
    ap = argparse.ArgumentParser()
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--apply",   action="store_true")
    args = ap.parse_args()
    dry_run = args.dry_run

    mapping = parse_replace_log(REPLACE_LOG)
    print(f"Replace log: {len(mapping)} source→original rename pairs found\n")

    original_names = set(mapping.values())

    to_rename: list[tuple[Path, Path]] = []
    already_ok: list[str] = []
    unknown: list[str] = []
    conflicts: list[tuple[str, str]] = []

    for current in sorted(AUDIO_DIR.glob("*.flac")):
        target_name = mapping.get(current.name)
        if target_name is None:
            # Not in rename log — either was never renamed, or came from slsk/slskd
            if current.name in original_names:
                already_ok.append(current.name)
            else:
                unknown.append(current.name)
            continue
        if target_name == current.name:
            already_ok.append(current.name)
            continue
        target = AUDIO_DIR / target_name
        if target.exists() and target != current:
            conflicts.append((current.name, target_name))
            continue
        to_rename.append((current, target))

    for current, target in to_rename:
        action = "WOULD RENAME" if dry_run else "RENAME"
        print(f"  {action}  {current.name}")
        print(f"            → {target.name}")

    if unknown:
        print(f"\nUNKNOWN (not in rename log — may already be correct):")
        for name in unknown:
            print(f"  {name}")

    if conflicts:
        print(f"\nCONFLICT (target already exists, skipped):")
        for src, tgt in conflicts:
            print(f"  {src} → {tgt}")

    print(f"\nSUMMARY ({'dry run' if dry_run else 'applied'})")
    print(f"  to rename:  {len(to_rename)}")
    print(f"  already ok: {len(already_ok)}")
    print(f"  unknown:    {len(unknown)}  (assume already correct or inspect manually)")
    print(f"  conflicts:  {len(conflicts)}")

    if not dry_run:
        for current, target in to_rename:
            current.rename(target)
        print(f"\n{len(to_rename)} files renamed.")


if __name__ == "__main__":
    main()
