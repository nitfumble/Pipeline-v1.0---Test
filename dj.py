#!/usr/bin/env python3
"""
dj — orchestrator for the music-library pipeline.

Subcommands:
  dj run [stage]   Run all stages in order, or just one. Pass-through args after
                   the stage go to that stage (e.g. `dj run tag --dry-run`).
                   --plan       Preview only (dry-run the stages that support it).
                   --from STAGE Resume the run starting at STAGE.
                   --keep-going Don't stop the run if a stage fails.
  dj status        Read-only health: ledger, queues, last sync, recent runs.
  dj config        Show the active config (--validate to validate instead).
  dj setup         First-time setup wizard.

Stages are executed as subprocesses (each pipeline is a standalone CLI), so a
failure in one stage can never corrupt the orchestrator. Every non-dry run is
appended to db/runs.jsonl as {ts, stage, duration_s, exit_code, ok}.
"""
import sys
import json
import time
import subprocess
import argparse
from datetime import datetime
from pathlib import Path

from config import load

REPO = Path(__file__).resolve().parent

# ── Stage DAG (ordered) ───────────────────────────────────────────────────────
# dry_flag: the closest thing each stage has to a no-write preview. Only `tag`
# has a true --dry-run today; others note that they have no preview yet.
STAGES = [
    {"name": "download", "script": "pipeline_00_download.py", "dry_flag": None},
    {"name": "embed",    "script": "pipeline_01_embed.py",    "dry_flag": None},
    {"name": "cluster",  "script": "pipeline_02_cluster.py",  "dry_flag": None},
    {"name": "tag",      "script": "pipeline_03_tag.py",      "dry_flag": "--dry-run"},
]
STAGE_NAMES = [s["name"] for s in STAGES]


def _runs_path(cfg):
    return cfg.paths.db / "runs.jsonl"


def _append_run(cfg, record: dict):
    p = _runs_path(cfg)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")


def _run_stage(cfg, stage: dict, extra=None, dry=False) -> bool:
    script = REPO / stage["script"]
    if not script.exists():
        print(f"  ✗ missing stage script: {script}")
        return False
    cmd = [sys.executable, str(script)]
    if dry and stage["dry_flag"]:
        cmd.append(stage["dry_flag"])
    if extra:
        cmd += extra

    print(f"\n{'=' * 64}\n▶  {stage['name']:<9} {' '.join(cmd[1:]) or '(no args)'}\n{'=' * 64}")
    t0 = time.time()
    proc = subprocess.run(cmd, cwd=str(REPO))
    dur = round(time.time() - t0, 1)
    ok = proc.returncode == 0

    if not dry:
        _append_run(cfg, {
            "ts": datetime.now().isoformat(timespec="seconds"),
            "stage": stage["name"],
            "duration_s": dur,
            "exit_code": proc.returncode,
            "ok": ok,
        })
    print(f"{'✓' if ok else '✗'}  {stage['name']} finished in {dur}s (exit {proc.returncode})")
    return ok


def _select(stage_arg, from_stage):
    if stage_arg:
        if stage_arg not in STAGE_NAMES:
            raise SystemExit(f"unknown stage '{stage_arg}'. choices: {', '.join(STAGE_NAMES)}")
        return [s for s in STAGES if s["name"] == stage_arg]
    if from_stage:
        if from_stage not in STAGE_NAMES:
            raise SystemExit(f"unknown stage '{from_stage}'. choices: {', '.join(STAGE_NAMES)}")
        return STAGES[STAGE_NAMES.index(from_stage):]
    return list(STAGES)


def cmd_run(cfg, args, extra):
    selected = _select(args.stage, args.from_stage)

    if args.plan:
        print("── Plan (dry-run preview; no mutations) ──")
        for s in selected:
            if s["dry_flag"]:
                _run_stage(cfg, s, dry=True)
            else:
                print(f"  • {s['name']:<9} would run  (no dry-run preview yet)")
        return

    for s in selected:
        ok = _run_stage(cfg, s, extra=extra)
        if not ok and not args.keep_going:
            raise SystemExit(
                f"\n✗ Stage '{s['name']}' failed — stopping.\n"
                f"  Fix it, then resume with:  dj run --from {s['name']}"
            )
    print("\n✓ Pipeline complete.")


def _count_json(path):
    try:
        with open(path, encoding="utf-8") as f:
            return len(json.load(f))
    except FileNotFoundError:
        return None
    except Exception:
        return "unreadable!"


def cmd_status(cfg, args, extra):
    print("=== dj status ===")
    print(f"  library : {cfg.paths.library_root}")
    print(f"  RB sync : {'on' if cfg.rekordbox.enabled else 'off (m3u8 mode)'}")

    n = _count_json(cfg.paths.ledger)
    print(f"  ledger  : {n if n is not None else 'none yet'} entries")

    for q, label in [("download_queue.json", "download queue"),
                     ("slsk_queue.json", "slsk queue")]:
        c = _count_json(cfg.paths.db / q)
        if c is not None:
            print(f"  {label:<14}: {c}")

    rp = _runs_path(cfg)
    if rp.exists():
        rows = [json.loads(l) for l in rp.read_text(encoding="utf-8").splitlines() if l.strip()]
        print("  recent runs:")
        for r in rows[-6:]:
            print(f"    {r['ts']}  {r['stage']:<9} {'✓' if r['ok'] else '✗'}  {r['duration_s']}s")
        last_ok = {r["stage"]: r["ts"] for r in rows if r["ok"]}
        missing = [s for s in STAGE_NAMES if s not in last_ok]
        if missing:
            print(f"  never completed: {', '.join(missing)}")
    else:
        print("  recent runs: none yet")


def cmd_config(cfg, args, extra):
    flag = "--validate" if args.validate else "--show"
    subprocess.run([sys.executable, str(REPO / "config.py"), flag])


def cmd_setup(cfg, args, extra):
    subprocess.run([sys.executable, str(REPO / "config.py"), "--init"])


def build_parser():
    p = argparse.ArgumentParser(prog="dj", description="Music-library pipeline orchestrator")
    sub = p.add_subparsers(dest="cmd", required=True)

    pr = sub.add_parser("run", help="Run all stages, or one (pass-through args after the stage)")
    pr.add_argument("stage", nargs="?", help=f"one of: {', '.join(STAGE_NAMES)} (omit = all)")
    pr.add_argument("--plan", action="store_true", help="Preview only — no mutations")
    pr.add_argument("--from", dest="from_stage", metavar="STAGE", help="Resume from this stage")
    pr.add_argument("--keep-going", action="store_true", help="Continue even if a stage fails")

    pc = sub.add_parser("config", help="Show/validate the active config")
    pc.add_argument("--validate", action="store_true", help="Validate instead of show")

    sub.add_parser("status", help="Read-only health summary")
    sub.add_parser("setup", help="First-time setup wizard")
    return p


def main(argv=None):
    parser = build_parser()
    args, extra = parser.parse_known_args(argv)

    if args.cmd == "setup":           # no config required yet
        return cmd_setup(None, args, extra)

    cfg = load()
    {"run": cmd_run, "status": cmd_status, "config": cmd_config}[args.cmd](cfg, args, extra)


if __name__ == "__main__":
    main()
