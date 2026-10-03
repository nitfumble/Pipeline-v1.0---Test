# STACKS — Claude Code handoff brief

**Read this first, then `stacks-implementation-brief.md`.** That older brief is still
accurate for the API surface (§1), design system (§2), and the mock-removal checklist
(§4) — don't repeat that work, build on it. This document is the *delta*: what the code
actually looks like right now, why it feels broken/slow, and the exact next steps.

You are working **inside the repo** (the folder with `app.py`, `config.py`, `dj.py`,
`web/index.html`). Edit files in place. Verify as you go — see §5 for how, given you
can't run the real pipeline.

---

## 0. Ground truth — current state of the code

- **`web/index.html` is the canonical frontend. `webpage__proof_of_concept_.html` is a
  design reference only — never edit or ship it.** They are ~95% identical: `index.html`
  is the POC plus (a) the setup wizard, (b) a server directory browser, (c) a
  `loadRealData()` that fetches `/api/tracks` + `/api/clusters` on boot.
  - Sanity check before editing: `grep -c "loadRealData" web/index.html` should be ≥1.
    If it's 0, you have the wrong file (the raw POC). Confirm with the user.

- **The app is a hybrid, and that's the "lost features" complaint.** The Tracks page
  reads the real DB. Everything else still runs the mock generators and shows fake data:
  - `purityCache` → `rand(55,92)` (Clusters purity %)
  - `rbLinked` / `spLinked` → `baseRng()<0.6` / `<0.4` (random booleans)
  - `assignTags()` + injected tag-drift staleness (Tag Authority)
  - `setInterval` fakes for queue status, pipeline progress, downloads
  Real-data pages and dice-rolling pages visibly disagree. That's the bug the user feels.

- **Backend `app.py`** implements only the read endpoints: `/api/tracks`,
  `/api/tracks/{id}`, `/api/clusters`, `/api/setup` (GET+POST), `/api/browse`. Everything
  else from `stacks-implementation-brief.md` §1 does not exist yet.

---

## 1. Why Tracks feels slow — two causes, both confirmed in the code

1. **Frontend pulls the whole library and renders every row.** `loadRealData()` calls
   `fetch("/api/tracks")` with **no query params**, so it gets all ~1800 rows.
   `renderTracks()` then builds the entire `<tbody>` as one string and attaches a click
   listener **and** a play listener to every single row.
2. **Backend rebuilds state per request.** `_load_clusters_json()` re-reads and re-parses
   `clusters.json` from disk on *every* `/api/tracks`, `/api/clusters`, and per-track
   call. No caching.

`app.py`'s `get_tracks()` **already supports** `search`, `cluster`, `genre`, `bpm_min`,
`bpm_max`, `sort`, `dir`, `page`, `page_size` server-side — the frontend just ignores all
of it. The fix is to actually use it.

---

## 2. WORK ORDER (do in this order, confirm each with the user before moving on)

### Task A — First-launch wizard: verify, don't rebuild
The wizard already exists (`#setup-wizard` in index.html, backed by `GET/POST /api/setup`).
It is **not** built fresh.
- Verify: fresh install (no `config.json`) → server still boots → frontend shows the
  wizard → submitting writes `config.json` + `secrets.json` → `setup_complete` flips true
  → app loads without restart. (`_cfg()` reloads per request, so no restart needed.)
- Acceptance: TestClient `GET /api/setup` on a temp dir returns `setup_complete:false`;
  after `POST /api/setup` with a valid `library_root`, returns `true`.

### Task B — Settings cog (NEW) — next to the day/night toggle
- **Frontend:** add a cog `.icon-btn` immediately after `#theme-toggle`. Opens a settings
  panel that reuses the wizard's field set (library_root, rekordbox enabled+master_db,
  spotify creds, slskd creds). Secrets show "already set — leave blank to keep" (the
  `*_set` booleans `/api/setup` already returns); never round-trip plaintext secrets.
- **Reuse `POST /api/setup` for edits** — it already does blank-means-keep for secrets and
  re-runs `ensure_dirs()`. No new save endpoint needed.
- **Factory reset (NEW endpoint):** add `POST /api/config/reset` to `app.py` that rewrites
  `config.json` from `config.template.json` and sets `setup_complete:false`. **Decision to
  confirm with user:** wipe `secrets.json` too, or keep credentials? Default suggestion:
  reset config, keep secrets (less destructive), with a separate "also clear credentials"
  checkbox in the UI. Guard it behind a typed-confirm ("type RESET").
- Acceptance: cog opens panel pre-filled from `GET /api/setup` values; save persists and
  is visible on reload without server restart; reset returns the app to wizard state.

### Task C — Tracks page: server-side pagination + load-more (CONFIRMED approach)
- **Frontend:**
  - Build a query string from current filter/sort/search/bpm state and pass it to
    `/api/tracks?...&page=N&page_size=100`. Use `total` from the response for the
    "N of M" count.
  - Replace "render all rows" with append-on-scroll (IntersectionObserver sentinel row)
    or a "Load more" button. Debounce search/filter input (~250 ms) and reset to page 1
    on any filter change.
  - **Critical dependency / gotcha:** other features iterate `STATE.tracks` client-side
    (Set Builder list, cluster tag counts, kNN "more like this", drift totals). If
    `STATE.tracks` becomes only the current page, those break. Keep them separate:
    - The **Tracks table** uses the paginated window.
    - kNN / centroid suggestions move **server-side** (`/api/tracks/{id}/neighbors`,
      `POST /api/tracks/neighbors-centroid` — brief §1, §3b). Don't try to do real
      embedding kNN over a partial client array.
    - Cluster math already uses `clusters.json` coords (full set) — leave it.
  - Flag every spot where you swap a client-side `STATE.tracks` loop for a server call so
    the user can review the blast radius.
- **Backend:** add a tiny cache for `clusters.json` keyed by file mtime so repeated
  requests don't re-parse it. (Module-level `{mtime: (by_path, clusters)}`, invalidate on
  mtime change.) This alone removes most per-request cost.
- Acceptance: first paint shows page 1 fast; scrolling loads more; filters/sort hit the
  server and reset paging; `total` reflects the full filtered count, not the page size.

---

## 3. Mock data still live in index.html (must be removed as each page goes real)
Follow `stacks-implementation-brief.md` §4 — it's a precise checklist. Priority by page:
- **Clusters:** `purityCache`, `rbLinked`, `spLinked` → real values from an enriched
  `/api/clusters` (counts, bpm range, purity-or-drop-the-column). Confirm with user
  whether to compute genre-purity % or drop the column (open decision from last session).
- **Tag Authority:** `assignTags()` + injected drift → real RB MyTags + Spotify state via
  `/api/tags/groups` and `/api/tags/diff`. Serve the taxonomy from Python, never hardcode
  tag lists in JS (brief §3c — also confirm whether "Set Position"/Floor/Flag are real).
- **Queue / Pipeline / Monitor:** all `setInterval` fakes → real subprocess + SSE
  (brief §3f, §3h). This is the biggest chunk; do it last.

---

## 4. Backend architecture rules to honor (don't violate while wiring)
- **Single ledger writer** (brief §3d): FastAPI edit/sync/run endpoints must hand work to
  one writer process, never write SQLite directly from a request handler. This exists to
  prevent the tag-wipe bug. Add a real run-lock (file lock), not an in-memory boolean.
- **Path handling:** always use `dj_paths.py` (`to_key`/`to_posix`/`to_win`/`to_path`).
  Never hand-roll a path conversion — that's what caused the ghost/orphan bugs.
- **Pipeline triggers:** structured payload → server-built `argv` from a per-script
  whitelist → `subprocess.run([...])`, never `shell=True`. Audit `pipeline_03_tag.py` for
  `input()` prompts before any unattended "Run Stage" — an interactive prompt will hang a
  subprocess (known open item).
- Keep `uvicorn` on `0.0.0.0`; same-origin frontend, so no CORS.

---

## 5. How to work (token-conscious user — this matters)
- **One page at a time, confirm before the next.** Targeted patches over rewrites. Don't
  redo confirmed work. Challenge the user's reasoning when it's genuinely off.
- **You CAN run** in this repo: the FastAPI app (via TestClient or live uvicorn),
  `node --check` on extracted JS, Python parse/import checks, synthetic DB fixtures.
- **You CANNOT run** the real pipeline (no `essentia`) or reach Rekordbox / slskd /
  Spotify. Say plainly when something can only be verified by the user on Windows/WSL.
- For Tracks verification, get the user's real `music.db` + `clusters.json` + `config.json`
  and test `/api/tracks` against the real schema (`app.py`'s `_row_to_track` assumes many
  columns; `dict(row).get(...)` makes missing ones safe, but confirm cluster join works).
- Caution from a prior session: the wired `index.html` was once overwritten with the raw
  POC. Before patching, always `grep -c loadRealData web/index.html`.

---

## 6. The .exe / WSL reality (tell the user, design around it)
The "one .exe installs everything" goal is ~90% reachable, with one hard limit: **an .exe
cannot silently install WSL itself** — `wsl --install` needs admin + a reboot
(Microsoft-controlled). The installer CAN bootstrap everything *after* WSL exists: create
the venvs, `pip install` both requirements files, `apt install libchromaprint-tools`
(fpcalc), make dirs, launch the server. `start_stacks.bat` already handles the
"WSL exists → run" path. So the user-facing story is: "run once to set up WSL + reboot,"
then the .exe handles the rest and the user only ever sees the web UI. Don't promise
zero-touch WSL provisioning.

---

## Appendix — file inventory
| File | Role | Canonical? |
|---|---|---|
| `web/index.html` | the real frontend | **YES — edit this** |
| `webpage__proof_of_concept_.html` | design reference / mock UX | no — never ship |
| `app.py` | FastAPI backend (read endpoints + wizard) | yes |
| `config.py` | single source of truth for config | yes |
| `dj_paths.py` | all path conversions | yes — never bypass |
| `dj.py` | pipeline orchestrator (subprocess per stage) | yes |
| `pipeline_00..03` | download / embed / cluster / tag | yes |
| `config.template.json` | defaults for fresh install + factory reset | yes |
| `start.sh` / `start_stacks.bat` | launch (venv mgmt + WSL bridge) | yes |
