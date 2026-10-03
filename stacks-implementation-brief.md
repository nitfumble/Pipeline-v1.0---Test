# STACKS — implementation brief (mockup → real app)

Context for whoever picks this up: the attached `stacks.html` is a single-file, mock-data-driven UI prototype (vanilla JS, no build step) built to validate UX for a music-library pipeline that runs in WSL — ledger SQLite DB, slskd downloads, Essentia analysis, embeddings, UMAP + HDBSCAN clustering, Rekordbox MyTags, Spotify. Nothing in it talks to anything real yet. This doc lists what doesn't follow directly from reading the HTML — the backend work, the design intent, and the things that will bite you if skipped.

## 1. Backend ↔ frontend connections (API surface)

The frontend currently does all filtering/sorting/kNN/state in-browser over a 200-track mock array. With a real ~1800+ track library, move this server-side. Suggested endpoints, named after the UI feature that needs them:

- `GET /api/tracks?search=&cluster=&genre=&set_position=&vibe=&extras=&bpm_min=&bpm_max=&sort=&dir=&page=` — backs the Tracks table and Set Builder's left list (same endpoint, different query params — they already share filter state shape in the JS, keep that symmetry server-side too).
- `GET /api/tracks/{id}` — detail panel.
- `GET /api/tracks/{id}/neighbors?k=5` — "more like this," real embedding-space kNN (see §4b).
- `POST /api/tracks/neighbors-centroid {track_ids: []}` — Set Builder suggestions, kNN against the mean of the set's embeddings.
- `GET /api/clusters` — counts, purity, bpm range, per-group tag counts, RB/Spotify link flags, drift counts.
- `GET /api/map?n_neighbors=&min_dist=` — precomputed 2D coordinates + cluster id/color for the current slider position (see §4a — this must NOT trigger a live UMAP run).
- `GET /api/tags/groups` — the Genre/Set Position/Vibe/Extras taxonomy, so the frontend never hardcodes tag lists.
- `GET /api/tags/diff?track_ids=&drift_only=` — Tag Authority diff table, optionally scoped (this scoping — "show only these tracks" — is already a frontend concept from the cross-page "Open in Tag Authority" buttons; just needs a query param).
- `POST /api/tags/{track_id} {group, tags: []}` — ledger edit from Tag Authority's edit mode. See §4d for the single-writer nuance.
- `POST /api/tags/{track_id}/sync {target: "rb"|"spotify"|"both"}` — the "Override → Queue" action.
- `GET /api/queue`, `POST /api/queue/{id}/revert`
- `POST /api/pipeline/run/{stage}`, `POST /api/pipeline/run-all`
- `GET /api/pipeline/events` (SSE) — stage, progress, log lines.
- `GET /api/downloads/events` (SSE) — slskd progress, proxied through the backend (don't have the browser talk to slskd directly — keeps one source of truth and keeps any slskd credentials server-side).
- `GET /api/audio/{track_id}` — range-request audio streaming (see §4e).
- Optional: `GET/POST /api/sets` if you want Set Builder to save/load named sets instead of one ephemeral in-memory set.

## 2. Design choices worth preserving

- CSS custom properties at the top of the file are the full design system (dark default, `[data-theme="light"]` override block). Don't fork colors per-component — everything reads from those variables on purpose.
- `--accent-signal` (lime) is reserved exclusively for "live / active / primary action" meaning; `--accent-warn` (coral) exclusively for drift/conflict. Don't reuse either decoratively elsewhere or the semantics blur.
- Cluster dot/chip colors are currently a hand-picked categorical palette keyed by mock cluster id. Real clusters come from HDBSCAN and won't have stable ids/order — assign colors by sorted cluster index into a fixed palette array, with "noise"/unclustered always the same gray regardless of index.
- The `.view-fill` / `.panel-fill` flex pattern (Tracks, Set Builder, Tag Authority) makes the table fill the viewport instead of a fixed pixel cap. Reuse this pattern for any new full-table page rather than reintroducing a `max-height`.
- The guardrail copy (Queue page banner, the note above Tag Authority's edit mode) documents real architecture, not filler — keep it accurate as the backend evolves rather than deleting it for being "just UI text."
- Chips that are meant to be clicked (Clusters page tag filters) are real `<button>` elements for keyboard/focus support; chips that are just display (track detail tags, ledger tags in diff view) stay `<span>`. Keep that distinction if you add more chip usages.

## 3. Code implementation requirements

### 3a. Precomputing UMAP for the two map sliders
Live UMAP runs are too slow to do per-request. Precompute a small grid instead:
- Pick a coarse grid — e.g. `n_neighbors ∈ {10,20,30,40,50}` × `min_dist ∈ {0,0.2,0.4,0.6,0.8}` (25 combos) rather than the finer step the mock's sliders currently imply. Snap the real slider's `step` to match whatever grid you choose.
- For each grid point, run `UMAP(n_neighbors=.., min_dist=.., n_components=2).fit_transform(embeddings)` once and cache `{track_id, x, y}` per point — a `map_layouts` table, or one JSON file per grid point under something like `cache/map/{n}_{d}.json`.
- This only needs to rerun when the embedding set changes meaningfully (new tracks clustered), not on every pipeline run — make it a separate manual/triggered step, not baked into the Cluster stage by default, since it's the expensive part.
- `GET /api/map?n_neighbors=&min_dist=` just looks up the nearest cached grid point — O(1), no UMAP import needed at request time.

### 3b. Real kNN (more-like-this + set centroid)
- Load the full embedding matrix into memory once at FastAPI startup (numpy array, indexed by track id). At ~1800 tracks a plain distance computation is sub-millisecond — no need for FAISS/Annoy at this scale; revisit only if the library grows by an order of magnitude.
- "More like this": distance from one track's full-dimensional embedding to all others, not the 2D map projection — the map coordinates are a visualization projection and will distort real similarity. Run kNN on the source embeddings.
- Set Builder centroid: `np.mean(embeddings[track_ids], axis=0)`, then the same nearest-neighbor lookup against that mean vector.

### 3c. Tag taxonomy
`GENRE_TAGS`, `VIBE_TAGS`, and `COMP_TAGS` in the mock were copied from your real config. `SET_POSITION_TAGS`, `FLOOR_TAGS`, and `FLAG_TAGS` were invented placeholders since they weren't in what you shared — confirm whether Set Position exists as a real concept yet, and supply real values for Floor/Flag, or drop those filters from the UI if they don't exist. Either way, serve the taxonomy from `/api/tags/groups` so it lives in one place (Python config) instead of being duplicated in JS.

### 3d. Ledger writes / the single-writer guardrail
Flagging this explicitly because it's a real tension, not a detail: Tag Authority's edit mode is built to feel instant, but your Phase C brief says the ledger should have exactly one writer (the orchestrator), specifically because of the tag-wipe bug. Keep both by not having FastAPI write SQLite directly from the edit endpoint — instead have it hand the edit to whichever single process is the designated ledger writer (a small persistent worker draining a `pending_edits` table or in-memory queue, processed within well under a second). From the UI it still feels synchronous; architecturally only one process ever touches the DB. Apply the same pattern to sync/override actions and pipeline triggers — one entry point, not ad hoc subprocess spawns per request. Also add a real run-lock (file lock or persisted flag, not just an in-memory boolean) so a server restart mid-run doesn't leave a stale "running" state that blocks everything.

### 3e. Making the play button real
- Audio files already live on disk at `tracks.path`, on the same filesystem FastAPI runs on (WSL) — no transfer step needed, just serve them.
- Add `GET /api/audio/{track_id}` with HTTP Range support so scrubbing works (verify whether Starlette's `FileResponse` handles `Range` in your version, or implement a small streaming response that respects it).
- On the frontend, replace the WebAudio metronome-click placeholder with a real `<audio>` element pointed at that endpoint. Wire `timeupdate`/`loadedmetadata`/`ended` to the existing elapsed/duration display and scrubber, and `audio.currentTime = x` to the click-to-seek logic that's already built — it's a rewire, not a rebuild.
- The BPM metronome click can stay as an optional toggle (some people like a click track while browsing) but shouldn't be the only thing that happens on "play."
- Same origin (frontend served by the same FastAPI app) means no CORS config needed.

### 3f. Live monitor / pipeline triggers
- Replace the client-side `setInterval` simulations (run progress, fake log lines, fake downloads) with `StreamingResponse(..., media_type="text/event-stream")` endpoints the orchestrator actually pushes to. The frontend's log-append code barely changes — swap the `setInterval` callback for an `EventSource` `onmessage` handler.
- slskd has its own API — poll or subscribe to it from the backend and re-emit over your own SSE channel rather than having the browser talk to slskd directly.
- "Run Stage" buttons should invoke the real per-stage script (subprocess, captured stdout/stderr forwarded line-by-line into the SSE stream) — audit those scripts first for any interactive prompts that would hang a subprocess call.

### 3g. Routing everything through the frontend, WSL just for logs
The FastAPI process still has to run somewhere in WSL (`uvicorn ...`), but that's a one-time "start the server" step, not a per-task one — a `start.sh`, or autostart via WSL's systemd support if you have it enabled. Once it's running, the Pipeline page's terminal panel is meant to replace watching a WSL window day-to-day. The one thing to double check: any existing script that needs interactive input (confirmations, manual file moves) needs to either become fully automated or get a real UI control before "Run Stage" can safely call it unattended.

### 3h. Real pipeline stages and argument wiring (v0.4 update)
The Run Pipeline page now models your actual scripts instead of generic placeholder stages, in real execution order:

```
pipeline_00.py  →  pipeline_01.py  →  [manual: import to Rekordbox]  →  pipeline_02.py  →  pipeline_03.py
```

plus a separate, non-sequential **Utilities** section below the main list containing `sp2slsk.py` (yt-dlp → Soulseek upgrade). Each runnable stage's card renders one control per CLI flag — a toggle chip for every `store_true` arg, a `<select>` for `--sweep` (bare flag = "all", or a value), a number input for things like `--commit-every`/`--workers`/`--limit`, and a toggle+number combo for `--debug` on pipeline_01 (`nargs="?", const=1`). Every card shows a live `$ python3.11 {script} {args}` preview built from the current control state, purely client-side — useful as a sanity check before wiring it to anything real.

What this means for the backend:
- The run-trigger endpoint should accept a **structured** payload (`{script: "pipeline_00", args: {"--retry-failed": true, "--sweep": "bandcamp", ...}}`), not a raw command string — build `argv` server-side from a whitelist of known flags per script and pass it straight to `subprocess.run([...])` (never `shell=True` with string concatenation). The frontend's arg specs (flag name, kind, allowed values) should really live in one place — consider generating them from each script's `argparse` definition at startup (`ArgumentParser` objects can be introspected) so the UI and the actual CLI can never drift apart.
- The "manual RB import" step can't be triggered or detected from here — Rekordbox does that import itself. The UI's "mark as done" checkbox is a manual checkpoint that pauses "Run full pipeline" client-side until checked, then resumes `pipeline_02`. That's good enough for v1. A nicer version later: if Rekordbox's local DB exposes a last-modified timestamp or track count you can poll, you could auto-detect "the import happened" instead of relying on the user remembering to tick a box — not required to ship this, just worth knowing it's possible.
- `sp2slsk.py` is intentionally outside the chained "Run full pipeline" sequence (it's a maintenance/upgrade utility, not a pipeline stage) — keep it that way; don't fold it into the main run-all chain.
- Add this to the mock-removal checklist in §4: `PIPELINE_STAGES`/`UTILITY_SCRIPTS` arg-control default values and the `LOG_LINES` dictionary (now keyed by real script id — `p00`/`p01`/`p02`/`p03`/`sp2slsk`) are still placeholder text simulating plausible output; once stages run for real, stream actual stdout instead.


## 4. Mock data to remove — explicit checklist

- Track generator: `makeTrack`, `genArtist`, `genTitle`, `genKey`, `genScenePath`, the `A_FRAG`/`B_FRAG`/`TITLES`/`SUFFIXES`/`LABEL_CODES` name-fragment arrays, `CLUSTER_DEFS` (fixed fake centers/colors/genres/bpm ranges/counts), `NOISE`.
- `assignTags()` and the injected-drift logic in the `tagState` build step (random RB/Spotify "last synced" mirrors with fake staleness) — replace with real RB MyTags + Spotify state read at request time.
- `SET_POSITION_TAGS` / `FLOOR_TAGS` / `FLAG_TAGS` — see §3c.
- `METRIC_COLUMNS`'s random generators (`sp_energy`, `vocals_prob`, `party_score`, `mood_*`, etc.) — these already map 1:1 to your real `DB_SCHEMA` columns; just point `get()` at the real row instead of a `rand()` call.
- `rbLinked` / `spLinked` (random booleans) → real check against RB/Spotify playlist existence.
- `purityCache` (random %) → a real cluster-purity metric if you compute one, or drop the column.
- `newDownload` / `downloadsTick` → real slskd queue via its API.
- `PIPELINE` object, `LOG_LINES` dictionary, and `runStage`'s `setInterval` progress fake-out → real subprocess execution + SSE (§3f).
- `queueTick`'s fixed 2.5s/5.5s status transitions → real status pushed by whatever actually processes the queue.
- `regenerateLayout()`'s seeded-jitter fake projection → fully replaced by the precomputed UMAP cache (§3a).
- `PIPELINE_STAGES`/`UTILITY_SCRIPTS` argument defaults and the `LOG_LINES` dictionary (keyed `p00`/`p01`/`p02`/`p03`/`sp2slsk`) — these are plausible-sounding placeholder output; once a run-trigger endpoint exists, stream real stdout/stderr instead (§3h).

## 5. Splitting into real files (matches your planned "no-build frontend" scaffold)

Single-file was a prototyping convenience, not a recommendation. Suggested layout once it's wired to FastAPI:

```
web/
  index.html
  css/
    tokens.css        (the :root / [data-theme] variable block)
    components.css    (everything else)
  js/
    state.js           (the STATE object + constants)
    api.js              (fetch wrappers for every endpoint in §1)
    map.js, tracks.js, clusters.js, tagauthority.js,
    pipeline.js, monitor.js, queue.js, setbuilder.js,
    player.js, theme.js
    main.js             (init, wires everything together)
```
Plain ES modules (`<script type="module" src="js/main.js">`), no bundler needed — matches what you scoped for C2. FastAPI mounts `web/` via `StaticFiles`. This is a reorganization, not a rewrite — the render/setup functions move as-is into their own files.

## 6. Other things worth getting right

- **Pagination/virtualization**: the Tracks table renders every matching row into the DOM at once — fine at 200 mock tracks, worth a "load more" or virtual scroll once it's the real ~1800+.
- **Error states**: every mock render assumes data exists and runs always succeed. Real downloads fail, Essentia chokes on a corrupt file, stages error out — Monitor/Pipeline/Queue need a visible failure state, not just "done."
- **localStorage**: it's disabled in this prototype because it was built and tested as a Claude.ai artifact, which sandboxes browser storage. That restriction doesn't apply to a real standalone page — once this is served by your own FastAPI app, normal `localStorage` for theme/last-used-filters is fine and is the right tool for it.
- **SQLite write concurrency**: confirm whatever process ends up as the single ledger writer uses WAL mode or short-lived connections so the FastAPI read endpoints don't get locked out during a write burst.
- **Binding**: keep `uvicorn` bound to `0.0.0.0` inside WSL so the existing localhost-forwarding setup keeps working from the Windows browser. No auth needed for a single-user localhost tool — just don't expose the port past localhost without adding some.
- **Rollout order**: Tracks first (read-only, simplest), then Library Map (needs the UMAP precompute step), then Clusters/Tag Authority (needs taxonomy + drift logic), then Set Builder (needs centroid kNN), then Pipeline/Monitor/Queue last (needs the orchestrator-subprocess + SSE work — the biggest single chunk of this list). Wiring one page at a time and confirming it before moving on will be much less painful than connecting everything simultaneously.
