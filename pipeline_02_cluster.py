#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pipeline_02_cluster.py
======================
Stage 2: cluster the music library and generate DJ playlists.

Flow:
  Cell 1 — Load features from DB(s)
            CLUSTER_OLD=True  → combine NEW_DB + OLD_DB (dedup by filename)
            CLUSTER_OLD=False → NEW_DB only
  Cell 2 — UMAP dimensionality reduction (20D clustering + 2D visualisation)
  Cell 3 — HDBSCAN parameter sweep (15 combos, ranked table + auto-recommendation)
  Cell 4 — Final clustering with chosen parameters
  Cell 5 — Playlist generation
            Round 1: main clusters  → playlists/clusters/
            Round 2: re-cluster noise → playlists/clusters_noise/
            Unclassified → playlists/clusters/_unclassified/
  Cell 6 — 2D visualisation (optional, requires matplotlib)

MANUAL_PARAMS = True  → read sweep table, set params below
MANUAL_PARAMS = False → auto-selected by weighted quality score

Run AFTER pipeline_01_embed.py.
Run BEFORE pipeline_03_rb_tag.py.

Path note: playlists are written with Windows-style paths (E:\\...)
for Rekordbox compatibility, even when running in WSL.
"""

import sqlite3, json, shutil
import numpy as np
import pandas as pd
from pathlib import Path
from collections import Counter

import umap
import hdbscan
from sklearn.metrics import silhouette_score
from sklearn.cluster import KMeans, AgglomerativeClustering

import os
import argparse
import logging

# Canonical path handling + atomic writes — single source of truth (dj_paths.py)
from dj_paths import to_win, to_path, atomic_write_json
from config import load

_parser = argparse.ArgumentParser(description="DJ Pipeline — Cluster & generate playlists")
_parser.add_argument("--manual-params",action="store_true", help="Use MANUAL_* params instead of auto")
_parser.add_argument("--skip-viz",     action="store_true", help="Skip Cell 6 visualisation")
_parser.add_argument("--skip-sweep",   action="store_true", help="Skip sweep, use manual params directly")
_parser.add_argument("--no-interactive", action="store_true")
_args = _parser.parse_args()

# ══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════════════════
# ── Load config — single source of truth (config.py / config.json) ────────────
cfg = load()
_cl = cfg.cluster

NEW_ROOT  = cfg.paths.library_root
NEW_AUDIO = cfg.paths.audio
NEW_DB    = cfg.paths.music_db
LOG_DIR   = cfg.paths.logs
# (pipe02 reads the DB and writes playlists only — it never touches the ledger)

# Essentia label file lives with the models: configured dir, else repo-bundled.
_cfg_models = to_path(cfg.embed.model_dir) if cfg.embed.model_dir else None
TRAINED_DIR = _cfg_models if (_cfg_models and _cfg_models.exists()) \
              else (Path(__file__).resolve().parent / "models")

PLAYLIST_DIR       = cfg.paths.playlists / "clusters"
NOISE_PLAYLIST_DIR = cfg.paths.playlists / "clusters_noise"
CLUSTERS_JSON      = cfg.paths.playlists / "clusters.json"   # machine-readable: web map + more-like-this
TAGS_JSON          = cfg.paths.playlists / "tags.json"         # auto-computed tag assignments for web frontend
LABEL_FILE         = TRAINED_DIR / "discogs-maest-30s-pw-519l-2.json"

# ── Feature weights (≈ sum 1.0) ───────────────────────────────────────────────
W_EMBED, W_GENRE, W_BPM = _cl.w_embed, _cl.w_genre, _cl.w_bpm

# ── UMAP ─────────────────────────────────────────────────────────────────────
UMAP_N_NEIGHBORS  = _cl.umap_n_neighbors
UMAP_MIN_DIST     = _cl.umap_min_dist
UMAP_N_COMPONENTS = _cl.umap_n_components
UMAP_METRIC       = _cl.umap_metric
UMAP_RANDOM_STATE = _cl.umap_random_state

# ── HDBSCAN sweep ─────────────────────────────────────────────────────────────
SWEEP_CLUSTER_SELECTION = list(_cl.sweep_cluster_selection)
SWEEP_MIN_CLUSTER_SIZE  = list(_cl.sweep_min_cluster_size)
SWEEP_MIN_SAMPLES       = list(_cl.sweep_min_samples)

# ── Quality score weights ─────────────────────────────────────────────────────
SCORE_W = dict(cfg.cluster._d["score_w"])

# ── Manual parameter selection ────────────────────────────────────────────────
MANUAL_PARAMS = _args.manual_params or _args.skip_sweep
MANUAL_MIN_CLUSTER_SIZE  = _cl.manual_min_cluster_size
MANUAL_MIN_SAMPLES       = _cl.manual_min_samples
MANUAL_CLUSTER_SELECTION = _cl.manual_cluster_selection

# ── Playlist target size ──────────────────────────────────────────────────────
TARGET_MIN = _cl.target_min
TARGET_MAX = _cl.target_max

# TARGET_MIN = 30   # allow smaller playlists
# TARGET_MAX = 150  # prefer tighter playlists

# ── Genre taxonomy ────────────────────────────────────────────────────────────
ELECTRONIC_GENRES = [
    "Techno", "House", "Trance", "Tech House", "Hard Techno",
    "Electro", "Breakbeat", "Hardcore", "Hard Trance", "Acid",
    "Hard House", "Jungle", "Deep House", "Drum N Bass", "Progressive House",
    "Euro House", "UK Garage", "Tribal", "Progressive Trance", "Breaks",
    "Dubstep", "Bassline", "Electro House", "Minimal", "Tech Trance",
    "Downtempo", "Disco", "Speed Garage", "Garage House", "Synth-Pop",
    "Deep Techno", "Donk", "Experimental", "Acid House", "Psy-Trance",
    "Dance-Pop", "Ambient", "Eurodance", "Hardstyle", "Happy Hardcore",
    "Schranz", "Tribal House", "Nu-Disco", "Grime",
]
NON_ELECTRONIC_PARENTS = ["Rock", "Funk / Soul", "Pop", "Hip Hop", "Reggae"]
GENRE_THRESHOLD = _cl.genre_threshold

# ── Init ──────────────────────────────────────────────────────────────────────
LOG_DIR.mkdir(parents=True, exist_ok=True)
cfg.paths.tmp.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "pipeline_02.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)

print("=== Pipeline 02: Clustering ===")
print(f"  NEW_DB:      {NEW_DB}")
print(f"  Playlists:   {PLAYLIST_DIR}\n")

# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _decode(blob, dtype=np.float32):
    return np.frombuffer(bytes(blob), dtype=dtype) if blob else None


def _resolve(path_str: str) -> str | None:
    """Verify the file exists on disk and return a Windows-style path for .m3u8
    (Rekordbox needs E:\\... even under WSL). Returns None if missing.
    Handles both old- and new-DB path formats via dj_paths."""
    return to_win(path_str) if to_path(path_str).exists() else None


def _load_features(db_path: Path, maest_n: int) -> list[dict]:
    """
    Load tracks with embeddings + genre scores from a single DB.
    Returns list of record dicts. Empty list if DB doesn't exist.
    """
    if not db_path.exists():
        print(f"  !! DB not found: {db_path}")
        return []
    conn = sqlite3.connect(db_path)
    rows = conn.execute("""
        SELECT
            t.path, t.bpm, t.genre,
            t.mood_aggressive, t.mood_relaxed, t.mood_sad, t.mood_party,
            tm.embeddings, tm.genre_scores
        FROM tracks t
        JOIN track_metadata tm ON t.path = tm.track_path
        WHERE tm.embeddings IS NOT NULL
          AND tm.genre_scores IS NOT NULL
    """).fetchall()
    conn.close()

    records = []
    for row in rows:
        path, bpm, genre, agg, relaxed, sad, mood_party, emb_blob, gs_blob = row
        emb = _decode(emb_blob)
        gs  = _decode(gs_blob)
        if emb is None or gs is None:    continue
        if len(emb) != 1280:             continue
        if len(gs)  != maest_n:          continue
        # Clamp extreme BPM outliers — treat as missing, median-filled later
        if bpm and (bpm < 70 or bpm > 180):
            bpm = None
        records.append({
            "path":           path,
            "bpm":            bpm or 0.0,
            "genre":          genre or "",
            "mood_aggressive": agg        or 0.0,
            "mood_relaxed":    relaxed    or 0.0,
            "mood_sad":        sad        or 0.0,
            "mood_party":      mood_party or 0.0,
            "emb": emb,
            "gs":  gs,
        })
    return records


# ══════════════════════════════════════════════════════════════════════════════
#%% Cell 1: Load features
# ══════════════════════════════════════════════════════════════════════════════
print("── Cell 1: Load features ──")

with open(LABEL_FILE) as f:
    maest_classes = json.load(f)["classes"]
maest_n = len(maest_classes)

# Single library (v1.0): one DB.
records = _load_features(NEW_DB, maest_n)
print(f"  DB: {NEW_DB}")

print(f"  Tracks with embeddings: {len(records)}")

if not records:
    raise SystemExit("No tracks with embeddings found. Run pipeline_01 first.")

df = pd.DataFrame(records)
print(f"  Valid records: {len(df)}")


# ── Genre taxonomy helpers ────────────────────────────────────────────────────
# 1. Define the functions
def _build_elec_idx():
    idx = {}
    for genre in ELECTRONIC_GENRES:
        for i, cls in enumerate(maest_classes):
            if cls.lower() == f"electronic---{genre}".lower():
                idx[genre] = i; break
        if genre not in idx:
            norm = genre.lower().replace(" ", "").replace("-", "")
            for i, cls in enumerate(maest_classes):
                if cls.startswith("Electronic---"):
                    cn = cls.replace("Electronic---", "").lower().replace(" ", "").replace("-", "")
                    if cn == norm:
                        idx[genre] = i; break
    return idx

def _build_nonelec_idx():
    return {
        p: [i for i, cls in enumerate(maest_classes)
            if cls.split("---")[0].strip().lower() == p.lower()]
        for p in NON_ELECTRONIC_PARENTS
    }

# 2. Call them to get the dictionaries
elec_idx    = _build_elec_idx()
nonelec_idx = _build_nonelec_idx()

# 3. Use the dictionaries in the vectorised genre assignment
elec_indices   = list(elec_idx.values())
elec_names     = list(elec_idx.keys())
genre_mat_full = np.stack(df["gs"].values)

elec_scores = genre_mat_full[:, elec_indices]
best_elec_i = np.argmax(elec_scores, axis=1)
best_elec_s = elec_scores[np.arange(len(df)), best_elec_i]

top_genres = np.where(
    best_elec_s >= GENRE_THRESHOLD,
    np.array(elec_names)[best_elec_i],
    "Others"
)

for parent, indices in nonelec_idx.items():
    if not indices:
        continue
    parent_scores = genre_mat_full[:, indices].max(axis=1)
    override = (top_genres == "Others") & (parent_scores >= 0.03) & (parent_scores > best_elec_s)
    top_genres[override] = parent

df["top_genre"] = top_genres

# Multi-label genre list: all genres above GENRE_THRESHOLD per track
_genre_lists = []
for _i in range(len(df)):
    _labels = [elec_names[_j] for _j, _s in enumerate(elec_scores[_i]) if _s >= GENRE_THRESHOLD]
    for _parent, _indices in nonelec_idx.items():
        if _indices and genre_mat_full[_i, _indices].max() >= 0.03:
            _labels.append(_parent)
    _genre_lists.append(_labels if _labels else ["Others"])
df["genre_list"] = _genre_lists

# ── Build combined feature matrix ─────────────────────────────────────────────
emb_mat   = np.stack(df["emb"].values).astype(np.float32)
genre_mat = np.stack(df["gs"].values).astype(np.float32)

emb_mat   /= np.linalg.norm(emb_mat,   axis=1, keepdims=True) + 1e-8
genre_mat /= np.linalg.norm(genre_mat, axis=1, keepdims=True) + 1e-8

bpm_raw  = df["bpm"].fillna(df["bpm"].median()).values.astype(np.float32)
p5, p95  = np.percentile(bpm_raw, 5), np.percentile(bpm_raw, 95)
bpm_norm = np.clip((bpm_raw - p5) / (p95 - p5 + 1e-8), 0, 1).reshape(-1, 1)

print(f"  BPM p5={p5:.0f}  p95={p95:.0f}")

combined = np.concatenate([
    W_EMBED * emb_mat,
    W_GENRE * genre_mat,
    W_BPM   * bpm_norm,
], axis=1)

print(f"  Feature matrix: {combined.shape}  "
      f"(embed×{W_EMBED} + genre×{W_GENRE} + bpm×{W_BPM})")


# ══════════════════════════════════════════════════════════════════════════════
#%% Cell 2: UMAP
# ══════════════════════════════════════════════════════════════════════════════
print("\n── Cell 2: UMAP ──")
print(f"  n_neighbors={UMAP_N_NEIGHBORS}  min_dist={UMAP_MIN_DIST}  "
      f"n_components={UMAP_N_COMPONENTS}  metric={UMAP_METRIC}")

reducer = umap.UMAP(
    n_neighbors  = UMAP_N_NEIGHBORS,
    min_dist     = UMAP_MIN_DIST,
    n_components = UMAP_N_COMPONENTS,
    metric       = UMAP_METRIC,
    random_state = UMAP_RANDOM_STATE,
    low_memory   = False,
    verbose      = True,
)
X_umap = reducer.fit_transform(combined)
print(f"  UMAP output: {X_umap.shape}")


# ══════════════════════════════════════════════════════════════════════════════
#%% Cell 3: HDBSCAN parameter sweep
# ══════════════════════════════════════════════════════════════════════════════
print("\n── Cell 3: HDBSCAN sweep ──")

bpm_arr = df["bpm"].values.astype(float)


def _cluster_stats(labels, df, X_umap, bpm_arr):
    n_total    = len(labels)
    n_noise    = int((labels == -1).sum())
    noise_pct  = n_noise / n_total
    valid_mask = labels >= 0
    n_valid    = valid_mask.sum()
    cluster_ids = sorted(set(labels[valid_mask]))
    n_clusters  = len(cluster_ids)

    sizes, genre_purities, bpm_stds = [], [], []
    for cid in cluster_ids:
        mask  = labels == cid
        size  = mask.sum()
        sizes.append(size)
        cluster_genres = df["top_genre"].values[mask]
        top_n = Counter(cluster_genres).most_common(1)[0][1]
        genre_purities.append(top_n / size)
        bpm_in = bpm_arr[mask]
        bpm_in = bpm_in[bpm_in > 0]
        bpm_stds.append(float(np.std(bpm_in)) if len(bpm_in) > 1 else 0.0)

    sizes = np.array(sizes)
    pct_in_target = float(((sizes >= TARGET_MIN) & (sizes <= TARGET_MAX)).mean()) \
                    if len(sizes) > 0 else 0.0

    sil = 0.0
    if n_valid > 100 and n_clusters > 1:
        sample_n = min(3000, n_valid)
        idx      = np.where(valid_mask)[0]
        rng      = np.random.default_rng(42)
        sample   = rng.choice(idx, size=sample_n, replace=False)
        try:
            sil = float(silhouette_score(X_umap[sample], labels[sample]))
        except Exception:
            sil = 0.0

    return {
        "n_clusters":    n_clusters,
        "noise_pct":     noise_pct,
        "pct_in_target": pct_in_target,
        "genre_purity":  float(np.median(genre_purities)) if genre_purities else 0.0,
        "bpm_std":       float(np.median(bpm_stds))       if bpm_stds       else 999.0,
        "silhouette":    sil,
        "size_min":      int(sizes.min())        if len(sizes) else 0,
        "size_median":   int(np.median(sizes))   if len(sizes) else 0,
        "size_max":      int(sizes.max())        if len(sizes) else 0,
    }


def _quality_score(s):
    noise_score  = max(0.0, 1.0 - s["noise_pct"]  / 0.3)
    target_score = s["pct_in_target"]
    purity_score = s["genre_purity"]
    bpm_score    = max(0.0, 1.0 - s["bpm_std"] / 20.0)
    sil_score    = max(0.0, min(1.0, (s["silhouette"] + 1) / 2))
    return (
        SCORE_W["noise"]        * noise_score  +
        SCORE_W["target_range"] * target_score +
        SCORE_W["genre_purity"] * purity_score +
        SCORE_W["bpm_cohesion"] * bpm_score    +
        SCORE_W["silhouette"]   * sil_score
    )


results = []
total = len(SWEEP_MIN_CLUSTER_SIZE) * len(SWEEP_MIN_SAMPLES) * len(SWEEP_CLUSTER_SELECTION)
done    = 0

if not _args.skip_sweep:
    for mcs in SWEEP_MIN_CLUSTER_SIZE:
        for ms in SWEEP_MIN_SAMPLES:
            for csm in SWEEP_CLUSTER_SELECTION:
                if ms >= mcs:
                    continue
                done += 1
                print(f"  [{done}/{total}] mcs={mcs:>4}  ms={ms:>2}  csm={csm:<4} ...", end=" ", flush=True)

                labels_sw = hdbscan.HDBSCAN(
                    min_cluster_size         = mcs,
                    min_samples              = ms,
                    metric                   = "euclidean",
                    cluster_selection_method = csm,
                ).fit_predict(X_umap)

                s = _cluster_stats(labels_sw, df, X_umap, bpm_arr)
                s.update({"min_cluster_size": mcs, "min_samples": ms, "csm": csm, "labels": labels_sw})
                s["score"] = _quality_score(s)
                results.append(s)

                print(f"clusters={s['n_clusters']:>4}  "
                      f"noise={s['noise_pct']*100:>5.1f}%  "
                      f"target={s['pct_in_target']*100:>5.1f}%  "
                      f"purity={s['genre_purity']*100:>5.1f}%  "
                      f"bpm_std={s['bpm_std']:>5.1f}  "
                      f"sil={s['silhouette']:>+.3f}  "
                      f"score={s['score']:.3f}")

    print("\n=== SWEEP RESULTS ===")
    header = (f"{'mcs':>5}  {'ms':>3}  {'#cls':>5}  "
              f"{'noise%':>7}  {'target%':>8}  {'purity%':>8}  "
              f"{'bpm_std':>8}  {'sil':>6}  {'med_sz':>7}  {'score':>6}")
    print(header)
    print("─" * len(header))

    results_sorted = sorted(results, key=lambda x: x["score"], reverse=True)

    # Save sweep results for reuse
    _sweep_save = [{k: v for k, v in s.items() if k != "labels"} for s in results_sorted]
    atomic_write_json(NEW_ROOT / "db" / "last_sweep.json", _sweep_save)
    np.save(str(NEW_ROOT / "db" / "umap_embedding.npy"), X_umap)
    logging.info(f"Sweep results saved — {len(results_sorted)} combos")

    for s in results_sorted:
        marker = " ◀ AUTO" if s is results_sorted[0] else ""
        print(f"{s['min_cluster_size']:>5}  {s['min_samples']:>3}  "
              f"{s['n_clusters']:>5}  "
              f"{s['noise_pct']*100:>7.1f}  "
              f"{s['pct_in_target']*100:>8.1f}  "
              f"{s['genre_purity']*100:>8.1f}  "
              f"{s['bpm_std']:>8.1f}  "
              f"{s['silhouette']:>+6.3f}  "
              f"{s['size_median']:>7}  "
              f"{s['score']:>6.3f}{marker}")

    best = results_sorted[0]
    print(f"\n✦ AUTO-RECOMMENDATION: "
          f"min_cluster_size={best['min_cluster_size']}  "
          f"min_samples={best['min_samples']}  "
          f"(score={best['score']:.3f})")
    print(f"  → {best['n_clusters']} clusters  "
          f"{best['noise_pct']*100:.1f}% noise  "
          f"{best['genre_purity']*100:.1f}% median genre purity")

    if MANUAL_PARAMS:
        print(f"\n  MANUAL_PARAMS=True — using mcs={MANUAL_MIN_CLUSTER_SIZE} "
              f"ms={MANUAL_MIN_SAMPLES}")
    else:
        print("\n  MANUAL_PARAMS=False — auto-selected params will be used in Cell 4.")

else:
    print(f"  --skip-sweep: going straight to manual params "
          f"mcs={MANUAL_MIN_CLUSTER_SIZE}  ms={MANUAL_MIN_SAMPLES}  csm={MANUAL_CLUSTER_SELECTION}")
    results_sorted = []
    best = {
        "min_cluster_size": MANUAL_MIN_CLUSTER_SIZE,
        "min_samples":      MANUAL_MIN_SAMPLES,
        "csm":              MANUAL_CLUSTER_SELECTION,
    }


# ══════════════════════════════════════════════════════════════════════════════
#%% Cell 4: Final clustering
# ══════════════════════════════════════════════════════════════════════════════
print("\n── Cell 4: Final clustering ──")

if MANUAL_PARAMS:
    chosen_mcs = MANUAL_MIN_CLUSTER_SIZE
    chosen_ms  = MANUAL_MIN_SAMPLES
    chosen_csm = MANUAL_CLUSTER_SELECTION  # add this to config
else:
    chosen_mcs = best["min_cluster_size"]
    chosen_ms  = best["min_samples"]
    chosen_csm = best["csm"]

cached = next(
    (s for s in results
     if s["min_cluster_size"] == chosen_mcs 
     and s["min_samples"] == chosen_ms
     and s["csm"] == chosen_csm),
    None,
)

if cached:
    labels = cached["labels"]
    print("  (Using cached sweep result)")
else:
    labels = hdbscan.HDBSCAN(
        min_cluster_size         = chosen_mcs,
        min_samples              = chosen_ms,
        metric                   = "euclidean",
        cluster_selection_method = "leaf",
    ).fit_predict(X_umap)

df["cluster"] = labels

# ── Alternate clustering methods for Library Map method-switch buttons ────────
print("\n── Computing alternate clustering methods (K-Means, Hierarchical) ──")
_cm_paths   = df["path"].tolist()
_cm_methods = {}

_cm_methods["hdbscan"] = {
    "label":       "HDBSCAN",
    "description": "Density-based; finds natural shapes, allows noise",
    "variants": [
        {"key": "fine",   "label": "Fine",   "default": True,
         "assignments": labels.tolist()},
        {"key": "coarse", "label": "Coarse", "default": False,
         "assignments": hdbscan.HDBSCAN(
             min_cluster_size         = max(int(chosen_mcs * 2.5), 30),
             min_samples              = chosen_ms,
             metric                   = "euclidean",
             cluster_selection_method = "leaf",
         ).fit_predict(X_umap).tolist()},
    ],
}

_kmeans_variants = []
for _k in (8, 16):
    print(f"  K-Means k={_k}…", end=" ", flush=True)
    try:
        _km_labels = KMeans(n_clusters=_k, random_state=UMAP_RANDOM_STATE, n_init="auto").fit_predict(X_umap)
        _kmeans_variants.append({"key": f"k{_k}", "label": f"{_k} groups", "default": _k==8,
                                  "assignments": _km_labels.tolist()})
        print("✓")
    except Exception as _e:
        print(f"✗ {_e}")
_cm_methods["kmeans"] = {
    "label":       "K-Means",
    "description": "Centroid-based; balanced, equal-sized groups",
    "variants":    _kmeans_variants,
}

_agglo_variants = []
for _n in (10, 20):
    print(f"  Hierarchical n={_n}…", end=" ", flush=True)
    try:
        _ag_labels = AgglomerativeClustering(n_clusters=_n).fit_predict(X_umap)
        _agglo_variants.append({"key": f"n{_n}", "label": f"{_n} groups", "default": _n==10,
                                 "assignments": _ag_labels.tolist()})
        print("✓")
    except Exception as _e:
        print(f"✗ {_e}")
_cm_methods["agglo"] = {
    "label":       "Hierarchical",
    "description": "Ward linkage; top-down dendrogram structure",
    "variants":    _agglo_variants,
}

_CM_JSON = NEW_ROOT / "db" / "cluster_methods.json"
atomic_write_json(_CM_JSON, {"paths": _cm_paths, "methods": _cm_methods})
print(f"  ✓ {_CM_JSON}  (3 methods × 2 variants each)")

final_stats = _cluster_stats(labels, df, X_umap, bpm_arr)
print(f"\n  Clusters:            {final_stats['n_clusters']}")
print(f"  Noise tracks:        {int(final_stats['noise_pct']*len(df))} "
      f"({final_stats['noise_pct']*100:.1f}%)")
print(f"  In target range:     {final_stats['pct_in_target']*100:.1f}% of clusters")
print(f"  Median size:         {final_stats['size_median']} tracks")
print(f"  Size range:          {final_stats['size_min']} – {final_stats['size_max']}")
print(f"  Median genre purity: {final_stats['genre_purity']*100:.1f}%")
print(f"  Median BPM std:      {final_stats['bpm_std']:.1f}")
print(f"  Silhouette:          {final_stats['silhouette']:+.3f}")

cluster_summaries = []
for cid in sorted(set(labels)):
    if cid == -1: continue
    mask           = labels == cid
    size           = mask.sum()
    cluster_genres = df["top_genre"].values[mask]
    top_g, top_n   = Counter(cluster_genres).most_common(1)[0]
    purity         = top_n / size
    bpms           = bpm_arr[mask]
    bpms           = bpms[bpms > 0]
    cluster_summaries.append({
        "id":       cid,
        "size":     size,
        "top_genre":top_g,
        "purity":   purity,
        "bpm_lo":   int(bpms.min())        if len(bpms) else 0,
        "bpm_hi":   int(bpms.max())        if len(bpms) else 0,
        "bpm_med":  float(np.median(bpms)) if len(bpms) else 0.0,
        "is_new":   False,
    })

cluster_summaries.sort(key=lambda x: x["size"], reverse=True)

print(f"\n  Top clusters by size:")
print(f"  {'ID':>5}  {'Size':>5}  {'Genre':>22}  {'BPM range':>12}  {'Purity%':>8}")
print("  " + "─" * 60)
for cs in cluster_summaries[:20]:
    print(f"  {cs['id']:>5}  {cs['size']:>5}  "
          f"{cs['top_genre']:>22}  "
          f"{cs['bpm_lo']:>3}–{cs['bpm_hi']:<3} BPM  "
          f"{cs['purity']*100:>7.1f}%")
if len(cluster_summaries) > 20:
    print(f"  ... and {len(cluster_summaries)-20} more clusters")


# ══════════════════════════════════════════════════════════════════════════════
#%% Cell 5: Generate playlists
# ══════════════════════════════════════════════════════════════════════════════
print("\n── Cell 5: Playlists ──")

# Build into temp dirs that live in the SAME parent as the final dirs, so the
# swap is a same-directory atomic rename (no fragile cross-directory move).
PLAYLIST_DIR.parent.mkdir(parents=True, exist_ok=True)
TMP_PLAYLIST = PLAYLIST_DIR.with_name(".clusters_tmp")
TMP_NOISE    = NOISE_PLAYLIST_DIR.with_name(".clusters_noise_tmp")
for d in [TMP_PLAYLIST, TMP_NOISE]:
    if d.exists(): shutil.rmtree(d)
    d.mkdir(parents=True)

# ── 5a: Round-2 re-cluster noise tracks ──────────────────────────────────────
noise_mask_initial = df["cluster"].values == -1
n_noise_initial    = int(noise_mask_initial.sum())
print(f"\n── 5a: Re-cluster {n_noise_initial} noise tracks ──")

if n_noise_initial >= 50:
    noise_idx    = np.where(noise_mask_initial)[0]
    X_noise      = X_umap[noise_idx]
    RECLUSTER_MCS = chosen_mcs
    RECLUSTER_MS  = max(3, chosen_ms // 2)
    print(f"  Re-cluster params: mcs={RECLUSTER_MCS}  ms={RECLUSTER_MS}")

    noise_labels = hdbscan.HDBSCAN(
        min_cluster_size         = RECLUSTER_MCS,
        min_samples              = RECLUSTER_MS,
        metric                   = "euclidean",
        cluster_selection_method = "leaf",
    ).fit_predict(X_noise)

    n_new          = len([l for l in set(noise_labels) if l >= 0])
    n_still_noise  = int((noise_labels == -1).sum())
    print(f"  New clusters found:      {n_new}")
    print(f"  Still noise after R2:    {n_still_noise} "
          f"({100*n_still_noise/len(df):.1f}% of library)")

    max_id      = int(max(l for l in df["cluster"].values if l >= 0))
    cluster_col = df["cluster"].values.copy()
    for local_id in sorted(set(noise_labels)):
        if local_id == -1: continue
        global_id  = max_id + 1 + int(local_id)
        local_mask = noise_labels == local_id
        cluster_col[noise_idx[local_mask]] = global_id
    df["cluster"] = cluster_col
else:
    print("  Too few noise tracks to re-cluster — skipping.")

# ── 5a.5: clusters.json — machine-readable layout for the web map + "more-like-this"
# 2D UMAP gives the map coordinates; raw 1280-d embeddings stay in the DB for kNN.
from datetime import datetime as _dt
print("\n── Writing clusters.json (2D layout for web map) ──")
X_2d = umap.UMAP(
    n_neighbors=UMAP_N_NEIGHBORS, min_dist=0.1, n_components=2,
    metric=UMAP_METRIC, random_state=UMAP_RANDOM_STATE,
).fit_transform(combined)

def _normalise_2d(pts, w=960, h=600, pad=20):
    """Scale (N,2) UMAP output to SVG viewport [pad, w-pad] x [pad, h-pad]."""
    import numpy as _np
    xs, ys = pts[:, 0], pts[:, 1]
    xr = float(xs.max() - xs.min()) or 1.0
    yr = float(ys.max() - ys.min()) or 1.0
    nx = pad + (xs - xs.min()) / xr * (w - 2 * pad)
    ny = pad + (ys - ys.min()) / yr * (h - 2 * pad)
    return nx, ny

_nx, _ny = _normalise_2d(X_2d)

_clusters_payload = {
    "generated":  _dt.now().isoformat(timespec="seconds"),
    "n_tracks":   int(len(df)),
    "n_clusters": int(len({c for c in df["cluster"].values if c >= 0})),
    "umap":       {"n_neighbors": UMAP_N_NEIGHBORS, "min_dist": 0.1,
                   "metric": UMAP_METRIC, "random_state": UMAP_RANDOM_STATE},
    "tracks": [
        {
            "path":     str(df["path"].values[i]),
            "path_win": to_win(df["path"].values[i]),
            "cluster":  int(df["cluster"].values[i]),
            "bpm":      (None if pd.isna(df["bpm"].values[i]) else round(float(df["bpm"].values[i]), 1)),
            "genre":    str(df["top_genre"].values[i]),
            "x":        round(float(_nx[i]), 1),
            "y":        round(float(_ny[i]), 1),
        }
        for i in range(len(df))
    ],
}
CLUSTERS_JSON.parent.mkdir(parents=True, exist_ok=True)
atomic_write_json(CLUSTERS_JSON, _clusters_payload)
print(f"  ✓ {CLUSTERS_JSON}  ({len(df)} tracks, {_clusters_payload['n_clusters']} clusters)")

# ── 5a.6: umap_projections.json — pre-computed 2D UMAP for map viewer parameter sweep ──
print("\n── Computing 2D UMAP projection grid for map viewer ──")
_PROJ_N = [10, 15, 30]
_PROJ_D = [0.0, 0.1]
_proj_paths = [str(df["path"].values[i]) for i in range(len(df))]
_proj_data  = {}

for _pn in _PROJ_N:
    for _pd in _PROJ_D:
        _key = f"n{_pn}_d{_pd:.1f}"
        # reuse the already-computed projection for the default params to save time
        if _pn == UMAP_N_NEIGHBORS and abs(_pd - 0.1) < 1e-6:
            _proj_data[_key] = [[round(float(_nx[i]), 1), round(float(_ny[i]), 1)] for i in range(len(df))]
            print(f"  {_key} ✓ (reused default)")
            continue
        print(f"  {_key}…", end=" ", flush=True)
        try:
            _pts = umap.UMAP(
                n_neighbors=_pn, min_dist=_pd, n_components=2,
                metric=UMAP_METRIC, random_state=UMAP_RANDOM_STATE,
            ).fit_transform(combined)
            _pnx, _pny = _normalise_2d(_pts)
            _proj_data[_key] = [[round(float(_pnx[i]), 1), round(float(_pny[i]), 1)] for i in range(len(df))]
            print("✓")
        except Exception as _pe:
            print(f"✗ {_pe}")

_PROJ_JSON = NEW_ROOT / "db" / "umap_projections.json"
atomic_write_json(_PROJ_JSON, {
    "generated":         _dt.now().isoformat(timespec="seconds"),
    "n_neighbors_options": _PROJ_N,
    "min_dist_options":  _PROJ_D,
    "paths":             _proj_paths,
    "projections":       _proj_data,
})
print(f"  ✓ {_PROJ_JSON}  ({len(_proj_data)} projections)")

# ── 5a.7: tags.json — auto-computed tag assignments for the web frontend ─────
# Genre comes from MAEST genre scores (more accurate than the DB genre column).
# Mood, danceability, vocal/instrumental come from Essentia analysis columns.
# Users without Rekordbox (who skip pipeline_03) still get tags in the frontend.
print("\n── Writing tags.json (auto-computed tags) ──")

_conn_t = sqlite3.connect(str(NEW_DB))
_tag_extra = {r[0]: {"mood_happy": r[1], "danceability": r[2], "party_score": r[3], "vocals_prob": r[4]}
              for r in _conn_t.execute(
                  "SELECT path, mood_happy, danceability, party_score, vocals_prob FROM tracks WHERE path IS NOT NULL"
              ).fetchall()}
_conn_t.close()

_path_to_maest      = dict(zip(df["path"].values, df["top_genre"].values))
_path_to_maest_list = dict(zip(df["path"].values, df["genre_list"].tolist()))

_MOOD_THRESH = 0.55
_DANCE_HI    = 0.65
_DANCE_LO    = 0.30
_VOCAL_HI    = 0.65
_VOCAL_LO    = 0.30
_PARTY_HI    = 0.75

_track_tags = {}
for _, _r in df.iterrows():
    _path    = _r["path"]
    _bpm_v   = float(_r["bpm"] or 0)
    _ex      = _tag_extra.get(_path, {})
    _mood_agg = float(_r["mood_aggressive"] or 0)
    _mood_hap = float(_ex.get("mood_happy") or 0)
    _mood_rel = float(_r["mood_relaxed"] or 0)
    _mood_sad = float(_r["mood_sad"] or 0)
    _mood_par = float(_r["mood_party"] or 0)
    _dance    = float(_ex.get("danceability") or 0)
    _party    = float(_ex.get("party_score") or 0)
    _vocals   = float(_ex.get("vocals_prob") or 0)

    _mood_tags = []
    if _mood_agg > _MOOD_THRESH: _mood_tags.append("Aggressive")
    if _mood_hap > _MOOD_THRESH: _mood_tags.append("Euphoric")
    if _mood_rel > _MOOD_THRESH: _mood_tags.append("Deep")
    if _mood_sad > _MOOD_THRESH: _mood_tags.append("Melancholic")
    if _mood_par > _MOOD_THRESH: _mood_tags.append("Uplifting")

    _dance_tags = []
    if _dance > _DANCE_HI:
        _dance_tags.append("Driving" if _bpm_v > 132 else "Groovy")
    elif _dance < _DANCE_LO:
        _dance_tags.append("Chill")
    if _party > _PARTY_HI:
        _dance_tags.append("Peak")

    _comp_tags = []
    if _vocals > _VOCAL_HI:    _comp_tags.append("Vocal")
    elif _vocals < _VOCAL_LO:  _comp_tags.append("Instrumental")

    _track_tags[_path] = {
        "genre":        _path_to_maest_list.get(_path) or [_path_to_maest.get(_path, _r["genre"] or "")],
        "mood":         _mood_tags,
        "danceability": _dance_tags,
        "components":   _comp_tags,
    }

_tags_payload = {
    "generated": _dt.now().isoformat(timespec="seconds"),
    "n_tracks":  len(_track_tags),
    "tracks":    _track_tags,
}
TAGS_JSON.parent.mkdir(parents=True, exist_ok=True)
atomic_write_json(TAGS_JSON, _tags_payload)
print(f"  ✓ {TAGS_JSON}  ({len(_track_tags)} tracks)")

# ── 5b: Build final cluster summaries ────────────────────────────────────────
max_r1_id = max(cs["id"] for cs in cluster_summaries) if cluster_summaries else -1

cluster_summaries = []
for cid in sorted(set(df["cluster"].values)):
    if cid == -1: continue
    mask           = df["cluster"].values == cid
    size           = int(mask.sum())
    cluster_genres = df["top_genre"].values[mask]
    top_g, top_n   = Counter(cluster_genres).most_common(1)[0]
    purity         = top_n / size
    bpms           = bpm_arr[mask]
    bpms           = bpms[bpms > 0]
    cluster_summaries.append({
        "id":       cid,
        "size":     size,
        "top_genre":top_g,
        "purity":   purity,
        "bpm_lo":   int(bpms.min())        if len(bpms) else 0,
        "bpm_hi":   int(bpms.max())        if len(bpms) else 0,
        "bpm_med":  float(np.median(bpms)) if len(bpms) else 0.0,
        "is_new":   cid > max_r1_id,
    })

cluster_summaries.sort(key=lambda x: x["size"], reverse=True)

new_clusters = [c for c in cluster_summaries if c["is_new"]]
if new_clusters:
    print(f"\n  Round-2 clusters ({len(new_clusters)}):")
    for c in sorted(new_clusters, key=lambda x: x["size"], reverse=True)[:15]:
        print(f"    size={c['size']:>4}  "
              f"genre={c['top_genre']:<20}  "
              f"purity={c['purity']*100:.0f}%  "
              f"BPM={c['bpm_lo']}–{c['bpm_hi']}")


# ── 5c: Playlist helpers ──────────────────────────────────────────────────────
def _playlist_name(cs):
    bpm_lo_r = int(round(cs["bpm_lo"] / 2) * 2)
    bpm_hi_r = int(round(cs["bpm_hi"] / 2) * 2)
    flag     = " !" if cs["purity"] < 0.5 else ""
    r2_flag  = " [R2]" if cs["is_new"] else ""
    return f"{cs['top_genre']} — {bpm_lo_r}–{bpm_hi_r} BPM [{cs['size']}]{flag}{r2_flag}"

def _safe_name(name):
    for ch in ('/', '\\', ':', '*', '?', '"', '<', '>', '|', '!'):
        name = name.replace(ch, '-')
    return name.strip()


# ── 5d: Write playlists ───────────────────────────────────────────────────────
written  = 0
missing  = []

for cs in cluster_summaries:
    cid     = cs["id"]
    mask    = df["cluster"].values == cid
    subset  = df[mask].copy().sort_values("bpm")
    name    = _playlist_name(cs)
    out_dir = TMP_NOISE if cs["is_new"] else TMP_PLAYLIST
    out_path = out_dir / f"{_safe_name(name)}.m3u8"

    with out_path.open("w", encoding="utf-8") as f:
        f.write("#EXTM3U\n")
        f.write(f"# Genre: {cs['top_genre']}  "
                f"Purity: {cs['purity']*100:.0f}%  "
                f"BPM: {cs['bpm_lo']}–{cs['bpm_hi']}"
                f"{'  [Round-2]' if cs['is_new'] else ''}\n")
        for path in subset["path"]:
            resolved = _resolve(path)
            if resolved:
                f.write(resolved + "\n")
            else:
                missing.append(path)
    written += 1

# Truly unclassified tracks
truly_noise = df[df["cluster"] == -1]["path"].tolist()
if truly_noise:
    unc_dir = TMP_PLAYLIST / "_unclassified"
    unc_dir.mkdir(exist_ok=True)

    # Group unclassified by genre
    unc_df = df[df["cluster"] == -1].copy()
    for genre, group in unc_df.groupby("top_genre"):
        unc_path = unc_dir / f"{_safe_name(genre)} [{len(group)}].m3u8"
        with unc_path.open("w", encoding="utf-8") as f:
            f.write("#EXTM3U\n")
            for path in group.sort_values("bpm")["path"]:
                resolved = _resolve(path)
                if resolved:
                    f.write(resolved + "\n")
                else:
                    missing.append(path)

# Missing files log
if missing:
    mp_path = LOG_DIR / f"missing_files [{len(missing)}].txt"
    with mp_path.open("w", encoding="utf-8") as f:
        f.write("# Files referenced in DB but not found on disk\n")
        f.write("# These may have been moved or deleted\n")
        for p in missing:
            f.write(str(p) + "\n")
    print(f"\n  !! {len(missing)} files not found on disk → {mp_path.name}")

# Atomic swap — same-directory renames, with a recoverable backup. If the
# second rename ever fails, the previous playlists survive in <name>.bak.
for tmp, live in [(TMP_PLAYLIST, PLAYLIST_DIR), (TMP_NOISE, NOISE_PLAYLIST_DIR)]:
    bak = live.with_name(live.name + ".bak")
    if bak.exists(): shutil.rmtree(bak)
    if live.exists(): os.rename(str(live), str(bak))   # move old aside (atomic)
    os.rename(str(tmp), str(live))                      # promote new (atomic)
    if bak.exists(): shutil.rmtree(bak)                 # only after success
logging.info("Playlist dirs atomically swapped")

# Summary
total_r1      = len([c for c in cluster_summaries if not c["is_new"]])
total_r2      = len(new_clusters)
n_classified  = sum(c["size"] for c in cluster_summaries)

print(f"\n  ✓ {written} playlists written")
print(f"    Round 1: {total_r1} clusters → {PLAYLIST_DIR}")
print(f"    Round 2: {total_r2} clusters → {NOISE_PLAYLIST_DIR}")
print(f"    Unclassified: {len(truly_noise)} "
      f"({100*len(truly_noise)/len(df):.1f}%)")
print(f"    Coverage: {n_classified}/{len(df)} tracks "
      f"({100*n_classified/len(df):.1f}%)")

sizes     = [c["size"] for c in cluster_summaries]
in_target = sum(1 for s in sizes if TARGET_MIN <= s <= TARGET_MAX)
print(f"    In {TARGET_MIN}–{TARGET_MAX} range: "
      f"{in_target}/{len(sizes)} clusters "
      f"({100*in_target/len(sizes):.0f}%)")

genre_counts = Counter(cs["top_genre"] for cs in cluster_summaries)
print(f"\n  Genre distribution:")
for genre, count in genre_counts.most_common(15):
    print(f"    {count:>3}  {'█'*min(count,20):<20}  {genre}")


# ══════════════════════════════════════════════════════════════════════════════
#%% Cell 6: 2D Visualisation (optional)
# ══════════════════════════════════════════════════════════════════════════════

if not _args.skip_viz:
    try:
        import matplotlib
        import matplotlib.pyplot as plt
        import matplotlib.cm as cm

        print("\n── Cell 6: 2D visualisation ──")
        print("  Using 2D layout from clusters.json step...")

        # X_2d already computed above (clusters.json step) — reuse it.

        fig, axes = plt.subplots(1, 2, figsize=(18, 8))

        # Left: coloured by cluster
        ax      = axes[0]
        fin_labels = df["cluster"].values
        noise_mask = fin_labels == -1
        n_cls      = max(fin_labels) + 1
        cmap       = matplotlib.colormaps.get_cmap("tab20").resampled(min(n_cls, 20))

        ax.scatter(X_2d[noise_mask, 0], X_2d[noise_mask, 1],
                c="lightgrey", s=3, alpha=0.3, label="Noise", zorder=1)
        for cid in range(n_cls):
            m = fin_labels == cid
            if m.sum() == 0: continue
            ax.scatter(X_2d[m, 0], X_2d[m, 1],
                    c=[cmap(cid % 20)], s=6, alpha=0.6, zorder=2)

        ax.set_title(f"Clusters (mcs={chosen_mcs}, ms={chosen_ms})\n"
                    f"{final_stats['n_clusters']} clusters  "
                    f"{final_stats['noise_pct']*100:.0f}% noise", fontsize=11)
        ax.set_xlabel("UMAP-1"); ax.set_ylabel("UMAP-2")
        ax.set_facecolor("#f8f8f8")

        # Right: coloured by genre
        ax2        = axes[1]
        genre_list = sorted(set(df["top_genre"]))
        genre_cmap = matplotlib.colormaps.get_cmap("tab20").resampled(len(genre_list))
        for i, genre in enumerate(genre_list):
            m = df["top_genre"].values == genre
            ax2.scatter(X_2d[m, 0], X_2d[m, 1],
                        c=[genre_cmap(i)], s=4, alpha=0.5,
                        label=genre if m.sum() > 100 else "_")

        ax2.set_title("Top taxonomy genre per track", fontsize=11)
        ax2.set_xlabel("UMAP-1")
        ax2.set_facecolor("#f8f8f8")
        ax2.legend(fontsize=7, markerscale=2,
                bbox_to_anchor=(1.01, 1), loc="upper left")

        plt.suptitle(f"Music library — {len(df)} tracks  "
                    f"({len(df)} tracks)", fontsize=13)
        plt.tight_layout()

        plot_path = LOG_DIR / "cluster_plot.png"
        plt.savefig(plot_path, dpi=150, bbox_inches="tight")
        print(f"  Saved: {plot_path}")
        # plt.show()

    except ImportError:
        print("  matplotlib not available — skipping visualisation.")
    except Exception as e:
        print(f"  Visualisation error: {e}")

print("\n✓ Pipeline 02 complete.")
print("  Next: pipeline_03_rb_tag.py")
