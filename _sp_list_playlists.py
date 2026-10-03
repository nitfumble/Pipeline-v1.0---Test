#!/usr/bin/env python3
"""Fetch all Spotify playlists and print as JSON. Used by the web UI playlist picker.

Exits 0 with JSON array on success.
Exits 1 with JSON {"error": ..., "message": ...} on stderr if auth is missing.
Never opens a browser — only works with a pre-cached Spotify token.
"""
import json, sys
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent))

from config import load
import spotipy as sp
from spotipy.oauth2 import SpotifyOAuth
from spotipy.cache_handler import CacheFileHandler

cfg     = load()
secrets = cfg.secrets.get("spotify", {})

auth_manager = SpotifyOAuth(
    client_id     = secrets["client_id"],
    client_secret = secrets["client_secret"],
    scope         = cfg.spotify.scopes,
    redirect_uri  = cfg.spotify.redirect_uri,
    cache_handler = CacheFileHandler(cache_path=str(cfg.spotify.token_cache)),
    open_browser  = False,
)

# Only use the cached token — never open a browser or prompt
token_info = auth_manager.get_cached_token()
if not token_info:
    print(json.dumps({
        "error": "no_token",
        "message": "No Spotify auth token cached. Run pipeline_00_download.py from a terminal once to complete the OAuth flow.",
    }), file=sys.stderr)
    sys.exit(1)

# Refresh silently if expired (uses refresh_token, no browser needed)
if auth_manager.is_token_expired(token_info):
    try:
        token_info = auth_manager.refresh_access_token(token_info["refresh_token"])
    except Exception as e:
        print(json.dumps({"error": "refresh_failed", "message": str(e)}), file=sys.stderr)
        sys.exit(1)

client = sp.Spotify(auth=token_info["access_token"])

out, offset = {}, 0
while True:
    page = client.current_user_playlists(limit=50, offset=offset)
    for p in (page.get("items") or []):
        out[p["name"]] = p["id"]
    if not page.get("next"):
        break
    offset += 50

print(json.dumps([{"name": n, "id": i} for n, i in out.items()]))
