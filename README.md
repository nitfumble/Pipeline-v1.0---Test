# STACKS

A DJ music library management pipeline: downloads/imports tracks, embeds and
clusters them (Essentia/TensorFlow audio ML), tags and syncs playlists to
Rekordbox, with a small web UI (`app.py`) for browsing and configuring your
library.

## Installing

1. Download `STACKS-Setup.exe` (from the project's Releases page) and run it.
2. The installer checks for WSL and Python 3.11, installs whatever's
   missing, and sets up the project's Python environments automatically.
   - **If WSL isn't installed yet**, Windows needs one restart to finish
     enabling it. The installer tells you when this is about to happen —
     let it restart, log back in, and it picks up automatically where it
     left off. You don't need to re-run anything by hand.
   - First-time setup (installing Python packages, ML libraries, etc.) can
     take several minutes — this is normal, it only happens once.
3. When setup finishes, STACKS opens in your browser and walks you through
   first-run configuration (library location, Rekordbox, Spotify, slskd).

No existing files, accounts, or credentials are bundled in the installer —
you point it at your own music library during that first-run setup.

## Developing locally

If you're working on STACKS itself (this repo), you don't need the
installer — just run:

```
start_stacks.bat
```

from Windows (it hands off to WSL and runs [start.sh](start.sh)), or
`./start.sh` directly from inside WSL. Both manage their own Python venvs
automatically (see [installer/bootstrap_env.sh](installer/bootstrap_env.sh),
shared with the installer so there's one copy of that logic) and reuse them
on subsequent runs.

## Testing the installer

The installer targets a machine that has none of this already installed —
which your own dev machine doesn't, since you're using it daily. Use
**Windows Sandbox** (built into Windows 11 Pro) to test the real first-run
experience without touching your own WSL/Python setup:

1. **One-time**: enable it — *Settings → Apps → Optional features → More
   Windows features → Windows Sandbox* (needs one reboot, same as any
   optional Windows feature).
2. **Each test**: launch Windows Sandbox from the Start menu (it always
   boots clean). Copy the freshly built `installer\output\STACKS-Setup.exe`
   into the sandbox window and run it exactly as a new user would.
   - This is the only reliable way to test the WSL-install → reboot →
     auto-resume path end to end, since a sandbox can actually reboot from
     a clean, WSL-less state — your own machine can't, because it already
     has WSL.
3. Close the sandbox when you're done. It discards everything automatically
   — no cleanup, and nothing it does can affect your real machine.

Build the installer itself with Inno Setup:
```
"C:\Program Files\Inno Setup 7\ISCC.exe" installer\stacks.iss
```
which produces `installer\output\STACKS-Setup.exe`.
