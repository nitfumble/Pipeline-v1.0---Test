<#
.SYNOPSIS
  Provisions everything STACKS needs on a fresh Windows machine: WSL, a
  Python 3.11 environment inside it, and the two project venvs. Runs BEFORE
  Python exists on the machine, so this has to be PowerShell, not Python.

.DESCRIPTION
  Called by installer/stacks.iss during setup, and safe to run standalone
  from an elevated PowerShell prompt while developing/testing it (that's the
  point of keeping this logic out of Pascal Script).

  Every phase here is idempotent -- re-running this script (e.g. after the
  one unavoidable WSL reboot) just re-checks reality and continues from
  wherever it actually got to, rather than tracking its own "I did step N"
  state. That avoids a second source of truth that could drift from what's
  actually installed.

  Progress is reported by repeatedly overwriting a status file the Inno
  Setup wizard polls:
      <StateDir>\status.txt   "<percent>|<phase text>"           (live)
      <StateDir>\done.txt     "OK" | "REBOOT" | "FAIL|<message>"  (final)

.PARAMETER AppDir
  The installed STACKS directory (Inno Setup's {app}) -- where
  installer/bootstrap_env.sh and the rest of the repo live.

.PARAMETER StateDir
  Where to write status.txt / done.txt. Defaults to a per-machine location
  so it survives the reboot that triggered this script's own re-invocation.
#>
param(
    [Parameter(Mandatory = $true)][string]$AppDir,
    # The original downloaded Setup.exe (Inno's {srcexe}) -- survives the one
    # possible reboot, unlike Inno's self-extracted temp copy, so RunOnce can
    # point back at it reliably.
    [string]$SrcExe = $AppDir,
    [string]$StateDir = "$env:ProgramData\STACKS"
)

$ErrorActionPreference = 'Stop'
New-Item -ItemType Directory -Force -Path $StateDir | Out-Null
$StatusFile = Join-Path $StateDir 'status.txt'
$DoneFile   = Join-Path $StateDir 'done.txt'
Remove-Item -Force -ErrorAction SilentlyContinue $DoneFile

function Write-Status([int]$Percent, [string]$Phase) {
    "$Percent|$Phase" | Set-Content -Path $StatusFile -Encoding UTF8
}

function Write-Done([string]$Value) {
    $Value | Set-Content -Path $DoneFile -Encoding UTF8
}

function Fail([string]$Message) {
    Write-Done "FAIL|$Message"
    exit 1
}

# ── Phase 1: WSL itself ──────────────────────────────────────────────────────
Write-Status 5 'Checking for WSL...'

function Test-WslReady {
    if (-not (Get-Command wsl.exe -ErrorAction SilentlyContinue)) { return $false }
    # A registered distro, and it's actually runnable (not just the shim present).
    $distros = (wsl.exe -l -q 2>$null) -join ''
    if ([string]::IsNullOrWhiteSpace($distros)) { return $false }
    wsl.exe -u root -- true 2>$null
    return ($LASTEXITCODE -eq 0)
}

if (-not (Test-WslReady)) {
    Write-Status 10 'Installing WSL (this is the one step that may need a restart)...'
    # --no-launch: we drive everything ourselves afterwards; we don't want
    # the distro's interactive first-run (username/password) prompt at all --
    # every command below runs explicitly as root instead, so no named Linux
    # user is ever required.
    Start-Process -FilePath 'wsl.exe' -ArgumentList '--install', '--no-launch' -Wait -NoNewWindow

    if (-not (Test-WslReady)) {
        # Needs the pending reboot to finish enabling the WSL/VM platform
        # Windows features. Register this exact installer to silently
        # re-run at next logon and pick up where it left off (every phase
        # here just re-checks reality, so the resumed run naturally skips
        # whatever's already done).
        $runOnceKey = 'HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\RunOnce'
        New-ItemProperty -Path $runOnceKey -Name 'STACKSResume' -PropertyType String `
            -Value "`"$SrcExe`" /SILENT" -Force | Out-Null
        Write-Done 'REBOOT'
        exit 0
    }
}

# ── Phase 2: Ubuntu packages (python3.11, venv, fpcalc) ─────────────────────
Write-Status 30 'Installing Python 3.11 and audio tools inside WSL...'

$aptInstall = @'
set -e
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
if ! apt-get install -y -qq python3.11 python3.11-venv libchromaprint-tools 2>/dev/null; then
    # Some Ubuntu releases (e.g. 24.04, which defaults to 3.12) don't carry
    # python3.11 in their default repos -- deadsnakes backports it reliably.
    apt-get install -y -qq software-properties-common
    add-apt-repository -y ppa:deadsnakes/ppa
    apt-get update -qq
    apt-get install -y -qq python3.11 python3.11-venv libchromaprint-tools
fi
'@

wsl.exe -u root -- bash -c $aptInstall
if ($LASTEXITCODE -ne 0) { Fail 'Could not install python3.11 / libchromaprint-tools inside WSL (see WSL output above).' }

# ── Phase 3: venvs + pip deps (shared with start.sh) ────────────────────────
Write-Status 55 'Setting up the Python environments (this can take a few minutes)...'

$wslAppDir = (wsl.exe wslpath "$AppDir").Trim()
if (-not $wslAppDir) { Fail "Could not translate '$AppDir' to a WSL path." }

wsl.exe -u root -- bash -c "cd '$wslAppDir' && bash installer/bootstrap_env.sh" | Out-Null
if ($LASTEXITCODE -ne 0) { Fail 'Setting up the Python environments failed (see log above).' }

# ── Phase 4: large assets (ML models, slskd) ────────────────────────────────
Write-Status 85 'Downloading ML models and slskd...'
& (Join-Path $PSScriptRoot 'Fetch-Assets.ps1') -AppDir $AppDir -StatusCallback {
    param($pct, $msg) Write-Status (85 + [int]($pct * 0.12)) $msg
}
if ($LASTEXITCODE -ne 0) { Fail 'Downloading required assets failed -- check your internet connection and retry.' }

# ── Phase 5: verify ──────────────────────────────────────────────────────────
Write-Status 98 'Verifying the install...'
wsl.exe -u root -- bash -c "cd '$wslAppDir' && VENV=\$(bash installer/bootstrap_env.sh) && \$VENV/bin/python check_env.py --all"
if ($LASTEXITCODE -ne 0) { Fail 'Dependency verification failed after install (check_env.py reported missing packages).' }

Write-Status 100 'Done.'
Write-Done 'OK'
