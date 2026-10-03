<#
.SYNOPSIS
  Downloads the large assets STACKS needs that aren't checked into git:
  the Essentia/TensorFlow ML model weights and the slskd binary.

.DESCRIPTION
  Pulled out of git on purpose (see ../.gitignore) -- they're ~425MB
  combined and don't belong in source control. Instead they're attached to
  a GitHub Release on this repo as two zips and fetched here at install
  time.

  NOTE: $ReleaseBaseUrl below points at a release that doesn't exist yet.
  Update the tag once it's published (see installer/README.md for how the
  release is prepared -- model files only, "song_embeddings.pkl" is dev
  cache data and is deliberately NOT included).

.PARAMETER AppDir
  The installed STACKS directory (Inno Setup's {app}).

.PARAMETER StatusCallback
  Optional scriptblock invoked as StatusCallback($percent0to100, $message)
  so the caller (Setup-Prerequisites.ps1) can relay progress to the wizard.
#>
param(
    [Parameter(Mandatory = $true)][string]$AppDir,
    [scriptblock]$StatusCallback = { param($pct, $msg) }
)

$ErrorActionPreference = 'Stop'

$ReleaseBaseUrl = 'https://github.com/nitfumble/Pipeline-v1.0---Test/releases/download/v1.0-assets'
$Assets = @(
    @{ Name = 'models.zip';                   ExtractTo = Join-Path $AppDir 'models' }
    @{ Name = 'slskd-0.25.1-win-x64.zip';      ExtractTo = Join-Path $AppDir 'slskd' }
)

function Get-DirSizeOk([string]$Dir, [long]$MinBytes) {
    if (-not (Test-Path $Dir)) { return $false }
    $size = (Get-ChildItem -Recurse -File -Path $Dir -ErrorAction SilentlyContinue | Measure-Object -Sum Length).Sum
    return ($size -ge $MinBytes)
}

# Cheap idempotency check: if the target already looks populated (e.g. a
# re-run after the WSL reboot, or re-running the installer to repair),
# don't re-download ~425MB for nothing.
if ((Get-DirSizeOk (Join-Path $AppDir 'models') 300000000) -and (Test-Path (Join-Path $AppDir 'slskd\slskd.exe'))) {
    & $StatusCallback 100 'Assets already present.'
    exit 0
}

$tempDir = Join-Path $env:TEMP 'stacks-assets'
New-Item -ItemType Directory -Force -Path $tempDir | Out-Null

$count = $Assets.Count
for ($i = 0; $i -lt $count; $i++) {
    $asset = $Assets[$i]
    $basePct = [int](($i / $count) * 100)
    $url = "$ReleaseBaseUrl/$($asset.Name)"
    $zipPath = Join-Path $tempDir $asset.Name

    & $StatusCallback $basePct "Downloading $($asset.Name)..."
    try {
        Invoke-WebRequest -Uri $url -OutFile $zipPath -UseBasicParsing
    } catch {
        Write-Error "Failed to download $url : $_"
        exit 1
    }

    & $StatusCallback ($basePct + [int](50 / $count)) "Extracting $($asset.Name)..."
    New-Item -ItemType Directory -Force -Path $asset.ExtractTo | Out-Null
    try {
        Expand-Archive -Path $zipPath -DestinationPath $asset.ExtractTo -Force
    } catch {
        Write-Error "Failed to extract $zipPath : $_"
        exit 1
    }
    Remove-Item -Force -ErrorAction SilentlyContinue $zipPath
}

& $StatusCallback 100 'Assets ready.'
exit 0
