# Run 990 pipeline with --force on step 04.
$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent (Split-Path -Parent (Split-Path -Parent $PSScriptRoot))
Set-Location $RepoRoot

Write-Host "[01] fetch_zip_to_county"
python python/ingest/990_givingtuesday/01_fetch_zip_to_county.py
if (-not $?) { exit 1 }

Write-Host "[02] fetch_bmf"
python python/ingest/990_givingtuesday/02_fetch_bmf.py
if (-not $?) { exit 1 }

Write-Host "[03] build_ein_list"
python python/ingest/990_givingtuesday/03_build_ein_list.py
if (-not $?) { exit 1 }

Write-Host "[04] fetch_990_givingtuesday --force"
python python/ingest/990_givingtuesday/04_fetch_990_givingtuesday.py --force
if (-not $?) { exit 1 }

Write-Host "Done."
