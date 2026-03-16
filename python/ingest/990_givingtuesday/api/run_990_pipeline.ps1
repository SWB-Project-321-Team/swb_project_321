# Run 990 pipeline (01 -> 02 -> 03 -> 04) from repo root.
# Usage in PowerShell: .\run_990_pipeline.ps1   (from this folder)
#                      or from repo root: .\python\ingest\990_givingtuesday\api\run_990_pipeline.ps1
$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent (Split-Path -Parent (Split-Path -Parent (Split-Path -Parent $PSScriptRoot)))
Set-Location $RepoRoot

Write-Host "[01] fetch_zip_to_county"
python python/ingest/990_givingtuesday/api/01_fetch_zip_to_county.py
if (-not $?) { exit 1 }

Write-Host "[02] fetch_bmf"
python python/ingest/990_givingtuesday/api/02_fetch_bmf.py
if (-not $?) { exit 1 }

Write-Host "[03] build_ein_list"
python python/ingest/990_givingtuesday/api/03_build_ein_list.py
if (-not $?) { exit 1 }

Write-Host "[04] fetch_990_givingtuesday"
python python/ingest/990_givingtuesday/api/04_fetch_990_givingtuesday.py
if (-not $?) { exit 1 }

Write-Host "Done."
