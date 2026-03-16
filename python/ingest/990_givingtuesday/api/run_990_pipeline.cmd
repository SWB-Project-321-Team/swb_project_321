@echo off
REM Run 990 pipeline (01 -> 02 -> 03 -> 04) from repo root.
REM Usage: from repo root, run:  python\ingest\990_givingtuesday\api\run_990_pipeline.cmd
setlocal
set "REPO_ROOT=%~dp0..\..\..\.."
cd /d "%REPO_ROOT%"
if errorlevel 1 (echo Failed to cd to repo root. & exit /b 1)

echo [01] fetch_zip_to_county
python python/ingest/990_givingtuesday/api/01_fetch_zip_to_county.py
if errorlevel 1 (echo Step 01 failed. & exit /b 1)

echo [02] fetch_bmf
python python/ingest/990_givingtuesday/api/02_fetch_bmf.py
if errorlevel 1 (echo Step 02 failed. & exit /b 1)

echo [03] build_ein_list
python python/ingest/990_givingtuesday/api/03_build_ein_list.py
if errorlevel 1 (echo Step 03 failed. & exit /b 1)

echo [04] fetch_990_givingtuesday
python python/ingest/990_givingtuesday/api/04_fetch_990_givingtuesday.py
if errorlevel 1 (echo Step 04 failed. & exit /b 1)

echo Done.
endlocal
