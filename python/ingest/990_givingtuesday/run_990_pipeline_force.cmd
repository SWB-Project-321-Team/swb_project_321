@echo off
REM Run 990 pipeline with --force on step 04 (re-fetch all EINs from API).
setlocal
set "REPO_ROOT=%~dp0..\..\.."
cd /d "%REPO_ROOT%"
if errorlevel 1 (echo Failed to cd to repo root. & exit /b 1)

echo [01] fetch_zip_to_county
python python/ingest/990_givingtuesday/01_fetch_zip_to_county.py
if errorlevel 1 (echo Step 01 failed. & exit /b 1)

echo [02] fetch_bmf
python python/ingest/990_givingtuesday/02_fetch_bmf.py
if errorlevel 1 (echo Step 02 failed. & exit /b 1)

echo [03] build_ein_list
python python/ingest/990_givingtuesday/03_build_ein_list.py
if errorlevel 1 (echo Step 03 failed. & exit /b 1)

echo [04] fetch_990_givingtuesday --force
python python/ingest/990_givingtuesday/04_fetch_990_givingtuesday.py --force
if errorlevel 1 (echo Step 04 failed. & exit /b 1)

echo Done.
endlocal
