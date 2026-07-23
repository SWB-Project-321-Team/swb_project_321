@echo off
REM Run 990 pipeline with --force on step 04 (re-fetch all EINs from API).
setlocal
set "REPO_ROOT=%~dp0..\..\..\.."
cd /d "%REPO_ROOT%"
if errorlevel 1 (echo Failed to cd to repo root. & exit /b 1)

echo [01] fetch_zip_to_county
python python/ingest/givingtuesday_990/api/01_fetch_zip_to_county.py
if errorlevel 1 (echo Step 01 failed. & exit /b 1)

echo [02] fetch_bmf
python python/ingest/givingtuesday_990/api/02_fetch_bmf.py
if errorlevel 1 (echo Step 02 failed. & exit /b 1)

echo [03] build_ein_list
python python/ingest/givingtuesday_990/api/03_build_ein_list.py
if errorlevel 1 (echo Step 03 failed. & exit /b 1)

echo [04] fetch_givingtuesday_990 --force
python python/ingest/givingtuesday_990/api/04_fetch_givingtuesday_990.py --force
if errorlevel 1 (echo Step 04 failed. & exit /b 1)

echo Done.
endlocal
