# 990 pipeline

Run these scripts **in order** from the repo root to fetch Form 990 data for organizations in the 18 benchmark counties (GEOID_reference.xlsx).

| Order | Script | What it does |
|-------|--------|---------------|
| 01 | `01_fetch_zip_to_county.py` | Downloads ZIP→county FIPS crosswalk → `01_data/reference/zip_to_county_fips.csv` |
| 02 | `02_fetch_bmf.py` | Downloads IRS EO BMF by state (SD, MN, MT, AZ) → `01_data/raw/irs_bmf/` |
| 03 | `03_build_ein_list.py` | Builds EIN list from GEOID ref + zip-to-county + BMF → `01_data/reference/eins_in_benchmark_regions.csv` |
| 04 | `04_fetch_990_givingtuesday.py` | Fetches 990 (Basic 120) from GivingTuesday API → `01_data/raw/givingtuesday_990/` |

**Run the whole pipeline (Windows):**

- **Command Prompt or PowerShell:**  
  From repo root: `python\ingest\990_givingtuesday\run_990_pipeline.cmd`  
  With force on step 04: `python\ingest\990_givingtuesday\run_990_pipeline_force.cmd`
- **PowerShell:**  
  From the `990_givingtuesday` folder: `.\run_990_pipeline.ps1` or `.\run_990_pipeline_force.ps1`

**Or run steps by hand (from repo root):**

```powershell
cd "C:\Users\eilke\Desktop\Github Repo\swb_project_321"
python python/ingest/990_givingtuesday/01_fetch_zip_to_county.py
python python/ingest/990_givingtuesday/02_fetch_bmf.py
python python/ingest/990_givingtuesday/03_build_ein_list.py
python python/ingest/990_givingtuesday/04_fetch_990_givingtuesday.py
```

(In PowerShell use `;` instead of `&&` if you put commands on one line.)

See `docs/990_data_fetch_plan.md` for details. Step 04 supports `--force` to re-fetch all EINs and skips EINs that already have a response file by default (resume).
