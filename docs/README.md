# docs/

Project documentation (methodology, data sources, structure, and decisions). Lives in the repo with the code.

## Core (top-level)

| Doc | Description |
|-----|--------------|
| [methodology.md](methodology.md) | Analytical approach, definitions, and assumptions. |
| [data_sources.md](data_sources.md) | Data sources, provenance, and refresh. |
| [benchmark_regions.md](benchmark_regions.md) | How benchmark regions are chosen and where outputs live. |
| [decisions.md](decisions.md) | Architecture and process decisions (ADRs or notes). |
| [project_purpose_methods_data_sources_variables.md](project_purpose_methods_data_sources_variables.md) | Project purpose, scope, deliverables, variable checklist (client-facing overview). |

## By topic

### [onedrive/](onedrive/) — Data location and setup

| Doc | Description |
|-----|--------------|
| [OneDrive_Structure.md](onedrive/OneDrive_Structure.md) | How OneDrive and GitHub fit together; folder layout and medallion rules. |
| [ONEDRIVE_LOCAL_SETUP.md](onedrive/ONEDRIVE_LOCAL_SETUP.md) | Local OneDrive sync and project folder setup. |

### [990/](990/) — Form 990 and IRS pipeline

| Doc | Description |
|-----|--------------|
| [990_irs_teos_s3_plan.md](990/990_irs_teos_s3_plan.md) | IRS TEOS 990 XML → S3 streaming plan; bucket layout; scripts 01–03. |
| [990_data_fetch_plan.md](990/990_data_fetch_plan.md) | Plan to fetch 990 data (GivingTuesday, BMF, EIN list, ZIP–county). |
| [990_api_403_investigation.md](990/990_api_403_investigation.md) | GivingTuesday API 403 investigation and workarounds. |

---

Data-specific docs (e.g. data dictionary, limitations) live in OneDrive `03_docs/` next to the data.
