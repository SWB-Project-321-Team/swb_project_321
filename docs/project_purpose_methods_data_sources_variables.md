# SWB Project 321 — Purpose, Methods, Data Sources, and Variable Plan

**Client:** Black Hills Area Community Foundation (BHACF)  
**Project:** Black Hills area non-profit landscape review (SWB Project 321)

This document consolidates the project’s **purpose**, **analytical approach**, **data sources**, and a **practical variable checklist** to guide (1) the analysis-ready dataset and (2) the final report.

---

## 1) Purpose of the project

BHACF suspects the Black Hills region has:
- a comparatively **weak culture of philanthropy**,
- a **small donor network**, and
- **financially constrained nonprofit infrastructure**, relative to peer rural communities and neighboring regions.

**Primary objective:** produce **quantitative benchmarking results** the client can use for:
- internal strategy (where to focus fundraising and capacity-building), and
- external communications (fundraising materials and narrative).

The analysis will use **publicly available datasets**, with emphasis on **compilations of federal Form 990 filings**, and will clearly document **assumptions, limitations, and data gaps**.

---

## 2) Scope and definitions

### 2.1 Target geography (Black Hills Area)
The target region is the **7-county** Black Hills Area:
- **Butte**
- **Meade**
- **Lawrence**
- **Pennington**
- **Custer**
- **Oglala Lakota**
- **Fall River**

### 2.2 Benchmarking approach
Select **3–5 benchmark regions** (county-based) with the client. Candidate examples mentioned in project discussions include: **Sioux Falls, Billings, Flagstaff, Missoula**.

### 2.3 What “nonprofit landscape” means (in this project)
Nonprofit landscape is defined as **not-for-profit organizations that file federal 990 forms**, summarized by:
- organization type / field,
- size and budget,
- financial health,
- annual disbursements / spending patterns.

### 2.4 What “donation landscape” means (in this project)
Donation landscape includes (subject to data availability):
- scale of individual giving (proxy measures),
- grant flows (foundation and/or donor-advised fund context),
- public funding inflows (federal awards),
- volunteerism (community engagement proxy).

---

## 3) Deliverables

### Deliverable 1 — Analysis-ready dataset
A package of human-readable files (CSV/Excel, and optionally Parquet for efficiency) containing the cleaned, standardized tables required to generate the report.

### Deliverable 2 — Final report (PDF)
A report that includes:
- data sources used,
- assumptions and limitations,
- statistical methods,
- benchmark comparisons with tables and high-quality figures,
- documented data gaps that may motivate a follow-on project.

---

## 4) Recommended analytical grain and data model

### 4.1 Core grain
**EIN × tax year** is the correct nonprofit filing grain.

All region comparisons happen after aggregating EIN×year to:
- **county × year**, and then
- **region × year** (Black Hills vs benchmarks).

### 4.2 Key join keys
- **EIN** (organization identifier)
- **tax year** (or filing year; choose one and standardize)
- **county_fips** (derived via ZIP→county crosswalk from organization address)
- **region_id** (mapping of counties → “Black Hills” and benchmark regions)

### 4.3 Recommended “core tables” in the analysis-ready dataset
This is the minimal table set that keeps the pipeline reproducible and makes the report easy to regenerate:

1. `dim_org` (organization identity/classification)
2. `fact_990_filing` (filing metadata by EIN×tax_year)
3. `fact_990_financial` (financials by EIN×tax_year)
4. `bridge_org_geo` (EIN×tax_year → ZIP/county_fips/region_id)
5. `fact_region_year_nonprofit` (aggregated nonprofit metrics by region×year)
6. `fact_region_year_giving` (aggregated giving/funding/volunteer proxies by region×year)
7. `dim_region` (region definitions and benchmark groupings)

---

## 5) Methods (end-to-end)

### 5.1 Data acquisition
- Prioritize **GivingTuesday 990 Data Commons** as the primary 990 compilation source (API and tooling).
- Bring in additional sources for:
  - benchmark selection (community typologies/context),
  - denominators (population),
  - inflation adjustment,
  - donation/giving proxies and funding inflows.

### 5.2 Data cleaning and standardization (minimum rules)
Borrowing from the example SD nonprofit-sector report methodology:
- Remove duplicate entries within tax years when present.
- Verify/clean EINs that look invalid or non-local (spot-check edge cases).
- Handle anomalous financial values (e.g., set **negative assets to zero** only if clearly erroneous; otherwise flag).
- Standardize:
  - tax year definitions,
  - units (dollars),
  - geographic mapping approach.

### 5.3 Inflation adjustment
Convert all money variables to **constant dollars** using CPI (select a base year, document it, apply consistently).

### 5.4 Aggregation and benchmarking outputs
For each region and year:
- compute nonprofit “size and scope” indicators,
- compute financial pattern indicators,
- compute giving/funding/volunteer proxies,
- compare Black Hills vs benchmarks.

### 5.5 Statistical comparisons
Use a consistent framework:
- Descriptive statistics + visualization first.
- Then inferential tests where appropriate:
  - **ANOVA** for approximately normal outcomes across regions,
  - **Kruskal–Wallis** for non-normal distributions,
  - Post-hoc tests (e.g., **Tukey** / **Dunn**) to identify which pairs differ.
- Report effect sizes and clearly state limitations due to missingness/small-N.

---

## 6) Data sources (priority-ordered)

### Tier 1 (required to meet project objectives)

1) **Form 990 compilations (primary nonprofit data)**  
**Recommended primary:** GivingTuesday 990 Data Commons  
Use for: EIN×tax_year financial and filing variables, plus parsing tools and API access.

2) **Benchmark selection/context**
- **American Communities Project (ACP)**  
Use for: community typologies and contextual similarity metrics for selecting benchmark regions.
- **ICPSR** (source repository for ACP; useful depending on access)

3) **Population denominators**
- **U.S. Census** (ACS / population estimates)  
Use for: per-capita metrics (nonprofits per 10k residents; revenue per capita; etc.)

4) **Inflation adjustment**
- **BLS CPI** (via BLS or FRED)  
Use for: converting nominal dollars to constant dollars.

### Tier 2 (strongly recommended to address “donation landscape”)

5) **Individual giving proxy**
- **IRS Tax return tabulations by county/state** (itemized charitable contributions where available)  
Use for: region-level proxy for individual giving capacity/behavior.

6) **Federal funding inflows**
- **USASpending.gov**  
Use for: federal awards (grants/contracts) into the region; document filtering choices (exclude municipalities / non-501(c) recipients as needed).

7) **Foundation grant flows**
- **Candid / Foundation Center** (membership may be required)  
Use for: foundation grants to recipients located in the region; document coverage gaps and any manual lookups.

8) **Nonprofit employment and wages**
- **BLS QCEW** (and nonprofit-specific extractions where available)  
Use for: nonprofit employment footprint and wages by county.

9) **Volunteerism**
- **Current Population Survey Volunteer Supplement**  
Use for: volunteer rate and/or volunteer participation proxy (likely at state or larger geography; document granularity limits).

### Tier 3 (optional / exploratory)

10) **DAF context**
- **DAF Research Collaborative (DAFRC) National Study on Donor Advised Funds (2024)**  
Use for: national DAF behavioral context (payout rates, “time-to-grant”, contributions/grants timing, donor demographics).  
Note: typically **not county-resolved**, so best used as contextual framing unless region-identifiable sponsor data is available.

11) **Philanthropy research context**
- **Lilly Family School of Philanthropy / Lake Institute**  
Use for: interpretive context about rural and/or faith-linked giving; may not provide directly joinable county-year variables.

---

## 7) Variable checklist (copy/paste-ready)

### 7.1 Core 990 variables (EIN × tax_year)

#### Keys and metadata
- `ein` (string)
- `tax_year` (int)
- `form_type` (990 / 990-EZ / 990-PF / 990-N if available)
- `filing_date` (date, if available)
- `tax_period_begin`, `tax_period_end` (date, if available)
- `return_type_source` (e.g., GT / other compilation identifier)

#### Organization identity and classification (`dim_org`)
- `org_name`
- `doing_business_as` (optional)
- `ntee_code` (preferred) or closest available classification
- `subsection_code` / `org_type` (public charity vs private foundation when available)
- `ruling_year` (if available)

#### Geography (derived; stored in `bridge_org_geo`)
- `address_line1`, `city`, `state`, `zip`
- `county_fips` (derived from ZIP)
- `county_name` (lookup)
- `region_id` (Black Hills vs benchmark region)
- `region_name`

#### Financials (`fact_990_financial`)
Core “size/budget/disbursements” fields:
- `total_revenue`
- `total_expenses`
- `total_assets`
- `total_liabilities`
- `net_assets` (or `total_assets - total_liabilities`)

Spending mix (recommended):
- `program_service_expenses`
- `management_general_expenses`
- `fundraising_expenses`

Disbursements (if available in compilation):
- `grants_paid` (or closest available)
- `other_assistance_paid` (optional)

Revenue composition (recommended when available):
- `contributions_and_grants`
- `program_service_revenue`
- `investment_income`
- `other_revenue`

Workforce (optional but valuable):
- `num_employees` (if available)
- `total_compensation` (if available)

---

### 7.2 Derived nonprofit metrics (county×year and region×year)

Counts and density:
- `nonprofit_count` (# unique EINs filing)
- `nonprofit_density_per_10k` = nonprofit_count / population * 10,000

Financial aggregates (levels):
- `total_revenue_sum`, `total_expenses_sum`, `total_assets_sum`
- `revenue_per_capita`, `expenses_per_capita`, `assets_per_capita`

Distributional indicators (recommended):
- `median_revenue`, `p25_revenue`, `p75_revenue`, `p90_revenue`
- same for expenses/assets

Financial health proxies:
- `share_operating_surplus` = share with (revenue - expenses) > 0
- `share_positive_net_assets` = share with net_assets > 0

Field composition:
- counts and shares by **NTEE major group** (e.g., arts/culture, health, education, human services, other)

Inflation-adjusted versions:
- apply CPI deflator to all dollar variables (document base year)

---

### 7.3 Benchmark selection variables (ACP / ICPSR / Census)
Goal: support defensible benchmark selection using community similarity.

Minimum:
- `county_fips`
- `population`
- `rurality_proxy` (from ACP typology or Census-based rural measures, depending on availability)
- `median_household_income`
- `poverty_rate`
- `education_attainment` (e.g., % bachelor’s)
- `race_ethnicity_composition` (optional, if relevant and client-approved)
- ACP community type label (if using ACP)

---

### 7.4 Donation / funding landscape variables (region×year)

**IRS giving proxy (tax returns):**
- `charitable_contributions_total`
- `num_returns` (if provided)
- `charitable_contributions_per_capita`

**USASpending federal inflows:**
- `federal_award_total`
- `federal_award_grants_total`
- `federal_award_contracts_total`
- `num_awards`
- `top_awarding_agencies` (for narrative; optional)

**Candid/Foundation Center grants:**
- `foundation_grants_total_to_region`
- `num_foundation_grants`
- `median_grant_size` (optional)
- funder concentration metrics (optional)

**Volunteerism (CPS supplement; likely state-level):**
- `volunteer_rate`
- `volunteer_hours` (if available)

**Nonprofit employment footprint (QCEW):**
- `nonprofit_employment`
- `nonprofit_wages_total`

---

### 7.5 DAF context variables (DAFRC; optional)
Use when regional identifiers exist or for national context framing:
- account size group share (<$50k, $50k–$1M, etc.)
- `contributions_amount`, `grants_amount`, `assets_end_of_year`
- payout rate metrics
- “time-to-grant” / “half-life” style measures described in the report
- donor demographic bands (if available)

---

## 8) Known limitations to document (must be explicit)

1) **Coverage limitations in 990-based data**
- Religious organizations may not be required to register.
- Very small nonprofits may not file 990 series returns.
- Filing thresholds and requirements vary by form series and period.

2) **Geography limitations**
- County assignment depends on address quality and ZIP→county crosswalk accuracy.
- Some organizations may operate across multiple counties; filings usually reflect a primary address.

3) **Donation landscape is partly indirect**
- Donor counts/demographics are not fully observable from 990 filings alone.
- IRS tax return giving is a proxy and may reflect itemizers only (depending on the IRS table used).
- DAFRC data is typically not county-specific.

4) **Comparability across regions**
- Small sample sizes in rural counties can make inference unstable; prioritize effect sizes and confidence intervals plus transparent caveats.

---

## 9) Recommended reporting structure (mirrors proven example pattern)

1) **Size and scope** of nonprofits (counts, density, field mix)  
2) **Financial patterns** (revenues, expenses, assets, surplus shares, per-capita)  
3) **Community philanthropy indicators** (individual giving proxy; foundation flows; federal inflows)  
4) **Volunteerism and economic footprint** (volunteer rate; nonprofit employment/wages)  
5) **Benchmark comparisons** with clear tables/figures and tests where appropriate  
6) **Limitations and data gaps** (explicit, with recommendations)

---

## 10) Appendix: Source list from project materials

**From the SWB Project 321 materials and example report pattern:**
- GivingTuesday 990 Data Commons
- American Communities Project (and ICPSR)
- Candid / Foundation Center (membership may be required)
- USASpending.gov
- BLS QCEW
- CPS Volunteer Supplement
- IRS Tax return county/state tables (charitable contributions proxy)
- CPI (BLS/FRED) for inflation adjustment
- Lilly Family School of Philanthropy / Lake Institute (context)

