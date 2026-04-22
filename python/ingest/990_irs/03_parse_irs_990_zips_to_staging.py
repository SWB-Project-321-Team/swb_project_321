"""
Parse IRS Form 990 TEOS XML from S3 ZIPs and write staging (one row per filing).

Filters by GEOID/county only: keeps only rows whose org address ZIP maps to a county in
GEOID_reference.csv (via zip_to_county_fips.csv). Legacy: --benchmark-zips uses a precomputed
ZIP list when GEOID reference or zip_to_county are missing.

Reads ZIPs from s3://{bucket}/{prefix}/zips/year={YEAR}/*.zip, extracts .xml members,
parses each (EIN, tax_year, form_type, revenue, expenses, assets, org address zip),
filters by GEOID/county (or legacy benchmark ZIPs), joins region,
writes 01_data/staging/filing/irs_990_filings.parquet.
Output includes a source_zip column so --skip-processed can skip already-processed ZIPs on reruns.

Uses same DATA root and bucket/prefix as 01/02. Run from repo root.

Environment: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION.
Optional: IRS_990_S3_BUCKET, IRS_990_S3_PREFIX.

Run from repo root:
  python python/ingest/990_irs/03_parse_irs_990_zips_to_staging.py
  python python/ingest/990_irs/03_parse_irs_990_zips_to_staging.py --skip-processed
"""

import os
import re
import sys
import tempfile
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed as _as_completed
from io import BytesIO
from pathlib import Path
from tqdm import tqdm

# On Windows, tqdm uses stderr by default; PowerShell can treat that as a terminating error.
# Force progress output to stdout so the script runs without NativeCommandError.
_tqdm_kw = {"file": sys.stdout} if sys.platform == "win32" else {}

# Chunk size when streaming S3 body to temp file (avoids holding full ZIP in RAM)
_STREAM_CHUNK = 16 * 1024 * 1024  # 16 MB (larger = fewer read/write syscalls)
# Min size (bytes) to use parallel range download when writing to temp (single get_object otherwise)
_PARALLEL_DOWNLOAD_MIN_BYTES = 20 * 1024 * 1024  # 20 MB

# Prefer lxml for much faster XML parsing (C extension); fall back to stdlib.
try:
    from lxml import etree as ET
    _XML_PARSER = "lxml"
except ImportError:
    import xml.etree.ElementTree as ET  # type: ignore[no-redef]
    _XML_PARSER = "stdlib"

import boto3
import pandas as pd

# Add python/ to path so utils.paths can be imported when script is run from repo root.
_SCRIPT_DIR = Path(__file__).resolve()
_PYTHON_DIR = _SCRIPT_DIR.parents[2]
if str(_PYTHON_DIR) not in sys.path:
    sys.path.insert(0, str(_PYTHON_DIR))
from utils.paths import DATA

# Load secrets/.env so AWS credentials and region are set (same as 00, 02, 03).
_REPO_ROOT = Path(__file__).resolve().parents[3]
_ENV_FILE = _REPO_ROOT / "secrets" / ".env"
if _ENV_FILE.exists():
    with open(_ENV_FILE, encoding="utf-8-sig") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                key, value = key.strip(), value.strip()
                if value and value[0] in ("'", '"') and value[-1] == value[0]:
                    value = value[1:-1]
                if key:
                    os.environ.setdefault(key, value)

# Default paths: GEOID reference (county) + ZIP→county for geography filter; benchmark ZIP list (legacy); staging output.
GEOID_REFERENCE_CSV = DATA / "reference" / "GEOID_reference.csv"
ZIP_TO_COUNTY_CSV = DATA / "reference" / "zip_to_county_fips.csv"
BENCHMARK_ZIPS_CSV = DATA / "reference" / "zip_codes_in_benchmark_regions.csv"
STAGING_FILING_DIR = DATA / "staging" / "filing"
DEFAULT_OUTPUT = STAGING_FILING_DIR / "irs_990_filings.parquet"

# S3 bucket and prefix (overridable via env or --bucket/--prefix).
DEST_BUCKET = os.environ.get("IRS_990_S3_BUCKET", "swb-321-irs990-teos")
PREFIX = os.environ.get("IRS_990_S3_PREFIX", "bronze/irs990/teos_xml")


def _session() -> boto3.Session:
    """Build boto3 session using AWS_DEFAULT_REGION (default us-east-2)."""
    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-2")
    return boto3.Session(region_name=region)


def _normalize_ein(ein: str) -> str:
    """Return EIN as 9-digit string, zero-padded, no hyphen or spaces."""
    s = str(ein).strip().replace("-", "").replace(" ", "")
    return s.zfill(9) if len(s) <= 9 else s[:9]


def _normalize_form_type(raw: str) -> str:
    """Return form_type only if it looks like a 990 return type; else default to 990 (rejects officer names etc.)."""
    s = str(raw).strip()
    if not s or " " in s:
        return "990"
    u = s.upper().replace("-", "")
    if not u.startswith("990"):
        return "990"
    if u == "990":
        return "990"
    if u in ("990EZ", "990PF", "990T", "990N"):
        return u
    if len(u) <= 8 and u[3:].isalpha():
        return u
    return "990"


# Regex for fast EIN extraction from raw XML. Use word boundaries so we match <EIN> or
# <EmployerIdentificationNumber> but not <PreparerFirmEIN> (preparer EIN appears before filer EIN in some returns).
_EIN_TAG_PATTERN = re.compile(
    rb"<[^>]*\b(?:EIN|EmployerIdentificationNumber)\b[^>]*>\s*([0-9\-\s]{8,12})",
    re.IGNORECASE,
)


def _extract_ein_fast(xml_bytes: bytes) -> str | None:
    """Extract EIN from raw XML using regex; returns normalized 9-digit EIN or None. Used to skip full parse when EIN not in filter set."""
    m = _EIN_TAG_PATTERN.search(xml_bytes)
    if not m:
        return None
    raw = m.group(1).decode("ascii", errors="ignore").strip().replace("-", "").replace(" ", "")
    if not raw.isdigit() or len(raw) < 9:
        return None
    return raw.zfill(9) if len(raw) <= 9 else raw[:9]


# Bulk-part ZIP filename pattern (e.g. 2021_TEOS_XML_01A.zip). Excludes per-filing OBJECT_ID-style names (e.g. 2021_TEOS_XML_17903370.zip).
_BULK_PART_FILENAME_PATTERN = re.compile(r"^\d{4}_TEOS_XML_\d{2}[A-Z]?\.zip$", re.IGNORECASE)


def _filter_bulk_part_keys(keys: list[str]) -> list[str]:
    """Keep only S3 keys whose filename looks like a bulk part (01A, 01B, …), not per-filing or error objects."""
    return [k for k in keys if _BULK_PART_FILENAME_PATTERN.match(k.split("/")[-1])]


def _local_tag(tag: str) -> str:
    """Return local part of XML tag (strip namespace)."""
    if tag and "}" in tag:
        return tag.split("}", 1)[1]
    return tag or ""


# Single-pass extraction: map each IRS tag (local name) to (row_key, 'text'|'number').
# First occurrence wins when multiple tags map to the same key.
_TAG_MAP: dict[str, tuple[str, str]] = {}
for _tag in ("EIN", "EmployerIdentificationNumber", "ein"):
    _TAG_MAP[_tag] = ("ein", "text")
for _tag in ("TaxYr", "TaxYear", "TaxPeriodYear", "TaxPeriod"):
    _TAG_MAP[_tag] = ("tax_year", "text")
# Only ReturnType/FormType (and Cd variants in some schemas); Form990PartVIISectionAGrp holds officer names, not return type
for _tag in ("ReturnType", "FormType", "ReturnTypeCd", "FormTypeCd"):
    _TAG_MAP[_tag] = ("form_type", "text")
# Revenue: Part I line 12; prefer CY (current year); IRSx uses CYTotalRevenueAmt, etc.
for _tag in (
    "TotalRevenueCurrentYearAmt", "TotalRevenueAmt", "CYTotalRevenueAmt",
    "PYTotalRevenueAmt", "RevenueAmt", "TotalRevenue", "GrossReceiptsAmt",
    "TotalRevAndExpnssAmt",  # 990-PF: total revenue (sum of revenue lines)
):
    _TAG_MAP[_tag] = ("revenue", "number")
# Expenses: Part I line 18
for _tag in (
    "TotalExpensesAmt", "TotalExpensesCurrentYearAmt", "CYTotalExpensesAmt",
    "PYTotalExpensesAmt", "ExpensesAmt", "TotalExpenses", "TotalFunctionalExpensesAmt",
    "TotalExpensesRevAndExpnssAmt", "TotalExpensesDsbrsChrtblAmt",  # 990-PF
):
    _TAG_MAP[_tag] = ("expenses", "number")
# Assets: Part I line 20 (EOY = end of year); prefer EOY so we don't capture BOY by first-occurrence
# EOYAmt: in DOM path handled parent-aware (Form990TotalAssetsGrp / SumOfTotalLiabilitiesGrp); in regex/iterparse first occurrence -> assets
for _tag in (
    "TotalAssetsEOYAmt", "TotalAssetsEndOfYearAmt", "TotalAssetsAmt",
    "AssetsAmt", "TotalAssets", "TotalAssetsEOYFMVAmt", "FMVAssetsEOYAmt",
    "BookValueAssetsEOYAmt",  # 990-T
    "EOYAmt",  # 990-EZ Grps; DOM uses parent to set assets or total_liabilities
):
    _TAG_MAP[_tag] = ("assets", "number")
# Net assets: Part I line 22 (EOY only so first occurrence is current year)
for _tag in (
    "NetAssetsOrFundBalancesEOYAmt", "TotNetAstOrFundBalancesEOYAmt", "NoDonorRstrNetAssestsEOYAmt",
):
    _TAG_MAP[_tag] = ("net_assets", "number")
# Liabilities: Part I line 21 (EOY only so we don't capture BOY)
for _tag in ("TotalLiabilitiesEOYAmt",):
    _TAG_MAP[_tag] = ("total_liabilities", "number")
# Functional expenses: Part I / Part IX
for _tag in ("TotalProgramServiceExpensesAmt", "ProgramServiceExpensesAmt", "ProgramServicesAmt"):
    _TAG_MAP[_tag] = ("program_service_expenses", "number")
for _tag in ("ManagementAndGeneralExpensesAmt", "ManagementAndGeneralAmt"):
    _TAG_MAP[_tag] = ("management_general_expenses", "number")
for _tag in ("FundraisingExpensesAmt", "CYTotalFundraisingExpenseAmt", "TotalFundraisingExpenseAmt", "FundraisingAmt"):
    _TAG_MAP[_tag] = ("fundraising_expenses", "number")
# Contributions/grants: Part I line 8
for _tag in (
    "ContributionsGiftsGrantsEtcAmt", "ContriRcvdRevAndExpnssAmt",
    "CYContributionsGrantsAmt", "PYContributionsGrantsAmt",
):
    _TAG_MAP[_tag] = ("contributions_grants", "number")
# Program service revenue: Part I line 9
for _tag in ("ProgramServiceRevenueAmt", "CYProgramServiceRevenueAmt", "PYProgramServiceRevenueAmt"):
    _TAG_MAP[_tag] = ("program_service_revenue", "number")
# Grants paid: Part I line 13
for _tag in (
    "GrantsAndSimilarAmountsPaidAmt", "CYGrantsAndSimilarPaidAmt", "PYGrantsAndSimilarPaidAmt",
    "ContriPaidRevAndExpnssAmt", "ContriPaidDsbrsChrtblAmt",
):
    _TAG_MAP[_tag] = ("grants_paid", "number")
# High-value revenue composition (project variable checklist §7.1); Part I lines 10–11
for _tag in ("TotalNetInvstIncmAmt", "NetInvstIncmAmt", "CYInvestmentIncomeAmt", "PYInvestmentIncomeAmt", "NetInvestmentIncomeAmt"):
    _TAG_MAP[_tag] = ("investment_income", "number")
for _tag in ("OtherRevenueAmt", "OthrRevenueAmt", "CYOtherRevenueAmt", "PYOtherRevenueAmt"):
    _TAG_MAP[_tag] = ("other_revenue", "number")
for _tag in ("GovernmentGrantAmt", "GovernmentGrantsAmt", "GovtGrantAmt"):
    _TAG_MAP[_tag] = ("government_grants", "number")
# NTEE / activity classification (when present in XML); Part III and BMF-style tags
for _tag in (
    "ActivityCd", "ActivityCodeTxt", "ActivityCode", "NTEEcd", "NTEE",
    "PrimaryActivityCd", "ProgSrvcAccomActyCd",
):
    _TAG_MAP[_tag] = ("ntee_code", "text")
for _tag in ("ReturnTs",):
    _TAG_MAP[_tag] = ("filing_ts", "text")
for _tag in ("TaxPeriodBeginDt",):
    _TAG_MAP[_tag] = ("tax_period_begin_dt", "text")
for _tag in ("TaxPeriodEndDt",):
    _TAG_MAP[_tag] = ("tax_period_end_dt", "text")
for _tag in ("PrimaryExemptPurposeTxt", "MissionDesc", "ExemptPurposeDesc", "ActivityOrMissionDesc", "DescriptionProgramSrvcAccomTxt"):
    _TAG_MAP[_tag] = ("exempt_purpose_txt", "text")
for _tag in ("Organization501cTypeTxt",):
    _TAG_MAP[_tag] = ("subsection_code", "text")
# Optional: Form 990 Part I / org descriptor (IRS instructions)
for _tag in ("TotalEmployeeCnt", "EmployeeCnt"):
    _TAG_MAP[_tag] = ("employee_cnt", "number")
for _tag in ("FormationYr",):
    _TAG_MAP[_tag] = ("formation_yr", "text")
# Excess (revenue − expenses): 990 line 19, 990-EZ line 18, 990-PF Part I
for _tag in ("ExcessOrDeficitForYearAmt", "CYRevenuesLessExpensesAmt", "ExcessRevenueOverExpensesAmt"):
    _TAG_MAP[_tag] = ("excess_or_deficit", "number")

# Case-insensitive tag -> (row_key, 'text'|'number') for single-pass regex.
_TAG_MAP_LOOKUP = {k.lower(): v for k, v in _TAG_MAP.items()}

# Part I totals: only take from main form body (IRS990/990EZ/990PF/990T) so schedule line items don't override.
_MAIN_FORM_AMOUNTS_ONLY = frozenset({
    "revenue", "expenses", "assets", "net_assets", "total_liabilities",
    "program_service_expenses", "management_general_expenses", "fundraising_expenses",
    "contributions_grants", "program_service_revenue", "grants_paid",
    "investment_income", "other_revenue", "government_grants", "excess_or_deficit",
})

# Single-pass regex: match any known tag (whole-word only to avoid e.g. PreparerFirmEIN), capture (tag, value).
_RE_ANY_TAG = re.compile(
    rb"<[^>]*(?<![a-zA-Z0-9])("
    + b"|".join(re.escape(k.encode("ascii")) for k in _TAG_MAP)
    + rb")(?![a-zA-Z0-9])[^>]*>\s*([^<]+)",
    re.IGNORECASE,
)

# Pre-compiled amount patterns (avoid recompiling in _first_amount_after_tag).
_RE_AMOUNT_REVENUE = re.compile(
    rb"<[^>]*?(?:TotalRevenueCurrentYearAmt|TotalRevenueAmt|CYTotalRevenueAmt|RevenueAmt|TotalRevenue)[^>]*>\s*([0-9,\.]+)",
    re.IGNORECASE | re.DOTALL,
)
_RE_AMOUNT_EXPENSES = re.compile(
    rb"<[^>]*?(?:TotalExpensesAmt|TotalExpensesCurrentYearAmt|CYTotalExpensesAmt|ExpensesAmt|TotalExpenses)[^>]*>\s*([0-9,\.]+)",
    re.IGNORECASE | re.DOTALL,
)
_RE_AMOUNT_ASSETS = re.compile(
    rb"<[^>]*?(?:TotalAssetsEOYAmt|TotalAssetsEndOfYearAmt|TotalAssetsAmt|AssetsAmt|TotalAssets)[^>]*>\s*([0-9,\.]+)",
    re.IGNORECASE | re.DOTALL,
)


def _num(v) -> float | None:
    """Return float if v is int/float, else None."""
    return v if isinstance(v, (int, float)) else None


def _extract_filer_from_element(filer_elem) -> dict[str, str | None]:
    """Extract org name and address from a Filer element (ReturnHeader/Filer). Returns dict with org_name, address_line1, city, state, zip."""
    out: dict[str, str | None] = {
        "org_name": None,
        "address_line1": None,
        "city": None,
        "state": None,
        "zip": None,
    }
    for child in filer_elem.iter():
        local = _local_tag(child.tag)
        text = _elem_text(child)
        if text is None:
            continue
        if local == "BusinessNameLine1Txt":
            # First one under Filer is org name (Filer contains EIN, then BusinessName/BusinessNameLine1Txt)
            if out["org_name"] is None:
                out["org_name"] = text.strip()
        elif local == "AddressLine1Txt" and out["address_line1"] is None:
            out["address_line1"] = text.strip()
        elif local == "CityNm":
            out["city"] = text.strip()
        elif local == "StateAbbreviationCd":
            out["state"] = text.strip()
        elif local == "ZIPCd":
            out["zip"] = text.strip()
    return out


# Columns that are not applicable (expected empty) for certain form types; when empty we set "NA".
_FORM_EXPECTED_NA: dict[str, frozenset[str]] = {
    "990T": frozenset({
        "revenue", "expenses", "net_assets", "total_liabilities",
        "contributions_grants", "program_service_expenses", "program_service_revenue",
        "grants_paid", "investment_income", "other_revenue", "government_grants",
        "excess_or_deficit", "employee_cnt", "formation_yr",
        "fundraising_expenses", "management_general_expenses", "exempt_purpose_txt",
    }),
    "990EZ": frozenset({
        "employee_cnt", "formation_yr",
    }),
    "990PF": frozenset({
        "exempt_purpose_txt", "program_service_expenses", "program_service_revenue",
    }),
}
NA_STRING = "NA"


def _fill_expected_na(row: dict) -> None:
    """For columns that are not applicable for this form_type, set value to NA_STRING when empty."""
    form_type = row.get("form_type")
    if not form_type:
        return
    cols = _FORM_EXPECTED_NA.get(form_type)
    if not cols:
        return
    for col in cols:
        if col not in row:
            continue
        v = row[col]
        is_empty = v is None or (isinstance(v, str) and v.strip() == "")
        if is_empty:
            row[col] = NA_STRING


def _row_from_found(
    found: dict[str, str | float | None], source_name: str = ""
) -> dict:
    """Build one row dict from extracted tag values (ein required)."""
    ein = _normalize_ein(str(found["ein"]))
    tax_yr = found.get("tax_year")
    if isinstance(tax_yr, str):
        if not tax_yr.isdigit() and len(tax_yr) >= 4:
            tax_yr = tax_yr[:4]
    raw_form = str(found.get("form_type") or "990").strip() or "990"
    form_type = _normalize_form_type(raw_form)
    row = {
        "ein": ein,
        "tax_year": int(tax_yr) if tax_yr and str(tax_yr).isdigit() else None,
        "form_type": form_type,
        "filing_ts": _str_or_none(found.get("filing_ts")),
        "tax_period_begin_dt": _str_or_none(found.get("tax_period_begin_dt")),
        "tax_period_end_dt": _str_or_none(found.get("tax_period_end_dt")),
        "org_name": _str_or_none(found.get("org_name")),
        "address_line1": _str_or_none(found.get("address_line1")),
        "city": _str_or_none(found.get("city")),
        "state": _str_or_none(found.get("state")),
        "zip": _normalize_zip(found.get("zip")) or None,
        "exempt_purpose_txt": _str_or_none(found.get("exempt_purpose_txt")),
        "subsection_code": _str_or_none(found.get("subsection_code")),
        "revenue": _num(found.get("revenue")),
        "expenses": _num(found.get("expenses")),
        "assets": _num(found.get("assets")),
        "net_assets": _num(found.get("net_assets")),
        "total_liabilities": _num(found.get("total_liabilities")),
        "program_service_expenses": _num(found.get("program_service_expenses")),
        "management_general_expenses": _num(found.get("management_general_expenses")),
        "fundraising_expenses": _num(found.get("fundraising_expenses")),
        "contributions_grants": _num(found.get("contributions_grants")),
        "program_service_revenue": _num(found.get("program_service_revenue")),
        "grants_paid": _num(found.get("grants_paid")),
        "investment_income": _num(found.get("investment_income")),
        "other_revenue": _num(found.get("other_revenue")),
        "government_grants": _num(found.get("government_grants")),
        "ntee_code": _str_or_none(found.get("ntee_code")),
        "employee_cnt": _num(found.get("employee_cnt")),
        "formation_yr": _str_or_none(found.get("formation_yr")),
        "excess_or_deficit": _num(found.get("excess_or_deficit")),
        "source_file": source_name,
    }
    _fill_expected_na(row)
    return row


def _str_or_none(v) -> str | None:
    """Return stripped string if v is non-empty, else None."""
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def _elem_text(elem) -> str | None:
    """First non-empty text from element, direct children, or grandchildren (for nested amount wrappers)."""
    if elem.text and elem.text.strip():
        return elem.text.strip()
    for c in elem:
        if c.text and c.text.strip():
            return c.text.strip()
    # Some schemas wrap amount in one level of nesting (e.g. USAmountType); check grandchildren only
    for c in elem:
        for g in c:
            if g.text and g.text.strip():
                return g.text.strip()
    return None


def _is_descendant(elem, ancestor, parent_map: dict) -> bool:
    """True if elem is ancestor or a descendant of ancestor."""
    if elem is ancestor:
        return True
    p = parent_map.get(elem)
    while p is not None:
        if p is ancestor:
            return True
        p = parent_map.get(p)
    return False


def _return_header_and_form_body(root):
    """Return (ReturnHeader or None, main form element or None, ReturnData or None). Form body is IRS990, IRS990EZ, IRS990PF, or IRS990T under ReturnData."""
    return_header = None
    return_data = None
    for child in root:
        local = _local_tag(child.tag)
        if local == "ReturnHeader":
            return_header = child
        elif local == "ReturnData":
            return_data = child
    form_body = None
    if return_data is not None:
        for child in return_data:
            if _local_tag(child.tag) in ("IRS990", "IRS990EZ", "IRS990PF", "IRS990T"):
                form_body = child
                break
    return return_header, form_body, return_data


def _extract_990_single_pass(root) -> dict[str, str | float | None] | None:
    """
    One tree walk over the whole XML: collect first occurrence of each needed tag.
    - Filer (org name, address): only from ReturnHeader.
    - Part I totals (revenue, expenses, assets, etc.): only from main form body
      (IRS990/990EZ/990PF/990T) so schedule line items don't override.
    - All other fields (ntee_code, exempt_purpose_txt, subsection_code, formation_yr,
      employee_cnt, filing_ts, tax_year, form_type, etc.): first occurrence anywhere
      in the return (header, main form, or schedules).
    Parent-aware: EOYAmt under SumOfTotalLiabilitiesGrp -> total_liabilities;
    under Form990TotalAssetsGrp -> assets. Prefer non-zero net_assets for 990-PF
    when first TotNetAstOrFundBalancesEOYAmt is 0.
    Returns dict with keys ein, tax_year, form_type, revenue, expenses, assets, ...
    """
    parent_map = {c: p for p in root.iter() for c in p}
    return_header, form_body, return_data = _return_header_and_form_body(root)
    found: dict[str, str | float | None] = {}
    for elem in root.iter():
        local = _local_tag(elem.tag)
        in_header = return_header is not None and _is_descendant(elem, return_header, parent_map)
        in_form = form_body is not None and _is_descendant(elem, form_body, parent_map)
        # Filer (org name, address): only from ReturnHeader
        if local == "Filer":
            if not in_header:
                continue
            for k, v in _extract_filer_from_element(elem).items():
                if k not in found and v is not None:
                    found[k] = v
            continue
        # EOYAmt (assets / total_liabilities): only from main form body
        if local == "EOYAmt":
            if not in_form:
                continue
            parent = parent_map.get(elem)
            parent_local = _local_tag(parent.tag) if parent is not None else None
            text = _elem_text(elem)
            if text is not None:
                text_clean = text.replace(",", "").strip()
                try:
                    val = float(text_clean)
                except ValueError:
                    val = None
                if val is not None:
                    if parent_local == "SumOfTotalLiabilitiesGrp":
                        if "total_liabilities" not in found:
                            found["total_liabilities"] = val
                    elif parent_local == "Form990TotalAssetsGrp":
                        found["assets"] = val
                    elif "assets" not in found:
                        found["assets"] = val
            continue
        # 990/990-EZ: Organization501c3Ind (X); 990-PF: Organization501c3ExemptPFInd (X) → 501(c)(3)
        if local in ("Organization501c3Ind", "Organization501c3ExemptPFInd"):
            text = _elem_text(elem)
            if text and "subsection_code" not in found:
                found["subsection_code"] = "c3"
            continue
        if local not in _TAG_MAP:
            continue
        key, kind = _TAG_MAP[local]
        # Part I totals: only from main form body so schedules don't override
        if key in _MAIN_FORM_AMOUNTS_ONLY and not in_form:
            continue
        # 990-PF: first TotNetAstOrFundBalancesEOYAmt can be 0; allow overwriting with later non-zero
        # 990: prefer MissionDesc (fuller mission) over ActivityOrMissionDesc when both present
        if key in found and not (key == "net_assets" and found[key] == 0) and not (key == "exempt_purpose_txt" and local == "MissionDesc"):
            continue
        text = _elem_text(elem)
        if text is None:
            continue
        if kind == "number":
            text_clean = text.replace(",", "").strip()
            try:
                found[key] = float(text_clean)
            except ValueError:
                found[key] = None
        else:
            found[key] = text
    return found if found.get("ein") else None


# Optional: regex-only extraction (no DOM). Faster but more fragile to schema/format changes.
_RE_TAX_YR = re.compile(
    rb"<[^>]*(?:TaxYr|TaxYear|TaxPeriodYear|TaxPeriod)[^>]*>\s*([0-9]{4})",
    re.IGNORECASE,
)
_RE_FORM_TYPE = re.compile(
    rb"<[^>]*(?:ReturnType|FormType|ReturnTypeCd|FormTypeCd)[^>]*>\s*([^<]+)",
    re.IGNORECASE,
)
_REVENUE_NAMES = rb"TotalRevenueCurrentYearAmt|TotalRevenueAmt|CYTotalRevenueAmt|RevenueAmt|TotalRevenue"
_EXPENSE_NAMES = rb"TotalExpensesAmt|TotalExpensesCurrentYearAmt|CYTotalExpensesAmt|ExpensesAmt|TotalExpenses"
_ASSET_NAMES = rb"TotalAssetsEOYAmt|TotalAssetsEndOfYearAmt|TotalAssetsAmt|AssetsAmt|TotalAssets"


def _first_amount_after_tag(xml_bytes: bytes, tag_pattern: bytes) -> float | None:
    """Find first element whose local name matches tag_pattern and return following amount."""
    pat = re.compile(
        rb"<[^>]*?(?:" + tag_pattern + rb")[^>]*>\s*([0-9,\.]+)",
        re.IGNORECASE | re.DOTALL,
    )
    m = pat.search(xml_bytes)
    if not m:
        return None
    try:
        return float(m.group(1).decode("ascii", errors="ignore").replace(",", "").strip())
    except ValueError:
        return None


def _parse_990_regex_single_pass(xml_bytes: bytes, source_name: str = "") -> dict | None:
    """
    Extract 990 row with one regex pass over xml_bytes (all tags + values). Faster than multi-pass regex.
    Returns None if EIN not found. Used when --fast-parse is set.
    """
    found: dict[str, str | float | None] = {}
    for m in _RE_ANY_TAG.finditer(xml_bytes):
        tag_local = m.group(1).decode("ascii", errors="ignore").strip().lower()
        if tag_local not in _TAG_MAP_LOOKUP:
            continue
        key, kind = _TAG_MAP_LOOKUP[tag_local]
        if key in found and not (key == "net_assets" and found[key] == 0) and not (key == "exempt_purpose_txt" and tag_local == "missiondesc"):
            continue
        val_str = m.group(2).decode("ascii", errors="ignore").strip()
        if kind == "number":
            try:
                found[key] = float(val_str.replace(",", ""))
            except ValueError:
                found[key] = None
        else:
            found[key] = val_str
    # 990/990-EZ: Organization501c3Ind; 990-PF: Organization501c3ExemptPFInd → subsection_code c3
    if "subsection_code" not in found:
        m501 = re.search(
            rb"<[^>]*(?:Organization501c3Ind|Organization501c3ExemptPFInd)[^>]*>\s*([^<]+)",
            xml_bytes, re.IGNORECASE
        )
        if m501 and m501.group(1).strip():
            found["subsection_code"] = "c3"
    if "ein" not in found or not found["ein"]:
        return None
    return _row_from_found(found, source_name)


def _parse_990_regex_only(xml_bytes: bytes, source_name: str = "") -> dict | None:
    """
    Extract 990 row using only regex (multi-pass). Prefer _parse_990_regex_single_pass for --fast-parse.
    Returns None if EIN not found or parsing fails.
    """
    return _parse_990_regex_single_pass(xml_bytes, source_name)


def _parse_990_iterparse(xml_bytes: bytes, source_name: str = "") -> dict | None:
    """
    Extract 990 row using lxml iterparse: stream through XML, collect first occurrence of each tag, clear elements.
    Lower memory and often faster for large XMLs. lxml only; returns None if not available or on error.
    """
    if _XML_PARSER != "lxml":
        return None
    found: dict[str, str | float | None] = {}
    try:
        for _event, elem in ET.iterparse(BytesIO(xml_bytes), events=("end",)):
            local = _local_tag(elem.tag)
            if local == "Filer":
                for k, v in _extract_filer_from_element(elem).items():
                    if k not in found and v is not None:
                        found[k] = v
                elem.clear()
                continue
            if local in ("Organization501c3Ind", "Organization501c3ExemptPFInd"):
                text = _elem_text(elem)
                if text and "subsection_code" not in found:
                    found["subsection_code"] = "c3"
                elem.clear()
                continue
            if local in _TAG_MAP:
                key, kind = _TAG_MAP[local]
                if key not in found or (key == "net_assets" and found[key] == 0) or (key == "exempt_purpose_txt" and local == "MissionDesc"):
                    text = _elem_text(elem)
                    if text is not None:
                        if kind == "number":
                            try:
                                found[key] = float(text.replace(",", "").strip())
                            except ValueError:
                                found[key] = None
                        else:
                            found[key] = text
            elem.clear()
            # Don't break early: net_assets, program_service_expenses, etc. often appear after revenue/expenses
    except Exception:
        return None
    if "ein" not in found or not found["ein"]:
        return None
    return _row_from_found(found, source_name)


def parse_990_xml(
    xml_bytes: bytes,
    source_name: str = "",
    use_regex_only: bool = False,
    use_iterparse: bool = False,
) -> dict | None:
    """
    Minimal 990 TEOS XML parser. Extracts EIN, tax year, form type, revenue, expenses, assets.
    When use_regex_only is True, uses single-pass regex (fastest). When use_iterparse is True and lxml
    is available, uses iterparse (lower memory for large XMLs). Otherwise one-pass DOM (fromstring).
    """
    if use_regex_only:
        return _parse_990_regex_only(xml_bytes, source_name)
    if use_iterparse and _XML_PARSER == "lxml":
        row = _parse_990_iterparse(xml_bytes, source_name)
        if row is not None:
            return row
    try:
        root = ET.fromstring(xml_bytes)
    except Exception:
        return None
    found = _extract_990_single_pass(root)
    if not found:
        return None
    return _row_from_found(found, source_name)


def list_zip_keys_for_year(
    bucket: str,
    prefix: str,
    year: int,
    session: boto3.Session,
    bulk_part_only: bool = True,
) -> list[str]:
    """List S3 object keys for .zip files under zips/year={year}/. If bulk_part_only, keep only bulk-part filenames (e.g. 01A.zip)."""
    s3 = session.client("s3")
    prefix_key = f"{prefix}/zips/year={year}/"
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix_key):
        for obj in page.get("Contents", []):
            k = obj.get("Key", "")
            if k.endswith(".zip"):
                keys.append(k)
    keys = sorted(keys)
    if bulk_part_only:
        keys = _filter_bulk_part_keys(keys)
    return keys


def _parse_one_xml(
    name: str,
    xml_bytes: bytes,
    use_regex_only: bool,
    use_iterparse: bool = False,
) -> dict | None:
    """Parse one XML into a row dict; used by within-ZIP parallel workers. Returns None on skip/failure."""
    return parse_990_xml(
        xml_bytes,
        source_name=name,
        use_regex_only=use_regex_only,
        use_iterparse=use_iterparse,
    )


def _parse_chunk(
    to_parse: list[tuple[str, bytes]],
    use_regex_only: bool,
    use_iterparse: bool,
    zip_workers: int,
) -> list[dict]:
    """Parse a chunk of (name, xml_bytes); sequential or parallel. Returns list of row dicts."""
    rows: list[dict] = []
    if zip_workers <= 1 or len(to_parse) == 0:
        for name, xml_bytes in to_parse:
            row = parse_990_xml(
                xml_bytes,
                source_name=name,
                use_regex_only=use_regex_only,
                use_iterparse=use_iterparse,
            )
            if row:
                rows.append(row)
    else:
        n_workers = min(zip_workers, len(to_parse))
        with ThreadPoolExecutor(max_workers=n_workers) as pool:
            futures = {
                pool.submit(
                    _parse_one_xml,
                    name,
                    xml_bytes,
                    use_regex_only,
                    use_iterparse,
                ): (name, xml_bytes)
                for name, xml_bytes in to_parse
            }
            for future in _as_completed(futures):
                try:
                    row = future.result()
                    if row:
                        rows.append(row)
                except Exception:
                    continue
    return rows


def _download_zip_from_s3(
    bucket: str,
    key: str,
    session: boto3.Session,
    stream_to_temp_mb: float | None = 50.0,
    download_workers: int = 0,
) -> tuple[BytesIO | str, bool]:
    """
    Download one S3 object (ZIP) into memory (BytesIO) or a temp file path.
    Returns (zip_path_or_buf, use_temp). Caller must delete temp file when use_temp is True.
    When download_workers >= 2 and object size is known and > _PARALLEL_DOWNLOAD_MIN_BYTES,
    uses parallel Range requests to fill a temp file for faster download.
    """
    zip_name = key.split("/")[-1]
    s3 = session.client("s3")
    content_length = None
    if download_workers >= 2:
        try:
            head = s3.head_object(Bucket=bucket, Key=key)
            content_length = head.get("ContentLength")
        except Exception:
            pass
    use_temp = stream_to_temp_mb is not None and (
        content_length is None or content_length > stream_to_temp_mb * 1024 * 1024
    )
    if use_temp and content_length is not None and content_length >= _PARALLEL_DOWNLOAD_MIN_BYTES and download_workers >= 2:
        print(f"    [download] {zip_name}: parallel range ({content_length // (1024*1024):,} MB, {download_workers} workers)", flush=True)
        # Parallel range download: create temp file, then fetch byte ranges in parallel.
        with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as tf:
            zip_path = tf.name
        chunk_size = max(_STREAM_CHUNK, (content_length + download_workers - 1) // download_workers)
        n_parts = max(1, (int(content_length) + chunk_size - 1) // chunk_size)
        n_workers = min(download_workers, n_parts)

        def fetch_range(part_index: int) -> tuple[int, bytes]:
            start = part_index * chunk_size
            end = min(start + chunk_size, int(content_length)) - 1
            r = s3.get_object(Bucket=bucket, Key=key, Range=f"bytes={start}-{end}")
            return (start, r["Body"].read())

        with open(zip_path, "wb") as f:
            f.truncate(int(content_length))
        with ThreadPoolExecutor(max_workers=n_workers) as pool:
            for start, data in pool.map(
                lambda i: fetch_range(i), range(n_parts)
            ):
                with open(zip_path, "r+b") as f:
                    f.seek(start)
                    f.write(data)
        with open(zip_path, "rb") as f:
            header = f.read(2)
        if len(header) < 2 or header != b"PK":
            try:
                os.unlink(zip_path)
            except OSError:
                pass
            raise ValueError(f"Not a ZIP: {key.split('/')[-1]}")
        return (zip_path, True)
    # Single get_object (stream to temp or read into memory).
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"]
    content_length = content_length or obj.get("ContentLength")
    if use_temp:
        size_mb = (content_length or 0) // (1024 * 1024)
        print(f"    [download] {zip_name}: streaming to temp ({size_mb:,} MB)", flush=True)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as tf:
            while True:
                chunk = body.read(_STREAM_CHUNK)
                if not chunk:
                    break
                tf.write(chunk)
            zip_path = tf.name
        with open(zip_path, "rb") as f:
            header = f.read(2)
        if len(header) < 2 or header != b"PK":
            try:
                os.unlink(zip_path)
            except OSError:
                pass
            raise ValueError(f"Not a ZIP: {key.split('/')[-1]}")
        return (zip_path, True)
    body_bytes = body.read()
    if len(body_bytes) < 2 or body_bytes[:2] != b"PK":
        raise ValueError(f"Not a ZIP: {key.split('/')[-1]}")
    size_mb = len(body_bytes) // (1024 * 1024)
    print(f"    [download] {zip_name}: in-memory ({size_mb:,} MB)", flush=True)
    return (BytesIO(body_bytes), False)


def _parse_zip_content(
    zf_arg: BytesIO | str,
    key: str,
    ein_set: set[str] | None,
    use_regex_only: bool,
    use_iterparse: bool,
    zip_workers: int,
    zip_parse_batch: int | None,
) -> list[dict]:
    """Parse already-downloaded ZIP (path or BytesIO) into list of row dicts. Does not delete files."""
    zip_name = key.split("/")[-1]
    rows = []
    try:
        with zipfile.ZipFile(zf_arg, "r") as zf:
            xml_names = [n for n in zf.namelist() if n.lower().endswith(".xml")]
            print(f"    [parse] {zip_name}: opening ZIP, {len(xml_names):,} XMLs to process", flush=True)
            batch_size = zip_parse_batch or 0
            if batch_size <= 0:
                to_parse = []
                for name in zf.namelist():
                    if not name.lower().endswith(".xml"):
                        continue
                    try:
                        xml_bytes = zf.read(name)
                        if ein_set is not None:
                            ein_fast = _extract_ein_fast(xml_bytes)
                            if ein_fast is None or ein_fast not in ein_set:
                                continue
                        to_parse.append((name, xml_bytes))
                    except Exception:
                        continue
                rows = _parse_chunk(to_parse, use_regex_only, use_iterparse, zip_workers)
            else:
                chunk: list[tuple[str, bytes]] = []
                for name in zf.namelist():
                    if not name.lower().endswith(".xml"):
                        continue
                    try:
                        xml_bytes = zf.read(name)
                        if ein_set is not None:
                            ein_fast = _extract_ein_fast(xml_bytes)
                            if ein_fast is None or ein_fast not in ein_set:
                                continue
                        chunk.append((name, xml_bytes))
                        if len(chunk) >= batch_size:
                            rows.extend(_parse_chunk(chunk, use_regex_only, use_iterparse, zip_workers))
                            chunk.clear()
                    except Exception:
                        continue
                if chunk:
                    rows.extend(_parse_chunk(chunk, use_regex_only, use_iterparse, zip_workers))
    except zipfile.BadZipFile:
        print(f"    Skip (bad ZIP): {key.split('/')[-1]}", file=sys.stderr, flush=True)
        return []
    return rows


def parse_zip_from_s3(
    bucket: str,
    key: str,
    session: boto3.Session,
    ein_set: set[str] | None = None,
    stream_to_temp_mb: float | None = 50.0,
    download_workers: int = 0,
    use_regex_only: bool = False,
    use_iterparse: bool = False,
    zip_workers: int = 1,
    zip_parse_batch: int | None = None,
) -> list[dict]:
    """
    Download one S3 object, parse as ZIP, extract .xml members and parse each into a row dict.
    When ein_set is provided, EIN is extracted first (regex); only filings in ein_set get a full parse.
    Objects that are not valid ZIPs are skipped; returns [] for those.
    When stream_to_temp_mb is set and object size exceeds it (or size unknown), stream body to a temp file
    and open ZipFile on that file to avoid holding the full ZIP in memory.
    When download_workers >= 2 and ZIP is large, uses parallel S3 Range requests for faster download.
    When zip_workers > 1, XMLs inside the ZIP are parsed in parallel (after reading all members sequentially).
    When use_iterparse is True (and not use_regex_only), use lxml iterparse for lower memory on large XMLs.
    When zip_parse_batch is set, read and parse in batches of that size to cap memory (don't hold all XML bytes at once).
    """
    zip_path_or_buf = None
    use_temp = False
    try:
        zip_path_or_buf, use_temp = _download_zip_from_s3(
            bucket, key, session,
            stream_to_temp_mb=stream_to_temp_mb,
            download_workers=download_workers,
        )
    except ValueError as e:
        print(f"    Skip: {e}", file=sys.stderr, flush=True)
        return []
    try:
        return _parse_zip_content(
            zip_path_or_buf, key, ein_set,
            use_regex_only=use_regex_only,
            use_iterparse=use_iterparse,
            zip_workers=zip_workers,
            zip_parse_batch=zip_parse_batch,
        )
    finally:
        if use_temp and zip_path_or_buf and isinstance(zip_path_or_buf, str) and os.path.isfile(zip_path_or_buf):
            try:
                os.unlink(zip_path_or_buf)
            except OSError:
                pass


def _write_part_parquet(parts_dir: Path, year: int, zip_name: str, rows: list[dict]) -> None:
    """Write one Parquet file for a ZIP under parts_dir/year=YYYY/{stem}.parquet (silver layer)."""
    part_dir = parts_dir / f"year={year}"
    part_dir.mkdir(parents=True, exist_ok=True)
    stem = Path(zip_name).stem
    out_path = part_dir / f"{stem}.parquet"
    try:
        pd.DataFrame(rows).to_parquet(out_path, index=False)
    except Exception:
        out_path = part_dir / f"{stem}.csv"
        pd.DataFrame(rows).to_csv(out_path, index=False)


def _process_one_zip(
    bucket: str,
    key: str,
    ein_set: set[str] | None,
    region: str,
    stream_to_temp_mb: float = 50.0,
    download_workers: int = 0,
    use_regex_only: bool = False,
    use_iterparse: bool = False,
    zip_workers: int = 1,
    zip_parse_batch: int | None = None,
) -> tuple[str, list[dict]]:
    """
    Top-level worker: download one S3 ZIP, parse, return (zip_filename, rows).
    Used by ProcessPoolExecutor; must be picklable (module-level, no closures).
    zip_workers: parallel XML parses within this ZIP. zip_parse_batch: cap in-ZIP memory by batching.
    """
    session = boto3.Session(region_name=region)
    rows = parse_zip_from_s3(
        bucket,
        key,
        session,
        ein_set=ein_set,
        stream_to_temp_mb=stream_to_temp_mb,
        download_workers=download_workers,
        use_regex_only=use_regex_only,
        use_iterparse=use_iterparse,
        zip_workers=zip_workers,
        zip_parse_batch=zip_parse_batch,
    )
    return (key.split("/")[-1], rows)


def _normalize_zip(zip_val: str | None) -> str:
    """Return 5-digit ZIP string; empty or invalid -> empty string for non-match."""
    if zip_val is None or pd.isna(zip_val):
        return ""
    s = str(zip_val).strip().replace(" ", "").replace("-", "")
    digits = "".join(c for c in s if c.isdigit())
    if len(digits) < 5:
        return ""
    return digits[:5]


def load_benchmark_zip_set(csv_path: Path) -> tuple[set[str], dict[str, str] | None]:
    """
    Load benchmark ZIP list (from location_processing/03). CSV must have ZIP column;
    optional Region (or cluster) column for region mapping.
    Returns (set of normalized 5-digit ZIPs, zip->region dict or None).
    """
    if not csv_path.exists():
        return (set(), None)
    df = pd.read_csv(csv_path)
    zip_col = next((c for c in df.columns if "zip" in c.lower()), None)
    if not zip_col:
        return (set(), None)
    zip_norm = df[zip_col].astype(str).apply(_normalize_zip)
    valid = zip_norm.str.len() == 5
    zip_set = set(zip_norm[valid].unique())
    region_col = next(
        (c for c in df.columns if str(c).lower() in ("region", "cluster")), None
    )
    zip_to_region = None
    if region_col:
        zip_to_region = dict(
            zip(zip_norm[valid], df.loc[valid, region_col].astype(str).str.strip())
        )
    return (zip_set, zip_to_region)


def _find_region_column_geoid(ref: pd.DataFrame) -> str | None:
    """Return column name for region/cluster in GEOID reference, or None."""
    for c in ref.columns:
        if c and str(c).lower() in ("region", "cluster_name", "cluster", "benchmark_region", "area"):
            return c
    if "Region" in ref.columns:
        return "Region"
    return None


def load_geoid_reference_set(csv_path: Path) -> tuple[set[str], dict[str, str] | None]:
    """
    Load GEOID reference (benchmark counties). CSV must have GEOID column (5-digit county FIPS).
    Returns (set of GEOIDs, geoid->region dict or None).
    """
    if not csv_path.exists():
        return (set(), None)
    df = pd.read_csv(csv_path)
    geoid_col = next((c for c in df.columns if "geoid" in c.lower()), None)
    if not geoid_col:
        return (set(), None)
    geoid_norm = df[geoid_col].astype(str).str.strip().str.zfill(5)
    valid = geoid_norm.str.len() == 5
    geoid_set = set(geoid_norm[valid].unique())
    region_col = _find_region_column_geoid(df)
    geoid_to_region = None
    if region_col:
        geoid_to_region = dict(
            zip(geoid_norm[valid], df.loc[valid, region_col].astype(str).str.strip())
        )
    return (geoid_set, geoid_to_region)


def load_zip_to_geoid(csv_path: Path) -> dict[str, str]:
    """
    Load zip_to_county_fips and return dict mapping normalized 5-digit ZIP to 5-digit GEOID (county FIPS).
    One row per ZIP (primary county) when crosswalk has duplicates.
    """
    if not csv_path.exists():
        return {}
    df = pd.read_csv(csv_path)
    zip_col = next((c for c in df.columns if "zip" in c.lower()), df.columns[0])
    fips_candidates = [
        c for c in df.columns
        if "fips" in c.lower() or ("county" in c.lower() and "name" not in c.lower())
    ]
    fips_col = fips_candidates[0] if fips_candidates else df.columns[1]
    df["_zip"] = df[zip_col].astype(str).str.replace(r"\D", "", regex=True).str[:5]
    df["_geoid"] = df[fips_col].astype(str).str.strip().str.zfill(5)
    df = df[["_zip", "_geoid"]].drop_duplicates(subset=["_zip"], keep="first").dropna(subset=["_geoid"])
    valid = (df["_zip"].str.len() == 5) & (df["_geoid"].str.len() == 5)
    return dict(zip(df.loc[valid, "_zip"], df.loc[valid, "_geoid"]))


def main() -> None:
    import argparse
    from datetime import date

    parser = argparse.ArgumentParser(
        description="Parse IRS 990 TEOS ZIPs from S3 and write staging"
    )
    parser.add_argument("--bucket", default=DEST_BUCKET, help="S3 bucket")
    parser.add_argument("--prefix", default=PREFIX, help="S3 prefix")
    parser.add_argument(
        "--region", default=os.environ.get("AWS_DEFAULT_REGION", "us-east-2")
    )
    parser.add_argument("--start-year", type=int, default=2021)
    parser.add_argument("--end-year", type=int, default=None)
    parser.add_argument(
        "--geoid-reference",
        type=Path,
        default=GEOID_REFERENCE_CSV,
        help="GEOID reference CSV (benchmark counties). With --zip-to-county, keep rows whose org ZIP maps to a county in this list. Default: DATA/reference/GEOID_reference.csv",
    )
    parser.add_argument(
        "--zip-to-county",
        type=Path,
        default=ZIP_TO_COUNTY_CSV,
        help="ZIP to county FIPS crosswalk. With --geoid-reference, map org ZIP to GEOID and filter. Default: DATA/reference/zip_to_county_fips.csv",
    )
    parser.add_argument(
        "--benchmark-zips",
        type=Path,
        default=BENCHMARK_ZIPS_CSV,
        help="Legacy: CSV with ZIP (and optional Region); keep only rows whose org ZIP is in this list. Ignored if --geoid-reference and --zip-to-county exist.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Output Parquet path (default: DATA/staging/filing/irs_990_filings.parquet)",
    )
    parser.add_argument(
        "--skip-processed",
        action="store_true",
        help="Skip ZIPs already present in output (read output, process only new S3 keys, merge and overwrite)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        metavar="N",
        help="Process N ZIPs in parallel (default 1). Use 2–4 for faster runs; more may hit memory or S3 limits.",
    )
    parser.add_argument(
        "--zip-workers",
        type=int,
        default=1,
        metavar="N",
        help="Within each ZIP: parse N XMLs in parallel (default 1). Use 4–8 for large bulk-part ZIPs.",
    )
    parser.add_argument(
        "--zip-parse-batch",
        type=int,
        default=None,
        metavar="N",
        help="Within each ZIP: read and parse at most N XMLs at a time (default: all). Lowers memory for huge ZIPs.",
    )
    parser.add_argument(
        "--in-memory-mb",
        type=float,
        default=2000.0,
        metavar="MB",
        help="ZIPs smaller than this (MB) are kept in memory for faster I/O; larger ones stream to temp file (default 2000 = 2 GB).",
    )
    parser.add_argument(
        "--download-workers",
        type=int,
        default=4,
        metavar="N",
        help="When N>=2, large ZIPs (>20 MB) use N parallel S3 Range requests for faster download (default 4). Use 0 to disable.",
    )
    parser.add_argument(
        "--prefetch",
        action="store_true",
        default=True,
        help="When processing ZIPs one-by-one: download next ZIP while parsing current (default True). Set --no-prefetch to disable.",
    )
    parser.add_argument(
        "--no-prefetch",
        action="store_false",
        dest="prefetch",
        help="Disable prefetch (download next ZIP while parsing current).",
    )
    parser.add_argument(
        "--output-full",
        type=Path,
        default=None,
        metavar="PATH",
        help="Also write full unfiltered Parquet here (all parsed rows, no geography filter). One parse produces both --output and --output-full when set.",
    )
    parser.add_argument(
        "--fast-parse",
        action="store_true",
        help="Use single-pass regex XML extraction (no DOM). Fastest; slightly more fragile to schema changes.",
    )
    parser.add_argument(
        "--stream-parse",
        action="store_true",
        help="Use lxml iterparse for DOM path (when not --fast-parse). Lower memory for large XMLs.",
    )
    parser.add_argument(
        "--output-parts-dir",
        type=Path,
        default=None,
        metavar="DIR",
        help="Silver layer: write one Parquet per ZIP under DIR/year=YYYY/ (e.g. 2021_TEOS_XML_01A.parquet). Parse is unfiltered so parts are complete; use merge script to filter later.",
    )
    parser.add_argument(
        "--max-zips",
        type=int,
        default=None,
        metavar="N",
        help="Process at most N ZIPs total (across all years). Useful for sampling or quick runs.",
    )
    parser.add_argument(
        "--only-zip-key",
        type=str,
        default=None,
        metavar="FILENAME",
        help="Process only the ZIP whose filename (basename) equals this (e.g. 2025_TEOS_XML_01A.zip).",
    )
    args = parser.parse_args()

    end_year = args.end_year or date.today().year
    if args.start_year > end_year:
        print("start-year must be <= end-year", file=sys.stderr)
        sys.exit(1)

    start = time.perf_counter()

    def _elapsed() -> str:
        e = time.perf_counter() - start
        return f"{e:.1f} s ({e / 60:.1f} min)"

    print("03_parse_irs_990_zips_to_staging: starting. Timer started.")
    print(f"  Bucket: {args.bucket}, prefix: {args.prefix}, region: {args.region}")
    print(f"  Year range: {args.start_year}–{end_year}")
    print(f"  Output: {args.output}")
    print(f"  Skip already processed ZIPs: {args.skip_processed}")
    print(f"  Workers (parallel ZIPs): {args.workers}; within-ZIP (parallel XMLs): {args.zip_workers}", end="")
    if args.zip_parse_batch is not None:
        print(f"; within-ZIP batch cap: {args.zip_parse_batch}", end="")
    if getattr(args, "in_memory_mb", None) is not None:
        print(f"; in-memory ZIPs up to {args.in_memory_mb} MB", end="")
    if getattr(args, "download_workers", 0) >= 2:
        print(f"; download parallel: {args.download_workers}", end="")
    if getattr(args, "prefetch", False):
        print("; prefetch next ZIP", end="")
    print()
    print(f"  XML parser: {_XML_PARSER}")
    print(f"  Fast parse (single-pass regex): {args.fast_parse}; stream-parse (iterparse): {args.stream_parse}")
    if args.output_full is not None:
        print(f"  Full (unfiltered) output: {args.output_full}")
        print("  (Parsing all XMLs to produce full output; then applying geography filter for main output.)")
    if args.output_parts_dir is not None:
        print(f"  Silver (per-ZIP Parquet): {args.output_parts_dir}/year=YYYY/")
        print("  (Parsing all XMLs so each part file is complete; merge script can filter later.)")
    if getattr(args, "max_zips", None) is not None:
        print(f"  Max ZIPs to process: {args.max_zips}")
    print()

    os.environ["AWS_DEFAULT_REGION"] = args.region
    session = _session()

    # Geography filter: GEOID reference (county) + ZIP→county, or legacy benchmark ZIP list.
    geoid_reference_set: set[str] = set()
    zip_to_geoid: dict[str, str] = {}
    geoid_to_region: dict[str, str] | None = None
    if args.geoid_reference.exists() and args.zip_to_county.exists():
        geoid_reference_set, geoid_to_region = load_geoid_reference_set(args.geoid_reference)
        zip_to_geoid = load_zip_to_geoid(args.zip_to_county)
        if geoid_reference_set and zip_to_geoid:
            print(f"  Filter by GEOID/county: {args.geoid_reference.name} ({len(geoid_reference_set)} counties) + {args.zip_to_county.name} ({len(zip_to_geoid):,} ZIPs).", flush=True)
            if geoid_to_region:
                print(f"  Region column will be set from GEOID reference.", flush=True)
        else:
            geoid_reference_set = set()
            zip_to_geoid = {}
            geoid_to_region = None
        print()

    benchmark_zip_set = set()
    benchmark_zip_to_region = None
    if not geoid_reference_set and args.benchmark_zips.exists():
        benchmark_zip_set, benchmark_zip_to_region = load_benchmark_zip_set(args.benchmark_zips)
        if benchmark_zip_set:
            print(f"  Benchmark ZIPs: {args.benchmark_zips} ({len(benchmark_zip_set):,} ZIPs). Rows with org ZIP in this list will be kept.", flush=True)
            if benchmark_zip_to_region:
                print(f"  Region column will be set from benchmark ZIP list.", flush=True)
        print()

    print(f"  Elapsed: {_elapsed()}", flush=True)
    print()

    # If --skip-processed, load existing output to get set of already-processed ZIP filenames.
    processed_zips = set()
    existing_df = None
    if args.skip_processed and args.output.exists():
        print("Loading existing output to determine already-processed ZIPs...", flush=True)
        try:
            if args.output.suffix.lower() == ".parquet":
                existing_df = pd.read_parquet(args.output)
            else:
                existing_df = pd.read_csv(args.output)
            if "source_zip" in existing_df.columns:
                processed_zips = set(existing_df["source_zip"].dropna().astype(str).unique())
                print(f"  Found {len(existing_df):,} existing rows from {len(processed_zips):,} ZIP(s); will skip those ZIPs.", flush=True)
            else:
                print("  Existing file has no source_zip column; processing all ZIPs (output will be overwritten).", flush=True)
                existing_df = None
        except Exception as e:
            print(f"  Could not read existing output: {e}; processing all ZIPs.", flush=True)
            existing_df = None
            processed_zips = set()
        print()
    elif args.skip_processed:
        print("Skip-processed requested but output file does not exist; processing all ZIPs.", flush=True)
        print()

    # When --output-full or --output-parts-dir is set, parse all XMLs so full/parts are complete; then apply EIN filter for main output.
    ein_set_for_parse = None  # Filter by GEOID/county only; no EIN pre-filter during parse

    # List and process ZIPs from S3 for each year (only keys not in processed_zips when --skip-processed).
    all_rows = []
    total_zips_processed = 0
    max_zips = getattr(args, "max_zips", None)
    num_years = end_year - args.start_year + 1
    print(f"Listing and parsing S3 ZIPs by year ({args.start_year}–{end_year}, {num_years} year(s))...", flush=True)
    for year in tqdm(range(args.start_year, end_year + 1), desc="Years", unit="year", **_tqdm_kw):
        if max_zips is not None and total_zips_processed >= max_zips:
            print(f"  Reached --max-zips={max_zips}; stopping.", flush=True)
            break
        print(f"\n  --- Year {year} --- (elapsed: {_elapsed()})", flush=True)
        keys = list_zip_keys_for_year(args.bucket, args.prefix, year, session)
        if getattr(args, "only_zip_key", None):
            keys = [k for k in keys if k.split("/")[-1] == args.only_zip_key]
        if not keys:
            print(f"  [{year}] No bulk-part .zip objects in S3; skipping year.", flush=True)
            continue
        if processed_zips:
            keys = [k for k in keys if k.split("/")[-1] not in processed_zips]
            if not keys:
                print(f"  [{year}] All bulk-part ZIP(s) for this year already processed; skipping year.", flush=True)
                continue
            print(f"  [{year}] Found {len(keys)} bulk-part ZIP(s) not yet processed; processing...", flush=True)
        else:
            print(f"  [{year}] Found {len(keys)} bulk-part ZIP(s); processing...", flush=True)
        if max_zips is not None:
            remaining = max_zips - total_zips_processed
            keys = keys[:remaining]
            print(f"  [{year}] Limiting to {len(keys)} ZIP(s) (--max-zips).", flush=True)
        rows_before_year = len(all_rows)
        stream_mb = getattr(args, "in_memory_mb", 2000.0)
        download_workers = getattr(args, "download_workers", 0)
        prefetch = getattr(args, "prefetch", False) and args.workers <= 1 and len(keys) > 1
        if args.workers <= 1:
            if prefetch:
                print(f"  [{year}] Mode: prefetch (download next ZIP while parsing current)", flush=True)
                prefetched = None
                next_future = None
                with ThreadPoolExecutor(max_workers=1) as prefetch_executor:
                    for i, key in enumerate(tqdm(keys, desc=f"ZIPs {year}", unit="zip", leave=False, **_tqdm_kw)):
                        zip_name = key.split("/")[-1]
                        if prefetched is not None:
                            zip_path_or_buf, use_temp = prefetched
                        else:
                            try:
                                zip_path_or_buf, use_temp = _download_zip_from_s3(
                                    args.bucket, key, session,
                                    stream_to_temp_mb=stream_mb,
                                    download_workers=download_workers,
                                )
                            except ValueError as e:
                                tqdm.write(f"    Skip: {e}")
                                continue
                        if i + 1 < len(keys):
                            next_future = prefetch_executor.submit(
                                _download_zip_from_s3,
                                args.bucket, keys[i + 1], session,
                                stream_to_temp_mb=stream_mb,
                                download_workers=download_workers,
                            )
                        try:
                            rows = _parse_zip_content(
                                zip_path_or_buf, key, ein_set_for_parse,
                                use_regex_only=args.fast_parse,
                                use_iterparse=args.stream_parse,
                                zip_workers=args.zip_workers,
                                zip_parse_batch=args.zip_parse_batch,
                            )
                        finally:
                            if use_temp and isinstance(zip_path_or_buf, str) and os.path.isfile(zip_path_or_buf):
                                try:
                                    os.unlink(zip_path_or_buf)
                                except OSError:
                                    pass
                        prefetched = next_future.result() if next_future is not None else None
                        next_future = None
                        for row in rows:
                            row["source_zip"] = zip_name
                        if args.output_parts_dir is not None and rows:
                            _write_part_parquet(args.output_parts_dir, year, zip_name, rows)
                        all_rows.extend(rows)
                        tqdm.write(f"    {zip_name}: {len(rows):,} filings extracted")
            else:
                print(f"  [{year}] Mode: sequential (no prefetch)", flush=True)
                for key in tqdm(keys, desc=f"ZIPs {year}", unit="zip", leave=False, **_tqdm_kw):
                    zip_name = key.split("/")[-1]
                    rows = parse_zip_from_s3(
                        args.bucket,
                        key,
                        session,
                        ein_set=ein_set_for_parse,
                        stream_to_temp_mb=stream_mb,
                        download_workers=download_workers,
                        use_regex_only=args.fast_parse,
                        use_iterparse=args.stream_parse,
                        zip_workers=args.zip_workers,
                        zip_parse_batch=args.zip_parse_batch,
                    )
                    for row in rows:
                        row["source_zip"] = zip_name
                    if args.output_parts_dir is not None and rows:
                        _write_part_parquet(args.output_parts_dir, year, zip_name, rows)
                    all_rows.extend(rows)
                    tqdm.write(f"    {zip_name}: {len(rows):,} filings extracted")
        else:
            print(f"  [{year}] Mode: {args.workers} worker processes (parallel ZIPs)", flush=True)
            from concurrent.futures import ProcessPoolExecutor, as_completed
            with ProcessPoolExecutor(max_workers=args.workers) as executor:
                futures = {
                    executor.submit(
                        _process_one_zip,
                        args.bucket,
                        key,
                        ein_set_for_parse,
                        args.region,
                        stream_mb,
                        download_workers,
                        args.fast_parse,
                        args.stream_parse,
                        args.zip_workers,
                        args.zip_parse_batch,
                    ): key
                    for key in keys
                }
                for future in tqdm(as_completed(futures), total=len(futures), desc=f"ZIPs {year}", unit="zip", leave=False, **_tqdm_kw):
                    zip_name, rows = future.result()
                    for row in rows:
                        row["source_zip"] = zip_name
                    if args.output_parts_dir is not None and rows:
                        _write_part_parquet(args.output_parts_dir, year, zip_name, rows)
                    all_rows.extend(rows)
                    tqdm.write(f"    {zip_name}: {len(rows):,} filings extracted")
        rows_added_this_year = len(all_rows) - rows_before_year
        total_zips_processed += len(keys)
        print(f"  [{year}] Year subtotal: {rows_added_this_year:,} filings (cumulative new: {len(all_rows):,}) | Elapsed: {_elapsed()}", flush=True)
    print()

    # Build dataframe from new rows; merge with existing if --skip-processed and we loaded existing.
    if existing_df is not None and all_rows:
        df_new = pd.DataFrame(all_rows)
        df = pd.concat([existing_df, df_new], ignore_index=True, copy=False)
        print(f"Combined {len(existing_df):,} existing rows + {len(df_new):,} new rows = {len(df):,} total.", flush=True)
    elif existing_df is not None and not all_rows:
        df = existing_df
        print("No new rows parsed; keeping existing output as-is.", flush=True)
    elif not all_rows:
        print("No filings parsed and no existing data; exiting.", file=sys.stderr)
        sys.exit(1)
    else:
        df = pd.DataFrame(all_rows)

    # Ensure source_zip exists for backward compatibility (e.g. when no --skip-processed).
    if "source_zip" not in df.columns:
        df["source_zip"] = None

    # Optionally write full unfiltered Parquet (before geography filter).
    if args.output_full is not None:
        df["region"] = None
        args.output_full.parent.mkdir(parents=True, exist_ok=True)
        print(f"Writing full (unfiltered) output to {args.output_full}... | Elapsed: {_elapsed()}", flush=True)
        try:
            df.to_parquet(args.output_full, index=False)
            print(f"  Wrote {len(df):,} rows to Parquet: {args.output_full}", flush=True)
        except Exception:
            out_csv = args.output_full.with_suffix(".csv")
            df.to_csv(out_csv, index=False)
            print(f"  Parquet unavailable; wrote {len(df):,} rows to CSV: {args.output_full}", flush=True)
        print()

    # Apply filter: GEOID/county (from GEOID reference + zip_to_county), or legacy benchmark ZIPs.
    if geoid_reference_set and zip_to_geoid:
        zip_col = "zip" if "zip" in df.columns else None
        if zip_col is not None:
            df_zip_norm = df[zip_col].astype(str).apply(_normalize_zip)
            df_geoid = df_zip_norm.map(zip_to_geoid)
            before = len(df)
            df = df[df_geoid.isin(geoid_reference_set)].copy()
            df_geoid = df_geoid.loc[df.index]
            print(f"After GEOID/county filter: {len(df):,} rows retained (from {before:,} before filter).", flush=True)
            if geoid_to_region:
                df["region"] = df_geoid.map(geoid_to_region)
                print("Region column populated from GEOID reference.", flush=True)
            else:
                df["region"] = None
        else:
            print("GEOID reference and zip-to-county provided but no zip column in data; no filter applied.", flush=True)
            df["region"] = None
    elif benchmark_zip_set:
        zip_col = "zip" if "zip" in df.columns else None
        if zip_col is not None:
            df_zip_norm = df[zip_col].astype(str).apply(_normalize_zip)
            before = len(df)
            df = df[df_zip_norm.isin(benchmark_zip_set)].copy()
            df_zip_norm = df_zip_norm.loc[df.index]
            print(f"After benchmark-ZIP filter: {len(df):,} rows retained (from {before:,} before filter).", flush=True)
            if benchmark_zip_to_region:
                df["region"] = df_zip_norm.map(benchmark_zip_to_region)
                print("Region column populated from benchmark ZIP list.", flush=True)
            else:
                df["region"] = None
        else:
            print("Benchmark ZIP list provided but no zip column in data; no filter applied.", flush=True)
            df["region"] = None
    else:
        print("No GEOID/county, benchmark-ZIP, or EIN filter applied; all parsed rows retained.", flush=True)
        if "region" not in df.columns:
            df["region"] = None

    if "region" not in df.columns:
        df["region"] = None
    print()

    # Write main output: Parquet preferred, CSV fallback if Parquet unavailable.
    args.output.parent.mkdir(parents=True, exist_ok=True)
    print(f"Writing output to {args.output}... | Elapsed: {_elapsed()}", flush=True)
    try:
        df.to_parquet(args.output, index=False)
        print(f"  Wrote {len(df):,} rows to Parquet: {args.output}", flush=True)
    except Exception:
        out_csv = args.output.with_suffix(".csv")
        df.to_csv(out_csv, index=False)
        args.output = out_csv
        print(f"  Parquet unavailable; wrote {len(df):,} rows to CSV: {args.output}", flush=True)
    print()
    print(f"Done. Total elapsed: {_elapsed()}")


if __name__ == "__main__":
    main()
