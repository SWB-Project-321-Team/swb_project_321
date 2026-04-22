"""
Tests for the GivingTuesday datamart build/filter artifact contract.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pandas as pd
import pytest

_FILE_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _FILE_DIR.parent.parent
_DATAMART_DIR = _REPO_ROOT / "python" / "ingest" / "990_givingtuesday" / "datamart"

_COMMON_PATH = _DATAMART_DIR / "common.py"
_COMMON_SPEC = importlib.util.spec_from_file_location("gt_datamart_common", _COMMON_PATH)
if _COMMON_SPEC is None or _COMMON_SPEC.loader is None:
    raise ImportError(f"Unable to load GT datamart common module from {_COMMON_PATH}")
gt_common = importlib.util.module_from_spec(_COMMON_SPEC)
sys.modules.setdefault("gt_datamart_common", gt_common)
_COMMON_SPEC.loader.exec_module(gt_common)

_FILTER_PATH = _DATAMART_DIR / "08_filter_benchmark_outputs_local.py"
_FILTER_SPEC = importlib.util.spec_from_file_location("gt_filter_outputs_local", _FILTER_PATH)
if _FILTER_SPEC is None or _FILTER_SPEC.loader is None:
    raise ImportError(f"Unable to load GT datamart filter module from {_FILTER_PATH}")
gt_filter = importlib.util.module_from_spec(_FILTER_SPEC)


def _load_module(module_name: str, file_name: str):
    path = _DATAMART_DIR / file_name
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load GT datamart module from {path}")
    module = importlib.util.module_from_spec(spec)
    previous_common = sys.modules.get("common")
    sys.modules["common"] = gt_common
    spec.loader.exec_module(module)
    if previous_common is not None:
        sys.modules["common"] = previous_common
    else:
        sys.modules.pop("common", None)
    return module


gt_filter = _load_module("gt_filter_outputs_local", "08_filter_benchmark_outputs_local.py")
gt_build_basic = _load_module("gt_build_basic_local", "06_build_basic_allforms_presilver.py")
gt_build_mixed = _load_module("gt_build_mixed_local", "07_build_basic_plus_combined_presilver.py")
gt_upload_bronze = _load_module("gt_upload_bronze_local", "09_upload_bronze_presilver_outputs.py")
gt_upload_silver = _load_module("gt_upload_silver_local", "10_upload_silver_filtered_outputs.py")
gt_verify_curated = _load_module("gt_verify_curated_local", "11_verify_curated_outputs_local_s3.py")
gt_extract_analysis = _load_module("gt_extract_analysis_local", "13_extract_basic_analysis_variables_local.py")
gt_upload_analysis = _load_module("gt_upload_analysis_local", "14_upload_analysis_outputs.py")
gt_orchestrator = _load_module("gt_orchestrator_local", "run_990_datamart_pipeline.py")


def _presilver_row(
    row: dict[str, object],
    *,
    admitted_by_basic_geography: str = "1",
    admitted_by_combined_key: str = "0",
    admission_basis: str = "basic_geography",
) -> dict[str, object]:
    """Attach the step-06 admission metadata required by the filtered-only contract."""
    merged = dict(row)
    merged["presilver_admitted_by_basic_geography"] = admitted_by_basic_geography
    merged["presilver_admitted_by_combined_key"] = admitted_by_combined_key
    merged["presilver_admission_basis"] = admission_basis
    return merged


def test_artifact_registry_lists_dual_filtered_outputs_and_manifests() -> None:
    bronze_names = [artifact.local_path.name for artifact in gt_common.bronze_presilver_artifacts()]
    silver_names = [artifact.local_path.name for artifact in gt_common.silver_filtered_artifacts("both")]
    analysis_names = [artifact.local_path.name for artifact in gt_common.analysis_output_artifacts()]

    assert bronze_names == [
        "givingtuesday_990_basic_allforms_presilver.parquet",
        "givingtuesday_990_basic_plus_combined_presilver.parquet",
    ]
    assert silver_names == [
        "givingtuesday_990_basic_allforms_benchmark.parquet",
        "manifest_basic_allforms_filtered.json",
        "schema_snapshot_basic_allforms_filtered.json",
        "givingtuesday_990_filings_benchmark.parquet",
        "manifest_filtered.json",
        "schema_snapshot_filings_filtered.json",
    ]
    assert analysis_names == [
        "givingtuesday_990_basic_allforms_analysis_variables.parquet",
        "givingtuesday_990_basic_allforms_analysis_region_metrics.parquet",
        "givingtuesday_990_basic_allforms_analysis_variable_coverage.csv",
        "givingtuesday_basic_analysis_variable_mapping.md",
        "givingtuesday_datamart_pipeline.md",
    ]
    assert [target.target_id for target in gt_common.filter_targets("both")] == ["basic", "mixed"]


def test_basic_dedup_priority_includes_pf_revenue_and_expense_fields() -> None:
    """PF duplicates should be scored with the same fields used by analysis."""
    assert "ANREEXTOREEX" in gt_common.BASIC_DEDUP_PRIORITY_COLUMNS
    assert "ARETEREXPNSS" in gt_common.BASIC_DEDUP_PRIORITY_COLUMNS


def test_step14_uploads_registered_analysis_outputs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    analysis_parquet = tmp_path / "givingtuesday_990_basic_allforms_analysis_variables.parquet"
    region_parquet = tmp_path / "givingtuesday_990_basic_allforms_analysis_region_metrics.parquet"
    coverage_csv = tmp_path / "givingtuesday_990_basic_allforms_analysis_variable_coverage.csv"
    mapping_md = tmp_path / "givingtuesday_basic_analysis_variable_mapping.md"
    analysis_parquet.write_bytes(b"analysis")
    region_parquet.write_bytes(b"region")
    coverage_csv.write_text("variable_name\nanalysis_total_revenue_amount\n", encoding="utf-8")
    mapping_md.write_text("# mapping\n", encoding="utf-8")

    artifacts = [
        gt_common.PipelineArtifact("analysis_variables", analysis_parquet, gt_common.ANALYSIS_PREFIX, "application/octet-stream", "analysis_output"),
        gt_common.PipelineArtifact("analysis_region_metrics", region_parquet, gt_common.ANALYSIS_PREFIX, "application/octet-stream", "analysis_output"),
        gt_common.PipelineArtifact("analysis_coverage", coverage_csv, gt_common.ANALYSIS_META_PREFIX, "text/csv", "analysis_metadata"),
        gt_common.PipelineArtifact("analysis_mapping", mapping_md, gt_common.ANALYSIS_META_PREFIX, "text/markdown", "analysis_metadata"),
    ]

    uploaded: list[tuple[str, str, str, str]] = []

    monkeypatch.setattr(gt_upload_analysis, "load_env_from_secrets", lambda: None)
    monkeypatch.setattr(gt_upload_analysis, "analysis_output_artifacts", lambda: artifacts)
    monkeypatch.setattr(gt_upload_analysis, "should_skip_upload", lambda *args, **kwargs: False)
    monkeypatch.setattr(gt_upload_analysis, "parallel_map", lambda tasks, worker_count, desc, unit, fn: [fn(task) for task in tasks])

    def fake_upload(local_path: Path, bucket: str, key: str, region: str, extra_args=None):
        uploaded.append((local_path.name, bucket, key, extra_args.get("ContentType", "") if extra_args else ""))

    monkeypatch.setattr(gt_upload_analysis, "upload_file_with_progress", fake_upload)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "14_upload_analysis_outputs.py",
            "--bucket",
            "example-bucket",
            "--region",
            "us-east-2",
        ],
    )

    gt_upload_analysis.main()

    assert uploaded == [
        (
            "givingtuesday_990_basic_allforms_analysis_variables.parquet",
            "example-bucket",
            "silver/givingtuesday_990/analysis/givingtuesday_990_basic_allforms_analysis_variables.parquet",
            "application/octet-stream",
        ),
        (
            "givingtuesday_990_basic_allforms_analysis_region_metrics.parquet",
            "example-bucket",
            "silver/givingtuesday_990/analysis/givingtuesday_990_basic_allforms_analysis_region_metrics.parquet",
            "application/octet-stream",
        ),
        (
            "givingtuesday_990_basic_allforms_analysis_variable_coverage.csv",
            "example-bucket",
            "silver/givingtuesday_990/analysis/metadata/givingtuesday_990_basic_allforms_analysis_variable_coverage.csv",
            "text/csv",
        ),
        (
            "givingtuesday_basic_analysis_variable_mapping.md",
            "example-bucket",
            "silver/givingtuesday_990/analysis/metadata/givingtuesday_basic_analysis_variable_mapping.md",
            "text/markdown",
        ),
    ]


def test_emit_specific_registry_and_stale_detection(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    mixed_filtered = gt_common.PipelineArtifact(
        artifact_id="basic_plus_combined_filtered",
        local_path=tmp_path / "givingtuesday_990_filings_benchmark.parquet",
        s3_prefix=gt_common.SILVER_PREFIX,
        content_type="application/octet-stream",
        category="silver_filtered",
    )
    mixed_manifest = gt_common.PipelineArtifact(
        artifact_id="basic_plus_combined_filtered_manifest",
        local_path=tmp_path / "manifest_filtered.json",
        s3_prefix=gt_common.SILVER_PREFIX,
        content_type="application/json",
        category="silver_filtered_manifest",
    )
    basic_schema = gt_common.PipelineArtifact(
        artifact_id="basic_allforms_filtered_schema_snapshot",
        local_path=tmp_path / "schema_snapshot_basic_allforms_filtered.json",
        s3_prefix=gt_common.SILVER_META_PREFIX,
        content_type="application/json",
        category="silver_metadata",
    )
    mixed_schema = gt_common.PipelineArtifact(
        artifact_id="basic_plus_combined_filtered_schema_snapshot",
        local_path=tmp_path / "schema_snapshot_filings_filtered.json",
        s3_prefix=gt_common.SILVER_META_PREFIX,
        content_type="application/json",
        category="silver_metadata",
    )
    monkeypatch.setattr(gt_common, "FILTERED_MIXED_ARTIFACT", mixed_filtered)
    monkeypatch.setattr(gt_common, "FILTERED_MIXED_MANIFEST_ARTIFACT", mixed_manifest)
    monkeypatch.setattr(gt_common, "FILTERED_BASIC_SCHEMA_ARTIFACT", basic_schema)
    monkeypatch.setattr(gt_common, "FILTERED_MIXED_SCHEMA_ARTIFACT", mixed_schema)
    monkeypatch.setattr(
        gt_common,
        "FILTER_TARGET_MIXED",
        gt_common.FilterTarget("mixed", gt_common.BASIC_PLUS_COMBINED_PARQUET, mixed_filtered, mixed_manifest),
    )
    mixed_filtered.local_path.write_text("stale", encoding="utf-8")
    mixed_schema.local_path.write_text("{}", encoding="utf-8")
    mixed_manifest.local_path.write_text("{}", encoding="utf-8")

    names = [artifact.local_path.name for artifact in gt_common.silver_filtered_artifacts("basic")]
    assert names == [
        "givingtuesday_990_basic_allforms_benchmark.parquet",
        "manifest_basic_allforms_filtered.json",
        "schema_snapshot_basic_allforms_filtered.json",
    ]
    warnings = gt_common.stale_output_warnings("basic")
    assert any("givingtuesday_990_filings_benchmark.parquet" in warning for warning in warnings)


def test_filter_outputs_split_basic_only_and_mixed_schemas_and_write_manifests(tmp_path: Path) -> None:
    geoid_path = tmp_path / "GEOID_reference.csv"
    zip_path = tmp_path / "zip_to_county_fips.csv"
    basic_input = tmp_path / "givingtuesday_990_basic_allforms_presilver.parquet"
    mixed_input = tmp_path / "givingtuesday_990_basic_plus_combined_presilver.parquet"
    basic_output = tmp_path / "givingtuesday_990_basic_allforms_benchmark.parquet"
    mixed_output = tmp_path / "givingtuesday_990_filings_benchmark.parquet"
    basic_manifest = tmp_path / "manifest_basic_allforms_filtered.json"
    mixed_manifest = tmp_path / "manifest_filtered.json"

    pd.DataFrame(
        [
            {"GEOID": "46093", "Cluster_name": "BlackHills"},
            {"GEOID": "30063", "Cluster_name": "Missoula"},
        ]
    ).to_csv(geoid_path, index=False)
    pd.DataFrame(
        [
            {"ZIP": "57701", "FIPS": "46093"},
            {"ZIP": "59801", "FIPS": "30063"},
        ]
    ).to_csv(zip_path, index=False)

    base_rows = [
        {
            "ein": "123456789",
            "tax_year": "2023",
            "form_type": "990",
            "FILERUSZIP": "57701",
            "TOTREVCURYEA": "120",
            "TOTEXPCURYEA": "100",
            "TOTALRREVENU": "",
            "TOTALEEXPENS": "",
            "FILERNAME1": "Bench Org",
            "FILERUSSTATE": "SD",
            "from_basic": "1",
            "from_combined": "0",
        },
        {
            "ein": "987654321",
            "tax_year": "2023",
            "form_type": "990",
            "FILERUSZIP": "99999",
            "TOTREVCURYEA": "10",
            "TOTEXPCURYEA": "5",
            "TOTALRREVENU": "",
            "TOTALEEXPENS": "",
            "FILERNAME1": "Outside Org",
            "FILERUSSTATE": "CA",
            "from_basic": "1",
            "from_combined": "0",
        },
    ]
    pd.DataFrame(base_rows).to_parquet(basic_input, index=False)
    pd.DataFrame(
        [
            {**base_rows[0], "from_combined": "1", "field_fill_count_from_basic": "2", "combined_only_metric": "alpha"},
            {**base_rows[1], "from_combined": "1", "field_fill_count_from_basic": "0", "combined_only_metric": "beta"},
        ]
    ).to_parquet(mixed_input, index=False)

    geoid_df = gt_common.load_gt_geoid_reference(geoid_path)
    zip_df = gt_common.load_gt_zip_to_fips(zip_path)
    roi_zip_df = gt_common.build_gt_roi_zip_map(zip_df, geoid_df)

    gt_filter._filter_one_target(
        target_id="basic",
        input_path=basic_input,
        output_path=basic_output,
        manifest_path=basic_manifest,
        geoid_df=geoid_df,
        roi_zip_df=roi_zip_df,
        geoid_csv=geoid_path,
        zip_to_county_csv=zip_path,
        tax_year_min=2022,
        tax_year_max=2024,
        skip_if_unchanged=False,
        force_rebuild=False,
    )
    gt_filter._filter_one_target(
        target_id="mixed",
        input_path=mixed_input,
        output_path=mixed_output,
        manifest_path=mixed_manifest,
        geoid_df=geoid_df,
        roi_zip_df=roi_zip_df,
        geoid_csv=geoid_path,
        zip_to_county_csv=zip_path,
        tax_year_min=2022,
        tax_year_max=2024,
        skip_if_unchanged=False,
        force_rebuild=False,
    )

    basic_df = pd.read_parquet(basic_output)
    mixed_df = pd.read_parquet(mixed_output)
    basic_manifest_payload = json.loads(basic_manifest.read_text(encoding="utf-8"))
    mixed_manifest_payload = json.loads(mixed_manifest.read_text(encoding="utf-8"))

    assert len(basic_df) == 1
    assert len(mixed_df) == 1
    assert "field_fill_count_from_basic" not in basic_df.columns
    assert "combined_only_metric" not in basic_df.columns
    assert mixed_df["field_fill_count_from_basic"].tolist() == ["2"]
    assert mixed_df["combined_only_metric"].tolist() == ["alpha"]
    assert basic_df["county_fips"].tolist() == ["46093"]
    assert mixed_df["county_fips"].tolist() == ["46093"]
    assert basic_df["region"].tolist() == ["BlackHills"]
    assert mixed_df["region"].tolist() == ["BlackHills"]
    assert basic_df["derived_income_amount"].tolist() == ["20"]
    assert mixed_df["derived_income_amount"].tolist() == ["20"]
    assert basic_df["derived_income_derivation"].tolist() == ["derived:TOTREVCURYEA-minus-TOTEXPCURYEA"]
    assert mixed_df["derived_income_derivation"].tolist() == ["derived:TOTREVCURYEA-minus-TOTEXPCURYEA"]
    assert basic_manifest_payload["rows_output"] == 1
    assert mixed_manifest_payload["rows_output"] == 1
    assert basic_manifest_payload["input_path"] == str(basic_input)
    assert mixed_manifest_payload["input_path"] == str(mixed_input)
    assert basic_manifest_payload["output_path"] == str(basic_output)
    assert mixed_manifest_payload["output_path"] == str(mixed_output)
    assert basic_manifest_payload["tax_year_min"] == 2022
    assert basic_manifest_payload["tax_year_max"] == 2024
    assert mixed_manifest_payload["tax_year_min"] == 2022
    assert mixed_manifest_payload["tax_year_max"] == 2024
    assert basic_manifest_payload["rows_after_geo_filter"] == 1
    assert mixed_manifest_payload["rows_after_geo_filter"] == 1
    assert basic_manifest_payload["target_zip5_count"] == 2
    assert mixed_manifest_payload["target_zip5_count"] == 2


def test_roi_zip_map_rejects_ambiguous_benchmark_zip_assignments() -> None:
    geoid_df = pd.DataFrame(
        [
            {"county_fips": "46093", "region": "BlackHills"},
            {"county_fips": "46103", "region": "BlackHills"},
        ]
    )
    zip_df = pd.DataFrame(
        [
            {"zip5": "57701", "county_fips": "46093"},
            {"zip5": "57701", "county_fips": "46103"},
        ]
    )

    with pytest.raises(RuntimeError, match="ambiguous"):
        gt_common.build_gt_roi_zip_map(
            gt_filter.pl.from_pandas(zip_df),
            gt_filter.pl.from_pandas(geoid_df),
        )


def test_step06_normalizes_low_risk_basic_fields(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    raw_dir = tmp_path / "raw"
    meta_dir = tmp_path / "meta"
    staging_dir = tmp_path / "staging"
    cache_dir = tmp_path / "cache"
    raw_dir.mkdir()
    meta_dir.mkdir()
    staging_dir.mkdir()
    cache_dir.mkdir()

    catalog_csv = meta_dir / "datamart_catalog.csv"
    geoid_path = tmp_path / "GEOID_reference.csv"
    zip_path = tmp_path / "zip_to_county_fips.csv"
    output_path = staging_dir / "givingtuesday_990_basic_allforms_presilver.parquet"
    manifest_path = staging_dir / "manifest_basic_allforms_presilver.json"
    combined_cache_parquet = cache_dir / "combined_normalized.parquet"
    combined_cache_manifest = cache_dir / "combined_normalized_manifest.json"

    pd.DataFrame(
        [
            {"dataset_id": "combined", "title": "Combined Forms Datamart", "form_type": "", "download_url": "https://example.org/combined.csv"},
            {"dataset_id": "basic-990", "title": "Basic Fields", "form_type": "990", "download_url": "https://example.org/basic_990.csv"},
            {"dataset_id": "basic-990ez", "title": "Basic Fields", "form_type": "990EZ", "download_url": "https://example.org/basic_990ez.csv"},
            {"dataset_id": "basic-990pf", "title": "Basic Fields", "form_type": "990PF", "download_url": "https://example.org/basic_990pf.csv"},
        ]
    ).to_csv(catalog_csv, index=False)

    pd.DataFrame(
        [
            {"FILEREIN": "200000001", "TAXYEAR": "2023", "RETURNTYPE": "990", "FILERNAME1": "Alpha Org", "FILERUSZIP": "57701", "FILERUSSTATE": "SD"}
        ]
    ).to_csv(raw_dir / "basic_990.csv", index=False)
    pd.DataFrame(
        [
            {
                "FILEREIN": "111111111",
                "TAXYEAR": "2023",
                "RETURNTYPE": "990EZ",
                "FILERNAME1": "  Example   Org  ",
                "FILERUSZIP": "57701-1234",
                "FILERUSSTATE": " sd ",
                "FILERPHONE": "(605) 555-1212",
                "TAXPERBEGIN": "01/01/2023",
                "TAXPEREND": "12/31/2023",
            }
        ]
    ).to_csv(raw_dir / "basic_990ez.csv", index=False)
    pd.DataFrame(
        [
            {"FILEREIN": "300000001", "TAXYEAR": "2023", "RETURNTYPE": "990PF", "FILERNAME1": "Gamma Foundation", "FILERUSZIP": "57701", "FILERUSSTATE": "SD"}
        ]
    ).to_csv(raw_dir / "basic_990pf.csv", index=False)
    pd.DataFrame(
        [
            {"FILEREIN": "999999999", "TAXYEAR": "2023", "RETURNTYPE": "990", "FILERUSZIP": "57701", "FILERUSSTATE": "SD"}
        ]
    ).to_csv(raw_dir / "combined.csv", index=False)
    pd.DataFrame([{"GEOID": "46093", "Cluster_name": "BlackHills"}]).to_csv(geoid_path, index=False)
    pd.DataFrame([{"ZIP": "57701", "FIPS": "46093"}]).to_csv(zip_path, index=False)

    monkeypatch.setattr(gt_build_basic, "load_env_from_secrets", lambda: None)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "06_build_basic_allforms_presilver.py",
            "--catalog-csv",
            str(catalog_csv),
            "--raw-dir",
                str(raw_dir),
                "--geoid-csv",
                str(geoid_path),
                "--zip-to-county",
                str(zip_path),
                "--combined-cache-parquet",
                str(combined_cache_parquet),
                "--combined-cache-manifest-json",
                str(combined_cache_manifest),
                "--output",
                str(output_path),
            "--build-manifest-json",
            str(manifest_path),
            "--no-skip-if-unchanged",
        ],
    )

    gt_build_basic.main()

    built = pd.read_parquet(output_path)
    row = built.loc[built["ein"] == "111111111"].iloc[0]
    assert row["FILERNAME1"] == "EXAMPLE ORG"
    assert row["FILERUSZIP"] == "57701"
    assert row["FILERUSSTATE"] == "SD"
    assert row["FILERPHONE"] == "6055551212"
    assert row["TAXPERBEGIN"] == "2023-01-01"
    assert row["TAXPEREND"] == "2023-12-31"


def test_step06_prefers_amended_or_final_basic_duplicate_upstream(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    raw_dir = tmp_path / "raw"
    meta_dir = tmp_path / "meta"
    staging_dir = tmp_path / "staging"
    cache_dir = tmp_path / "cache"
    raw_dir.mkdir()
    meta_dir.mkdir()
    staging_dir.mkdir()
    cache_dir.mkdir()

    catalog_csv = meta_dir / "datamart_catalog.csv"
    geoid_path = tmp_path / "GEOID_reference.csv"
    zip_path = tmp_path / "zip_to_county_fips.csv"
    output_path = staging_dir / "givingtuesday_990_basic_allforms_presilver.parquet"
    manifest_path = staging_dir / "manifest_basic_allforms_presilver.json"
    combined_cache_parquet = cache_dir / "combined_normalized.parquet"
    combined_cache_manifest = cache_dir / "combined_normalized_manifest.json"

    pd.DataFrame(
        [
            {"dataset_id": "combined", "title": "Combined Forms Datamart", "form_type": "", "download_url": "https://example.org/combined.csv"},
            {"dataset_id": "basic-990", "title": "Basic Fields", "form_type": "990", "download_url": "https://example.org/basic_990.csv"},
            {"dataset_id": "basic-990ez", "title": "Basic Fields", "form_type": "990EZ", "download_url": "https://example.org/basic_990ez.csv"},
            {"dataset_id": "basic-990pf", "title": "Basic Fields", "form_type": "990PF", "download_url": "https://example.org/basic_990pf.csv"},
        ]
    ).to_csv(catalog_csv, index=False)

    pd.DataFrame(
        [
            {
                "FILEREIN": "111111111",
                "TAXYEAR": "2023",
                "RETURNTYPE": "990",
                "FILERNAME1": "Example Org",
                "FILERUSZIP": "57701",
                "FILERUSSTATE": "SD",
                "FILERPHONE": "6055550000",
                "TAXPEREND": "2023-12-31",
                "URL": "https://example.org/original.xml",
                "AMENDERETURN": "",
                "FINALRRETURN": "",
                "TOTREVCURYEA": "500",
            },
            {
                "FILEREIN": "111111111",
                "TAXYEAR": "2023",
                "RETURNTYPE": "990",
                "FILERNAME1": "Example Org",
                "FILERUSZIP": "57701",
                "FILERUSSTATE": "SD",
                "FILERPHONE": "",
                "TAXPEREND": "2024-01-15",
                "URL": "https://example.org/amended.xml",
                "AMENDERETURN": "X",
                "FINALRRETURN": "",
                "TOTREVCURYEA": "450",
            },
        ]
    ).to_csv(raw_dir / "basic_990.csv", index=False)
    pd.DataFrame(
        [
            {"FILEREIN": "222222222", "TAXYEAR": "2023", "RETURNTYPE": "990EZ", "FILERNAME1": "EZ Org", "FILERUSZIP": "57701", "FILERUSSTATE": "SD"}
        ]
    ).to_csv(raw_dir / "basic_990ez.csv", index=False)
    pd.DataFrame(
        [
            {"FILEREIN": "333333333", "TAXYEAR": "2023", "RETURNTYPE": "990PF", "FILERNAME1": "PF Org", "FILERUSZIP": "57701", "FILERUSSTATE": "SD"}
        ]
    ).to_csv(raw_dir / "basic_990pf.csv", index=False)
    pd.DataFrame(
        [
            {"FILEREIN": "999999999", "TAXYEAR": "2023", "RETURNTYPE": "990", "FILERUSZIP": "57701", "FILERUSSTATE": "SD"}
        ]
    ).to_csv(raw_dir / "combined.csv", index=False)
    pd.DataFrame([{"GEOID": "46093", "Cluster_name": "BlackHills"}]).to_csv(geoid_path, index=False)
    pd.DataFrame([{"ZIP": "57701", "FIPS": "46093"}]).to_csv(zip_path, index=False)

    monkeypatch.setattr(gt_build_basic, "load_env_from_secrets", lambda: None)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "06_build_basic_allforms_presilver.py",
            "--catalog-csv",
            str(catalog_csv),
            "--raw-dir",
                str(raw_dir),
                "--geoid-csv",
                str(geoid_path),
                "--zip-to-county",
                str(zip_path),
                "--combined-cache-parquet",
                str(combined_cache_parquet),
                "--combined-cache-manifest-json",
                str(combined_cache_manifest),
                "--output",
                str(output_path),
            "--build-manifest-json",
            str(manifest_path),
            "--no-skip-if-unchanged",
        ],
    )

    gt_build_basic.main()

    built = pd.read_parquet(output_path)
    row = built.loc[(built["ein"] == "111111111") & (built["tax_year"] == "2023") & (built["returntype_norm"] == "990")].iloc[0]
    assert len(built.loc[(built["ein"] == "111111111") & (built["tax_year"] == "2023") & (built["returntype_norm"] == "990")]) == 1
    assert row["AMENDERETURN"] == "X"
    assert row["URL"] == "https://example.org/amended.xml"
    assert row["TOTREVCURYEA"] == "450"


def test_step07_prefers_most_complete_basic_duplicate_and_normalizes_combined_fields(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_dir = tmp_path / "raw"
    meta_dir = tmp_path / "meta"
    staging_dir = tmp_path / "staging"
    raw_dir.mkdir()
    meta_dir.mkdir()
    staging_dir.mkdir()

    catalog_csv = meta_dir / "datamart_catalog.csv"
    basic_parquet = staging_dir / "givingtuesday_990_basic_allforms_presilver.parquet"
    output_path = staging_dir / "givingtuesday_990_basic_plus_combined_presilver.parquet"
    manifest_path = staging_dir / "manifest_basic_plus_combined_presilver.json"
    combined_cache_parquet = tmp_path / "cache" / "combined_normalized.parquet"
    combined_cache_manifest = tmp_path / "cache" / "combined_normalized_manifest.json"
    geoid_path = tmp_path / "GEOID_reference.csv"
    zip_path = tmp_path / "zip_to_county_fips.csv"

    pd.DataFrame(
        [
            {"dataset_id": "combined", "title": "Combined Forms Datamart", "form_type": "", "download_url": "https://example.org/combined.csv"},
            {"dataset_id": "basic-990", "title": "Basic Fields", "form_type": "990", "download_url": "https://example.org/basic_990.csv"},
            {"dataset_id": "basic-990ez", "title": "Basic Fields", "form_type": "990EZ", "download_url": "https://example.org/basic_990ez.csv"},
            {"dataset_id": "basic-990pf", "title": "Basic Fields", "form_type": "990PF", "download_url": "https://example.org/basic_990pf.csv"},
        ]
    ).to_csv(catalog_csv, index=False)
    pd.DataFrame([{"GEOID": "46093", "Cluster_name": "BlackHills"}]).to_csv(geoid_path, index=False)
    pd.DataFrame([{"ZIP": "57701", "FIPS": "46093"}]).to_csv(zip_path, index=False)

    pd.DataFrame(
        [
            {
                "FILEREIN": "111111111",
                "TAXYEAR": "2023",
                "RETURNTYPE": "990-EZ",
                "FILERNAME1": " example org ",
                "FILERUSZIP": "57701-0000",
                "FILERUSSTATE": " sd ",
                "FILERPHONE": "",
                "TAXPERBEGIN": "2023/01/01",
                "TAXPEREND": "12/31/2023",
                "TOLIEOOYY": "",
            }
        ]
    ).to_csv(raw_dir / "combined.csv", index=False)

    pd.DataFrame(
        [
            _presilver_row(
                {
                "ein": "111111111",
                "tax_year": "2023",
                "returntype_norm": "990EZ",
                "form_type": "990EZ",
                "FILERNAME1": "EXAMPLE ORG",
                "FILERUSZIP": "57701",
                "FILERUSSTATE": "SD",
                "FILERPHONE": "",
                "TAXPERBEGIN": "2023-01-01",
                "TAXPEREND": "2023-12-31",
                "TOLIEOOYY": "",
                "OFFICERSIGNDATE": "2024-01-01",
                "source_dataset_id": "basic-990ez",
                "source_dataset_title": "Basic Fields",
                "source_filename": "basic_990ez.csv",
                "from_basic": "1",
                "from_combined": "0",
                }
            ),
            _presilver_row(
                {
                "ein": "111111111",
                "tax_year": "2023",
                "returntype_norm": "990EZ",
                "form_type": "990EZ",
                "FILERNAME1": "EXAMPLE ORG",
                "FILERUSZIP": "57701",
                "FILERUSSTATE": "SD",
                "FILERPHONE": "6055551212",
                "TAXPERBEGIN": "2023-01-01",
                "TAXPEREND": "2023-12-31",
                "TOLIEOOYY": "335",
                "OFFICERSIGNDATE": "2024-01-10",
                "source_dataset_id": "basic-990ez",
                "source_dataset_title": "Basic Fields",
                "source_filename": "basic_990ez.csv",
                "from_basic": "1",
                "from_combined": "0",
                }
            ),
        ]
    ).to_parquet(basic_parquet, index=False)

    monkeypatch.setattr(gt_build_mixed, "load_env_from_secrets", lambda: None)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "07_build_basic_plus_combined_presilver.py",
            "--catalog-csv",
            str(catalog_csv),
            "--raw-dir",
            str(raw_dir),
            "--basic-parquet",
            str(basic_parquet),
            "--output",
            str(output_path),
            "--build-manifest-json",
            str(manifest_path),
            "--combined-cache-parquet",
            str(combined_cache_parquet),
            "--combined-cache-manifest-json",
            str(combined_cache_manifest),
            "--geoid-csv",
            str(geoid_path),
            "--zip-to-county",
            str(zip_path),
            "--no-skip-if-unchanged",
        ],
    )

    gt_build_mixed.main()

    built = pd.read_parquet(output_path)
    row = built.loc[(built["ein"] == "111111111") & (built["tax_year"] == "2023")].iloc[0]
    assert row["FILERNAME1"] == "EXAMPLE ORG"
    assert row["FILERUSZIP"] == "57701"
    assert row["FILERUSSTATE"] == "SD"
    assert row["TAXPERBEGIN"] == "2023-01-01"
    assert row["TAXPEREND"] == "2023-12-31"
    assert row["FILERPHONE"] == "6055551212"
    assert row["TOLIEOOYY"] == "335"
    assert int(row["field_fill_count_from_basic"]) >= 2
    assert combined_cache_parquet.exists()
    assert combined_cache_manifest.exists()
    assert row["roi_admitted_by_basic"] == "1"
    assert row["roi_admitted_by_combined"] == "1"
    assert row["roi_admission_basis"] == "both"


def test_step07_prefers_amended_basic_duplicate_before_completeness(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_dir = tmp_path / "raw"
    meta_dir = tmp_path / "meta"
    staging_dir = tmp_path / "staging"
    raw_dir.mkdir()
    meta_dir.mkdir()
    staging_dir.mkdir()

    catalog_csv = meta_dir / "datamart_catalog.csv"
    basic_parquet = staging_dir / "givingtuesday_990_basic_allforms_presilver.parquet"
    output_path = staging_dir / "givingtuesday_990_basic_plus_combined_presilver.parquet"
    manifest_path = staging_dir / "manifest_basic_plus_combined_presilver.json"
    combined_cache_parquet = tmp_path / "cache" / "combined_normalized.parquet"
    combined_cache_manifest = tmp_path / "cache" / "combined_normalized_manifest.json"
    geoid_path = tmp_path / "GEOID_reference.csv"
    zip_path = tmp_path / "zip_to_county_fips.csv"

    pd.DataFrame(
        [
            {"dataset_id": "combined", "title": "Combined Forms Datamart", "form_type": "", "download_url": "https://example.org/combined.csv"},
            {"dataset_id": "basic-990", "title": "Basic Fields", "form_type": "990", "download_url": "https://example.org/basic_990.csv"},
            {"dataset_id": "basic-990ez", "title": "Basic Fields", "form_type": "990EZ", "download_url": "https://example.org/basic_990ez.csv"},
            {"dataset_id": "basic-990pf", "title": "Basic Fields", "form_type": "990PF", "download_url": "https://example.org/basic_990pf.csv"},
        ]
    ).to_csv(catalog_csv, index=False)
    pd.DataFrame([{"GEOID": "46093", "Cluster_name": "BlackHills"}]).to_csv(geoid_path, index=False)
    pd.DataFrame([{"ZIP": "57701", "FIPS": "46093"}]).to_csv(zip_path, index=False)

    pd.DataFrame(
        [
            {
                "FILEREIN": "111111111",
                "TAXYEAR": "2023",
                "RETURNTYPE": "990",
                "FILERNAME1": " example org ",
                "FILERUSZIP": "57701-0000",
                "FILERUSSTATE": " sd ",
            }
        ]
    ).to_csv(raw_dir / "combined.csv", index=False)

    pd.DataFrame(
        [
            _presilver_row(
                {
                "ein": "111111111",
                "tax_year": "2023",
                "returntype_norm": "990",
                "form_type": "990",
                "FILERNAME1": "EXAMPLE ORG",
                "FILERUSZIP": "57701",
                "FILERUSSTATE": "SD",
                "FILERPHONE": "6055551212",
                "TAXPEREND": "2023-12-31",
                "AMENDERETURN": "",
                "FINALRRETURN": "",
                "URL": "https://example.org/original.xml",
                "source_dataset_id": "basic-990",
                "source_dataset_title": "Basic Fields",
                "source_filename": "basic_990.csv",
                "from_basic": "1",
                "from_combined": "0",
                }
            ),
            _presilver_row(
                {
                "ein": "111111111",
                "tax_year": "2023",
                "returntype_norm": "990",
                "form_type": "990",
                "FILERNAME1": "EXAMPLE ORG",
                "FILERUSZIP": "57701",
                "FILERUSSTATE": "SD",
                "FILERPHONE": "",
                "TAXPEREND": "2024-01-15",
                "AMENDERETURN": "X",
                "FINALRRETURN": "",
                "URL": "https://example.org/amended.xml",
                "source_dataset_id": "basic-990",
                "source_dataset_title": "Basic Fields",
                "source_filename": "basic_990.csv",
                "from_basic": "1",
                "from_combined": "0",
                }
            ),
        ]
    ).to_parquet(basic_parquet, index=False)

    monkeypatch.setattr(gt_build_mixed, "load_env_from_secrets", lambda: None)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "07_build_basic_plus_combined_presilver.py",
            "--catalog-csv",
            str(catalog_csv),
            "--raw-dir",
            str(raw_dir),
            "--basic-parquet",
            str(basic_parquet),
            "--output",
            str(output_path),
            "--build-manifest-json",
            str(manifest_path),
            "--combined-cache-parquet",
            str(combined_cache_parquet),
            "--combined-cache-manifest-json",
            str(combined_cache_manifest),
            "--geoid-csv",
            str(geoid_path),
            "--zip-to-county",
            str(zip_path),
            "--no-skip-if-unchanged",
        ],
    )

    gt_build_mixed.main()

    built = pd.read_parquet(output_path)
    row = built.loc[(built["ein"] == "111111111") & (built["tax_year"] == "2023")].iloc[0]
    assert row["AMENDERETURN"] == "X"
    assert row["URL"] == "https://example.org/amended.xml"


def test_step07_prefers_final_combined_duplicate_before_completeness(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_dir = tmp_path / "raw"
    meta_dir = tmp_path / "meta"
    staging_dir = tmp_path / "staging"
    raw_dir.mkdir()
    meta_dir.mkdir()
    staging_dir.mkdir()

    catalog_csv = meta_dir / "datamart_catalog.csv"
    basic_parquet = staging_dir / "givingtuesday_990_basic_allforms_presilver.parquet"
    output_path = staging_dir / "givingtuesday_990_basic_plus_combined_presilver.parquet"
    manifest_path = staging_dir / "manifest_basic_plus_combined_presilver.json"
    combined_cache_parquet = tmp_path / "cache" / "combined_normalized.parquet"
    combined_cache_manifest = tmp_path / "cache" / "combined_normalized_manifest.json"
    geoid_path = tmp_path / "GEOID_reference.csv"
    zip_path = tmp_path / "zip_to_county_fips.csv"

    pd.DataFrame(
        [
            {"dataset_id": "combined", "title": "Combined Forms Datamart", "form_type": "", "download_url": "https://example.org/combined.csv"},
            {"dataset_id": "basic-990", "title": "Basic Fields", "form_type": "990", "download_url": "https://example.org/basic_990.csv"},
            {"dataset_id": "basic-990ez", "title": "Basic Fields", "form_type": "990EZ", "download_url": "https://example.org/basic_990ez.csv"},
            {"dataset_id": "basic-990pf", "title": "Basic Fields", "form_type": "990PF", "download_url": "https://example.org/basic_990pf.csv"},
        ]
    ).to_csv(catalog_csv, index=False)
    pd.DataFrame([{"GEOID": "46093", "Cluster_name": "BlackHills"}]).to_csv(geoid_path, index=False)
    pd.DataFrame([{"ZIP": "57701", "FIPS": "46093"}]).to_csv(zip_path, index=False)

    pd.DataFrame(
        [
            {
                "FILEREIN": "111111111",
                "TAXYEAR": "2023",
                "RETURNTYPE": "990",
                "FILERNAME1": "Example Org",
                "FILERUSZIP": "57701",
                "FILERUSSTATE": "SD",
                "FILERPHONE": "6055551212",
                "TAXPEREND": "2023-12-31",
                "FINALRRETURN": "",
                "URL": "https://example.org/original.xml",
            },
            {
                "FILEREIN": "111111111",
                "TAXYEAR": "2023",
                "RETURNTYPE": "990",
                "FILERNAME1": "Example Org",
                "FILERUSZIP": "57701",
                "FILERUSSTATE": "SD",
                "FILERPHONE": "",
                "TAXPEREND": "2024-01-15",
                "FINALRRETURN": "X",
                "URL": "https://example.org/final.xml",
            },
        ]
    ).to_csv(raw_dir / "combined.csv", index=False)

    pd.DataFrame(
        [
            _presilver_row(
                {
                "ein": "111111111",
                "tax_year": "2023",
                "returntype_norm": "990",
                "form_type": "990",
                "FILERNAME1": "EXAMPLE ORG",
                "FILERUSZIP": "57701",
                "FILERUSSTATE": "SD",
                "source_dataset_id": "basic-990",
                "source_dataset_title": "Basic Fields",
                "source_filename": "basic_990.csv",
                "from_basic": "1",
                "from_combined": "0",
                }
            ),
        ]
    ).to_parquet(basic_parquet, index=False)

    monkeypatch.setattr(gt_build_mixed, "load_env_from_secrets", lambda: None)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "07_build_basic_plus_combined_presilver.py",
            "--catalog-csv",
            str(catalog_csv),
            "--raw-dir",
            str(raw_dir),
            "--basic-parquet",
            str(basic_parquet),
            "--output",
            str(output_path),
            "--build-manifest-json",
            str(manifest_path),
            "--combined-cache-parquet",
            str(combined_cache_parquet),
            "--combined-cache-manifest-json",
            str(combined_cache_manifest),
            "--geoid-csv",
            str(geoid_path),
            "--zip-to-county",
            str(zip_path),
            "--no-skip-if-unchanged",
        ],
    )

    gt_build_mixed.main()

    built = pd.read_parquet(output_path)
    kept = built.loc[(built["ein"] == "111111111") & (built["tax_year"] == "2023") & (built["form_type"] == "990")]
    assert len(kept) == 1
    row = kept.iloc[0]
    assert row["FINALRRETURN"] == "X"
    assert row["URL"] == "https://example.org/final.xml"
    assert row["FILERPHONE"] == ""


def test_step07_restricts_merge_to_roi_key_union_and_preserves_basic_rescue(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_dir = tmp_path / "raw"
    meta_dir = tmp_path / "meta"
    staging_dir = tmp_path / "staging"
    raw_dir.mkdir()
    meta_dir.mkdir()
    staging_dir.mkdir()

    catalog_csv = meta_dir / "datamart_catalog.csv"
    basic_parquet = staging_dir / "givingtuesday_990_basic_allforms_presilver.parquet"
    output_path = staging_dir / "givingtuesday_990_basic_plus_combined_presilver.parquet"
    manifest_path = staging_dir / "manifest_basic_plus_combined_presilver.json"
    combined_cache_parquet = tmp_path / "cache" / "combined_normalized.parquet"
    combined_cache_manifest = tmp_path / "cache" / "combined_normalized_manifest.json"
    geoid_path = tmp_path / "GEOID_reference.csv"
    zip_path = tmp_path / "zip_to_county_fips.csv"

    pd.DataFrame(
        [
            {"dataset_id": "combined", "title": "Combined Forms Datamart", "form_type": "", "download_url": "https://example.org/combined.csv"},
            {"dataset_id": "basic-990", "title": "Basic Fields", "form_type": "990", "download_url": "https://example.org/basic_990.csv"},
            {"dataset_id": "basic-990ez", "title": "Basic Fields", "form_type": "990EZ", "download_url": "https://example.org/basic_990ez.csv"},
            {"dataset_id": "basic-990pf", "title": "Basic Fields", "form_type": "990PF", "download_url": "https://example.org/basic_990pf.csv"},
        ]
    ).to_csv(catalog_csv, index=False)
    pd.DataFrame([{"GEOID": "46093", "Cluster_name": "BlackHills"}]).to_csv(geoid_path, index=False)
    pd.DataFrame([{"ZIP": "57701", "FIPS": "46093"}]).to_csv(zip_path, index=False)

    pd.DataFrame(
        [
            {
                "FILEREIN": "111111111",
                "TAXYEAR": "2023",
                "RETURNTYPE": "990",
                "FILERNAME1": "Combined Rescue",
                "FILERUSZIP": "99999",
                "FILERUSSTATE": "CA",
            },
            {
                "FILEREIN": "222222222",
                "TAXYEAR": "2023",
                "RETURNTYPE": "990",
                "FILERNAME1": "Combined Only",
                "FILERUSZIP": "57701",
                "FILERUSSTATE": "SD",
            },
            {
                "FILEREIN": "444444444",
                "TAXYEAR": "2023",
                "RETURNTYPE": "990",
                "FILERNAME1": "Outside Both",
                "FILERUSZIP": "99999",
                "FILERUSSTATE": "CA",
            },
        ]
    ).to_csv(raw_dir / "combined.csv", index=False)

    pd.DataFrame(
        [
            _presilver_row(
                {
                "ein": "111111111",
                "tax_year": "2023",
                "returntype_norm": "990",
                "form_type": "990",
                "FILERNAME1": "BASIC RESCUE",
                "FILERUSZIP": "57701",
                "FILERUSSTATE": "SD",
                "source_dataset_id": "basic-990",
                "source_dataset_title": "Basic Fields",
                "source_filename": "basic_990.csv",
                "from_basic": "1",
                "from_combined": "0",
                }
            ),
            _presilver_row(
                {
                "ein": "333333333",
                "tax_year": "2023",
                "returntype_norm": "990",
                "form_type": "990",
                "FILERNAME1": "Basic Only",
                "FILERUSZIP": "57701",
                "FILERUSSTATE": "SD",
                "source_dataset_id": "basic-990",
                "source_dataset_title": "Basic Fields",
                "source_filename": "basic_990.csv",
                "from_basic": "1",
                "from_combined": "0",
                }
            ),
        ]
    ).to_parquet(basic_parquet, index=False)

    monkeypatch.setattr(gt_build_mixed, "load_env_from_secrets", lambda: None)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "07_build_basic_plus_combined_presilver.py",
            "--catalog-csv",
            str(catalog_csv),
            "--raw-dir",
            str(raw_dir),
            "--basic-parquet",
            str(basic_parquet),
            "--output",
            str(output_path),
            "--build-manifest-json",
            str(manifest_path),
            "--combined-cache-parquet",
            str(combined_cache_parquet),
            "--combined-cache-manifest-json",
            str(combined_cache_manifest),
            "--geoid-csv",
            str(geoid_path),
            "--zip-to-county",
            str(zip_path),
            "--no-skip-if-unchanged",
        ],
    )

    gt_build_mixed.main()

    built = pd.read_parquet(output_path).sort_values(["ein"]).reset_index(drop=True)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert built["ein"].tolist() == ["111111111", "222222222", "333333333"]
    rescue_row = built.loc[built["ein"] == "111111111"].iloc[0]
    combined_only_row = built.loc[built["ein"] == "222222222"].iloc[0]
    basic_only_row = built.loc[built["ein"] == "333333333"].iloc[0]

    assert rescue_row["FILERUSZIP"] == "57701"
    assert rescue_row["FILERUSSTATE"] == "SD"
    assert rescue_row["roi_admitted_by_basic"] == "1"
    assert rescue_row["roi_admitted_by_combined"] == "0"
    assert rescue_row["roi_admission_basis"] == "basic"
    assert rescue_row["roi_geography_override_from_basic"] == "1"
    assert int(rescue_row["roi_geography_override_field_count_from_basic"]) >= 1

    assert combined_only_row["roi_admission_basis"] == "combined"
    assert combined_only_row["roi_admitted_by_basic"] == "0"
    assert combined_only_row["roi_admitted_by_combined"] == "1"
    assert combined_only_row["from_combined"] == "1"

    assert basic_only_row["roi_admission_basis"] == "basic"
    assert basic_only_row["from_combined"] == "0"
    assert basic_only_row["from_basic"] == "1"

    assert manifest["roi_basic_key_count"] == 2
    assert manifest["roi_combined_key_count"] == 1
    assert manifest["roi_key_union_count"] == 3
    assert manifest["rows_output"] == 3


def test_combined_cache_helper_reuses_manifest_when_raw_csv_is_unchanged(tmp_path: Path) -> None:
    combined_csv = tmp_path / "combined.csv"
    output_path = tmp_path / "cache" / "combined_normalized.parquet"
    manifest_path = tmp_path / "cache" / "combined_normalized_manifest.json"
    pd.DataFrame(
        [
            {"FILEREIN": "111111111", "TAXYEAR": "2023", "RETURNTYPE": "990EZ", "FILERNAME1": "Example Org"},
            {"FILEREIN": "222222222", "TAXYEAR": "2024", "RETURNTYPE": "990", "FILERNAME1": "Another Org"},
        ]
    ).to_csv(combined_csv, index=False)

    first_payload = gt_common.ensure_combined_normalized_cache(
        combined_csv_path=combined_csv,
        output_path=output_path,
        manifest_path=manifest_path,
        force_rebuild=False,
    )
    manifest_after_first_build = json.loads(manifest_path.read_text(encoding="utf-8"))
    second_payload = gt_common.ensure_combined_normalized_cache(
        combined_csv_path=combined_csv,
        output_path=output_path,
        manifest_path=manifest_path,
        force_rebuild=False,
    )
    manifest_after_second_call = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert output_path.exists()
    assert manifest_path.exists()
    assert first_payload["rows_output"] == 2
    assert second_payload["rows_output"] == 2
    assert manifest_after_first_build == manifest_after_second_call


def test_step13_extracts_gt_analysis_variables_and_flags_unresolved_concepts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_path = tmp_path / "givingtuesday_990_basic_allforms_benchmark.parquet"
    output_path = tmp_path / "givingtuesday_990_basic_allforms_analysis_variables.parquet"
    region_metrics_output_path = tmp_path / "givingtuesday_990_basic_allforms_analysis_region_metrics.parquet"
    coverage_path = tmp_path / "givingtuesday_990_basic_allforms_analysis_variable_coverage.csv"
    mapping_path = tmp_path / "docs" / "analysis" / "givingtuesday_basic_analysis_variable_mapping.md"
    bmf_lookup_2022 = tmp_path / "nccs_bmf_exact_year_lookup_year=2022.parquet"
    bmf_lookup_2023 = tmp_path / "nccs_bmf_exact_year_lookup_year=2023.parquet"
    bmf_lookup_2024 = tmp_path / "nccs_bmf_exact_year_lookup_year=2024.parquet"
    irs_bmf_dir = tmp_path / "irs_bmf"
    irs_bmf_dir.mkdir()
    irs_bmf_sd = irs_bmf_dir / "eo_sd.csv"

    pd.DataFrame(
        [
            {
                "ein": "111111111",
                "tax_year": "2023",
                "form_type": "990",
                "FILERNAME1": "ALPHA ORG",
                "FILERUSSTATE": "SD",
                "zip5": "57701",
                "county_fips": "46093",
                "region": "BlackHills",
                "TOTREVCURYEA": "100",
                "TOTALRREVENU": "",
                "TOTEXPCURYEA": "80",
                "TOTALEEXPENS": "",
                "TOASEOOYY": "100",
                "TOLIEOOYY": "20",
                "derived_income_amount": "20",
                "TOTPROSERREV": "60",
                "PROGSERVREVE": "",
                "TOTACASHCONT": "10",
                "NONCASCONTRI": "5",
                "ALLOOTHECONT": "2",
                "FOREGRANTOTA": "3",
                "GOVERNGRANTS": "4",
                "GRANTOORORGA": "1",
                "OPERATHOSPIT": "Y",
                "OPERATSCHOOL": "",
            },
            {
                "ein": "222222222",
                "tax_year": "2024",
                "form_type": "990EZ",
                "FILERNAME1": "BETA ORG",
                "FILERUSSTATE": "MT",
                "zip5": "59801",
                "county_fips": "30063",
                "region": "Missoula",
                "TOTREVCURYEA": "",
                "TOTALRREVENU": "50",
                "TOTEXPCURYEA": "",
                "TOTALEEXPENS": "40",
                "TOASEOOYY": "75",
                "TOLIEOOYY": "15",
                "derived_income_amount": "10",
                "TOTPROSERREV": "",
                "PROGSERVREVE": "12",
                "TOTACASHCONT": "",
                "NONCASCONTRI": "",
                "ALLOOTHECONT": "",
                "FOREGRANTOTA": "",
                "GOVERNGRANTS": "",
                "GRANTOORORGA": "",
                "OPERATHOSPIT": "0",
                "OPERATSCHOOL": "X",
            },
            {
                "ein": "333333333",
                "tax_year": "2024",
                "form_type": "990PF",
                "FILERNAME1": "GAMMA FOUNDATION",
                "FILERUSSTATE": "WY",
                "zip5": "82001",
                "county_fips": "56021",
                "region": "Laramie",
                "TOTREVCURYEA": "",
                "TOTALRREVENU": "",
                "TOTEXPCURYEA": "",
                "TOTALEEXPENS": "",
                "ANREEXTOREEX": "90",
                "ARETEREXPNSS": "70",
                "AREEROEXPENS": "20",
                "TOASEOOYY": "",
                "TOLIEOOYY": "",
                "derived_income_amount": "",
                "TOTPROSERREV": "",
                "PROGSERVREVE": "",
                "TOTACASHCONT": "",
                "NONCASCONTRI": "",
                "ALLOOTHECONT": "",
                "FOREGRANTOTA": "",
                "GOVERNGRANTS": "",
                "GRANTOORORGA": "",
                "OPERATHOSPIT": "",
                "OPERATSCHOOL": "",
            },
            {
                "ein": "444444444",
                "tax_year": "2023",
                "form_type": "990",
                "FILERNAME1": "DELTA ORG",
                "FILERUSSTATE": "SD",
                "zip5": "57701",
                "county_fips": "46093",
                "region": "BlackHills",
                "TOTREVCURYEA": "40",
                "TOTALRREVENU": "",
                "TOTEXPCURYEA": "20",
                "TOTALEEXPENS": "",
                "TOASEOOYY": "50",
                "TOLIEOOYY": "10",
                "derived_income_amount": "20",
                "TOTPROSERREV": "5",
                "PROGSERVREVE": "",
                "TOTACASHCONT": "",
                "NONCASCONTRI": "",
                "ALLOOTHECONT": "",
                "FOREGRANTOTA": "",
                "GOVERNGRANTS": "",
                "GRANTOORORGA": "",
                "OPERATHOSPIT": "0",
                "OPERATSCHOOL": "",
            },
            {
                "ein": "555555555",
                "tax_year": "2024",
                "form_type": "990",
                "FILERNAME1": "EPSILON ORG",
                "FILERUSSTATE": "SD",
                "zip5": "57701",
                "county_fips": "46093",
                "region": "BlackHills",
                "TOTREVCURYEA": "55",
                "TOTALRREVENU": "",
                "TOTEXPCURYEA": "25",
                "TOTALEEXPENS": "",
                "TOASEOOYY": "65",
                "TOLIEOOYY": "15",
                "derived_income_amount": "30",
                "TOTPROSERREV": "8",
                "PROGSERVREVE": "",
                "TOTACASHCONT": "",
                "NONCASCONTRI": "",
                "ALLOOTHECONT": "",
                "FOREGRANTOTA": "",
                "GOVERNGRANTS": "",
                "GRANTOORORGA": "",
                "OPERATHOSPIT": "",
                "OPERATSCHOOL": "",
            },
            {
                "ein": "666666666",
                "tax_year": "2024",
                "form_type": "990",
                "FILERNAME1": "ZETA ORG",
                "FILERUSSTATE": "SD",
                "zip5": "57701",
                "county_fips": "46093",
                "region": "BlackHills",
                "TOTREVCURYEA": "-10",
                "TOTALRREVENU": "",
                "TOTEXPCURYEA": "5",
                "TOTALEEXPENS": "",
                "TOASEOOYY": "10",
                "TOLIEOOYY": "2",
                "derived_income_amount": "-15",
                "TOTPROSERREV": "0",
                "PROGSERVREVE": "",
                "TOTACASHCONT": "",
                "NONCASCONTRI": "",
                "ALLOOTHECONT": "",
                "FOREGRANTOTA": "5",
                "GOVERNGRANTS": "0",
                "GRANTOORORGA": "0",
                "OPERATHOSPIT": "",
                "OPERATSCHOOL": "",
            },
        ]
    ).to_parquet(input_path, index=False)
    pd.DataFrame(
        [
            {
                "harm_ein": "444444444",
                "harm_tax_year": "2022",
                "harm_ntee_code": "A12",
                "harm_ntee_code__source_family": "nccs_bmf",
                "harm_ntee_code__source_variant": "legacy_bmf_lookup_2022",
                "harm_ntee_code__source_column": "NTEEFINAL",
                "harm_subsection_code": "4",
                "harm_subsection_code__source_family": "nccs_bmf",
                "harm_subsection_code__source_variant": "legacy_bmf_lookup_2022",
                "harm_subsection_code__source_column": "SUBSECCD",
            }
        ]
    ).to_parquet(bmf_lookup_2022, index=False)
    pd.DataFrame(
        [
            {
                "harm_ein": "111111111",
                "harm_tax_year": "2023",
                "harm_ntee_code": "E22",
                "harm_ntee_code__source_family": "nccs_bmf",
                "harm_ntee_code__source_variant": "legacy_bmf_lookup_2023",
                "harm_ntee_code__source_column": "NTEEFINAL",
                "harm_subsection_code": "3",
                "harm_subsection_code__source_family": "nccs_bmf",
                "harm_subsection_code__source_variant": "legacy_bmf_lookup_2023",
                "harm_subsection_code__source_column": "SUBSECCD",
            },
            {
                "harm_ein": "333333333",
                "harm_tax_year": "2023",
                "harm_ntee_code": "W24",
                "harm_ntee_code__source_family": "nccs_bmf",
                "harm_ntee_code__source_variant": "legacy_bmf_lookup_2023",
                "harm_ntee_code__source_column": "NTEEFINAL",
                "harm_subsection_code": "527",
                "harm_subsection_code__source_family": "nccs_bmf",
                "harm_subsection_code__source_variant": "legacy_bmf_lookup_2023",
                "harm_subsection_code__source_column": "SUBSECCD",
            },
        ]
    ).to_parquet(bmf_lookup_2023, index=False)
    pd.DataFrame(
        [
            {
                "harm_ein": "222222222",
                "harm_tax_year": "2024",
                "harm_ntee_code": "B43",
                "harm_ntee_code__source_family": "nccs_bmf",
                "harm_ntee_code__source_variant": "raw_bmf_lookup_2024",
                "harm_ntee_code__source_column": "NTEE_CD",
                "harm_subsection_code": "3",
                "harm_subsection_code__source_family": "nccs_bmf",
                "harm_subsection_code__source_variant": "raw_bmf_lookup_2024",
                "harm_subsection_code__source_column": "SUBSECTION",
            },
            {
                "harm_ein": "333333333",
                "harm_tax_year": "2024",
                "harm_ntee_code": "",
                "harm_ntee_code__source_family": "",
                "harm_ntee_code__source_variant": "",
                "harm_ntee_code__source_column": "",
                "harm_subsection_code": "",
                "harm_subsection_code__source_family": "",
                "harm_subsection_code__source_variant": "",
                "harm_subsection_code__source_column": "",
            },
        ]
    ).to_parquet(bmf_lookup_2024, index=False)
    irs_bmf_sd.write_text(
        "\n".join(
            [
                "EIN,NAME,STATE,ZIP,SUBSECTION,NTEE_CD",
                "555555555,EPSILON ORG,SD,57701,527,W24",
            ]
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(gt_extract_analysis, "load_env_from_secrets", lambda: None)
    monkeypatch.setattr(
        gt_extract_analysis,
        "discover_bmf_exact_year_lookup_inputs",
        lambda: {
            "2022": bmf_lookup_2022,
            "2023": bmf_lookup_2023,
            "2024": bmf_lookup_2024,
        },
    )
    monkeypatch.setattr(
        gt_extract_analysis,
        "discover_irs_bmf_raw_inputs",
        lambda: [irs_bmf_sd],
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "13_extract_basic_analysis_variables_local.py",
            "--input",
            str(input_path),
            "--output",
            str(output_path),
            "--region-metrics-output",
            str(region_metrics_output_path),
            "--coverage-csv",
            str(coverage_path),
            "--mapping-md",
            str(mapping_path),
        ],
    )

    gt_extract_analysis.main()

    analysis_df = pd.read_parquet(output_path)
    region_metrics_df = pd.read_parquet(region_metrics_output_path)
    coverage_df = pd.read_csv(coverage_path)
    mapping_text = mapping_path.read_text(encoding="utf-8")

    assert len(analysis_df) == 6
    assert "analysis_total_revenue_amount" in analysis_df.columns
    assert "analysis_total_revenue_amount_source_column" in analysis_df.columns
    assert "analysis_is_hospital" in analysis_df.columns
    assert "analysis_is_university" in analysis_df.columns
    assert "analysis_imputed_is_hospital" in analysis_df.columns
    assert "analysis_imputed_is_university" in analysis_df.columns
    assert "analysis_imputed_is_political_org" in analysis_df.columns
    assert "analysis_calculated_total_contributions_amount" in analysis_df.columns
    assert "analysis_calculated_grants_total_amount" in analysis_df.columns
    assert "analysis_calculated_grants_share_of_revenue_ratio" in analysis_df.columns
    assert "analysis_grants_share_of_revenue_quality_flag" in analysis_df.columns
    assert "analysis_calculated_net_asset_amount" in analysis_df.columns
    assert "analysis_calculated_months_of_reserves" in analysis_df.columns
    assert "analysis_ntee_code" in analysis_df.columns
    assert "analysis_calculated_ntee_broad_code" in analysis_df.columns
    assert "analysis_subsection_code" in analysis_df.columns
    assert "analysis_is_political_org" in analysis_df.columns
    assert analysis_df.loc[0, "analysis_total_revenue_amount"] == 100
    assert analysis_df.loc[1, "analysis_total_revenue_amount"] == 50
    assert analysis_df.loc[2, "analysis_total_revenue_amount"] == 90
    assert analysis_df.loc[0, "analysis_total_expense_amount"] == 80
    assert analysis_df.loc[1, "analysis_total_expense_amount"] == 40
    assert analysis_df.loc[2, "analysis_total_expense_amount"] == 70
    assert analysis_df.loc[0, "analysis_program_service_revenue_amount"] == 60
    assert analysis_df.loc[1, "analysis_program_service_revenue_amount"] == 12
    assert analysis_df.loc[0, "analysis_calculated_surplus_amount"] == 20
    assert analysis_df.loc[1, "analysis_calculated_surplus_amount"] == 10
    assert analysis_df.loc[0, "analysis_calculated_net_margin_ratio"] == 0.2
    assert analysis_df.loc[1, "analysis_calculated_net_margin_ratio"] == 0.2
    assert analysis_df.loc[0, "analysis_calculated_total_contributions_amount"] == 17
    assert pd.isna(analysis_df.loc[1, "analysis_calculated_total_contributions_amount"])
    assert analysis_df.loc[0, "analysis_calculated_grants_total_amount"] == 8
    assert pd.isna(analysis_df.loc[1, "analysis_calculated_grants_total_amount"])
    assert analysis_df.loc[0, "analysis_calculated_grants_share_of_revenue_ratio"] == 0.08
    assert pd.isna(analysis_df.loc[1, "analysis_calculated_grants_share_of_revenue_ratio"])
    assert pd.isna(analysis_df.loc[5, "analysis_calculated_grants_share_of_revenue_ratio"])
    assert analysis_df.loc[0, "analysis_grants_share_of_revenue_quality_flag"] == "valid_0_to_1"
    assert analysis_df.loc[5, "analysis_grants_share_of_revenue_quality_flag"] == "nonpositive_revenue"
    assert analysis_df.loc[0, "analysis_calculated_net_asset_amount"] == 80
    assert analysis_df.loc[1, "analysis_calculated_net_asset_amount"] == 60
    assert pd.isna(analysis_df.loc[2, "analysis_calculated_net_asset_amount"])
    assert analysis_df.loc[0, "analysis_calculated_months_of_reserves"] == 12
    assert analysis_df.loc[1, "analysis_calculated_months_of_reserves"] == 18
    assert pd.isna(analysis_df.loc[2, "analysis_calculated_months_of_reserves"])
    assert analysis_df.loc[0, "analysis_ntee_code"] == "E22"
    assert analysis_df.loc[1, "analysis_ntee_code"] == "B43"
    assert analysis_df.loc[2, "analysis_ntee_code"] == "W24"
    assert analysis_df.loc[3, "analysis_ntee_code"] == "A12"
    assert analysis_df.loc[4, "analysis_ntee_code"] == "W24"
    assert analysis_df.loc[0, "analysis_subsection_code"] == "3"
    assert analysis_df.loc[1, "analysis_subsection_code"] == "3"
    assert analysis_df.loc[2, "analysis_subsection_code"] == "527"
    assert analysis_df.loc[3, "analysis_subsection_code"] == "4"
    assert analysis_df.loc[4, "analysis_subsection_code"] == "527"
    assert analysis_df.loc[0, "analysis_calculated_ntee_broad_code"] == "E"
    assert analysis_df.loc[1, "analysis_calculated_ntee_broad_code"] == "B"
    assert analysis_df.loc[2, "analysis_calculated_ntee_broad_code"] == "W"
    assert analysis_df.loc[3, "analysis_calculated_ntee_broad_code"] == "A"
    assert analysis_df.loc[4, "analysis_calculated_ntee_broad_code"] == "W"
    assert analysis_df.loc[0, "analysis_total_revenue_amount_source_column"] == "TOTREVCURYEA"
    assert analysis_df.loc[1, "analysis_total_revenue_amount_source_column"] == "TOTALRREVENU"
    assert analysis_df.loc[2, "analysis_total_revenue_amount_source_column"] == "ANREEXTOREEX"
    assert analysis_df.loc[0, "analysis_total_expense_amount_source_column"] == "TOTEXPCURYEA"
    assert analysis_df.loc[1, "analysis_total_expense_amount_source_column"] == "TOTALEEXPENS"
    assert analysis_df.loc[2, "analysis_total_expense_amount_source_column"] == "ARETEREXPNSS"
    assert analysis_df.loc[0, "analysis_program_service_revenue_amount_source_column"] == "TOTPROSERREV"
    assert analysis_df.loc[1, "analysis_program_service_revenue_amount_source_column"] == "PROGSERVREVE"
    assert (
        analysis_df.loc[2, "analysis_calculated_surplus_amount_source_column"]
        == "derived:analysis_total_revenue_amount-minus-analysis_total_expense_amount"
    )
    assert analysis_df.loc[0, "analysis_calculated_total_contributions_amount_source_column"] == "derived:TOTACASHCONT+NONCASCONTRI+ALLOOTHECONT"
    assert analysis_df.loc[0, "analysis_calculated_grants_total_amount_source_column"] == "derived:FOREGRANTOTA+GOVERNGRANTS+GRANTOORORGA"
    assert (
        analysis_df.loc[0, "analysis_calculated_grants_share_of_revenue_ratio_source_column"]
        == "derived:analysis_calculated_grants_total_amount-divided-by-analysis_total_revenue_amount"
    )
    assert analysis_df.loc[0, "analysis_calculated_net_asset_amount_source_column"] == "derived:TOASEOOYY-minus-TOLIEOOYY"
    assert analysis_df.loc[0, "analysis_calculated_months_of_reserves_source_column"] == "derived:analysis_calculated_net_asset_amount-divided-by-analysis_total_expense_amount-times-12"
    assert analysis_df.loc[0, "analysis_ntee_code_source_family"] == "nccs_bmf"
    assert analysis_df.loc[0, "analysis_ntee_code_source_variant"] == "legacy_bmf_lookup_2023"
    assert analysis_df.loc[0, "analysis_ntee_code_source_column"] == "NTEEFINAL"
    assert analysis_df.loc[1, "analysis_ntee_code_source_column"] == "NTEE_CD"
    assert analysis_df.loc[2, "analysis_ntee_code_source_variant"] == "legacy_bmf_lookup_2023|nearest_year_fallback_from_2024_to_2023"
    assert analysis_df.loc[3, "analysis_ntee_code_source_variant"] == "legacy_bmf_lookup_2022|nearest_year_fallback_from_2023_to_2022"
    assert analysis_df.loc[4, "analysis_ntee_code_source_family"] == "irs_bmf"
    assert analysis_df.loc[4, "analysis_ntee_code_source_variant"] == "eo_sd.csv|ein_fallback"
    assert analysis_df.loc[4, "analysis_ntee_code_source_column"] == "NTEE_CD"
    assert analysis_df.loc[2, "analysis_subsection_code_source_variant"] == "legacy_bmf_lookup_2023|nearest_year_fallback_from_2024_to_2023"
    assert analysis_df.loc[3, "analysis_subsection_code_source_variant"] == "legacy_bmf_lookup_2022|nearest_year_fallback_from_2023_to_2022"
    assert analysis_df.loc[4, "analysis_subsection_code_source_family"] == "irs_bmf"
    assert analysis_df.loc[4, "analysis_subsection_code_source_column"] == "SUBSECTION"
    assert analysis_df.loc[0, "analysis_calculated_ntee_broad_code_source_column"] == "derived:first-letter-of-analysis_ntee_code"
    assert bool(analysis_df.loc[0, "analysis_is_hospital"]) is True
    assert bool(analysis_df.loc[1, "analysis_is_hospital"]) is False
    assert bool(analysis_df.loc[0, "analysis_is_university"]) is False
    assert bool(analysis_df.loc[1, "analysis_is_university"]) is True
    assert bool(analysis_df.loc[0, "analysis_is_political_org"]) is False
    assert bool(analysis_df.loc[2, "analysis_is_political_org"]) is True
    assert bool(analysis_df.loc[3, "analysis_is_political_org"]) is False
    assert bool(analysis_df.loc[4, "analysis_is_political_org"]) is True
    assert bool(analysis_df.loc[0, "analysis_imputed_is_hospital"]) is True
    assert bool(analysis_df.loc[1, "analysis_imputed_is_university"]) is True
    assert bool(analysis_df.loc[2, "analysis_imputed_is_political_org"]) is True
    assert bool(analysis_df.loc[3, "analysis_imputed_is_political_org"]) is False
    assert analysis_df.loc[3, "analysis_imputed_is_political_org_source_column"] == "analysis_ntee_code"
    assert bool(analysis_df.loc[4, "analysis_imputed_is_political_org"]) is True
    unresolved_rows = coverage_df.loc[coverage_df["status"].isin(["needs_confirmation", "unavailable"])]
    assert unresolved_rows.empty
    available_variables = set(coverage_df.loc[coverage_df["status"] == "available", "variable_name"].tolist())
    assert "analysis_calculated_total_contributions_amount" in available_variables
    assert "analysis_calculated_grants_total_amount" in available_variables
    assert "analysis_calculated_grants_share_of_revenue_ratio" in available_variables
    assert "analysis_grants_share_of_revenue_quality_flag" in available_variables
    assert "analysis_calculated_net_asset_amount" in available_variables
    assert "analysis_calculated_months_of_reserves" in available_variables
    assert "analysis_ntee_code" in available_variables
    assert "analysis_calculated_ntee_broad_code" in available_variables
    assert "analysis_is_political_org" in available_variables
    assert "analysis_imputed_is_hospital" in available_variables
    assert "analysis_imputed_is_university" in available_variables
    assert "analysis_imputed_is_political_org" in available_variables
    ntee_coverage = coverage_df.loc[
        (coverage_df["variable_name"] == "analysis_ntee_code")
        & (coverage_df["tax_year"].astype(str) == "2024")
        & (coverage_df["form_type"].astype(str) == "990PF")
    ]
    assert int(ntee_coverage["populated_rows"].iloc[0]) == 1
    assert "What data elements will be needed?" in mapping_text
    assert "For Limited Number Of Organizations (Form 990, EZ, PF)" in mapping_text
    assert "CODING_RULES.md" in mapping_text
    assert "NTEE filed classification code" in mapping_text
    assert "nearest-year EIN fallback" in mapping_text
    assert "analysis_is_political_org" in mapping_text
    assert "analysis_imputed_is_hospital" in mapping_text
    assert "analysis_imputed_is_university" in mapping_text
    assert "analysis_imputed_is_political_org" in mapping_text
    assert "analysis_ntee_code using prefixes E20, E21, E22, E24" in mapping_text
    assert "analysis_ntee_code using prefixes B40, B41, B42, B43, B50" in mapping_text
    assert "analysis_calculated_grants_share_of_revenue_ratio" in mapping_text

    assert len(region_metrics_df) == 8
    assert "analysis_calculated_normalized_total_revenue_per_nonprofit" in region_metrics_df.columns
    assert "analysis_calculated_region_grants_share_of_revenue_ratio" in region_metrics_df.columns
    assert "analysis_calculated_cleaned_region_grants_share_of_revenue_ratio" in region_metrics_df.columns
    assert "analysis_grants_share_valid_row_count" in region_metrics_df.columns
    assert "analysis_grants_share_gt_1_row_count" in region_metrics_df.columns
    assert "unique_nonprofit_ein_count" in region_metrics_df.columns
    assert "analysis_calculated_normalized_total_revenue_per_unique_nonprofit" in region_metrics_df.columns
    assert "analysis_calculated_normalized_net_asset_per_unique_nonprofit" in region_metrics_df.columns
    black_hills_all_2023 = region_metrics_df.loc[
        (region_metrics_df["region"] == "BlackHills")
        & (region_metrics_df["tax_year"].astype(str) == "2023")
        & (region_metrics_df["analysis_exclusion_variant"] == "all_rows")
    ].iloc[0]
    assert black_hills_all_2023["nonprofit_filing_count"] == 2
    assert black_hills_all_2023["unique_nonprofit_ein_count"] == 2
    assert black_hills_all_2023["analysis_total_revenue_amount_sum"] == 140
    assert black_hills_all_2023["analysis_calculated_grants_total_amount_sum"] == 8
    assert black_hills_all_2023["analysis_calculated_normalized_total_revenue_per_nonprofit"] == 70
    assert black_hills_all_2023["analysis_calculated_normalized_total_revenue_per_unique_nonprofit"] == 70
    assert round(float(black_hills_all_2023["analysis_calculated_region_grants_share_of_revenue_ratio"]), 6) == round(8 / 140, 6)
    assert round(float(black_hills_all_2023["analysis_calculated_cleaned_region_grants_share_of_revenue_ratio"]), 6) == round(8 / 100, 6)
    assert black_hills_all_2023["analysis_grants_share_valid_row_count"] == 1
    assert black_hills_all_2023["analysis_grants_share_gt_1_row_count"] == 0
    black_hills_all_2024 = region_metrics_df.loc[
        (region_metrics_df["region"] == "BlackHills")
        & (region_metrics_df["tax_year"].astype(str) == "2024")
        & (region_metrics_df["analysis_exclusion_variant"] == "all_rows")
    ].iloc[0]
    assert black_hills_all_2024["nonprofit_filing_count"] == 2
    assert black_hills_all_2024["unique_nonprofit_ein_count"] == 2
    assert round(float(black_hills_all_2024["analysis_calculated_region_grants_share_of_revenue_ratio"]), 6) == round(5 / 45, 6)
    assert pd.isna(black_hills_all_2024["analysis_calculated_cleaned_region_grants_share_of_revenue_ratio"])
    black_hills_excluded_2023 = region_metrics_df.loc[
        (region_metrics_df["region"] == "BlackHills")
        & (region_metrics_df["tax_year"].astype(str) == "2023")
        & (region_metrics_df["analysis_exclusion_variant"] == "exclude_imputed_hospital_university_political_org")
    ].iloc[0]
    assert black_hills_excluded_2023["nonprofit_filing_count"] == 1
    assert black_hills_excluded_2023["unique_nonprofit_ein_count"] == 1
    assert black_hills_excluded_2023["analysis_total_revenue_amount_sum"] == 40
    assert black_hills_excluded_2023["analysis_calculated_normalized_total_revenue_per_nonprofit"] == 40
    assert black_hills_excluded_2023["analysis_calculated_normalized_total_revenue_per_unique_nonprofit"] == 40
    assert pd.isna(black_hills_excluded_2023["analysis_calculated_region_grants_share_of_revenue_ratio"])


def test_name_proxy_flags_only_fill_missing_canonical_rows() -> None:
    canonical = pd.Series([pd.NA, pd.NA, pd.NA, False], dtype="boolean")
    names = pd.Series(
        [
            "RAPID CITY HOSPITAL AUXILIARY",
            "STATE UNIVERSITY FOUNDATION",
            "CITIZENS FOR FAIR BALLOT ACCESS",
            "STATE UNIVERSITY FOUNDATION",
        ],
        dtype="string",
    )

    hospital_proxy, hospital_source, hospital_confidence, hospital_filled = gt_extract_analysis._build_name_proxy_flag(
        names,
        canonical,
        (r"\bhospital\b",),
        (),
        (),
    )
    university_proxy, _, university_confidence, university_filled = gt_extract_analysis._build_name_proxy_flag(
        names,
        canonical,
        (),
        (r"\buniversity\b",),
        (),
    )
    political_proxy, _, political_confidence, political_filled = gt_extract_analysis._build_name_proxy_flag(
        names,
        canonical,
        (r"\bballot\b",),
        (),
        (),
    )

    assert bool(hospital_proxy.iloc[0]) is True
    assert hospital_source.iloc[0] == "FILERNAME1"
    assert hospital_confidence.iloc[0] == "high"
    assert bool(hospital_filled.iloc[0]) is True
    assert pd.isna(hospital_filled.iloc[1])
    assert pd.isna(hospital_filled.iloc[2])
    assert bool(hospital_filled.iloc[3]) is False

    assert pd.isna(university_proxy.iloc[0])
    assert bool(university_proxy.iloc[1]) is True
    assert university_confidence.iloc[1] == "medium"
    assert pd.isna(university_filled.iloc[1])
    assert pd.isna(university_confidence.iloc[3])
    assert bool(university_filled.iloc[3]) is False

    assert bool(political_proxy.iloc[2]) is True
    assert political_confidence.iloc[2] == "high"
    assert bool(political_filled.iloc[2]) is True


def test_imputed_source_labels_use_default_false_when_no_high_confidence_match() -> None:
    canonical = pd.Series([pd.NA, pd.NA, False], dtype="boolean")
    canonical_source = pd.Series([pd.NA, pd.NA, "analysis_ntee_code"], dtype="string")
    names = pd.Series(
        [
            "PLAIN COMMUNITY GROUP",
            "STATE UNIVERSITY FOUNDATION",
            "KNOWN NTEE ORG",
        ],
        dtype="string",
    )
    proxy_values, _, proxy_confidence, filled_values = gt_extract_analysis._build_name_proxy_flag(
        names,
        canonical,
        (),
        (r"\buniversity\b",),
        (),
    )
    imputed_values = filled_values.fillna(False).astype("boolean")
    imputed_source = canonical_source.astype("string")
    high_name_mask = canonical.isna() & proxy_confidence.fillna("").eq("high")
    default_false_mask = canonical.isna() & ~proxy_confidence.fillna("").eq("high")
    imputed_source.loc[high_name_mask] = "FILERNAME1"
    imputed_source.loc[default_false_mask] = "imputed_default_false"

    assert bool(imputed_values.iloc[0]) is False
    assert imputed_source.iloc[0] == "imputed_default_false"
    assert bool(proxy_values.iloc[1]) is True
    assert proxy_confidence.iloc[1] == "medium"
    assert bool(imputed_values.iloc[1]) is False
    assert imputed_source.iloc[1] == "imputed_default_false"
    assert bool(imputed_values.iloc[2]) is False
    assert imputed_source.iloc[2] == "analysis_ntee_code"

    proxy_values2, _, proxy_confidence2, filled_values2 = gt_extract_analysis._build_name_proxy_flag(
        pd.Series(["OTTIS UW KEN FBO DWU BIOLOGY DEPT"], dtype="string"),
        pd.Series([pd.NA], dtype="boolean"),
        (r"\bDWU\b.*\bBIOLOGY DEPT\b", r"\bBIOLOGY DEPT\b.*\bDWU\b"),
        (),
        (),
    )
    assert bool(proxy_values2.iloc[0]) is True
    assert proxy_confidence2.iloc[0] == "high"
    assert bool(filled_values2.iloc[0]) is True


def test_political_org_imputation_uses_subsection_before_name_or_default() -> None:
    canonical = pd.Series([pd.NA, pd.NA], dtype="boolean")
    canonical_source = pd.Series([pd.NA, pd.NA], dtype="string")
    subsection = pd.Series(["527", "3"], dtype="string")
    subsection_source = pd.Series(["SUBSECTION", "SUBSECTION"], dtype="string")
    names = pd.Series(["PLAIN GROUP", "VOTERS UNITED"], dtype="string")

    proxy_values, _, proxy_confidence, _filled_values = gt_extract_analysis._build_name_proxy_flag(
        names,
        canonical,
        (r"\bvoters?\b",),
        (),
        (),
    )

    imputed_values = canonical.astype("boolean").copy()
    imputed_source = canonical_source.astype("string")
    subsection_values = gt_extract_analysis._normalize_political_org_flag(subsection)
    subsection_mask = imputed_values.isna() & subsection_values.notna()
    imputed_values.loc[subsection_mask] = subsection_values.loc[subsection_mask]
    imputed_source.loc[subsection_mask] = subsection_source.loc[subsection_mask]
    high_name_mask = imputed_values.isna() & proxy_confidence.fillna("").eq("high")
    default_false_mask = imputed_values.isna() & ~proxy_confidence.fillna("").eq("high")
    imputed_values.loc[high_name_mask] = True
    imputed_values.loc[default_false_mask] = False
    imputed_source.loc[high_name_mask] = "FILERNAME1"
    imputed_source.loc[default_false_mask] = "imputed_default_false"

    assert bool(imputed_values.iloc[0]) is True
    assert imputed_source.iloc[0] == "SUBSECTION"
    assert bool(imputed_values.iloc[1]) is False
    assert imputed_source.iloc[1] == "SUBSECTION"


def test_orchestrator_parallelizes_steps_09_and_10_and_writes_timing_outputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    summary_json = tmp_path / "run_990_datamart_pipeline_summary.json"
    timings_csv = tmp_path / "run_990_datamart_pipeline_step_timings.csv"
    build_manifest = tmp_path / "manifest_basic_allforms_presilver.json"
    build_manifest.write_text(json.dumps({"rows_output": 1, "columns_output": 2, "output_path": "dummy"}), encoding="utf-8")

    call_log: list[tuple[str, object]] = []

    def fake_run_step(step_num: int, common_args: dict[str, str], extra_flags: dict[str, bool], *, mode: str = "serial"):
        call_log.append((mode, step_num))
        return {
            "step_num": step_num,
            "script_name": f"{step_num:02d}.py",
            "mode": mode,
            "elapsed_seconds": 0.123,
        }

    def fake_run_parallel(step_nums: list[int], common_args: dict[str, str], extra_flags: dict[str, bool]):
        call_log.append(("parallel_group", tuple(step_nums)))
        return [
            {
                "step_num": step_num,
                "script_name": f"{step_num:02d}.py",
                "mode": "parallel",
                "elapsed_seconds": 0.234,
            }
            for step_num in step_nums
        ]

    monkeypatch.setattr(gt_orchestrator, "_run_step", fake_run_step)
    monkeypatch.setattr(gt_orchestrator, "_run_parallel_steps", fake_run_parallel)
    monkeypatch.setattr(gt_common, "ORCHESTRATOR_SUMMARY_JSON", summary_json)
    monkeypatch.setattr(gt_common, "ORCHESTRATOR_STEP_TIMINGS_CSV", timings_csv)
    monkeypatch.setattr(gt_common, "BASIC_ALLFORMS_BUILD_MANIFEST_JSON", build_manifest)
    monkeypatch.setattr(gt_common, "BASIC_PLUS_COMBINED_BUILD_MANIFEST_JSON", tmp_path / "manifest_basic_plus_combined_presilver.json")
    monkeypatch.setattr(gt_common, "FILTERED_BASIC_MANIFEST_JSON", tmp_path / "manifest_basic_allforms_filtered.json")
    monkeypatch.setattr(gt_common, "FILTERED_MIXED_MANIFEST_JSON", tmp_path / "manifest_filtered.json")

    previous_common = sys.modules.get("common")
    sys.modules["common"] = gt_common
    try:
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "run_990_datamart_pipeline.py",
                "--start-step",
                "8",
                "--end-step",
                "14",
            ],
        )
        gt_orchestrator.main()
    finally:
        if previous_common is not None:
            sys.modules["common"] = previous_common
        else:
            sys.modules.pop("common", None)

    assert call_log == [
        ("serial", 8),
        ("parallel_group", (9, 10)),
        ("serial", 11),
        ("serial", 13),
        ("serial", 14),
    ]
    assert summary_json.exists()
    assert timings_csv.exists()
    summary_payload = json.loads(summary_json.read_text(encoding="utf-8"))
    assert summary_payload["parallel_late_stage"] is True
    assert [row["step_num"] for row in summary_payload["steps"]] == [8, 9, 10, 11, 13, 14]
    timings_df = pd.read_csv(timings_csv)
    assert timings_df["step_num"].tolist() == [8, 9, 10, 11, 13, 14]


def test_smoke_pipeline_steps_06_to_11_with_tiny_fixtures(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    raw_dir = tmp_path / "raw"
    meta_dir = tmp_path / "meta"
    staging_dir = tmp_path / "staging"
    raw_dir.mkdir()
    meta_dir.mkdir()
    staging_dir.mkdir()

    catalog_csv = meta_dir / "datamart_catalog.csv"
    geoid_csv = tmp_path / "GEOID_reference.csv"
    zip_csv = tmp_path / "zip_to_county_fips.csv"
    basic_output = staging_dir / "givingtuesday_990_basic_allforms_presilver.parquet"
    mixed_output = staging_dir / "givingtuesday_990_basic_plus_combined_presilver.parquet"
    basic_build_manifest = staging_dir / "manifest_basic_allforms_presilver.json"
    mixed_build_manifest = staging_dir / "manifest_basic_plus_combined_presilver.json"
    filtered_basic = staging_dir / "givingtuesday_990_basic_allforms_benchmark.parquet"
    filtered_mixed = staging_dir / "givingtuesday_990_filings_benchmark.parquet"
    filtered_basic_manifest = staging_dir / "manifest_basic_allforms_filtered.json"
    filtered_mixed_manifest = staging_dir / "manifest_filtered.json"
    filtered_basic_schema = meta_dir / "schema_snapshot_basic_allforms_filtered.json"
    filtered_mixed_schema = meta_dir / "schema_snapshot_filings_filtered.json"
    curated_report = meta_dir / "curated_output_size_verification_report.csv"
    artifact_manifest_json = meta_dir / "gt_pipeline_artifact_manifest.json"
    artifact_manifest_csv = meta_dir / "gt_pipeline_artifact_manifest.csv"
    combined_cache_parquet = tmp_path / "cache" / "combined_normalized.parquet"
    combined_cache_manifest = tmp_path / "cache" / "combined_normalized_manifest.json"

    basic_presilver_artifact = gt_common.PipelineArtifact("basic_allforms_presilver", basic_output, gt_common.BRONZE_PRESILVER_PREFIX, "application/octet-stream", "bronze_presilver")
    mixed_presilver_artifact = gt_common.PipelineArtifact("basic_plus_combined_presilver", mixed_output, gt_common.BRONZE_PRESILVER_PREFIX, "application/octet-stream", "bronze_presilver")
    basic_filtered_artifact = gt_common.PipelineArtifact("basic_allforms_filtered", filtered_basic, gt_common.SILVER_PREFIX, "application/octet-stream", "silver_filtered")
    mixed_filtered_artifact = gt_common.PipelineArtifact("basic_plus_combined_filtered", filtered_mixed, gt_common.SILVER_PREFIX, "application/octet-stream", "silver_filtered")
    basic_filtered_manifest_artifact = gt_common.PipelineArtifact("basic_allforms_filtered_manifest", filtered_basic_manifest, gt_common.SILVER_PREFIX, "application/json", "silver_filtered_manifest")
    mixed_filtered_manifest_artifact = gt_common.PipelineArtifact("basic_plus_combined_filtered_manifest", filtered_mixed_manifest, gt_common.SILVER_PREFIX, "application/json", "silver_filtered_manifest")
    basic_schema_artifact = gt_common.PipelineArtifact("basic_allforms_filtered_schema_snapshot", filtered_basic_schema, gt_common.SILVER_META_PREFIX, "application/json", "silver_metadata")
    mixed_schema_artifact = gt_common.PipelineArtifact("basic_plus_combined_filtered_schema_snapshot", filtered_mixed_schema, gt_common.SILVER_META_PREFIX, "application/json", "silver_metadata")

    pd.DataFrame(
        [
            {
                "dataset_id": "combined",
                "title": "Combined Forms Datamart",
                "form_type": "",
                "download_url": "https://example.org/combined.csv",
                "source_content_length_bytes": "1",
            },
            {
                "dataset_id": "basic-990",
                "title": "Basic Fields",
                "form_type": "990",
                "download_url": "https://example.org/basic_990.csv",
                "source_content_length_bytes": "1",
            },
            {
                "dataset_id": "basic-990ez",
                "title": "Basic Fields",
                "form_type": "990EZ",
                "download_url": "https://example.org/basic_990ez.csv",
                "source_content_length_bytes": "1",
            },
            {
                "dataset_id": "basic-990pf",
                "title": "Basic Fields",
                "form_type": "990PF",
                "download_url": "https://example.org/basic_990pf.csv",
                "source_content_length_bytes": "1",
            },
        ]
    ).to_csv(catalog_csv, index=False)

    pd.DataFrame([{"GEOID": "46093", "Cluster_name": "BlackHills"}]).to_csv(geoid_csv, index=False)
    pd.DataFrame([{"ZIP": "57701", "FIPS": "46093"}]).to_csv(zip_csv, index=False)

    pd.DataFrame(
        [
            {
                "FILEREIN": "123456789",
                "TAXYEAR": "2023",
                "RETURNTYPE": "990",
                "FILERUSZIP": "57701",
                "TOTREVCURYEA": "120",
                "TOTEXPCURYEA": "100",
                "TOTALRREVENU": "",
                "TOTALEEXPENS": "",
                "FILERNAME1": "Bench Org",
                "FILERUSSTATE": "SD",
            }
        ]
    ).to_csv(raw_dir / "basic_990.csv", index=False)
    pd.DataFrame(
        [
            {
                "FILEREIN": "222222222",
                "TAXYEAR": "2023",
                "RETURNTYPE": "990EZ",
                "FILERUSZIP": "57701",
                "TOTREVCURYEA": "",
                "TOTEXPCURYEA": "",
                "TOTALRREVENU": "50",
                "TOTALEEXPENS": "40",
                "FILERNAME1": "EZ Org",
                "FILERUSSTATE": "SD",
            }
        ]
    ).to_csv(raw_dir / "basic_990ez.csv", index=False)
    pd.DataFrame(
        [
            {
                "FILEREIN": "333333333",
                "TAXYEAR": "2023",
                "RETURNTYPE": "990PF",
                "FILERUSZIP": "57701",
                "TOTREVCURYEA": "",
                "TOTEXPCURYEA": "",
                "TOTALRREVENU": "",
                "TOTALEEXPENS": "",
                "ANREEXTOREEX": "90",
                "ARETEREXPNSS": "70",
                "FILERNAME1": "PF Org",
                "FILERUSSTATE": "SD",
                "ASSEOYOYY": "10",
            }
        ]
    ).to_csv(raw_dir / "basic_990pf.csv", index=False)
    pd.DataFrame(
        [
            {
                "FILEREIN": "123456789",
                "TAXYEAR": "2023",
                "RETURNTYPE": "990",
                "FILERUSZIP": "57701",
                "TOTREVCURYEA": "",
                "TOTEXPCURYEA": "",
                "TOTALRREVENU": "",
                "TOTALEEXPENS": "",
                "FILERNAME1": "Bench Org Combined",
                "FILERUSSTATE": "SD",
                "combined_only_metric": "alpha",
            }
        ]
    ).to_csv(raw_dir / "combined.csv", index=False)

    monkeypatch.setattr(gt_common, "BASIC_PRESILVER_ARTIFACT", basic_presilver_artifact)
    monkeypatch.setattr(gt_common, "MIXED_PRESILVER_ARTIFACT", mixed_presilver_artifact)
    monkeypatch.setattr(gt_common, "FILTERED_BASIC_ALLFORMS_ARTIFACT", basic_filtered_artifact)
    monkeypatch.setattr(gt_common, "FILTERED_MIXED_ARTIFACT", mixed_filtered_artifact)
    monkeypatch.setattr(gt_common, "FILTERED_BASIC_MANIFEST_ARTIFACT", basic_filtered_manifest_artifact)
    monkeypatch.setattr(gt_common, "FILTERED_MIXED_MANIFEST_ARTIFACT", mixed_filtered_manifest_artifact)
    monkeypatch.setattr(gt_common, "FILTERED_BASIC_SCHEMA_ARTIFACT", basic_schema_artifact)
    monkeypatch.setattr(gt_common, "FILTERED_MIXED_SCHEMA_ARTIFACT", mixed_schema_artifact)
    monkeypatch.setattr(
        gt_common,
        "FILTER_TARGET_BASIC",
        gt_common.FilterTarget("basic", basic_output, basic_filtered_artifact, basic_filtered_manifest_artifact),
    )
    monkeypatch.setattr(
        gt_common,
        "FILTER_TARGET_MIXED",
        gt_common.FilterTarget("mixed", mixed_output, mixed_filtered_artifact, mixed_filtered_manifest_artifact),
    )
    monkeypatch.setattr(gt_common, "CURATED_SIZE_REPORT_ARTIFACT", gt_common.PipelineArtifact("curated_output_size_verification_report", curated_report, gt_common.SILVER_META_PREFIX, "text/csv", "silver_metadata"))
    monkeypatch.setattr(gt_common, "GT_PIPELINE_ARTIFACT_MANIFEST_JSON_ARTIFACT", gt_common.PipelineArtifact("gt_pipeline_artifact_manifest_json", artifact_manifest_json, gt_common.SILVER_META_PREFIX, "application/json", "silver_metadata"))
    monkeypatch.setattr(gt_common, "GT_PIPELINE_ARTIFACT_MANIFEST_CSV_ARTIFACT", gt_common.PipelineArtifact("gt_pipeline_artifact_manifest_csv", artifact_manifest_csv, gt_common.SILVER_META_PREFIX, "text/csv", "silver_metadata"))
    monkeypatch.setattr(gt_filter, "FILTERED_BASIC_SCHEMA_ARTIFACT", basic_schema_artifact)
    monkeypatch.setattr(gt_filter, "FILTERED_MIXED_SCHEMA_ARTIFACT", mixed_schema_artifact)
    monkeypatch.setattr(
        gt_filter,
        "FILTER_TARGET_BASIC",
        gt_common.FilterTarget("basic", basic_output, basic_filtered_artifact, basic_filtered_manifest_artifact),
    )
    monkeypatch.setattr(
        gt_filter,
        "FILTER_TARGET_MIXED",
        gt_common.FilterTarget("mixed", mixed_output, mixed_filtered_artifact, mixed_filtered_manifest_artifact),
    )

    uploaded: dict[str, int] = {}

    def fake_upload(local_path: Path, bucket: str, key: str, region: str, extra_args=None):
        uploaded[key] = Path(local_path).stat().st_size

    def fake_should_skip(local_path: Path, bucket: str, key: str, region: str, overwrite: bool, **kwargs) -> bool:
        return False

    monkeypatch.setattr(gt_upload_bronze, "upload_file_with_progress", fake_upload)
    monkeypatch.setattr(gt_upload_bronze, "should_skip_upload", fake_should_skip)
    monkeypatch.setattr(gt_upload_silver, "upload_file_with_progress", fake_upload)
    monkeypatch.setattr(gt_upload_silver, "should_skip_upload", fake_should_skip)
    monkeypatch.setattr(gt_verify_curated, "upload_file_with_progress", fake_upload)
    monkeypatch.setattr(
        gt_verify_curated,
        "batch_s3_object_sizes",
        lambda bucket, path_to_s3_key, region, workers=None: {path: uploaded.get(key) for path, key in path_to_s3_key.items()},
    )
    monkeypatch.setattr(gt_verify_curated, "BASIC_ALLFORMS_BUILD_MANIFEST_JSON", basic_build_manifest)
    monkeypatch.setattr(gt_verify_curated, "BASIC_PLUS_COMBINED_BUILD_MANIFEST_JSON", mixed_build_manifest)
    monkeypatch.setattr(gt_verify_curated, "FILTERED_BASIC_MANIFEST_JSON", filtered_basic_manifest)
    monkeypatch.setattr(gt_verify_curated, "FILTERED_MIXED_MANIFEST_JSON", filtered_mixed_manifest)

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "06_build_basic_allforms_presilver.py",
            "--catalog-csv",
            str(catalog_csv),
            "--raw-dir",
            str(raw_dir),
            "--geoid-csv",
            str(geoid_csv),
            "--zip-to-county",
            str(zip_csv),
            "--combined-cache-parquet",
            str(combined_cache_parquet),
            "--combined-cache-manifest-json",
            str(combined_cache_manifest),
            "--output",
            str(basic_output),
            "--build-manifest-json",
            str(basic_build_manifest),
            "--force-rebuild",
        ],
    )
    gt_build_basic.main()

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "07_build_basic_plus_combined_presilver.py",
            "--catalog-csv",
            str(catalog_csv),
            "--raw-dir",
            str(raw_dir),
            "--basic-parquet",
            str(basic_output),
            "--output",
            str(mixed_output),
            "--build-manifest-json",
            str(mixed_build_manifest),
            "--combined-cache-parquet",
            str(combined_cache_parquet),
            "--combined-cache-manifest-json",
            str(combined_cache_manifest),
            "--geoid-csv",
            str(geoid_csv),
            "--zip-to-county",
            str(zip_csv),
            "--force-rebuild",
        ],
    )
    gt_build_mixed.main()

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "08_filter_benchmark_outputs_local.py",
            "--zip-to-county",
            str(zip_csv),
            "--geoid-csv",
            str(geoid_csv),
            "--emit",
            "both",
            "--force-rebuild",
        ],
    )
    gt_filter.main()

    monkeypatch.setattr(sys, "argv", ["09_upload_bronze_presilver_outputs.py", "--bucket", "test-bucket", "--region", "us-east-2"])
    gt_upload_bronze.main()

    monkeypatch.setattr(sys, "argv", ["10_upload_silver_filtered_outputs.py", "--bucket", "test-bucket", "--region", "us-east-2", "--emit", "both"])
    gt_upload_silver.main()

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "11_verify_curated_outputs_local_s3.py",
            "--bucket",
            "test-bucket",
            "--region",
            "us-east-2",
            "--emit",
            "both",
            "--report-csv",
            str(curated_report),
            "--artifact-manifest-json",
            str(artifact_manifest_json),
            "--artifact-manifest-csv",
            str(artifact_manifest_csv),
            "--no-verify-only-changed",
        ],
    )
    gt_verify_curated.main()

    assert basic_output.exists()
    assert mixed_output.exists()
    assert filtered_basic.exists()
    assert filtered_mixed.exists()
    assert filtered_basic_schema.exists()
    assert filtered_mixed_schema.exists()
    assert curated_report.exists()
    assert artifact_manifest_json.exists()
    assert artifact_manifest_csv.exists()
    report_df = pd.read_csv(curated_report)
    assert report_df["size_match"].astype(str).str.upper().tolist() == ["TRUE"] * len(report_df)
    manifest_payload = json.loads(artifact_manifest_json.read_text(encoding="utf-8"))
    artifact_ids = {row["artifact_id"] for row in manifest_payload["artifacts"]}
    assert "basic_allforms_presilver" in artifact_ids
    assert "basic_plus_combined_filtered_schema_snapshot" in artifact_ids
    assert "manifest_basic_allforms_presilver" in artifact_ids
