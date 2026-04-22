from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pandas as pd

try:
    import pytest
except ImportError:
    pytest = None

_FILE_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _FILE_DIR.parent.parent
_SCRIPT_PATH = _REPO_ROOT / "python" / "ingest" / "990_irs" / "00b_filter_bmf_to_benchmark_upload_silver.py"
_SPEC = importlib.util.spec_from_file_location("irs_bmf_filter_for_tests", _SCRIPT_PATH)
if _SPEC is None or _SPEC.loader is None:
    raise ImportError(f"Unable to load IRS BMF filter script from {_SCRIPT_PATH}")
irs_bmf_filter = importlib.util.module_from_spec(_SPEC)
sys.modules.setdefault("irs_bmf_filter_for_tests", irs_bmf_filter)
_SPEC.loader.exec_module(irs_bmf_filter)


def test_build_benchmark_zip_map_rejects_ambiguous_assignments() -> None:
    ref = pd.DataFrame({"GEOID": ["46093", "46095"]})
    zip_pairs = pd.DataFrame({"_zip": ["57701", "57701"], "_geoid": ["46093", "46095"]})

    if pytest is None:
        return
    with pytest.raises(RuntimeError, match="ambiguous"):
        irs_bmf_filter.build_benchmark_zip_map(ref, zip_pairs)


def test_filter_bmf_by_geoid_drops_state_mismatch_after_zip_match() -> None:
    bmf = pd.DataFrame(
        [
            {"ZIP": "57701", "STATE": "SD", "NAME": "Keep"},
            {"ZIP": "57701", "STATE": "WY", "NAME": "Drop"},
        ]
    )
    filtered = irs_bmf_filter.filter_bmf_by_geoid(
        bmf,
        {"46093"},
        {"57701": "46093"},
        {"46093": "BlackHills"},
        {"57701": "SD"},
    )
    assert filtered["NAME"].tolist() == ["Keep"]
    assert filtered["Region"].tolist() == ["BlackHills"]
