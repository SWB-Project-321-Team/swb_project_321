"""
Shared path definitions for ingest, transform, and export.

Repo root and project data root (OneDrive 01_data). Use these instead of
defining BASE/DATA in each script so paths stay consistent and can be
overridden via environment (e.g. SWB_321_DATA_ROOT for testing).
"""

import os
from pathlib import Path

# Repo root: python/utils/paths.py -> utils -> python -> repo
_HERE = Path(__file__).resolve().parent
_PYTHON = _HERE.parent
BASE = _PYTHON.parent

_DEFAULT_DATA = (
    BASE / "data" / "321_Black_Hills_Area_Community_Foundation_2025_08" / "01_data"
)
DATA = Path(os.environ.get("SWB_321_DATA_ROOT", str(_DEFAULT_DATA)))


def get_base() -> Path:
    """Return repo root (same as BASE)."""
    return BASE


def get_data_root() -> Path:
    """Return project 01_data root; overridable with SWB_321_DATA_ROOT."""
    return DATA
