"""
Legacy compatibility wrapper for the reordered GT datamart pipeline.

The canonical Bronze-upload step is now `09_upload_bronze_presilver_outputs.py`.
"""

from __future__ import annotations

import runpy
from pathlib import Path


if __name__ == "__main__":
    print(
        "[deprecation] 08_upload_bronze_unfiltered_outputs.py is a legacy wrapper. "
        "Use 09_upload_bronze_presilver_outputs.py.",
        flush=True,
    )
    target = Path(__file__).resolve().with_name("09_upload_bronze_presilver_outputs.py")
    runpy.run_path(str(target), run_name="__main__")
