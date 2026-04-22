"""
Legacy compatibility wrapper for the reordered GT datamart pipeline.

The canonical Silver-upload step is now `10_upload_silver_filtered_outputs.py`.
"""

from __future__ import annotations

import runpy
from pathlib import Path


if __name__ == "__main__":
    print("[deprecation] 10_upload_silver_filtered.py is a legacy wrapper. Use 10_upload_silver_filtered_outputs.py.", flush=True)
    target = Path(__file__).resolve().with_name("10_upload_silver_filtered_outputs.py")
    runpy.run_path(str(target), run_name="__main__")
