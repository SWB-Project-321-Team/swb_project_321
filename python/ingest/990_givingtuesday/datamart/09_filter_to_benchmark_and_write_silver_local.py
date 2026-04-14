"""
Legacy compatibility wrapper for the reordered GT datamart pipeline.

The canonical benchmark-filter step is now `08_filter_benchmark_outputs_local.py`.
"""

from __future__ import annotations

import runpy
from pathlib import Path


if __name__ == "__main__":
    print("[deprecation] 09_filter_to_benchmark_and_write_silver_local.py is a legacy wrapper. Use 08_filter_benchmark_outputs_local.py.", flush=True)
    target = Path(__file__).resolve().with_name("08_filter_benchmark_outputs_local.py")
    runpy.run_path(str(target), run_name="__main__")
