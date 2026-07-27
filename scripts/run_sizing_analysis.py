#!/usr/bin/env python3
"""Run one ad-hoc sizing analysis by id.

Reads the input from the sizing_analyses row, mines the ICP + measures
per-channel audience (no launch), and flips the row's status. Invoked by the
sizing_analysis.yml workflow (console "Run sizing" button) and runnable locally:

    doppler run -- ./venv/bin/python scripts/run_sizing_analysis.py --analysis-id SZ-...
"""
import argparse
import json
import logging
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.sizing_analysis import compute_sizing_analysis  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--analysis-id", dest="analysis_id", required=True)
    args = ap.parse_args()
    result = compute_sizing_analysis(args.analysis_id)
    print(json.dumps(result, default=str))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    sys.exit(main())
