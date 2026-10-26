"""Download both FINEP spreadsheets and load all finep.* tables, outside Airflow.

Used by the dev bootstrap; it runs the same steps as the finep_pipeline DAG.

    python pipelines/scripts/finep/bootstrap_full_load.py --output-dir /bootstrap-data/finep/raw
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from finep.downloader import collect_all  # noqa: E402
from finep.loader import TABLES, load_table  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--output-dir", type=Path, required=True, help="Where the spreadsheets are saved")
    parser.add_argument("--force", action="store_true", help="Download and reload even if nothing changed")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    failed = [r["dataset_type"] for r in collect_all(args.output_dir, force=args.force) if r.get("error")]
    if failed:
        raise SystemExit(f"FINEP download failed for: {', '.join(failed)}")
    for table in TABLES:
        load_table(table, force=args.force)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
