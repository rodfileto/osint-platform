import argparse
import sys

from .catalog import build_catalog, verify
from .config import Lake
from .silver import TABLES, build_table
from .sync import sync


def main() -> None:
    parser = argparse.ArgumentParser(prog="python -m openalex", description="OpenAlex bronze and silver")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_sync = sub.add_parser("sync", help="mirror the latest Parquet release into bronze (MinIO)")
    p_sync.add_argument("--workers", type=int, default=8)
    p_sync.add_argument("--dry-run", action="store_true")
    sub.add_parser("verify", help="check bronze sizes and record counts against the manifest")
    sub.add_parser("catalog", help="(re)create the DuckDB views over bronze and silver")
    p_silver = sub.add_parser("silver", help="build silver tables from bronze")
    p_silver.add_argument("tables", nargs="*", help=f"default: all ({', '.join(TABLES)})")
    p_silver.add_argument("--limit", type=int, help="build only N sample files per table")
    p_silver.add_argument("--force", action="store_true", help="rebuild even if up to date")
    p_silver.add_argument("--threads", type=int)
    args = parser.parse_args()

    def log(msg):
        print(msg, flush=True)

    lake = Lake.from_env()
    if args.cmd == "sync":
        ok = sync(lake, workers=args.workers, dry_run=args.dry_run, log=log)
    elif args.cmd == "verify":
        ok = verify(lake, log=log)
    elif args.cmd == "silver":
        unknown = set(args.tables) - set(TABLES)
        if unknown:
            parser.error(f"unknown silver tables: {', '.join(sorted(unknown))}")
        for table in args.tables or TABLES:
            build_table(lake, table, limit=args.limit, force=args.force, threads=args.threads, log=log)
        ok = True
    else:
        build_catalog(lake, log=log)
        ok = True
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
