"""Guards for the dbt gold build and publish (Airflow BashOperator pre-steps).

Ensures the processed bucket exists and that the selected month is the latest
silver month. Gold tables snapshot the latest month, so building or publishing
them for an older month would regress them. The scripts exit with status 2
when the run should be skipped; the Airflow tasks treat that as a skip.
"""

import sys


def ensure_processed_bucket() -> None:
    import downloader

    s3 = downloader.s3_client()
    downloader.ensure_bucket(s3, downloader.processed_bucket())


def latest_silver_month() -> str | None:
    """The newest month in the manifest whose transform has finished."""
    from manifest_tracker import get_db_connection

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT max(reference_month) FROM cnpj.download_manifest WHERE transformed_at IS NOT NULL"
            )
            return cur.fetchone()[0]
    finally:
        conn.close()


def skip(month: str, what: str) -> None:
    latest = latest_silver_month()
    if latest is not None and latest > month:
        print(
            f"Selected month {month} is older than the latest silver month {latest}; "
            f"skipping the {what}"
        )
        sys.exit(2)


def main() -> None:
    month = sys.argv[1] if len(sys.argv) > 1 else ""

    ensure_processed_bucket()

    if not month:
        print("No reference month selected; skipping the gold build")
        sys.exit(2)

    skip(month, "gold build")
    print(f"Building gold for {month}")


if __name__ == "__main__":
    main()
