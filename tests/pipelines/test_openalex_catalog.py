import json

import duckdb

from openalex.catalog import build_catalog


def test_catalog_shortens_bronze_ids_and_works_when_attached(tmp_path):
    bronze, silver, catalog = tmp_path / "bronze", tmp_path / "silver", tmp_path / "lake.duckdb"
    (bronze / "keywords" / "updated_date=2026-01-01").mkdir(parents=True)
    (silver / "work_keywords" / "updated_date=2026-01-01").mkdir(parents=True)
    con = duckdb.connect()
    con.execute(f"""COPY (SELECT * FROM (VALUES
        ('https://openalex.org/keywords/rdf/xml', 'RDF/XML'),
        ('https://openalex.org/keywords/xml', 'XML')) t(id, display_name))
        TO '{bronze}/keywords/updated_date=2026-01-01/part_0000.parquet' (FORMAT parquet)""")
    con.execute(f"""COPY (SELECT 'W1' AS work_id, 'rdf/xml' AS keyword_id)
        TO '{silver}/work_keywords/updated_date=2026-01-01/part_0000.parquet' (FORMAT parquet)""")
    (bronze / "manifest.json").write_text(json.dumps({"date": "2026-09-23", "entities": [
        {"entity": "keywords", "files": [{
            "url": "s3://openalex/data/parquet/keywords/updated_date=2026-01-01/part_0000.parquet",
            "meta": {"content_length": 1, "record_count": 2}}]}]}))

    build_catalog(bronze, silver, catalog, log=lambda _: None)

    con.execute(f"ATTACH '{catalog}' AS lake (READ_ONLY)")
    rows = con.execute(
        "SELECT w.work_id, k.id, k.display_name FROM lake.work_keywords w "
        "JOIN lake.keywords k ON k.id = w.keyword_id"
    ).fetchall()
    assert rows == [("W1", "rdf/xml", "RDF/XML")]
    assert con.execute("SELECT release_date::VARCHAR FROM lake.lake_release").fetchone() == (
        "2026-09-23",
    )
