from openalex.sync import FileSpec, manifest_files, parse_listing, plan, stale_paths

MANIFEST = {
    "date": "2026-09-23",
    "entities": [
        {"entity": "works", "files": [{
            "url": "s3://openalex/data/parquet/works/updated_date=2026-01-01/part_0000.parquet",
            "meta": {"content_length": 5, "record_count": 2}}]},
        {"entity": "authors", "files": [{
            "url": "s3://openalex/data/parquet/authors/updated_date=2026-01-01/part_0000.parquet",
            "meta": {"content_length": 9, "record_count": 1}}]},
    ],
}


def test_manifest_files_excludes_entities_and_maps_paths():
    files = manifest_files(MANIFEST, excluded={"authors"})
    assert files == [FileSpec("data/parquet/works/updated_date=2026-01-01/part_0000.parquet", 5, 2)]
    assert files[0].rel_path == "works/updated_date=2026-01-01/part_0000.parquet"


def test_plan_skips_only_matching_size_and_etag(tmp_path):
    f = FileSpec("data/parquet/works/u=1/a.parquet", 5, 1, "e1")
    dest = tmp_path / f.rel_path
    dest.parent.mkdir(parents=True)
    dest.write_bytes(b"12345")
    assert plan([f], tmp_path, {f.key: "e1"}) == []
    assert plan([f], tmp_path, {f.key: "old"}) == [f]
    dest.write_bytes(b"1234")
    assert plan([f], tmp_path, {f.key: "e1"}) == [f]


def test_stale_paths_only_within_synced_entities(tmp_path):
    keep = FileSpec("data/parquet/works/u=2/a.parquet", 1, 1)
    for rel in ["works/u=2/a.parquet", "works/u=1/old.parquet", "authors/u=1/x.parquet"]:
        (tmp_path / rel).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / rel).write_bytes(b"x")
    assert stale_paths(tmp_path, [keep]) == [tmp_path / "works/u=1/old.parquet"]


def test_parse_listing_reads_etags_and_token():
    xml = b"""<?xml version="1.0"?>
    <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <Contents><Key>data/parquet/works/a.parquet</Key><ETag>"abc-3"</ETag></Contents>
      <NextContinuationToken>tok</NextContinuationToken>
    </ListBucketResult>"""
    assert parse_listing(xml) == ({"data/parquet/works/a.parquet": "abc-3"}, "tok")
