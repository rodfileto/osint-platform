from pathlib import Path

from openalex.silver import connect, sample, stale, target


def test_short_id_macro():
    con = connect()
    rows = con.execute(
        "SELECT short_id('https://openalex.org/W123'), "
        "short_id('https://openalex.org/subfields/1707'), short_id(NULL), short_id(''), "
        "short_id('https://openalex.org/keywords/lopinavir/ritonavir')"
    ).fetchone()
    assert rows == ("W123", "1707", None, None, "lopinavir/ritonavir")


def test_sample_spreads_across_files():
    files = [Path(f"f{i}") for i in range(10)]
    assert sample(files, 3) == [Path("f0"), Path("f3"), Path("f6")]
    assert sample(files, 20) == files


def test_stale_compares_source_etag_and_output(tmp_path):
    out = tmp_path / "work_references"
    sources = {"works/u=1/a.parquet": (10, "e1"), "works/u=1/b.parquet": (10, "e2")}
    for key in sources:
        target(key, "works", out).parent.mkdir(parents=True, exist_ok=True)
        target(key, "works", out).write_bytes(b"x")
    built = {"works/u=1/a.parquet": "e1", "works/u=1/b.parquet": "old"}
    assert stale(sources, built, out, "works") == ["works/u=1/b.parquet"]
    assert stale(sources, built, out, "works", force=True) == sorted(sources)
    target("works/u=1/a.parquet", "works", out).unlink()
    assert stale(sources, built, out, "works") == sorted(sources)


def test_work_references_explodes_and_shortens_ids(tmp_path):
    from openalex.silver import SQL_DIR, build_file

    src, dest = tmp_path / "works.parquet", tmp_path / "out" / "refs.parquet"
    con = connect()
    con.execute(f"""
        COPY (SELECT * FROM (VALUES
            ('https://openalex.org/W2', 2020, ['https://openalex.org/W9', 'https://openalex.org/W1']),
            ('https://openalex.org/W3', 2021, []::VARCHAR[]),
            ('https://openalex.org/W1', 2019, NULL::VARCHAR[])
        ) t(id, publication_year, referenced_works)) TO '{src}' (FORMAT parquet)
    """)
    sql = (SQL_DIR / "work_references.sql").read_text()
    assert build_file(con, sql, str(src), dest) == 2
    assert con.execute(f"SELECT * FROM '{dest}'").fetchall() == [
        ("W2", "W1", 2020),
        ("W2", "W9", 2020),
    ]


def test_work_authorships_one_row_per_authorship(tmp_path):
    from openalex.silver import SQL_DIR, build_file

    src, dest = tmp_path / "works.parquet", tmp_path / "out" / "auth.parquet"
    con = connect()
    con.execute(f"""
        COPY (SELECT 'https://openalex.org/W1' AS id, 2020 AS publication_year, [
            {{'author': {{'display_name': 'Ana', 'id': 'https://openalex.org/A1',
                         'orcid': 'https://orcid.org/0000-0001-2345-678X'}},
             'author_position': 'first', 'raw_author_name': 'A.', 'raw_orcid': NULL,
             'is_corresponding': true,
             'institutions': [{{'id': 'https://openalex.org/I1'}}, {{'id': 'https://openalex.org/I1'}}],
             'countries': ['BR'], 'raw_affiliation_strings': ['Univ X']}},
            {{'author': {{'display_name': 'Bo', 'id': NULL, 'orcid': NULL}},
             'author_position': 'last', 'raw_author_name': 'B.',
             'raw_orcid': 'https://orcid.org/0000-0002-0000-0001',
             'is_corresponding': NULL, 'institutions': [], 'countries': [],
             'raw_affiliation_strings': []}}
        ] AS authorships) TO '{src}' (FORMAT parquet)
    """)
    sql = (SQL_DIR / "work_authorships.sql").read_text()
    assert build_file(con, sql, str(src), dest) == 2
    rows = con.execute(
        f"SELECT work_id, author_seq, author_id, orcid, is_corresponding, institution_ids "
        f"FROM '{dest}'"
    ).fetchall()
    assert rows == [
        ("W1", 1, "A1", "0000-0001-2345-678X", True, ["I1"]),
        ("W1", 2, None, "0000-0002-0000-0001", False, []),
    ]


def test_work_concepts_and_keywords_drop_repeated_ids(tmp_path):
    from openalex.silver import SQL_DIR, build_file

    src = tmp_path / "works.parquet"
    con = connect()
    con.execute(f"""
        COPY (SELECT 'https://openalex.org/W1' AS id, 2020 AS publication_year,
            [{{'id': 'https://openalex.org/C1', 'level': 0, 'score': 0.5::FLOAT}},
             {{'id': 'https://openalex.org/C1', 'level': 0, 'score': 0.5::FLOAT}},
             {{'id': 'https://openalex.org/C2', 'level': 2, 'score': 0.75::FLOAT}}] AS concepts,
            [{{'id': 'https://openalex.org/keywords/rdf/xml', 'score': 0.25::FLOAT}},
             {{'id': 'https://openalex.org/keywords/xml', 'score': 0.5::FLOAT}}] AS keywords
        ) TO '{src}' (FORMAT parquet)
    """)
    out = tmp_path / "concepts.parquet"
    assert build_file(con, (SQL_DIR / "work_concepts.sql").read_text(), str(src), out) == 2
    assert con.execute(f"SELECT concept_id, level, score FROM '{out}'").fetchall() == [
        ("C2", 2, 0.75), ("C1", 0, 0.5)
    ]
    out = tmp_path / "keywords.parquet"
    assert build_file(con, (SQL_DIR / "work_keywords.sql").read_text(), str(src), out) == 2
    assert con.execute(f"SELECT keyword_id FROM '{out}'").fetchall() == [("xml",), ("rdf/xml",)]


def test_work_abstracts_rebuilds_text_in_position_order(tmp_path):
    from openalex.silver import SQL_DIR, build_file

    src, dest = tmp_path / "works.parquet", tmp_path / "abstracts.parquet"
    con = connect()
    aii = '{"the": [0, 3], "\\"quoted\\"": [2], "fox": [1], "end.": [4]}'
    con.execute(f"""
        COPY (SELECT * FROM (VALUES
            ('https://openalex.org/W1', '{aii}'),
            ('https://openalex.org/W2', NULL),
            ('https://openalex.org/W3', '{{"truncated": [0], "[19'),
            ('https://openalex.org/W4', '{{"a": [0], "word": [1, 3], "word": [2]}}')
        ) t(id, abstract_inverted_index)) TO '{src}' (FORMAT parquet)
    """)
    assert build_file(con, (SQL_DIR / "work_abstracts.sql").read_text(), str(src), dest) == 2
    assert con.execute(f"SELECT * FROM '{dest}'").fetchall() == [
        ("W1", 'the fox "quoted" the end.'),
        ("W4", "a word word word"),  # a repeated key keeps the positions of both entries
    ]
