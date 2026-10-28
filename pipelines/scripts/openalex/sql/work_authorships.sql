SELECT
    short_id(id)                                              AS work_id,
    author_seq,
    a.author_position,
    short_id(a.author.id)                                     AS author_id,
    a.author.display_name                                     AS author_name,
    a.raw_author_name,
    nullif(regexp_extract(coalesce(a.author.orcid, a.raw_orcid),
                          '(\d{4}-\d{4}-\d{4}-\d{3}[\dX])'), '') AS orcid,
    coalesce(a.is_corresponding, false)                       AS is_corresponding,
    list_distinct(list_transform(a.institutions, lambda i: short_id(i.id))) AS institution_ids,
    a.countries,
    a.raw_affiliation_strings                                 AS raw_affiliations,
    publication_year
FROM (
    SELECT
        id,
        publication_year,
        unnest(authorships)                          AS a,
        unnest(generate_series(1, len(authorships))) AS author_seq
    FROM read_parquet('{src}')
    WHERE len(authorships) > 0
)
ORDER BY work_id, author_seq
