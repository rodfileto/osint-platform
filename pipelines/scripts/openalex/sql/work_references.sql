SELECT
    short_id(id)          AS citing,
    short_id(cited_url)   AS cited,
    publication_year      AS citing_year
FROM (
    SELECT id, publication_year, unnest(referenced_works) AS cited_url
    FROM read_parquet('{src}')
    WHERE len(referenced_works) > 0
)
ORDER BY citing, cited
