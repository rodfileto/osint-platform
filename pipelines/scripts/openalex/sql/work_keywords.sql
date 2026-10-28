SELECT
    short_id(id)        AS work_id,
    short_id(k.id)      AS keyword_id,
    max(k.score)        AS score,
    any_value(publication_year) AS publication_year
FROM (
    SELECT id, publication_year, unnest(keywords) AS k
    FROM read_parquet('{src}')
    WHERE len(keywords) > 0
)
GROUP BY ALL
ORDER BY work_id, score DESC, keyword_id
