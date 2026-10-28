SELECT
    short_id(id)        AS work_id,
    short_id(c.id)      AS concept_id,
    any_value(c.level)  AS level,
    max(c.score)        AS score,
    any_value(publication_year) AS publication_year
FROM (
    SELECT id, publication_year, unnest(concepts) AS c
    FROM read_parquet('{src}')
    WHERE len(concepts) > 0
)
GROUP BY ALL
ORDER BY work_id, score DESC, concept_id
