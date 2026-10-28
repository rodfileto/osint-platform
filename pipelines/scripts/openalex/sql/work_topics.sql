SELECT
    short_id(id)                        AS work_id,
    topic_rank,
    short_id(t.id)                      AS topic_id,
    t.score,
    short_id(t.subfield.id)::INTEGER    AS subfield_id,
    short_id(t.field.id)::INTEGER       AS field_id,
    short_id(t."domain".id)::INTEGER    AS domain_id,
    publication_year
FROM (
    SELECT
        id,
        publication_year,
        unnest(topics)                          AS t,
        unnest(generate_series(1, len(topics))) AS topic_rank
    FROM read_parquet('{src}')
    WHERE len(topics) > 0
)
ORDER BY work_id, topic_rank
