-- The JSON → MAP cast is fast but fails on the rare object that repeats a word as a key; those
-- rows (TRY_CAST gives NULL) are rebuilt from json_each, which keeps every entry.
WITH works AS (
    SELECT
        id,
        abstract_inverted_index AS inverted,
        TRY_CAST(abstract_inverted_index::JSON AS MAP(VARCHAR, INTEGER[])) AS words
    FROM read_parquet('{src}')
    WHERE json_valid(abstract_inverted_index)  -- upstream truncates some at ~32KB, leaving invalid JSON
)
SELECT
    short_id(id) AS work_id,
    array_to_string(
        list_transform(
            list_sort(flatten(list_transform(
                map_entries(words),
                lambda e: list_transform(e.value, lambda p: {'p': p, 'w': e.key})
            ))),
            lambda x: x.w
        ),
        ' '
    ) AS abstract
FROM works
WHERE words IS NOT NULL
UNION ALL
SELECT short_id(w.id), string_agg(e.key, ' ' ORDER BY p.p)
FROM works w, json_each(w.inverted) e, unnest(e.value::INTEGER[]) p(p)
WHERE w.words IS NULL
GROUP BY w.id
ORDER BY work_id
