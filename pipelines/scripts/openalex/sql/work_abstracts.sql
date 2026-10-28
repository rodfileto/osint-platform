SELECT
    short_id(id) AS work_id,
    array_to_string(
        list_transform(
            list_sort(flatten(list_transform(
                map_entries(abstract_inverted_index::JSON::MAP(VARCHAR, INTEGER[])),
                lambda e: list_transform(e.value, lambda p: {'p': p, 'w': e.key})
            ))),
            lambda x: x.w
        ),
        ' '
    ) AS abstract
FROM read_parquet('{src}')
WHERE json_valid(abstract_inverted_index)  -- upstream truncates some at ~32KB, leaving invalid JSON
ORDER BY work_id
