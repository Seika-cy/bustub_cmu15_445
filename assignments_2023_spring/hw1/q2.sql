SELECT
    premiered,
    primary_title || '(' || original_title || ')'
FROM
    titles
WHERE
    primary_title <> original_title
    AND type == 'movie'
    AND genres LIKE '%Action%'
ORDER BY
    premiered DESC,
    primary_title
LIMIT
    10;