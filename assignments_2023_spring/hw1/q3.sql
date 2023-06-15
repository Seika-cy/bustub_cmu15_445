SELECT
    primary_title,
    CASE
        WHEN ended IS NULL THEN strftime('%Y', 'now') - premiered
        ELSE ended - premiered
    END AS runtime
FROM
    titles
WHERE
    premiered IS NOT NULL
    AND type = 'tvSeries'
ORDER BY
    runtime DESC,
    primary_title ASC
LIMIT
    20;