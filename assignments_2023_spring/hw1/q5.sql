SELECT
    type,
    ROUND(AVG(rating), 2) AS avg_rating,
    MIN(rating),
    MAX(rating)
FROM
    akas
    INNER JOIN ratings ON ratings.title_id == akas.title_id
    INNER JOIN titles ON ratings.title_id == titles.title_id
WHERE
    language == 'de'
    AND (
        types == 'imdbDisplay'
        OR types == 'original'
    )
GROUP BY
    type
ORDER BY
    avg_rating;