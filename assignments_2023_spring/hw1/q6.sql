WITH batman_actors AS (
    SELECT
        DISTINCT(people.person_id),
        name
    FROM
        people
        INNER JOIN crew ON people.person_id = crew.person_id
    WHERE
        category == 'actor'
        AND characters LIKE '%"Batman"%'
)
SELECT
    name,
    ROUND(AVG(rating), 2) as avg_rating
FROM
    ratings
    INNER JOIN crew ON ratings.title_id == crew.title_id
    INNER JOIN batman_actors ON crew.person_id == batman_actors.person_id
GROUP BY
    batman_actors.person_id
ORDER BY
    avg_rating DESC
LIMIT
    10;
