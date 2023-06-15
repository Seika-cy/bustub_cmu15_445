SELECT
    DISTINCT(name)
FROM
    people
    INNER JOIN crew ON people.person_id == crew.person_id
WHERE
    category == 'director'
    AND title_id IN (
        SELECT
            title_id
        FROM
            people
            INNER JOIN crew ON people.person_id == crew.person_id
        WHERE
            category == 'actress'
            AND name LIKE 'Rose%'
    )
ORDER BY
    name;