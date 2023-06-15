WITH json_characters(characters) AS (
    SELECT
        characters
    FROM
        people,
        crew
    WHERE
        name = 'Leonardo DiCaprio'
        AND born = 1974
        AND people.person_id = crew.person_id
    ORDER BY
        characters
),
characters(character) AS(
    SELECT
        DISTINCT value as character
    FROM
        json_characters,
        json_each(json_characters.characters)
    WHERE
        character <> ''
        AND character NOT LIKE '%self%'
    ORDER BY
        character
)
SELECT
	GROUP_CONCAT(character)
FROM
	characters;