WITH longest_per_person AS (
    SELECT
        category,
        name,
        died,
        primary_title,
        runtime_minutes,
        DENSE_RANK() OVER (
            PARTITION BY crew.category
            ORDER BY
                people.died ASC,
                people.name ASC
        ) AS rank_num,
        DENSE_RANK() OVER (
            PARTITION BY crew.category,
            crew.person_id
            ORDER BY
                runtime_minutes DESC,
                titles.title_id ASC
        ) AS rank_num_runtime
    FROM
        crew
        INNER JOIN people ON people.person_id = crew.person_id
        INNER JOIN titles ON crew.title_id = titles.title_id
    WHERE
        people.died IS NOT NULL
        AND runtime_minutes IS NOT NULL
),
top_titles AS (
    SELECT
        category,
        name,
        died,
        primary_title,
        runtime_minutes,
        rank_num
    FROM
        longest_per_person
    WHERE
        rank_num <= 5
        AND rank_num_runtime = 1
)
SELECT
    *
FROM
    top_titles
ORDER BY
    category ASC,
    rank_num ASC;