WITH movie_counts AS (
    SELECT
        m.movie_id,
        m.title,
        COUNT(r.rating) AS rating_count
    FROM {{ ref('stg_movies') }} m
    JOIN {{ ref('stg_ratings') }} r
        ON m.movie_id = r.movie_id
    GROUP BY m.movie_id, m.title
)
SELECT *
FROM movie_counts
ORDER BY rating_count DESC
LIMIT 20