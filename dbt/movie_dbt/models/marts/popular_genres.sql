WITH genre_split AS (
    SELECT
        unnest(string_to_array(genres, '|')) AS genre,
        r.rating
    FROM {{ ref('stg_movies') }} m
    JOIN {{ ref('stg_ratings') }} r
        ON m.movie_id = r.movie_id
)
SELECT
    genre,
    COUNT(rating) AS rating_count,
    AVG(rating) AS avg_rating
FROM genre_split
GROUP BY genre
ORDER BY rating_count DESC
LIMIT 10