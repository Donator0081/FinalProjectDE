SELECT
    m.movie_id,
    m.title,
    AVG(r.rating) AS avg_rating,
    COUNT(r.rating) AS rating_count
FROM {{ ref('stg_movies') }} m
JOIN {{ ref('stg_ratings') }} r
    ON m.movie_id = r.movie_id
GROUP BY m.movie_id, m.title
ORDER BY avg_rating DESC
LIMIT 20