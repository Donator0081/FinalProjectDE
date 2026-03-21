with source as (
    select
        "movieId" as movie_id,
        title,
        genres
    from {{ source('movies', 'raw_movies') }}
)

select
    movie_id,
    title,
    genres,
    split_part(genres, '|', 1) as main_genre
from source