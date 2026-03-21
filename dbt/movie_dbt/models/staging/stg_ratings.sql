with source as (
    select
        "userId" as user_id,
        "movieId" as movie_id,
        rating,
        to_timestamp(timestamp) as rating_ts
    from {{ source('movies', 'raw_ratings') }}
)

select * from source
where rating is not null