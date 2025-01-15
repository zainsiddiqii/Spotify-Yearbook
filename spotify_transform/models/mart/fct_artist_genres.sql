with stg_artist as (
    select * from {{ ref("stg_artist") }}
),

fct_artist_genres as (
    select
        artist_id,
        unnest(genres) as genre
    from stg_artist
)

select * from fct_artist_genres