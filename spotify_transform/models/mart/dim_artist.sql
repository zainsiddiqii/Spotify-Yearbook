with stg_artist as (
    select * from {{ ref("stg_artist") }}
),

dim_artist as (
    select
        artist_id,
        artist_name,
        artist_url,
        followers,
        popularity,
        image_url
    
    from stg_artist
)

select * from dim_artist

