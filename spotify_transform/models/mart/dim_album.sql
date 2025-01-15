with stg_album as (
    select * from {{ ref("stg_album") }}
),

dim_album as (
    select
        album_id,
        album_name,
        album_url,
        total_tracks,
        release_year,
        album_image_url

    from stg_album
)

select * from dim_album