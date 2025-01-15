with raw_artist as (
    select * from {{ source('spotify_api', 'raw_artist') }}
),

artist_info as (
    select
        id::VARCHAR as artist_id,
        name::VARCHAR as artist_name,
        trim('\"' from external_urls.spotify::VARCHAR) as artist_url,
        followers.total::INTEGER as followers,
        genres::VARCHAR[] as genres,
        popularity::INTEGER as popularity,
        trim('\"' from images[2].url::VARCHAR) as image_url

    from raw_artist
)

select * from artist_info