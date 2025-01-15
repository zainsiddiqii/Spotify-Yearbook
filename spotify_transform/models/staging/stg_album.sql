with raw_track as (
    select * from {{ source('spotify_api', 'raw_track') }}
),

album_info as (
    select distinct
        trim('\"' from album.id::VARCHAR) as album_id,
        trim('\"' from album.name::VARCHAR) as album_name,
        trim('\"' from album.external_urls.spotify::VARCHAR) as album_url,
        album.total_tracks::INTEGER as total_tracks,
        trim(
            '\"' from left(album.release_date, 5)
        )::INTEGER as release_year,
        trim('\"' from album.images[2].url::VARCHAR) as album_image_url,
        trim('\"' from album.artists[0].id::VARCHAR) as artist_id

    from raw_track
)

select * from album_info