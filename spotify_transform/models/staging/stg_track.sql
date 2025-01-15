with raw_track as (
    select * from {{ source('spotify_api', 'raw_track') }}
),

track_info as (
    select
        id::VARCHAR as track_id,
        trim('\"' from external_urls.spotify::VARCHAR) as track_url,
        name::VARCHAR as track_name,
        duration_ms / 1000 as track_duration,
        explicit,
        popularity,
        case
            when array_length(artists::VARCHAR[]) > 1 then true
            else false
        end as multiple_artists,
        trim('\"' from album.id::VARCHAR) as album_id,
        trim('\"' from artists[0].id::VARCHAR) as artist_id

    from raw_track
    
    where id is not null
)

select * from track_info
