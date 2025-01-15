with raw_streaming_history as (
    select * from  {{ source("spotify_personal", "raw_streaming_history") }}
),

renamed as (
    select
        ts as end_timestamp,
        platform::VARCHAR as platform,
        ms_played,
        conn_country::VARCHAR as country_streamed_in,
        spotify_track_uri::VARCHAR as track_uri,
        reason_start,
        reason_end,
        shuffle,
        skipped,
        offline,
        incognito_mode

    from raw_streaming_history

    where
        spotify_track_uri is not null
),

transformed as (
    select
        end_timestamp,
        end_timestamp - to_milliseconds(ms_played) as start_timestamp,
        ms_played / 1000 as stream_duration,
        case
            when platform like '%tizen%' then 'tv'
            when platform like 'iOS%' then 'ios'
            when platform like '%ps4%' then 'playstation'
            when platform like '%web_player%' then 'browser'
            when platform like 'osx' or platform like 'Windows%' or platform = 'windows' then 'desktop'
            else platform
        end as medium,
        country_streamed_in,
        split(track_uri, ':')[3] as track_id,
        reason_start,
        reason_end,
        shuffle,
        skipped,
        offline,
        incognito_mode

    from renamed
)

select * from transformed