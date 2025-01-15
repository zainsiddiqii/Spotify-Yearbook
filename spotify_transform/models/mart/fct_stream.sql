with stg_streaming_history as (
    select * from {{ ref("stg_streaming_history") }}
),

stg_track as (
    select * from {{ ref("stg_track") }}
),

fct_stream as (
    select
        gen_random_uuid() as stream_id,
        track.track_id,
        track.album_id,
        track.artist_id,
        stream.start_timestamp,
        stream.end_timestamp,
        stream.stream_duration,
        stream.medium,
        stream.country_streamed_in,
        stream.reason_start,
        stream.reason_end,
        stream.shuffle,
        stream.skipped,
        stream.offline,
        stream.incognito_mode

    from stg_streaming_history as stream

    inner join stg_track as track
        on track.track_id = stream.track_id
)

select * from fct_stream