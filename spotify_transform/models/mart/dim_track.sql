with stg_track as (
    select * from {{ ref("stg_track") }}
),

dim_track as (
    select
        track_id,
        track_url,
        track_name,
        track_duration,
        explicit,
        popularity,
        multiple_artists

    from stg_track
)

select * from dim_track