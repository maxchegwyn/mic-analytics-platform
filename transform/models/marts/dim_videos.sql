with

metadata as (
    select
        video_id,
        title,
        publish_date,
        duration_iso,
        tags
    from {{ ref('stg_video_metadata') }}
),

playlists as (
    select
        video_id,
        playlist_title
    from {{ ref('stg_video_playlists') }}
),

content_type_flag as (
    select distinct
        video_id,
        true as is_exercise
    from playlists
    where playlist_title = 'Active Imagination Exercises'
        and video_id != 'SVWr2pXDQ5k'  -- explainer excluded; not a guided exercise
),

exercise_category as (
    select
        video_id,
        playlist_title as exercise_category
    from playlists
    where playlist_title != 'Active Imagination Exercises'
        and video_id != 'SVWr2pXDQ5k'  -- explainer excluded
    qualify row_number() over (partition by video_id order by playlist_title) = 1
)

select
    m.video_id,
    m.title,
    m.publish_date,
    m.duration_iso,
    m.tags,
    case when f.is_exercise is true then 'exercise' else 'other' end as content_type,
    c.exercise_category
from metadata as m
left join content_type_flag as f on m.video_id = f.video_id
left join exercise_category as c on m.video_id = c.video_id