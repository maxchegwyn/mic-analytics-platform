with source as (
    select * from {{ source('raw_youtube', 'raw_video_playlists') }}
),

cleaned as (
    select
        video_id,
        playlist_id,
        playlist_title
    from source
)

select * from cleaned