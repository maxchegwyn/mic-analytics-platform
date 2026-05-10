with source as (
    select * from {{ source('raw_youtube', 'raw_video_metadata') }}
),

cleaned as (
    select
        video_id,
        title,
        cast(publish_date as date)      as publish_date,
        duration_iso,
        tags
    from source
)

select * from cleaned
qualify row_number() over (partition by video_id order by publish_date) = 1