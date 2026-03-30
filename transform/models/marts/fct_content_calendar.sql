with

youtube as (
    select
        video_id                as content_id,
        title,
        publish_date            as published_at,
        'youtube_video'         as content_type,
        'YouTube'               as platform,
        cast(null as string)    as podcast
    from {{ ref('dim_videos') }}
    where publish_date is not null
),

blog as (
    select
        cast(post_id as string)             as content_id,
        title,
        cast(published_at as date)          as published_at,
        'blog_post'                         as content_type,
        'WordPress'                         as platform,
        cast(null as string)                as podcast
    from {{ ref('dim_blog_posts') }}
),

spotify as (
    select
        episode_id              as content_id,
        title,
        release_date            as published_at,
        'podcast_episode'       as content_type,
        'Spotify'               as platform,
        podcast
    from {{ ref('stg_spotify_episodes') }}
)

select * from youtube
union all
select * from blog
union all
select * from spotify