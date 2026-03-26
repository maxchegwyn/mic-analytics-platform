with mic as (
    select *, 'mic' as podcast
    from {{ source('raw_spotify', 'episodes_mic') }}
),

inner_work as (
    select *, 'inner_work' as podcast
    from {{ source('raw_spotify', 'episodes_inner_work') }}
),

combined as (
    select
        id                                      as episode_id,
        name                                    as title,
        description,
        cast(release_date as date)              as release_date,
        duration_ms / 60000.0                   as duration_minutes,
        podcast,
        external_urls__spotify                  as spotify_url
    from mic

    union all

    select
        id,
        name,
        description,
        cast(release_date as date),
        duration_ms / 60000.0,
        podcast,
        external_urls__spotify
    from inner_work
)

select * from combined