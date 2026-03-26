with source as (
    select * from {{ source('raw_wordpress', 'posts') }}
),

cleaned as (
    select
        id                          as post_id,
        slug,
        title__rendered             as title,
        date                        as published_at,
        modified                    as modified_at,
        status,
        link                        as url
    from source
    where status = 'publish'
)

select * from cleaned