with

ga4 as (
    select * from {{ ref('stg_ga4_events') }}
),

blog_posts as (
    select
        post_id,
        slug,
        url
    from {{ ref('dim_blog_posts') }}
),

joined as (
    select
        g.date,
        b.post_id,
        b.slug,
        concat(g.source, ' / ', g.medium)       as source_medium,
        count(*)                                 as sessions
    from ga4 as g
    inner join blog_posts as b
        on regexp_contains(g.page_location, concat('/', b.slug))
    group by g.date, b.post_id, b.slug, source_medium
)

select
    date,
    post_id,
    slug,
    source_medium,
    sessions
from joined