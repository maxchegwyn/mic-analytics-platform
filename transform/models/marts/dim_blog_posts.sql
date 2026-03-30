select
    post_id,
    slug,
    title,
    published_at,
    modified_at,
    url
from {{ ref('stg_wordpress_posts') }}