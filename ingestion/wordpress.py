"""
WordPress ingestion pipeline for MiC Analytics Platform.

Loads to BigQuery: raw_wordpress dataset
Resources:
  - posts       → all published posts (blog content)
  - pages       → all published pages
  - categories  → all post categories
  - tags        → all post tags

Auth: none — WordPress REST API is public for published content
Pagination: WordPress uses page-based pagination (page=1,2,3...)

Usage:
  python ingestion/wordpress.py

Environment variables required (.env):
  WORDPRESS_URL        — WordPress site URL (https://makeitconscious.com)
  BQ_PROJECT           — BigQuery project ID (make-it-conscious)
  BQ_DATASET_WORDPRESS — BigQuery dataset (raw_wordpress)

Write disposition: replace — post content and metadata can be edited at any
time. Full snapshot ensures current state is always reflected.
"""

import os
import dlt
import requests
from typing import Iterator, Any
from dotenv import load_dotenv

load_dotenv()

PAGE_SIZE = 100  # WordPress max per page


def wordpress_paginate(
    base_url: str,
    endpoint: str,
    params: dict = None,
) -> Iterator[dict]:
    """
    Generic WordPress REST API paginator.
    Uses page-based pagination. Stops when a page returns fewer
    results than PAGE_SIZE (last page).
    """
    params = params or {}
    params["per_page"] = PAGE_SIZE
    page = 1

    session = requests.Session()
    url = f"{base_url.rstrip('/')}/wp-json/wp/v2/{endpoint}"

    while True:
        params["page"] = page
        resp = session.get(url, params=params, timeout=30)

        # WordPress returns 400 when you request a page beyond the last one
        if resp.status_code == 400:
            break

        resp.raise_for_status()
        data = resp.json()

        if not data:
            break

        yield from data

        if len(data) < PAGE_SIZE:
            break

        page += 1


@dlt.resource(name="posts", write_disposition="replace", primary_key="id")
def posts(base_url: str) -> Iterator[Any]:
    """
    All published posts. Includes title, content, excerpt, slug,
    publish date, categories, tags, and author.
    """
    yield from wordpress_paginate(
        base_url,
        "posts",
        params={
            "status": "publish",
            "orderby": "date",
            "order": "asc",
            "_fields": (
                "id,date,date_gmt,modified,modified_gmt,slug,status,"
                "title,content,excerpt,author,categories,tags,link"
            ),
        },
    )


@dlt.resource(name="pages", write_disposition="replace", primary_key="id")
def pages(base_url: str) -> Iterator[Any]:
    """All published pages."""
    yield from wordpress_paginate(
        base_url,
        "pages",
        params={
            "status": "publish",
            "orderby": "date",
            "order": "asc",
            "_fields": (
                "id,date,date_gmt,modified,modified_gmt,slug,status,"
                "title,content,excerpt,author,parent,link"
            ),
        },
    )


@dlt.resource(name="categories", write_disposition="replace", primary_key="id")
def categories(base_url: str) -> Iterator[Any]:
    """All post categories."""
    yield from wordpress_paginate(
        base_url,
        "categories",
        params={
            "_fields": "id,name,slug,description,count,parent",
        },
    )


@dlt.resource(name="tags", write_disposition="replace", primary_key="id")
def tags(base_url: str) -> Iterator[Any]:
    """All post tags."""
    yield from wordpress_paginate(
        base_url,
        "tags",
        params={
            "_fields": "id,name,slug,description,count",
        },
    )


@dlt.source(name="wordpress")
def wordpress_source(base_url: str):
    """All WordPress resources as a single dlt source."""
    return [
        posts(base_url=base_url),
        pages(base_url=base_url),
        categories(base_url=base_url),
        tags(base_url=base_url),
    ]


def run_pipeline() -> None:
    base_url = os.environ.get("WORDPRESS_URL", "https://makeitconscious.com")
    bq_project = os.environ.get("BQ_PROJECT", "make-it-conscious")
    bq_dataset = os.environ.get("BQ_DATASET_WORDPRESS", "raw_wordpress")

    # Quick connectivity check
    resp = requests.get(
        f"{base_url.rstrip('/')}/wp-json/wp/v2/posts",
        params={"per_page": 1},
        timeout=10,
    )
    if resp.status_code == 404:
        raise EnvironmentError(
            "WordPress REST API not found (404). "
            "Ensure pretty permalinks are enabled: "
            "WordPress Admin > Settings > Permalinks > Post name."
        )
    resp.raise_for_status()

    pipeline = dlt.pipeline(
        pipeline_name="wordpress",
        destination=dlt.destinations.bigquery(project_id=bq_project),
        dataset_name=bq_dataset,
        progress="log",
    )

    print(f"Running WordPress pipeline → {bq_project}.{bq_dataset}")
    print(f"  Site URL: {base_url}")

    load_info = pipeline.run(wordpress_source(base_url=base_url))

    print(load_info)
    print("\nWordPress ingestion complete.")
    print(f"Tables written to {bq_project}.{bq_dataset}:")
    for resource in ["posts", "pages", "categories", "tags"]:
        print(f"  - {resource}")


if __name__ == "__main__":
    run_pipeline()