"""
WooCommerce ingestion pipeline for MiC Analytics Platform.

Loads to BigQuery: raw_woocommerce dataset
Resources:
  - orders          → all orders (all statuses)
  - customers       → all WooCommerce customers
  - products        → all products
  - product_variations → variations for variable products

Auth: WooCommerce REST API consumer key/secret (Basic Auth over HTTPS)
Pagination: WooCommerce uses page-based pagination (page=1,2,3...)

Usage:
  python ingestion/woocommerce.py

Environment variables required (.env):
  WOOCOMMERCE_URL               — Store URL (https://makeitconscious.com)
  WOOCOMMERCE_CONSUMER_KEY      — ck_...
  WOOCOMMERCE_CONSUMER_SECRET   — cs_...
  BQ_PROJECT                    — BigQuery project ID (make-it-conscious)
  BQ_DATASET_WOOCOMMERCE        — BigQuery dataset (raw_woocommerce)

Write disposition: replace — WooCommerce order statuses mutate over time
(pending → processing → completed → refunded). Full replace ensures clean state.
"""

import os
import dlt
import requests
from typing import Iterator, Any
from dotenv import load_dotenv

load_dotenv()

PAGE_SIZE = 100  # WooCommerce max per page


def woo_paginate(
    base_url: str,
    consumer_key: str,
    consumer_secret: str,
    endpoint: str,
    params: dict = None,
) -> Iterator[dict]:
    """
    Generic WooCommerce REST API paginator.

    WooCommerce uses page-based pagination. Stops when a page returns
    fewer results than PAGE_SIZE (last page).
    """
    params = params or {}
    params["per_page"] = PAGE_SIZE
    page = 1

    session = requests.Session()
    # WooCommerce REST API uses HTTP Basic Auth
    session.auth = (consumer_key, consumer_secret)

    url = f"{base_url.rstrip('/')}/wp-json/wc/v3/{endpoint}"

    while True:
        params["page"] = page
        resp = session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        if not data:
            break

        yield from data

        if len(data) < PAGE_SIZE:
            break

        page += 1


@dlt.resource(name="orders", write_disposition="replace", primary_key="id")
def orders(
    base_url: str,
    consumer_key: str,
    consumer_secret: str,
) -> Iterator[Any]:
    """
    All orders regardless of status.
    Statuses: pending, processing, on-hold, completed, cancelled, refunded, failed.
    """
    yield from woo_paginate(
        base_url,
        consumer_key,
        consumer_secret,
        "orders",
        params={"status": "any", "orderby": "date", "order": "asc"},
    )


@dlt.resource(name="customers", write_disposition="replace", primary_key="id")
def customers(
    base_url: str,
    consumer_key: str,
    consumer_secret: str,
) -> Iterator[Any]:
    """All WooCommerce registered customers."""
    yield from woo_paginate(
        base_url,
        consumer_key,
        consumer_secret,
        "customers",
        params={"orderby": "registered_date", "order": "asc"},
    )


@dlt.resource(name="products", write_disposition="replace", primary_key="id")
def products(
    base_url: str,
    consumer_key: str,
    consumer_secret: str,
) -> Iterator[Any]:
    """All products including drafts and private."""
    yield from woo_paginate(
        base_url,
        consumer_key,
        consumer_secret,
        "products",
        params={"status": "any"},
    )


@dlt.source(name="woocommerce")
def woocommerce_source(
    base_url: str,
    consumer_key: str,
    consumer_secret: str,
):
    """All WooCommerce resources as a single dlt source."""
    return [
        orders(base_url=base_url, consumer_key=consumer_key, consumer_secret=consumer_secret),
        customers(base_url=base_url, consumer_key=consumer_key, consumer_secret=consumer_secret),
        products(base_url=base_url, consumer_key=consumer_key, consumer_secret=consumer_secret),
    ]


def run_pipeline() -> None:
    base_url = os.environ.get("WOOCOMMERCE_URL")
    consumer_key = os.environ.get("WOOCOMMERCE_CONSUMER_KEY")
    consumer_secret = os.environ.get("WOOCOMMERCE_CONSUMER_SECRET")
    bq_project = os.environ.get("BQ_PROJECT", "make-it-conscious")
    bq_dataset = os.environ.get("BQ_DATASET_WOOCOMMERCE", "raw_woocommerce")

    if not all([base_url, consumer_key, consumer_secret]):
        raise EnvironmentError(
            "Missing WooCommerce credentials. Ensure these are set in .env:\n"
            "  WOOCOMMERCE_URL\n"
            "  WOOCOMMERCE_CONSUMER_KEY\n"
            "  WOOCOMMERCE_CONSUMER_SECRET"
        )

    # Quick auth check before running the full pipeline
    resp = requests.get(
        f"{base_url.rstrip('/')}/wp-json/wc/v3/orders",
        auth=(consumer_key, consumer_secret),
        params={"per_page": 1},
        timeout=10,
    )
    if resp.status_code == 401:
        raise EnvironmentError(
            "WooCommerce credentials rejected (401). "
            "Check WOOCOMMERCE_CONSUMER_KEY and WOOCOMMERCE_CONSUMER_SECRET."
        )
    if resp.status_code == 404:
        raise EnvironmentError(
            "WooCommerce REST API not found (404). "
            "Ensure WooCommerce is active and pretty permalinks are enabled "
            "in WordPress: Settings > Permalinks > Post name."
        )

    pipeline = dlt.pipeline(
        pipeline_name="woocommerce",
        destination=dlt.destinations.bigquery(project_id=bq_project),
        dataset_name=bq_dataset,
        progress="log",
    )

    print(f"Running WooCommerce pipeline → {bq_project}.{bq_dataset}")
    print(f"  Store URL: {base_url}")

    load_info = pipeline.run(
        woocommerce_source(
            base_url=base_url,
            consumer_key=consumer_key,
            consumer_secret=consumer_secret,
        )
    )

    print(load_info)
    print("\nWooCommerce ingestion complete.")
    print(f"Tables written to {bq_project}.{bq_dataset}:")
    for resource in ["orders", "customers", "products"]:
        print(f"  - {resource}")


if __name__ == "__main__":
    run_pipeline()