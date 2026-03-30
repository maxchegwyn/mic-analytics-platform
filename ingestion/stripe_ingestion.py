"""
Stripe ingestion pipeline for MiC Analytics Platform.

Loads to BigQuery: raw_stripe dataset
Resources:
  - customers       → all Stripe customers
  - charges         → all charges (successful, failed, refunded)
  - payment_intents → all payment intents
  - invoices        → all invoices (Billing)
  - subscriptions   → all subscriptions (Billing)
  - products        → all products
  - prices          → all prices

Auth: Stripe restricted API key (rk_live_...)
Pagination: Stripe uses cursor-based pagination (starting_after = last object ID)

Usage:
  python ingestion/stripe_ingestion.py

Environment variables required (.env):
  STRIPE_API_KEY       — Stripe restricted API key (rk_live_...)
  BQ_PROJECT           — BigQuery project ID (make-it-conscious)
  BQ_DATASET_STRIPE    — BigQuery dataset for Stripe raw data (raw_stripe)

Write disposition: replace (full snapshot on every run — same rationale as
Mailchimp: Stripe objects mutate over time, replace ensures clean current state)
"""

import os
import dlt
import requests
from typing import Iterator, Any
from dotenv import load_dotenv

load_dotenv()

STRIPE_BASE_URL = "https://api.stripe.com/v1"
PAGE_SIZE = 100  # Stripe max per page
STRIPE_START_DATE = 1640995200  # 2022-01-01 00:00:00 UTC as Unix timestamp


def stripe_paginate(
    api_key: str,
    endpoint: str,
    params: dict = None,
) -> Iterator[dict]:
    """
    Generic Stripe cursor paginator.

    Stripe uses `starting_after` (last object ID) for pagination.
    Yields individual objects — not pages.
    """
    params = params or {}
    params["limit"] = PAGE_SIZE

    session = requests.Session()
    session.auth = (api_key, "")  # Stripe uses HTTP basic auth, key as username

    url = f"{STRIPE_BASE_URL}/{endpoint}"
    last_id = None

    while True:
        if last_id:
            params["starting_after"] = last_id

        resp = session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        objects = data.get("data", [])
        for obj in objects:
            yield obj

        if not data.get("has_more", False):
            break

        last_id = objects[-1]["id"]


@dlt.resource(name="customers", write_disposition="replace", primary_key="id")
def customers(api_key: str) -> Iterator[Any]:
    """All Stripe customers."""
    yield from stripe_paginate(api_key, "customers")


@dlt.resource(name="charges", write_disposition="replace", primary_key="id")
def charges(api_key: str) -> Iterator[Any]:
    """
    All charges regardless of status (succeeded, failed, refunded).
    Expand customer field to get email without a separate lookup.
    Fetches full history from 2022-01-01 onward.
    """
    yield from stripe_paginate(
        api_key,
        "charges",
        params={
            "expand[]": "data.customer",
            "created[gte]": STRIPE_START_DATE,
        },
    )


@dlt.resource(name="payment_intents", write_disposition="replace", primary_key="id")
def payment_intents(api_key: str) -> Iterator[Any]:
    """All payment intents."""
    yield from stripe_paginate(api_key, "payment_intents")


@dlt.resource(name="invoices", write_disposition="replace", primary_key="id")
def invoices(api_key: str) -> Iterator[Any]:
    """
    All invoices. Covers both WooCommerce and LearnWorlds transactions
    since both route through the same Stripe account.
    """
    yield from stripe_paginate(api_key, "invoices")


@dlt.resource(name="subscriptions", write_disposition="replace", primary_key="id")
def subscriptions(api_key: str) -> Iterator[Any]:
    """All subscriptions — all statuses (active, cancelled, past_due etc.)"""
    yield from stripe_paginate(
        api_key,
        "subscriptions",
        params={"status": "all"},
    )


@dlt.resource(name="products", write_disposition="replace", primary_key="id")
def products(api_key: str) -> Iterator[Any]:
    """All products including archived."""
    yield from stripe_paginate(
        api_key,
        "products",
        params={"active": "false"},  # active=false returns ALL including inactive
    )


@dlt.resource(name="prices", write_disposition="replace", primary_key="id")
def prices(api_key: str) -> Iterator[Any]:
    """All prices including inactive."""
    yield from stripe_paginate(
        api_key,
        "prices",
        params={"active": "false"},  # returns all including inactive
    )


@dlt.source(name="stripe")
def stripe_source(api_key: str):
    """All Stripe resources as a single dlt source."""
    return [
        customers(api_key=api_key),
        charges(api_key=api_key),
        payment_intents(api_key=api_key),
        invoices(api_key=api_key),
        subscriptions(api_key=api_key),
        products(api_key=api_key),
        prices(api_key=api_key),
    ]


def run_pipeline() -> None:
    api_key = os.environ.get("STRIPE_API_KEY")
    bq_project = os.environ.get("BQ_PROJECT", "make-it-conscious")
    bq_dataset = os.environ.get("BQ_DATASET_STRIPE", "raw_stripe")

    if not api_key:
        raise EnvironmentError(
            "STRIPE_API_KEY not set. Add it to your .env file.\n"
            "Format: rk_live_... (restricted key) or sk_live_... (secret key)\n"
            "Find it in Stripe: Developers > API keys"
        )

    # Quick auth check before running the full pipeline
    resp = requests.get(
        f"{STRIPE_BASE_URL}/customers",
        auth=(api_key, ""),
        params={"limit": 1},
        timeout=10,
    )
    if resp.status_code == 401:
        raise EnvironmentError(
            "Stripe API key rejected (401). Check STRIPE_API_KEY in your .env file."
        )
    if resp.status_code == 403:
        raise EnvironmentError(
            "Stripe API key lacks permission (403). "
            "Ensure the restricted key has Read access to Customers."
        )

    pipeline = dlt.pipeline(
        pipeline_name="stripe",
        destination=dlt.destinations.bigquery(project_id=bq_project),
        dataset_name=bq_dataset,
        progress="log",
    )

    print(f"Running Stripe pipeline → {bq_project}.{bq_dataset}")

    load_info = pipeline.run(stripe_source(api_key=api_key))

    print(load_info)
    print("\nStripe ingestion complete.")
    print(f"Tables written to {bq_project}.{bq_dataset}:")
    for resource in ["customers", "charges", "payment_intents", "invoices",
                     "subscriptions", "products", "prices"]:
        print(f"  - {resource}")


if __name__ == "__main__":
    run_pipeline()