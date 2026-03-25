"""
JotForm ingestion pipeline for MiC Analytics Platform.

Loads to BigQuery: raw_jotform dataset
Resources:
  - submissions_standard    → all submissions from the standard PDA form
  - submissions_manual_opt  → all submissions from the manual opt-in PDA form

Auth: JotForm API key
Pagination: JotForm uses offset-based pagination (offset, limit)

Usage:
  python ingestion/jotform.py

  Optionally override form IDs via command-line arguments:
  python ingestion/jotform.py --standard-form-id 123456 --manual-opt-form-id 789012

Environment variables required (.env):
  JOTFORM_API_KEY            — JotForm API key
  JOTFORM_FORM_ID_STANDARD   — Form ID for standard PDA form
  JOTFORM_FORM_ID_MANUAL_OPT — Form ID for manual opt-in PDA form
  BQ_PROJECT                 — BigQuery project ID (make-it-conscious)
  BQ_DATASET_JOTFORM         — BigQuery dataset (raw_jotform)

Write disposition: replace — ensures deleted or edited submissions don't
persist as stale rows. Full snapshot on every run.
"""

import os
import argparse
import dlt
import requests
from typing import Iterator, Any
from dotenv import load_dotenv

load_dotenv()

JOTFORM_BASE_URL = "https://api.jotform.com"
PAGE_SIZE = 1000  # JotForm max per request


def jotform_paginate(
    api_key: str,
    form_id: str,
) -> Iterator[dict]:
    """
    Fetch all submissions for a given JotForm form ID.
    Uses offset pagination. Yields individual submission records.
    Includes all submissions regardless of status (active, deleted).
    """
    offset = 0

    while True:
        resp = requests.get(
            f"{JOTFORM_BASE_URL}/form/{form_id}/submissions",
            params={
                "apiKey": api_key,
                "limit": PAGE_SIZE,
                "offset": offset,
                "orderby": "created_at",
                "direction": "ASC",
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        if data.get("responseCode") != 200:
            raise RuntimeError(
                f"JotForm API error for form {form_id}: "
                f"{data.get('message', 'Unknown error')}"
            )

        submissions = data.get("content", [])

        if not submissions:
            break

        yield from submissions

        result_set = data.get("resultSet", {})
        total = int(result_set.get("count", 0))
        offset += PAGE_SIZE

        if offset >= total:
            break


def make_submissions_resource(
    name: str,
    form_id: str,
    api_key: str,
):
    """
    Factory function that returns a dlt resource for a given form.
    Using a factory avoids the closure variable capture issue in loops.
    """

    @dlt.resource(name=name, write_disposition="replace", primary_key="id")
    def _resource() -> Iterator[Any]:
        yield from jotform_paginate(api_key=api_key, form_id=form_id)

    return _resource


@dlt.source(name="jotform")
def jotform_source(
    api_key: str,
    standard_form_id: str,
    manual_opt_form_id: str,
):
    """All JotForm resources as a single dlt source."""
    return [
        make_submissions_resource(
            name="submissions_standard",
            form_id=standard_form_id,
            api_key=api_key,
        ),
        make_submissions_resource(
            name="submissions_manual_opt",
            form_id=manual_opt_form_id,
            api_key=api_key,
        ),
    ]


def parse_args():
    parser = argparse.ArgumentParser(description="JotForm ingestion pipeline")
    parser.add_argument(
        "--standard-form-id",
        default=None,
        help="Override JOTFORM_FORM_ID_STANDARD from .env",
    )
    parser.add_argument(
        "--manual-opt-form-id",
        default=None,
        help="Override JOTFORM_FORM_ID_MANUAL_OPT from .env",
    )
    return parser.parse_args()


def run_pipeline() -> None:
    args = parse_args()

    api_key = os.environ.get("JOTFORM_API_KEY")
    standard_form_id = args.standard_form_id or os.environ.get("JOTFORM_FORM_ID_STANDARD")
    manual_opt_form_id = args.manual_opt_form_id or os.environ.get("JOTFORM_FORM_ID_MANUAL_OPT")
    bq_project = os.environ.get("BQ_PROJECT", "make-it-conscious")
    bq_dataset = os.environ.get("BQ_DATASET_JOTFORM", "raw_jotform")

    if not api_key:
        raise EnvironmentError(
            "JOTFORM_API_KEY not set. Add it to your .env file.\n"
            "Find it in JotForm: Account Settings > API"
        )
    if not standard_form_id:
        raise EnvironmentError(
            "JOTFORM_FORM_ID_STANDARD not set. Add it to your .env file or "
            "pass --standard-form-id as a command-line argument."
        )
    if not manual_opt_form_id:
        raise EnvironmentError(
            "JOTFORM_FORM_ID_MANUAL_OPT not set. Add it to your .env file or "
            "pass --manual-opt-form-id as a command-line argument."
        )

    # Quick auth check
    resp = requests.get(
        f"{JOTFORM_BASE_URL}/user",
        params={"apiKey": api_key},
        timeout=10,
    )
    if resp.status_code == 401 or resp.json().get("responseCode") == 401:
        raise EnvironmentError(
            "JotForm API key rejected. Check JOTFORM_API_KEY in your .env file."
        )

    pipeline = dlt.pipeline(
        pipeline_name="jotform",
        destination=dlt.destinations.bigquery(project_id=bq_project),
        dataset_name=bq_dataset,
        progress="log",
    )

    print(f"Running JotForm pipeline → {bq_project}.{bq_dataset}")
    print(f"  Standard form ID:   {standard_form_id}")
    print(f"  Manual opt form ID: {manual_opt_form_id}")

    load_info = pipeline.run(
        jotform_source(
            api_key=api_key,
            standard_form_id=standard_form_id,
            manual_opt_form_id=manual_opt_form_id,
        )
    )

    print(load_info)
    print("\nJotForm ingestion complete.")
    print(f"Tables written to {bq_project}.{bq_dataset}:")
    print("  - submissions_standard")
    print("  - submissions_manual_opt")


if __name__ == "__main__":
    run_pipeline()