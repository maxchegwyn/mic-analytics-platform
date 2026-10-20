"""Write docs/data-freshness.md: when each raw dataset was last loaded.

Reads table metadata only (no queries, so no BigQuery query cost). The daily
workflow commits the file once a month, which also counts as repository
activity and keeps GitHub from disabling the schedule after 60 idle days.
"""
import os
from datetime import datetime, timezone

from google.cloud import bigquery

BQ_PROJECT = os.environ.get("BQ_PROJECT", "make-it-conscious")
DATASETS = [
    ("YouTube", "raw_youtube"),
    ("Mailchimp", "raw_mailchimp"),
    ("Stripe", "raw_stripe"),
    ("WooCommerce", "raw_woocommerce"),
    ("JotForm", "raw_jotform"),
    ("WordPress", "raw_wordpress"),
    ("Spotify", "raw_spotify"),
    ("dbt marts", "dbt_prod"),
]
OUT = os.path.join(os.path.dirname(__file__), "..", "docs", "data-freshness.md")


def last_modified(client, dataset):
    latest = None
    for item in client.list_tables(f"{BQ_PROJECT}.{dataset}"):
        if item.table_id.startswith("_dlt"):
            continue
        modified = client.get_table(item.reference).modified
        if latest is None or modified > latest:
            latest = modified
    return latest


def main():
    client = bigquery.Client(project=BQ_PROJECT)
    now = datetime.now(timezone.utc)
    lines = [
        "# Data freshness",
        "",
        f"Last load per source, from BigQuery table metadata. Updated {now:%Y-%m-%d %H:%M} UTC.",
        "",
        "| Source | Dataset | Last loaded (UTC) |",
        "|---|---|---|",
    ]
    for label, dataset in DATASETS:
        try:
            ts = last_modified(client, dataset)
            when = f"{ts:%Y-%m-%d %H:%M}" if ts else "no tables"
        except Exception as e:
            when = f"unreadable ({type(e).__name__})"
        lines.append(f"| {label} | `{dataset}` | {when} |")
        print(f"{label}: {when}")
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w") as f:
        f.write("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
