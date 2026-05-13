"""
generate_synthetic_data.py
MiC Analytics Platform — Synthetic Data Generator

Reads from make-it-conscious.dbt_prod, applies transformations, and writes
to make-it-conscious.dbt_synthetic. The public Power BI report connects to
dbt_synthetic. The dbt models in this repo are identical for both datasets.

Transformations applied:
  - fct_revenue:   `amount` scaled by SYNTHETIC_SCALE_FACTOR
  - dim_customers: `total_wc_spend` and `total_stripe_spend` scaled by the same factor
  - All other tables copied unchanged — they contain no sensitive absolute figures

Email addresses are anonymised in fct_revenue and dim_customers using a
deterministic MD5 hash. The same real email always maps to the same synthetic
email, so join relationships between tables are fully preserved.

All ratios, conversion rates, type distributions, and percentage splits are
real — only absolute monetary figures are scaled.

Prerequisites:
  - Python 3.9+
  - pip install google-cloud-bigquery pandas pyarrow --break-system-packages
  - Application Default Credentials:
      gcloud auth application-default login
  - SYNTHETIC_SCALE_FACTOR set as an environment variable:
      export SYNTHETIC_SCALE_FACTOR=<your value>
      python scripts/generate_synthetic_data.py

The scale factor is intentionally not stored in this file or any committed
file. Set it in your terminal session only.

Author: Max Chegwyn
"""

import hashlib
import os

import pandas as pd
from google.cloud import bigquery

# ── Configuration ──────────────────────────────────────────────────────────────

PROJECT        = "make-it-conscious"
SOURCE_DATASET = "dbt_prod"
TARGET_DATASET = "dbt_synthetic"

# Read multiplier from environment — never hardcode this value
_raw = os.environ.get("SYNTHETIC_SCALE_FACTOR")
if not _raw:
    raise EnvironmentError(
        "SYNTHETIC_SCALE_FACTOR environment variable is not set.\n"
        "Set it in your terminal before running:\n"
        "  export SYNTHETIC_SCALE_FACTOR=<your value>"
    )
SCALE_FACTOR = float(_raw)

client = bigquery.Client(project=PROJECT)

# ── Helpers ────────────────────────────────────────────────────────────────────

def synthetic_email(real_email: str) -> str:
    """
    Deterministic anonymisation via MD5 hash. The same real email always
    produces the same synthetic email, preserving join relationships across
    fct_revenue and dim_customers.
    """
    h = hashlib.md5(real_email.encode()).hexdigest()[:10]
    return f"user_{h}@example.com"


def read_table(table_name: str) -> pd.DataFrame:
    query = f"SELECT * FROM `{PROJECT}.{SOURCE_DATASET}.{table_name}`"
    print(f"  Reading {table_name}...")
    return client.query(query).to_dataframe()


def write_table(df: pd.DataFrame, table_name: str) -> None:
    table_ref = f"{PROJECT}.{TARGET_DATASET}.{table_name}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print(f"  Written {table_name}: {len(df):,} rows.")


def copy_unchanged(table_name: str) -> None:
    """Copy a table that contains no sensitive figures."""
    query = f"""
        CREATE OR REPLACE TABLE `{PROJECT}.{TARGET_DATASET}.{table_name}`
        AS SELECT * FROM `{PROJECT}.{SOURCE_DATASET}.{table_name}`
    """
    client.query(query).result()
    print(f"  Copied {table_name} unchanged.")


# ── Table transformations ──────────────────────────────────────────────────────

def transform_fct_revenue() -> None:
    """
    Scale the `amount` field by SCALE_FACTOR and anonymise emails.
    Row count and all other fields are unchanged, so platform mix,
    transaction counts, and all derived rates remain accurate.
    YouTube AdSense rows have a null email — handled gracefully below.
    """
    df = read_table("fct_revenue")

    df["amount"] = (df["amount"] * SCALE_FACTOR).round(2)

    df["email"] = df["email"].apply(
        lambda x: synthetic_email(x) if pd.notna(x) else x
    )

    write_table(df, "fct_revenue")


def transform_dim_customers() -> None:
    """
    Scale `total_wc_spend` and `total_stripe_spend` by SCALE_FACTOR
    and anonymise emails. All other fields — personality type, subscription
    dates, open rates, click rates, submission counts — are unchanged.
    """
    df = read_table("dim_customers")

    for col in ["total_wc_spend", "total_stripe_spend"]:
        if col in df.columns:
            df[col] = (df[col] * SCALE_FACTOR).round(2)

    df["email"] = df["email"].apply(
        lambda x: synthetic_email(x) if pd.notna(x) else x
    )

    write_table(df, "dim_customers")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    # Ensure target dataset exists
    dataset = bigquery.Dataset(f"{PROJECT}.{TARGET_DATASET}")
    dataset.location = "US"
    client.create_dataset(dataset, exists_ok=True)
    print(f"Target dataset ready: {PROJECT}.{TARGET_DATASET}\n")

    # Tables with no sensitive figures — copy unchanged
    print("Copying unchanged tables...")
    for table in [
        "dim_videos",
        "dim_blog_posts",
        "dim_campaigns",
        "fct_pda_takers",
        "fct_youtube_performance",
        "fct_content_calendar",
        "fct_blog_performance",
    ]:
        try:
            copy_unchanged(table)
        except Exception as e:
            print(f"  Skipped {table} (not yet built or error): {e}")

    print()

    # Tables requiring transformation
    print("Transforming sensitive tables...")
    transform_fct_revenue()
    transform_dim_customers()

    print("\nDone. Point Power BI at make-it-conscious.dbt_synthetic.")


if __name__ == "__main__":
    main()
