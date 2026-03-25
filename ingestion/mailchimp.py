"""
Mailchimp ingestion pipeline for MiC Analytics Platform.

Loads to BigQuery: raw_mailchimp dataset
Resources:
  - members        → all subscribers including unsubscribed and archived
  - campaigns      → all campaigns (sent, draft, etc.)
  - email_activity → per-member open/click activity per campaign (transformer)

Auth: Mailchimp API key (stored in .env as MAILCHIMP_API_KEY)
Data centre: derived from API key suffix (e.g. "us21" from "abc123-us21")

Usage:
  python ingestion/mailchimp.py

Environment variables required (.env):
  MAILCHIMP_API_KEY    — Mailchimp API key (format: <key>-<dc>, e.g. abc123-us21)
  BQ_PROJECT           — BigQuery project ID (make-it-conscious)
  BQ_DATASET_MAILCHIMP — BigQuery dataset for Mailchimp raw data (raw_mailchimp)

Note: email_activity pulls one API call per sent campaign. For accounts with
many campaigns this can be slow — dlt handles pagination automatically.
"""

import os
import dlt
from dlt.sources.rest_api import rest_api_source
from dotenv import load_dotenv

load_dotenv()


def get_data_centre(api_key: str) -> str:
    """Extract data centre from Mailchimp API key suffix (e.g. 'us21')."""
    parts = api_key.split("-")
    if len(parts) < 2:
        raise ValueError(
            "MAILCHIMP_API_KEY must include the data centre suffix "
            "(e.g. 'abc123-us21'). Check your Mailchimp account."
        )
    return parts[-1]


def build_mailchimp_source(api_key: str, list_id: str) -> dlt.sources.DltSource:
    """
    Build a dlt rest_api source for Mailchimp.

    Endpoints:
      /lists/{list_id}/members  — all contacts regardless of status
      /campaigns                — all campaigns
    """
    dc = get_data_centre(api_key)
    base_url = f"https://{dc}.api.mailchimp.com/3.0/"

    return rest_api_source(
        {
            "client": {
                "base_url": base_url,
                "auth": {
                    "type": "http_basic",
                    # Mailchimp basic auth: any username, API key as password
                    "username": "anystring",
                    "password": api_key,
                },
                "paginator": {
                    # Mailchimp uses offset pagination with a 'total_items' field
                    "type": "offset",
                    "limit": 1000,
                    "limit_param": "count",
                    "offset_param": "offset",
                    "total_path": "total_items",
                },
            },
            "resource_defaults": {
                "write_disposition": "replace",
            },
            "resources": [
                {
                    # All contacts — status filter omitted to include
                    # subscribed, unsubscribed, cleaned, and pending.
                    "name": "members",
                    "endpoint": {
                        "path": f"lists/{list_id}/members",
                        "params": {
                            # No status param = all statuses returned
                            "fields": (
                                "members.id,"
                                "members.email_address,"
                                "members.status,"
                                "members.timestamp_opt,"
                                "members.timestamp_signup,"
                                "members.last_changed,"
                                "members.tags,"
                                "members.merge_fields,"
                                "members.stats,"
                                "total_items"
                            ),
                        },
                        "data_selector": "members",
                    },
                    "primary_key": "id",
                },
                {
                    # All campaigns regardless of status
                    "name": "campaigns",
                    "endpoint": {
                        "path": "campaigns",
                        "params": {
                            "fields": (
                                "campaigns.id,"
                                "campaigns.type,"
                                "campaigns.status,"
                                "campaigns.settings.subject_line,"
                                "campaigns.settings.title,"
                                "campaigns.create_time,"
                                "campaigns.send_time,"
                                "campaigns.emails_sent,"
                                "campaigns.report_summary,"
                                "total_items"
                            ),
                        },
                        "data_selector": "campaigns",
                    },
                    "primary_key": "id",
                },
            ],
        },
        name="mailchimp",
    )


@dlt.resource(
    name="email_activity",
    write_disposition="replace",
    primary_key=["campaign_id", "email_id"],
)
def email_activity_resource(api_key: str):
    """
    Fetch per-member email activity (opens, clicks, bounces) for every
    sent campaign.

    This is a separate resource rather than a rest_api transformer because
    Mailchimp's email-activity endpoint uses cursor-based pagination
    (since_send_time token) rather than offset pagination, which conflicts
    with the global paginator on the rest_api source.

    Yields one record per member per campaign — same grain as
    fct_mailchimp_activity will expect.
    """
    import requests
    from requests.auth import HTTPBasicAuth

    dc = get_data_centre(api_key)
    base_url = f"https://{dc}.api.mailchimp.com/3.0"
    auth = HTTPBasicAuth("anystring", api_key)

    # Fetch sent campaign IDs
    response = requests.get(
        f"{base_url}/campaigns",
        auth=auth,
        params={"status": "sent", "count": 1000, "fields": "campaigns.id,total_items"},
        timeout=30,
    )
    response.raise_for_status()
    campaigns = response.json().get("campaigns", [])

    for campaign in campaigns:
        campaign_id = campaign["id"]
        offset = 0
        page_size = 1000

        while True:
            resp = requests.get(
                f"{base_url}/reports/{campaign_id}/email-activity",
                auth=auth,
                params={
                    "count": page_size,
                    "offset": offset,
                    "fields": (
                        "emails.campaign_id,"
                        "emails.email_id,"
                        "emails.email_address,"
                        "emails.activity,"
                        "total_items"
                    ),
                },
                timeout=30,
            )

            if resp.status_code == 404:
                # Some sent campaigns have no activity report yet — skip silently
                break

            resp.raise_for_status()
            data = resp.json()
            emails = data.get("emails", [])

            for record in emails:
                # Flatten activity list into separate rows? No — keep as nested
                # array. dlt will expand to a child table automatically.
                yield record

            total = data.get("total_items", 0)
            offset += page_size
            if offset >= total:
                break


def run_pipeline(mode: str = "full") -> None:
    """
    Run the Mailchimp ingestion pipeline.

    mode: 'full' — replace all tables (default, safe to re-run)
    """
    api_key = os.environ.get("MAILCHIMP_API_KEY")
    list_id = os.environ.get("MAILCHIMP_LIST_ID")
    bq_project = os.environ.get("BQ_PROJECT", "make-it-conscious")
    bq_dataset = os.environ.get("BQ_DATASET_MAILCHIMP", "raw_mailchimp")

    if not api_key:
        raise EnvironmentError(
            "MAILCHIMP_API_KEY not set. Add it to your .env file.\n"
            "Format: <key>-<datacenter>, e.g. abc123def456-us21\n"
            "Find it in Mailchimp: Account > Extras > API keys"
        )
    if not list_id:
        raise EnvironmentError(
            "MAILCHIMP_LIST_ID not set. Add it to your .env file.\n"
            "Find it in Mailchimp: Audience > Manage Audience > Settings > "
            "Audience name and defaults > Audience ID"
        )

    pipeline = dlt.pipeline(
        pipeline_name="mailchimp",
        destination=dlt.destinations.bigquery(project_id=bq_project),
        dataset_name=bq_dataset,
        progress="log",
    )

    # Run members and campaigns via rest_api source
    mc_source = build_mailchimp_source(api_key, list_id)

    # Run email_activity as a separate resource
    activity = email_activity_resource(api_key=api_key)

    print(f"Running Mailchimp pipeline → {bq_project}.{bq_dataset}")
    print(f"  Data centre: {get_data_centre(api_key)}")
    print(f"  List ID: {list_id}")

    load_info = pipeline.run([mc_source, activity])

    print(load_info)
    print("\nMailchimp ingestion complete.")
    print(f"Tables written to {bq_project}.{bq_dataset}:")
    print("  - members")
    print("  - campaigns")
    print("  - email_activity (+ email_activity__activity child table)")


if __name__ == "__main__":
    run_pipeline()