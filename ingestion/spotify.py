"""
Spotify ingestion pipeline for MiC Analytics Platform.

Loads to BigQuery: raw_spotify dataset
Resources:
  - episodes_mic         → all episodes from The Make it Conscious Podcast
  - episodes_inner_work  → all episodes from The MiC Inner Work Exercises Podcast

Auth: Spotify Client Credentials flow (no user login required)
      Uses Client ID + Client Secret to obtain a bearer token.
      Token is valid for 1 hour — refreshed automatically per run.

Pagination: Spotify uses offset-based pagination (offset, limit, max 50/page)

Usage:
  python ingestion/spotify.py

Environment variables required (.env):
  SPOTIFY_CLIENT_ID       — From Spotify Developer Dashboard
  SPOTIFY_CLIENT_SECRET   — From Spotify Developer Dashboard
  SPOTIFY_SHOW_ID_MIC     — Show ID for The Make it Conscious Podcast
  SPOTIFY_SHOW_ID_INNER_WORK — Show ID for The MiC Inner Work Exercises Podcast
  BQ_PROJECT              — BigQuery project ID (make-it-conscious)
  BQ_DATASET_SPOTIFY      — BigQuery dataset (raw_spotify)

Data pulled per episode:
  id, name, description, release_date, duration_ms, explicit,
  external_urls, languages, uri, type

Write disposition: replace — episode metadata can be updated (descriptions
edited, titles changed). Full snapshot ensures current state.
"""

import os
import dlt
import requests
from typing import Iterator, Any
from dotenv import load_dotenv

load_dotenv()

SPOTIFY_AUTH_URL = "https://accounts.spotify.com/api/token"
SPOTIFY_BASE_URL = "https://api.spotify.com/v1"
PAGE_SIZE = 50  # Spotify max per request for episodes


def get_access_token(client_id: str, client_secret: str) -> str:
    """
    Obtain a Spotify access token via Client Credentials flow.
    No user login required — suitable for public data access.
    Token is valid for 3600 seconds (1 hour).
    """
    resp = requests.post(
        SPOTIFY_AUTH_URL,
        data={"grant_type": "client_credentials"},
        auth=(client_id, client_secret),
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def spotify_episodes_paginate(
    access_token: str,
    show_id: str,
) -> Iterator[dict]:
    """
    Fetch all episodes for a given Spotify show ID.
    Uses offset pagination. Yields individual episode objects.
    """
    headers = {"Authorization": f"Bearer {access_token}"}
    offset = 0

    while True:
        resp = requests.get(
            f"{SPOTIFY_BASE_URL}/shows/{show_id}/episodes",
            headers=headers,
            params={
                "limit": PAGE_SIZE,
                "offset": offset,
                "market": "GB",  # Required — returns episodes available in UK
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        episodes = data.get("items", [])

        if not episodes:
            break

        for episode in episodes:
            # Add show_id to each episode for joining purposes
            episode["show_id"] = show_id
            yield episode

        total = data.get("total", 0)
        offset += PAGE_SIZE

        if offset >= total:
            break


def make_episodes_resource(name: str, show_id: str, access_token: str):
    """Factory function returning a dlt resource for a given show."""

    @dlt.resource(name=name, write_disposition="replace", primary_key="id")
    def _resource() -> Iterator[Any]:
        yield from spotify_episodes_paginate(
            access_token=access_token,
            show_id=show_id,
        )

    return _resource


@dlt.source(name="spotify")
def spotify_source(
    client_id: str,
    client_secret: str,
    show_id_mic: str,
    show_id_inner_work: str,
):
    """All Spotify resources as a single dlt source."""
    access_token = get_access_token(client_id, client_secret)

    return [
        make_episodes_resource(
            name="episodes_mic",
            show_id=show_id_mic,
            access_token=access_token,
        ),
        make_episodes_resource(
            name="episodes_inner_work",
            show_id=show_id_inner_work,
            access_token=access_token,
        ),
    ]


def run_pipeline() -> None:
    client_id = os.environ.get("SPOTIFY_CLIENT_ID")
    client_secret = os.environ.get("SPOTIFY_CLIENT_SECRET")
    show_id_mic = os.environ.get("SPOTIFY_SHOW_ID_MIC")
    show_id_inner_work = os.environ.get("SPOTIFY_SHOW_ID_INNER_WORK")
    bq_project = os.environ.get("BQ_PROJECT", "make-it-conscious")
    bq_dataset = os.environ.get("BQ_DATASET_SPOTIFY", "raw_spotify")

    if not all([client_id, client_secret]):
        raise EnvironmentError(
            "Missing Spotify credentials. Ensure these are set in .env:\n"
            "  SPOTIFY_CLIENT_ID\n"
            "  SPOTIFY_CLIENT_SECRET\n"
            "Find them at developer.spotify.com > Dashboard > your app"
        )
    if not show_id_mic:
        raise EnvironmentError(
            "SPOTIFY_SHOW_ID_MIC not set. Add it to your .env file.\n"
            "Find it in the Spotify show URL: open.spotify.com/show/XXXXXXX"
        )
    if not show_id_inner_work:
        raise EnvironmentError(
            "SPOTIFY_SHOW_ID_INNER_WORK not set. Add it to your .env file.\n"
            "Find it in the Spotify show URL: open.spotify.com/show/XXXXXXX"
        )

    # Quick auth check
    try:
        access_token = get_access_token(client_id, client_secret)
    except requests.HTTPError as e:
        raise EnvironmentError(
            f"Spotify authentication failed: {e}\n"
            "Check SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET in your .env file."
        )

    pipeline = dlt.pipeline(
        pipeline_name="spotify",
        destination=dlt.destinations.bigquery(project_id=bq_project),
        dataset_name=bq_dataset,
        progress="log",
    )

    print(f"Running Spotify pipeline → {bq_project}.{bq_dataset}")
    print(f"  MiC Podcast show ID:         {show_id_mic}")
    print(f"  Inner Work Exercises show ID: {show_id_inner_work}")

    load_info = pipeline.run(
        spotify_source(
            client_id=client_id,
            client_secret=client_secret,
            show_id_mic=show_id_mic,
            show_id_inner_work=show_id_inner_work,
        )
    )

    print(load_info)
    print("\nSpotify ingestion complete.")
    print(f"Tables written to {bq_project}.{bq_dataset}:")
    print("  - episodes_mic")
    print("  - episodes_inner_work")


if __name__ == "__main__":
    run_pipeline()