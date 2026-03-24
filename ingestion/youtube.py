import os
import time
from pathlib import Path
from datetime import datetime, timedelta
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from google.cloud import bigquery
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# --- CONFIG ---
CLIENT_SECRETS_FILE = os.environ.get("OAUTH_CLIENT_FILE")
TOKEN_FILE = os.environ.get("TOKEN_FILE")
BQ_PROJECT = os.environ.get("BQ_PROJECT")
BQ_DATASET = os.environ.get("BQ_DATASET")
BQ_TABLE = os.environ.get("BQ_TABLE")
YOUTUBE_START_DATE = os.environ.get("YOUTUBE_START_DATE", "2020-01-01")

SCOPES = [
    "https://www.googleapis.com/auth/yt-analytics.readonly",
    "https://www.googleapis.com/auth/yt-analytics-monetary.readonly",
    "https://www.googleapis.com/auth/youtube.readonly",
]


def get_authenticated_service():
    creds = None
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_FILE, "w") as token:
            token.write(creds.to_json())
    return build("youtubeAnalytics", "v2", credentials=creds)


def get_channel_videos():
    creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    youtube = build("youtube", "v3", credentials=creds)
    videos = []
    request = youtube.channels().list(part="contentDetails", mine=True)
    response = request.execute()
    uploads_playlist = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    next_page_token = None
    while True:
        pl_request = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=uploads_playlist,
            maxResults=50,
            pageToken=next_page_token,
        )
        pl_response = pl_request.execute()
        for item in pl_response["items"]:
            videos.append(item["contentDetails"]["videoId"])
        next_page_token = pl_response.get("nextPageToken")
        if not next_page_token:
            break
    return videos


def fetch_daily_metrics(service, video_id, start_date, end_date, retries=3):
    for attempt in range(retries):
        try:
            response = service.reports().query(
                ids="channel==MINE",
                startDate=start_date,
                endDate=end_date,
                metrics="views,estimatedMinutesWatched,subscribersGained,estimatedRevenue",
                dimensions="day,video",
                filters=f"video=={video_id}",
            ).execute()
            rows = []
            for row in response.get("rows", []):
                rows.append({
                    "date": row[0],
                    "video_id": row[1],
                    "views": row[2],
                    "estimated_minutes_watched": row[3],
                    "subscribers_gained": row[4],
                    "estimated_revenue": row[5],
                    "ingested_at": datetime.utcnow().isoformat(),
                })
            return rows
        except Exception:
            print(f"Attempt {attempt+1} failed for {video_id}")
            if attempt < retries - 1:
                time.sleep(5)
    print(f"Skipping {video_id} after {retries} attempts")
    return []


def load_to_bigquery(rows):
    if not rows:
        print("No rows to load.")
        return
    client = bigquery.Client(project=BQ_PROJECT)
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    df = pd.DataFrame(rows)
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"Loaded {len(rows)} rows to {table_id}")


def main():
    required = [CLIENT_SECRETS_FILE, TOKEN_FILE, BQ_PROJECT, BQ_DATASET, BQ_TABLE]
    if not all(required):
        print("Error: missing required environment variables. Check your .env file.")
        return
    print("Authenticating...")
    service = get_authenticated_service()
    print("Fetching video list...")
    video_ids = get_channel_videos()
    print(f"Found {len(video_ids)} videos")
    end_date = (datetime.utcnow() - timedelta(days=2)).strftime("%Y-%m-%d")
    all_rows = []
    for i, video_id in enumerate(video_ids):
        print(f"Fetching {i+1}/{len(video_ids)}: {video_id}")
        rows = fetch_daily_metrics(service, video_id, YOUTUBE_START_DATE, end_date)
        all_rows.extend(rows)
    print(f"Total rows: {len(all_rows)}")
    load_to_bigquery(all_rows)


if __name__ == "__main__":
    main()