import os
import time
import dlt
from datetime import datetime, timedelta, timezone
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from dotenv import load_dotenv

load_dotenv()

CLIENT_SECRETS_FILE = os.environ.get("OAUTH_CLIENT_FILE")
TOKEN_FILE = os.environ.get("TOKEN_FILE")
BQ_PROJECT = os.environ.get("BQ_PROJECT")
BQ_DATASET = os.environ.get("BQ_DATASET")
YOUTUBE_START_DATE = os.environ.get("YOUTUBE_START_DATE", "2013-01-01")
YOUTUBE_MODE = os.environ.get("YOUTUBE_MODE", "incremental")

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
                metrics="views,estimatedMinutesWatched,subscribersGained,estimatedRevenue,averageViewDuration,averageViewPercentage,likes,comments,shares",
                dimensions="day,video",
                filters=f"video=={video_id}",
            ).execute()
            rows = []
            for row in response.get("rows", []):
                rows.append({
                    "date": row[0],
                    "video_id": row[1],
                    "views": int(row[2]),
                    "estimated_minutes_watched": int(row[3]),
                    "subscribers_gained": int(row[4]),
                    "estimated_revenue": float(row[5]),
                    "average_view_duration": float(row[6]),
                    "average_view_percentage": float(row[7]),
                    "likes": int(row[8]),
                    "comments": int(row[9]),
                    "shares": int(row[10]),
                })
            return rows
        except Exception:
            print(f"Attempt {attempt+1} failed for {video_id}")
            if attempt < retries - 1:
                time.sleep(5)
    print(f"Skipping {video_id} after {retries} attempts")
    return []


@dlt.resource(
    name="raw_video_daily_metrics",
    write_disposition="append",
    primary_key=["date", "video_id"],
)
def youtube_daily_metrics(start_date: str, end_date: str):
    service = get_authenticated_service()
    video_ids = get_channel_videos()
    print(f"Found {len(video_ids)} videos")
    for i, video_id in enumerate(video_ids):
        print(f"Fetching {i+1}/{len(video_ids)}: {video_id}")
        rows = fetch_daily_metrics(service, video_id, start_date, end_date)
        yield from rows


def main():
    required = [CLIENT_SECRETS_FILE, TOKEN_FILE, BQ_PROJECT, BQ_DATASET]
    if not all(required):
        print("Error: missing required environment variables. Check your .env file.")
        return

    if YOUTUBE_MODE == "incremental":
        start_date = (datetime.now(timezone.utc) - timedelta(days=5)).strftime("%Y-%m-%d")
        print(f"Incremental mode — pulling from {start_date}")
    else:
        start_date = YOUTUBE_START_DATE
        print(f"Historical mode — pulling from {start_date}")

    end_date = (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%d")

    pipeline = dlt.pipeline(
        pipeline_name="mic_youtube",
        destination=dlt.destinations.bigquery(
            project_id=BQ_PROJECT,
        ),
        dataset_name=BQ_DATASET,
    )

    load_info = pipeline.run(
        youtube_daily_metrics(start_date=start_date, end_date=end_date)
    )
    print(load_info)


if __name__ == "__main__":
    main()