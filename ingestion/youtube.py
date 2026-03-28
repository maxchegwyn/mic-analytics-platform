import os
import time
import json
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

EXERCISE_PLAYLIST_IDS = {
    "PLeuFwuWK5O2MMxqoPMkk9eEbVIrbwq-7B": "Active Imagination Exercises",
    "PLeuFwuWK5O2PWOC6QlGwXS2aWnaPTTF-8": "Core Practice",
    "PLeuFwuWK5O2M4fntROuoX5ee6QtGeq9Ff": "Symbolic Investigation",
    "PLeuFwuWK5O2PgSRjN-7rs30f9gDFrpfpn": "Personality Alchemy",
    "PLeuFwuWK5O2Nejd2AcnxFZF2Tf_pUAr9M": "Anima Animus",
    "PLeuFwuWK5O2Ob_hGnYVbpuu6AkaYPaPhd": "Archetypal Encounters",
    "PLeuFwuWK5O2O4ATRp4jCkWLTiaaFmZiqw": "Shadow Work",
    "PLeuFwuWK5O2Pt3ovWFgI8XknvbAn1Eqco": "Emotional Integration",
    "PLeuFwuWK5O2NmxQBrDJV0V21v_YHA7gyI": "Ten Day Meditation Series",
    "PLeuFwuWK5O2OvmSpHOqOPlcOQJUnHcZZa": "Beyond the Limen",
    "PLeuFwuWK5O2Nyf14v0Q6yDGnOslYoXcSo": "Zodiac Series",
    "PLeuFwuWK5O2MtO5925N7A2vqKQQT1J8Mc": "Dream Investigation",
    "PLeuFwuWK5O2N9BObUU3fg7q3GtcKNG0tS": "Applied Series",
    "PLeuFwuWK5O2OZ9bFhdbjBzMJBo1pAl61Q": "Tarot Series",
}

SCOPES = [
    "https://www.googleapis.com/auth/yt-analytics.readonly",
    "https://www.googleapis.com/auth/yt-analytics-monetary.readonly",
    "https://www.googleapis.com/auth/youtube.readonly",
]


def get_authenticated_service():
    creds = None
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, 'r') as f:
            token_data = json.loads(f.read().strip())
        creds = Credentials.from_authorized_user_info(token_data, SCOPES)
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
    with open(TOKEN_FILE, 'r') as f:
        token_data = json.loads(f.read().strip())
    creds = Credentials.from_authorized_user_info(token_data, SCOPES)
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

def fetch_video_metadata(video_ids):
    with open(TOKEN_FILE, 'r') as f:
        token_data = json.loads(f.read().strip())
    creds = Credentials.from_authorized_user_info(token_data, SCOPES)
    youtube = build("youtube", "v3", credentials=creds)
    metadata = []
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i+50]
        request = youtube.videos().list(
            part="snippet,contentDetails",
            id=",".join(batch)
        )
        response = request.execute()
        for item in response.get("items", []):
            metadata.append({
                "video_id": item["id"],
                "title": item["snippet"]["title"],
                "publish_date": item["snippet"]["publishedAt"][:10],
                "channel_title": item["snippet"]["channelTitle"],
                "description": item["snippet"]["description"][:500] if item["snippet"].get("description") else None,
                "duration_iso": item["contentDetails"]["duration"],
                "tags": ",".join(item["snippet"].get("tags", [])),
            })
    return metadata

def fetch_playlist_memberships():
    with open(TOKEN_FILE, 'r') as f:
        token_data = json.loads(f.read().strip())
    creds = Credentials.from_authorized_user_info(token_data, SCOPES)
    youtube = build("youtube", "v3", credentials=creds)

    memberships = []
    for playlist_id, playlist_title in EXERCISE_PLAYLIST_IDS.items():
        print(f"Fetching playlist: {playlist_title}")
        next_page_token = None
        while True:
            pl_request = youtube.playlistItems().list(
                part="contentDetails",
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token,
            )
            pl_response = pl_request.execute()
            for item in pl_response.get("items", []):
                memberships.append({
                    "video_id": item["contentDetails"]["videoId"],
                    "playlist_id": playlist_id,
                    "playlist_title": playlist_title,
                })
            next_page_token = pl_response.get("nextPageToken")
            if not next_page_token:
                break

    return memberships

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

@dlt.resource(
    name="raw_video_metadata",
    write_disposition="replace",
    primary_key="video_id",
)
def youtube_video_metadata():
    video_ids = get_channel_videos()
    print(f"Fetching metadata for {len(video_ids)} videos")
    yield from fetch_video_metadata(video_ids)

@dlt.resource(
    name="raw_video_playlists",
    write_disposition="replace",
    primary_key=["video_id", "playlist_id"],
)
def youtube_playlist_memberships():
    print("Fetching playlist memberships")
    yield from fetch_playlist_memberships()

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
        [
            youtube_daily_metrics(start_date=start_date, end_date=end_date),
            youtube_video_metadata(),
            youtube_playlist_memberships(),
        ]
    )
    print(load_info)


if __name__ == "__main__":
    main()