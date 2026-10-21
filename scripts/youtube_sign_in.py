"""Sign in to YouTube once and store the token in GCP Secret Manager.

Run from the repo root on a machine with a browser and gcloud signed in:

    python scripts/youtube_sign_in.py

Opens Google's consent page (choose the channel's account), confirms which
channel was granted, then adds the token as a new version of the
YOUTUBE_TOKEN secret that the daily workflow reads. The token is piped to
gcloud and never written to disk or printed.

The OAuth app must be "In production" (Google Auth Platform > Audience).
In "Testing", Google issues refresh tokens that expire after 7 days.
"""
import os
import subprocess
import sys

from dotenv import load_dotenv
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

load_dotenv()

SCOPES = [
    "https://www.googleapis.com/auth/yt-analytics.readonly",
    "https://www.googleapis.com/auth/yt-analytics-monetary.readonly",
    "https://www.googleapis.com/auth/youtube.readonly",
]
CLIENT_SECRETS_FILE = os.environ.get("OAUTH_CLIENT_FILE")
SECRET_PROJECT = os.environ.get("BQ_PROJECT", "make-it-conscious")
SECRET_NAME = "YOUTUBE_TOKEN"


def main():
    if not CLIENT_SECRETS_FILE or not os.path.exists(CLIENT_SECRETS_FILE):
        sys.exit("OAUTH_CLIENT_FILE is not set or the file is missing.")

    flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
    # prompt=consent makes Google return a fresh refresh token.
    creds = flow.run_local_server(port=0, prompt="consent", access_type="offline")
    if not creds.refresh_token:
        sys.exit("Google returned no refresh token; nothing stored.")

    youtube = build("youtube", "v3", credentials=creds)
    items = youtube.channels().list(part="snippet", mine=True).execute().get("items", [])
    if not items:
        sys.exit("This account has no YouTube channel; nothing stored.")
    title = items[0]["snippet"]["title"]
    print(f"Signed in as channel: {title}")
    expected = os.environ.get("EXPECTED_CHANNEL", "Make it Conscious")
    if title.strip().lower() != expected.strip().lower():
        sys.exit(f"Not the {expected!r} channel; nothing stored. Run again and pick that channel.")

    result = subprocess.run(
        ["gcloud", "secrets", "versions", "add", SECRET_NAME,
         f"--project={SECRET_PROJECT}", "--data-file=-"],
        input=creds.to_json().encode(),
        capture_output=True,
    )
    if result.returncode != 0:
        sys.exit("gcloud failed: " + result.stderr.decode().strip())
    print(result.stderr.decode().strip())


if __name__ == "__main__":
    main()
