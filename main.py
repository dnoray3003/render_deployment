from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.responses import FileResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import boto3
from botocore.client import Config
from dotenv import load_dotenv
from pathlib import Path
import os
import subprocess
import secrets

load_dotenv()

app = FastAPI()

security = HTTPBasic()

APP_USERNAME = os.getenv("APP_USERNAME")
APP_PASSWORD = os.getenv("APP_PASSWORD")

WASABI_ENDPOINT = os.getenv("WASABI_ENDPOINT")
WASABI_ACCESS_KEY = os.getenv("WASABI_ACCESS_KEY")
WASABI_SECRET_KEY = os.getenv("WASABI_SECRET_KEY")
WASABI_BUCKET = os.getenv("WASABI_BUCKET")

VIDEO_PREFIX = ""
THUMBNAIL_PREFIX = "thumbnails/"
URL_EXPIRY = 300  # seconds

VIDEO_EXTENSIONS = (".mp4", ".mkv", ".mov", ".webm")

s3 = boto3.client(
    "s3",
    endpoint_url=WASABI_ENDPOINT,
    aws_access_key_id=WASABI_ACCESS_KEY,
    aws_secret_access_key=WASABI_SECRET_KEY,
    config=Config(signature_version="s3v4"),
)


def guess_content_type(filename: str) -> str:
    name = filename.lower()
    if name.endswith(".mp4"):
        return "video/mp4"
    if name.endswith(".webm"):
        return "video/webm"
    if name.endswith(".mov"):
        return "video/quicktime"
    if name.endswith(".mkv"):
        return "video/x-matroska"
    if name.endswith(".jpg") or name.endswith(".jpeg"):
        return "image/jpeg"
    return "application/octet-stream"


def is_video_file(key: str) -> bool:
    return key.lower().endswith(VIDEO_EXTENSIONS)


def thumbnail_key_from_video_key(video_key: str) -> str:
    return f"{THUMBNAIL_PREFIX}{Path(video_key).stem}.jpg"


def get_video_duration_seconds(video_url: str) -> float | None:
    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                video_url,
            ],
            capture_output=True,
            text=True,
            timeout=20,
        )

        if result.returncode != 0:
            return None

        output = result.stdout.strip()
        if not output:
            return None

        return round(float(output))
    except Exception:
        return None


def generate_presigned_object_url(object_key: str, content_type: str) -> str:
    return s3.generate_presigned_url(
        "get_object",
        Params={
            "Bucket": WASABI_BUCKET,
            "Key": object_key,
            "ResponseContentType": content_type,
        },
        ExpiresIn=URL_EXPIRY,
    )


def friendly_title_from_key(key: str) -> str:
    filename = Path(key).stem
    return filename.replace("-", " ").replace("_", " ").strip().title()


def format_bytes(size: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(size)

    for unit in units:
        if value < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.1f} {unit}"
        value /= 1024


def require_basic_auth(credentials: HTTPBasicCredentials = Depends(security)):
    username_ok = secrets.compare_digest(credentials.username, APP_USERNAME or "")
    password_ok = secrets.compare_digest(credentials.password, APP_PASSWORD or "")

    if not (username_ok and password_ok):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username or password",
            headers={"WWW-Authenticate": "Basic"},
        )

    return credentials.username


@app.get("/")
def serve_home(username: str = Depends(require_basic_auth)):
    return FileResponse("index.html")


@app.get("/videos")
def list_videos(username: str = Depends(require_basic_auth)):
    try:
        response = s3.list_objects_v2(Bucket=WASABI_BUCKET, Prefix=VIDEO_PREFIX)
        contents = response.get("Contents", [])

        videos = []

        for obj in contents:
            key = obj["Key"]

            if key == VIDEO_PREFIX or key.endswith("/"):
                continue

            if key.startswith(THUMBNAIL_PREFIX):
                continue

            if not is_video_file(key):
                continue

            thumbnail_key = thumbnail_key_from_video_key(key)

            video_url = generate_presigned_object_url(
                key,
                guess_content_type(key),
            )

            thumbnail_url = generate_presigned_object_url(
                thumbnail_key,
                "image/jpeg",
            )

            duration_seconds = get_video_duration_seconds(video_url)

            videos.append(
                {
                    "video": key,
                    "title": friendly_title_from_key(key),
                    "thumbnail": thumbnail_key,
                    "video_url": video_url,
                    "thumbnail_url": thumbnail_url,
                    "content_type": guess_content_type(key),
                    "size_bytes": obj.get("Size", 0),
                    "last_modified": obj.get("LastModified").isoformat() if obj.get("LastModified") else None,
                    "duration_seconds": duration_seconds,
                }
            )

        return {"videos": videos}

    except Exception as e:
        return {"error": str(e)}