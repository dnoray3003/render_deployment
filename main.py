from datetime import datetime
from pathlib import Path
import os
import secrets
import subprocess
import tempfile

import boto3
from botocore.client import Config
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.responses import FileResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials

load_dotenv()

app = FastAPI()
security = HTTPBasic()
BASE_DIR = Path(__file__).resolve().parent

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
DEFAULT_CATEGORY = "uncategorized"

s3 = boto3.client(
    "s3",
    endpoint_url=WASABI_ENDPOINT,
    aws_access_key_id=WASABI_ACCESS_KEY,
    aws_secret_access_key=WASABI_SECRET_KEY,
    config=Config(signature_version="s3v4"),
)


def safe_object_name(filename: str) -> str:
    original = Path(filename).name
    stem = Path(original).stem.replace(" ", "-")
    suffix = Path(original).suffix.lower()
    timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    return f"{stem}-{timestamp}{suffix}"



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



def normalize_category(category: str | None) -> str:
    cleaned = (category or DEFAULT_CATEGORY).strip().lower()
    return cleaned or DEFAULT_CATEGORY



def thumbnail_key_from_video_key(video_key: str) -> str:
    return f"{THUMBNAIL_PREFIX}{Path(video_key).stem}.jpg"



def get_video_duration_seconds(video_source: str) -> int | None:
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
                video_source,
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



def get_video_object_metadata(video_key: str) -> dict:
    try:
        response = s3.head_object(Bucket=WASABI_BUCKET, Key=video_key)
        metadata = response.get("Metadata", {})

        duration_value = metadata.get("duration_seconds")
        try:
            duration_seconds = int(duration_value) if duration_value is not None else None
        except ValueError:
            duration_seconds = None

        return {
            "duration_seconds": duration_seconds,
            "category": normalize_category(metadata.get("category")),
        }
    except Exception:
        return {
            "duration_seconds": None,
            "category": DEFAULT_CATEGORY,
        }



def save_video_category_metadata(video_key: str, category: str) -> None:
    head = s3.head_object(Bucket=WASABI_BUCKET, Key=video_key)

    content_type = head.get("ContentType") or guess_content_type(video_key)
    existing_metadata = head.get("Metadata", {}).copy()
    existing_metadata["category"] = normalize_category(category)

    if "duration_seconds" not in existing_metadata:
        existing_metadata["duration_seconds"] = ""

    s3.copy_object(
        Bucket=WASABI_BUCKET,
        Key=video_key,
        CopySource={"Bucket": WASABI_BUCKET, "Key": video_key},
        Metadata=existing_metadata,
        MetadataDirective="REPLACE",
        ContentType=content_type,
    )



def save_video_duration_metadata(video_key: str, duration_seconds: int) -> None:
    head = s3.head_object(Bucket=WASABI_BUCKET, Key=video_key)

    content_type = head.get("ContentType") or guess_content_type(video_key)
    existing_metadata = head.get("Metadata", {}).copy()
    existing_metadata["duration_seconds"] = str(duration_seconds)

    if "category" not in existing_metadata:
        existing_metadata["category"] = DEFAULT_CATEGORY

    s3.copy_object(
        Bucket=WASABI_BUCKET,
        Key=video_key,
        CopySource={"Bucket": WASABI_BUCKET, "Key": video_key},
        Metadata=existing_metadata,
        MetadataDirective="REPLACE",
        ContentType=content_type,
    )



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



def guess_upload_content_type(filename: str, fallback: str | None = None) -> str:
    if fallback and fallback != "application/octet-stream":
        return fallback
    return guess_content_type(filename)



def list_all_objects(prefix: str = VIDEO_PREFIX) -> list[dict]:
    contents: list[dict] = []
    continuation_token: str | None = None

    while True:
        params = {"Bucket": WASABI_BUCKET, "Prefix": prefix}
        if continuation_token:
            params["ContinuationToken"] = continuation_token

        response = s3.list_objects_v2(**params)
        contents.extend(response.get("Contents", []))

        if not response.get("IsTruncated"):
            break

        continuation_token = response.get("NextContinuationToken")

    return contents


@app.get("/")
def serve_home(username: str = Depends(require_basic_auth)):
    return FileResponse(BASE_DIR / "index.html")


@app.post("/upload-url")
async def create_upload_url(
    file_name: str,
    content_type: str | None = None,
    category: str | None = None,
    username: str = Depends(require_basic_auth),
):
    try:
        object_name = safe_object_name(file_name)

        if not is_video_file(object_name):
            return {"error": "Unsupported file type"}

        upload_content_type = guess_upload_content_type(object_name, content_type)
        clean_category = normalize_category(category)

        upload_url = s3.generate_presigned_url(
            "put_object",
            Params={
                "Bucket": WASABI_BUCKET,
                "Key": object_name,
                "ContentType": upload_content_type,
                "Metadata": {"category": clean_category},
            },
            ExpiresIn=300,
        )

        return {
            "upload_url": upload_url,
            "object_name": object_name,
            "content_type": upload_content_type,
            "category": clean_category,
        }

    except Exception as e:
        return {"error": str(e)}


@app.get("/videos")
def list_videos(username: str = Depends(require_basic_auth)):
    try:
        contents = list_all_objects(VIDEO_PREFIX)
        existing_keys = {obj["Key"] for obj in contents if "Key" in obj}
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
            metadata_info = get_video_object_metadata(key)

            video_url = generate_presigned_object_url(key, guess_content_type(key))
            thumbnail_url = None
            if thumbnail_key in existing_keys:
                thumbnail_url = generate_presigned_object_url(thumbnail_key, "image/jpeg")

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
                    "duration_seconds": metadata_info["duration_seconds"],
                    "category": metadata_info["category"],
                }
            )

        videos.sort(key=lambda item: item["last_modified"] or "", reverse=True)
        return {"videos": videos}

    except Exception as e:
        return {"error": str(e)}


@app.post("/generate-thumbnail")
async def generate_thumbnail_for_uploaded_video(
    object_name: str,
    username: str = Depends(require_basic_auth),
):
    temp_video_path = None
    temp_thumb_path = None

    try:
        if not is_video_file(object_name):
            return {"error": "Unsupported file type"}

        thumb_key = thumbnail_key_from_video_key(object_name)

        with tempfile.NamedTemporaryFile(delete=False, suffix=Path(object_name).suffix) as temp_video:
            temp_video_path = temp_video.name

        with tempfile.NamedTemporaryFile(delete=False, suffix=".jpg") as temp_thumb:
            temp_thumb_path = temp_thumb.name

        s3.download_file(WASABI_BUCKET, object_name, temp_video_path)

        result = subprocess.run(
            [
                "ffmpeg",
                "-y",
                "-ss",
                "00:00:05",
                "-i",
                temp_video_path,
                "-frames:v",
                "1",
                "-q:v",
                "2",
                temp_thumb_path,
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )

        if result.returncode != 0:
            return {"error": f"Thumbnail generation failed: {result.stderr}"}

        duration_seconds = get_video_duration_seconds(temp_video_path)
        if duration_seconds is not None:
            save_video_duration_metadata(object_name, duration_seconds)

        s3.upload_file(
            temp_thumb_path,
            WASABI_BUCKET,
            thumb_key,
            ExtraArgs={"ContentType": "image/jpeg"},
        )

        return {
            "message": "Thumbnail generated successfully",
            "video": object_name,
            "thumbnail": thumb_key,
        }

    except Exception as e:
        return {"error": str(e)}

    finally:
        if temp_video_path and os.path.exists(temp_video_path):
            os.remove(temp_video_path)
        if temp_thumb_path and os.path.exists(temp_thumb_path):
            os.remove(temp_thumb_path)


@app.post("/update-category")
async def update_category(
    object_name: str,
    category: str,
    username: str = Depends(require_basic_auth),
):
    try:
        if not is_video_file(object_name):
            return {"error": "Unsupported file type"}

        clean_category = normalize_category(category)
        save_video_category_metadata(object_name, clean_category)

        return {
            "message": "Category updated successfully",
            "video": object_name,
            "category": clean_category,
        }

    except Exception as e:
        return {"error": str(e)}


@app.delete("/delete-video")
async def delete_video(
    object_name: str,
    username: str = Depends(require_basic_auth),
):
    try:
        if not is_video_file(object_name):
            return {"error": "Unsupported file type"}

        thumb_key = thumbnail_key_from_video_key(object_name)

        s3.delete_object(Bucket=WASABI_BUCKET, Key=object_name)
        s3.delete_object(Bucket=WASABI_BUCKET, Key=thumb_key)

        return {
            "message": "Video deleted successfully",
            "video": object_name,
            "thumbnail": thumb_key,
        }

    except Exception as e:
        return {"error": str(e)}
