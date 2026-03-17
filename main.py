from fastapi import FastAPI, Depends, HTTPException, status, UploadFile, File
from fastapi.responses import FileResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import boto3
from botocore.client import Config
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime
import os
import subprocess
import secrets
import tempfile

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

def get_video_duration_metadata(video_key: str) -> int | None:
    try:
        response = s3.head_object(Bucket=WASABI_BUCKET, Key=video_key)
        metadata = response.get("Metadata", {})
        duration_value = metadata.get("duration_seconds")

        if duration_value is None:
            return None

        return int(duration_value)
    except Exception:
        return None
    
def save_video_duration_metadata(video_key: str, duration_seconds: int) -> None:
    head = s3.head_object(Bucket=WASABI_BUCKET, Key=video_key)

    content_type = head.get("ContentType") or guess_content_type(video_key)
    existing_metadata = head.get("Metadata", {}).copy()
    existing_metadata["duration_seconds"] = str(duration_seconds)

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

def guess_upload_content_type(filename: str, fallback: str | None = None) -> str:
    if fallback and fallback != "application/octet-stream":
        return fallback
    return guess_content_type(filename)


@app.get("/")
def serve_home(username: str = Depends(require_basic_auth)):
    return FileResponse("index.html")

@app.post("/upload")
async def upload_video(
    file: UploadFile = File(...),
    username: str = Depends(require_basic_auth),
):
    temp_video_path = None

    try:
        if not file.filename:
            return {"error": "No file selected"}

        object_name = safe_object_name(file.filename)

        if not is_video_file(object_name):
            return {"error": "Unsupported file type"}

        # Save uploaded file to a temp location in chunks
        with tempfile.NamedTemporaryFile(delete=False, suffix=Path(object_name).suffix) as temp_video:
            temp_video_path = temp_video.name

            while True:
                chunk = await file.read(1024 * 1024)  # 1 MB chunks
                if not chunk:
                    break
                temp_video.write(chunk)

        # Upload the temp file to Wasabi
        s3.upload_file(
            temp_video_path,
            WASABI_BUCKET,
            object_name,
            ExtraArgs={
                "ContentType": file.content_type or guess_content_type(object_name)
            },
        )

        # Generate thumbnail
        thumb_key = thumbnail_key_from_video_key(object_name)

        with tempfile.NamedTemporaryFile(delete=False, suffix=".jpg") as temp_thumb:
            temp_thumb_path = temp_thumb.name

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
        )

        if result.returncode == 0 and os.path.exists(temp_thumb_path):
            s3.upload_file(
                temp_thumb_path,
                WASABI_BUCKET,
                thumb_key,
                ExtraArgs={"ContentType": "image/jpeg"},
            )

        # Clean up thumbnail temp file
        if os.path.exists(temp_thumb_path):
            os.remove(temp_thumb_path)

        return {
            "message": "Upload successful",
            "video": object_name,
            "thumbnail": thumb_key,
        }

    except Exception as e:
        return {"error": str(e)}

    finally:
        await file.close()

        if temp_video_path and os.path.exists(temp_video_path):
            os.remove(temp_video_path)

@app.post("/upload-url")
async def create_upload_url(
    file_name: str,
    content_type: str | None = None,
    username: str = Depends(require_basic_auth),
):
    try:
        object_name = safe_object_name(file_name)

        if not is_video_file(object_name):
            return {"error": "Unsupported file type"}

        upload_content_type = guess_upload_content_type(object_name, content_type)

        upload_url = s3.generate_presigned_url(
            "put_object",
            Params={
                "Bucket": WASABI_BUCKET,
                "Key": object_name,
                "ContentType": upload_content_type,
            },
            ExpiresIn=300,
        )

        return {
            "upload_url": upload_url,
            "object_name": object_name,
            "content_type": upload_content_type,
        }

    except Exception as e:
        return {"error": str(e)}

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

            duration_seconds = get_video_duration_metadata(key)

            if duration_seconds is None:
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

    finally:
        if temp_video_path and os.path.exists(temp_video_path):
            os.remove(temp_video_path)
        if temp_thumb_path and os.path.exists(temp_thumb_path):
            os.remove(temp_thumb_path)