import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlsplit


@dataclass(frozen=True)
class Settings:
    source_url: str
    storage_mode: str
    bucket_name: str
    table_name: str
    s3_prefix: str
    max_pages: int
    http_timeout_seconds: int
    log_level: str
    s3_sse_mode: str
    s3_kms_key_id: str


def parse_iso_timestamp(value: str | None) -> datetime:
    if value:
        try:
            normalized = value.replace("Z", "+00:00")
            parsed = datetime.fromisoformat(normalized)
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except ValueError:
            pass
    return datetime.now(timezone.utc)


def get_execution_identity(event: dict[str, Any], context: Any) -> tuple[str, str]:
    execution_uuid = (
        str(event.get("id"))
        if event.get("id")
        else getattr(context, "aws_request_id", None)
        or str(uuid.uuid4())
    )
    execution_timestamp = parse_iso_timestamp(event.get("time")).strftime("%Y-%m-%dT%H:%M:%SZ")
    return execution_uuid, execution_timestamp


def get_int_env(name: str, default: int) -> int:
    raw = os.getenv(name, str(default))
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"Environment variable {name} must be an integer") from exc


def validate_source_url(source_url: str) -> str:
    parsed = urlsplit(source_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("SOURCE_URL must be a valid HTTP or HTTPS URL")
    return source_url


def get_settings() -> Settings:
    source_url = validate_source_url(os.getenv("SOURCE_URL", "http://books.toscrape.com/"))
    storage_mode = os.getenv("STORAGE_MODE", "BOTH").strip().lower()
    if storage_mode not in {"s3", "dynamodb", "both"}:
        raise ValueError("STORAGE_MODE must be one of: s3, dynamodb, both")

    s3_sse_mode = os.getenv("S3_SSE_MODE", "AES256").strip()
    if s3_sse_mode not in {"AES256", "aws:kms"}:
        raise ValueError("S3_SSE_MODE must be one of: AES256, aws:kms")

    return Settings(
        source_url=source_url,
        storage_mode=storage_mode,
        bucket_name=os.getenv("BUCKET_NAME", ""),
        table_name=os.getenv("TABLE_NAME", ""),
        s3_prefix=os.getenv("S3_PREFIX", "raw/books").strip("/"),
        max_pages=get_int_env("MAX_PAGES", 1),
        http_timeout_seconds=get_int_env("HTTP_TIMEOUT_SECONDS", 15),
        log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
        s3_sse_mode=s3_sse_mode,
        s3_kms_key_id=os.getenv("S3_KMS_KEY_ID", "").strip(),
    )
