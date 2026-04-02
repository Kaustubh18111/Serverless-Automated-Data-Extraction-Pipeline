import json
import logging
from datetime import datetime, timezone
from typing import Any

from requests import RequestException

from src.config import get_execution_identity, get_settings
from src.scraper import scrape_items
from src.storage.dynamodb import store_items_to_dynamodb
from src.storage.s3 import build_s3_object_key, store_payload_to_s3


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
        }

        for key, value in record.__dict__.items():
            if key in {
                "name",
                "msg",
                "args",
                "levelname",
                "levelno",
                "pathname",
                "filename",
                "module",
                "exc_info",
                "exc_text",
                "stack_info",
                "lineno",
                "funcName",
                "created",
                "msecs",
                "relativeCreated",
                "thread",
                "threadName",
                "processName",
                "process",
            }:
                continue
            payload[key] = value

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, default=str, ensure_ascii=False)


def configure_logger(level: str) -> logging.Logger:
    logger = logging.getLogger("data_extraction_pipeline")
    logger.setLevel(level)
    logger.propagate = False

    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)

    return logger


def build_payload(
    source_url: str,
    execution_uuid: str,
    extraction_timestamp: str,
    items: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "source_url": source_url,
        "execution_uuid": execution_uuid,
        "extraction_timestamp": extraction_timestamp,
        "item_count": len(items),
        "items": items,
    }


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    event = event or {}
    settings = get_settings()
    logger = configure_logger(settings.log_level)

    execution_uuid, extraction_timestamp = get_execution_identity(event, context)
    logger.info(
        "Pipeline execution started",
        extra={
            "source_url": settings.source_url,
            "storage_mode": settings.storage_mode,
            "max_pages": settings.max_pages,
            "execution_uuid": execution_uuid,
        },
    )

    try:
        items = scrape_items(
            source_url=settings.source_url,
            max_pages=settings.max_pages,
            timeout_seconds=settings.http_timeout_seconds,
        )
    except RequestException:
        logger.error(
            "HTTP request failed",
            extra={"source_url": settings.source_url, "execution_uuid": execution_uuid},
            exc_info=True,
        )
        raise
    except Exception:
        logger.error(
            "Scraping failed",
            extra={"source_url": settings.source_url, "execution_uuid": execution_uuid},
            exc_info=True,
        )
        raise

    payload = build_payload(
        source_url=settings.source_url,
        execution_uuid=execution_uuid,
        extraction_timestamp=extraction_timestamp,
        items=items,
    )

    result: dict[str, Any] = {
        "execution_uuid": execution_uuid,
        "extraction_timestamp": extraction_timestamp,
        "source_url": settings.source_url,
        "item_count": payload["item_count"],
        "stored_targets": [],
    }

    if settings.storage_mode in {"s3", "both"}:
        if not settings.bucket_name:
            raise ValueError(
                "BUCKET_NAME environment variable is required when STORAGE_MODE includes s3"
            )
        object_key = build_s3_object_key(settings.s3_prefix, extraction_timestamp, execution_uuid)
        s3_result = store_payload_to_s3(
            bucket_name=settings.bucket_name,
            key=object_key,
            payload=payload,
            sse_mode=settings.s3_sse_mode,
            kms_key_id=settings.s3_kms_key_id,
        )
        result["stored_targets"].append({"type": "s3", **s3_result})

    if settings.storage_mode in {"dynamodb", "both"}:
        if not settings.table_name:
            raise ValueError(
                "TABLE_NAME environment variable is required when STORAGE_MODE includes dynamodb"
            )
        ddb_result = store_items_to_dynamodb(
            table_name=settings.table_name,
            payload=payload,
            source_url=settings.source_url,
        )
        result["stored_targets"].append({"type": "dynamodb", **ddb_result})

    logger.info(
        "Pipeline execution completed",
        extra={
            "execution_uuid": execution_uuid,
            "item_count": payload["item_count"],
            "stored_targets": result["stored_targets"],
        },
    )

    return {
        "statusCode": 200,
        "body": json.dumps(result),
    }
