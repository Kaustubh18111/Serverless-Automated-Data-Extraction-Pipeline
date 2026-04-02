import hashlib
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import boto3
import requests
from bs4 import BeautifulSoup
from botocore.exceptions import BotoCoreError, ClientError
from requests import RequestException


LOGGER = logging.getLogger()
if not LOGGER.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    LOGGER.addHandler(handler)
LOGGER.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())


SESSION = requests.Session()
S3_CLIENT = boto3.client("s3")
DDB_RESOURCE: Any = boto3.resource("dynamodb")


def _log(level: str, message: str, **fields: Any) -> None:
    payload = {
        "message": message,
        **fields,
    }
    getattr(LOGGER, level.lower())(json.dumps(payload, default=str, ensure_ascii=False))


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso_timestamp(value: Optional[str]) -> datetime:
    if value:
        try:
            normalized = value.replace("Z", "+00:00")
            parsed = datetime.fromisoformat(normalized)
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except ValueError:
            pass
    return _utc_now()


def _execution_identity(event: Dict[str, Any], context: Any) -> Dict[str, str]:
    execution_uuid = (
        str(event.get("id"))
        if event.get("id")
        else getattr(context, "aws_request_id", None)
        or str(uuid.uuid4())
    )
    execution_timestamp = _parse_iso_timestamp(event.get("time")).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {
        "execution_uuid": execution_uuid,
        "execution_timestamp": execution_timestamp,
    }


def _get_int_env(name: str, default: int) -> int:
    raw_value = os.getenv(name, str(default))
    try:
        return int(raw_value)
    except ValueError as exc:
        raise ValueError(f"Environment variable {name} must be an integer") from exc


def _get_source_configuration() -> Dict[str, Any]:
    return {
        "source_url": os.getenv("SOURCE_URL", "http://books.toscrape.com/"),
        "storage_mode": os.getenv("STORAGE_MODE", "BOTH").strip().lower(),
        "bucket_name": os.getenv("BUCKET_NAME", ""),
        "table_name": os.getenv("TABLE_NAME", ""),
        "s3_prefix": os.getenv("S3_PREFIX", "raw/books").strip("/"),
        "max_pages": _get_int_env("MAX_PAGES", 1),
        "http_timeout_seconds": _get_int_env("HTTP_TIMEOUT_SECONDS", 15),
    }


def _fetch_page(url: str, timeout_seconds: int) -> str:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (compatible; ServerlessDataExtractionPipeline/1.0; "
            "+https://aws.amazon.com/lambda/)"
        )
    }
    response = SESSION.get(url, headers=headers, timeout=timeout_seconds)
    response.raise_for_status()
    return response.text


def _parse_books_page(html: str, page_url: str, page_number: int) -> List[Dict[str, Any]]:
    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception as exc:
        raise ValueError("Unable to parse HTML with BeautifulSoup") from exc

    cards = soup.select("article.product_pod")
    items: List[Dict[str, Any]] = []

    for index, card in enumerate(cards):
        title_tag = card.select_one("h3 a")
        price_tag = card.select_one(".price_color")
        availability_tag = card.select_one(".availability")
        link = str(title_tag.get("href")) if title_tag and title_tag.get("href") else None

        title_value = title_tag.get("title") if title_tag else ""
        title = title_value.strip() if isinstance(title_value, str) else str(title_value or "").strip()
        price = price_tag.get_text(strip=True) if price_tag else ""
        availability = " ".join(availability_tag.get_text(" ", strip=True).split()) if availability_tag else ""

        items.append(
            {
                "title": title,
                "price": price,
                "availability": availability,
                "product_url": urljoin(page_url, str(link)) if link else page_url,
                "page_number": page_number,
                "position": index,
            }
        )

    return items


def scrape_items(source_url: str, max_pages: int, timeout_seconds: int) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    current_url = source_url
    page_number = 1

    while current_url and page_number <= max_pages:
        html = _fetch_page(current_url, timeout_seconds)
        page_items = _parse_books_page(html, current_url, page_number)
        items.extend(page_items)

        soup = BeautifulSoup(html, "html.parser")
        next_link = soup.select_one("li.next a")
        next_href = str(next_link.get("href")) if next_link and next_link.get("href") else None
        if not next_href:
            break

        current_url = urljoin(current_url, next_href)
        page_number += 1

    return items


def _build_payload(event: Dict[str, Any], context: Any, source_url: str, items: List[Dict[str, Any]]) -> Dict[str, Any]:
    identity = _execution_identity(event, context)
    payload = {
        "source_url": source_url,
        "execution_uuid": identity["execution_uuid"],
        "extraction_timestamp": identity["execution_timestamp"],
        "item_count": len(items),
        "items": items,
    }
    return payload


def _s3_object_key(s3_prefix: str, extraction_timestamp: str, execution_uuid: str) -> str:
    date_part = extraction_timestamp[:10].replace("-", "")
    safe_prefix = s3_prefix.strip("/")
    return f"{safe_prefix}/{date_part}/books-{execution_uuid}.json" if safe_prefix else f"{date_part}/books-{execution_uuid}.json"


def _store_to_s3(bucket_name: str, key: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    body = json.dumps(payload, indent=2, ensure_ascii=False).encode("utf-8")
    try:
        S3_CLIENT.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=body,
            ContentType="application/json",
            ServerSideEncryption="AES256",
        )
    except (ClientError, BotoCoreError) as exc:
        raise RuntimeError(f"Failed to write payload to S3 bucket {bucket_name}") from exc

    return {
        "bucket_name": bucket_name,
        "object_key": key,
    }


def _stable_item_id(execution_uuid: str, page_number: int, position: int) -> str:
    digest_source = f"{execution_uuid}:{page_number}:{position}".encode("utf-8")
    return hashlib.sha256(digest_source).hexdigest()


def _store_to_dynamodb(table_name: str, payload: Dict[str, Any], source_url: str) -> Dict[str, Any]:
    table = DDB_RESOURCE.Table(table_name)
    stored_count = 0
    skipped_count = 0
    extraction_timestamp = payload["extraction_timestamp"]
    execution_uuid = payload["execution_uuid"]

    for item in payload["items"]:
        item_id = _stable_item_id(execution_uuid, int(item["page_number"]), int(item["position"]))
        record = {
            "item_id": item_id,
            "extraction_timestamp": extraction_timestamp,
            "execution_uuid": execution_uuid,
            "source_url": source_url,
            "page_number": int(item["page_number"]),
            "position": int(item["position"]),
            "title": item["title"],
            "price": item["price"],
            "availability": item["availability"],
            "product_url": item["product_url"],
        }

        try:
            table.put_item(
                Item=record,
                ConditionExpression="attribute_not_exists(item_id) AND attribute_not_exists(extraction_timestamp)",
            )
            stored_count += 1
        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code", "")
            if error_code == "ConditionalCheckFailedException":
                skipped_count += 1
                continue
            raise RuntimeError(f"Failed to write item to DynamoDB table {table_name}") from exc
        except BotoCoreError as exc:
            raise RuntimeError(f"Failed to write item to DynamoDB table {table_name}") from exc

    return {
        "table_name": table_name,
        "stored_count": stored_count,
        "skipped_count": skipped_count,
    }


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    event = event or {}
    config = _get_source_configuration()
    source_url = config["source_url"]
    storage_mode = config["storage_mode"]
    bucket_name = config["bucket_name"]
    table_name = config["table_name"]
    max_pages = config["max_pages"]
    timeout_seconds = config["http_timeout_seconds"]

    _log(
        "info",
        "Pipeline execution started",
        source_url=source_url,
        storage_mode=storage_mode,
        max_pages=max_pages,
    )

    try:
        items = scrape_items(source_url=source_url, max_pages=max_pages, timeout_seconds=timeout_seconds)
    except RequestException as exc:
        _log("error", "HTTP request failed", source_url=source_url, error=str(exc))
        raise
    except Exception as exc:
        _log("error", "Scraping failed", source_url=source_url, error=str(exc))
        raise

    payload = _build_payload(event, context, source_url, items)
    results: Dict[str, Any] = {
        "execution_uuid": payload["execution_uuid"],
        "extraction_timestamp": payload["extraction_timestamp"],
        "source_url": source_url,
        "item_count": payload["item_count"],
        "stored_targets": [],
    }

    if storage_mode not in {"s3", "dynamodb", "both"}:
        raise ValueError("STORAGE_MODE must be one of: s3, dynamodb, both")

    if storage_mode in {"s3", "both"}:
        if not bucket_name:
            raise ValueError("BUCKET_NAME environment variable is required when STORAGE_MODE includes s3")
        s3_key = _s3_object_key(config["s3_prefix"], payload["extraction_timestamp"], payload["execution_uuid"])
        try:
            s3_result = _store_to_s3(bucket_name, s3_key, payload)
            results["stored_targets"].append({"type": "s3", **s3_result})
        except Exception as exc:
            _log("error", "S3 write failed", bucket_name=bucket_name, error=str(exc))
            raise

    if storage_mode in {"dynamodb", "both"}:
        if not table_name:
            raise ValueError("TABLE_NAME environment variable is required when STORAGE_MODE includes dynamodb")
        try:
            ddb_result = _store_to_dynamodb(table_name, payload, source_url)
            results["stored_targets"].append({"type": "dynamodb", **ddb_result})
        except Exception as exc:
            _log("error", "DynamoDB write failed", table_name=table_name, error=str(exc))
            raise

    _log(
        "info",
        "Pipeline execution completed",
        execution_uuid=payload["execution_uuid"],
        item_count=payload["item_count"],
        stored_targets=results["stored_targets"],
    )
    return {
        "statusCode": 200,
        "body": json.dumps(results),
    }
