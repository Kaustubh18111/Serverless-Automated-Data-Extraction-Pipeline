import hashlib
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError

DYNAMODB_RESOURCE: Any = boto3.resource("dynamodb")


def stable_item_id(execution_uuid: str, page_number: int, position: int) -> str:
    digest_source = f"{execution_uuid}:{page_number}:{position}".encode()
    return hashlib.sha256(digest_source).hexdigest()


def store_items_to_dynamodb(
    table_name: str,
    payload: dict[str, Any],
    source_url: str,
) -> dict[str, Any]:
    table = DYNAMODB_RESOURCE.Table(table_name)
    stored_count = 0
    skipped_count = 0

    extraction_timestamp = payload["extraction_timestamp"]
    execution_uuid = payload["execution_uuid"]

    for item in payload["items"]:
        item_id = stable_item_id(execution_uuid, int(item["page_number"]), int(item["position"]))
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
                ConditionExpression=(
                    "attribute_not_exists(item_id) "
                    "AND attribute_not_exists(extraction_timestamp)"
                ),
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
