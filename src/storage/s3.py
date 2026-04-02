import json
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError

S3_CLIENT = boto3.client("s3")


def build_s3_object_key(s3_prefix: str, extraction_timestamp: str, execution_uuid: str) -> str:
    date_part = extraction_timestamp[:10].replace("-", "")
    safe_prefix = s3_prefix.strip("/")
    if safe_prefix:
        return f"{safe_prefix}/{date_part}/books-{execution_uuid}.json"
    return f"{date_part}/books-{execution_uuid}.json"


def store_payload_to_s3(
    bucket_name: str,
    key: str,
    payload: dict[str, Any],
    sse_mode: str,
    kms_key_id: str,
) -> dict[str, str]:
    put_args: dict[str, Any] = {
        "Bucket": bucket_name,
        "Key": key,
        "Body": json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
        "ContentType": "application/json",
    }

    if sse_mode == "aws:kms":
        put_args["ServerSideEncryption"] = "aws:kms"
        if kms_key_id:
            put_args["SSEKMSKeyId"] = kms_key_id
    else:
        put_args["ServerSideEncryption"] = "AES256"

    try:
        S3_CLIENT.put_object(**put_args)
    except (ClientError, BotoCoreError) as exc:
        raise RuntimeError(f"Failed to write payload to S3 bucket {bucket_name}") from exc

    return {
        "bucket_name": bucket_name,
        "object_key": key,
    }
