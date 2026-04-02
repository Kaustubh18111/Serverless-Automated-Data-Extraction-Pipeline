# Serverless Automated Data Extraction Pipeline

## Overview

This project uses:
- AWS Lambda (Python 3.10+)
- Amazon EventBridge for scheduled orchestration
- Amazon S3 for raw JSON storage
- Amazon DynamoDB for structured NoSQL storage
- AWS IAM for least-privilege access control
- `requests`, `beautifulsoup4`, and `boto3`

## Storage mode

Set the Lambda environment variable `STORAGE_MODE` to one of:
- `S3`
- `DYNAMODB`
- `BOTH`

The Lambda function uses the following environment variables:
- `BUCKET_NAME`
- `TABLE_NAME`
- `SOURCE_URL`
- `MAX_PAGES`
- `HTTP_TIMEOUT_SECONDS`
- `S3_PREFIX`
- `LOG_LEVEL`

## Packaging the Lambda Layer

The dependency layer is defined under `layer/data_extraction_layer/`.

To package the layer with AWS SAM:
1. Keep the dependency list in `layer/data_extraction_layer/requirements.txt`.
2. Run SAM build so AWS SAM installs the packages into the layer artifact.

Example:
- `sam build --use-container`

AWS SAM will resolve the layer declared in `template.yaml` and package `requests` plus `beautifulsoup4` into the Lambda Layer.

## Deploying the SAM application

1. Build the application:
   - `sam build --use-container`
2. Deploy interactively:
   - `sam deploy --guided`
3. Confirm the stack parameters and capabilities when prompted.

## Notes

- The EventBridge rule triggers the Lambda function every 12 hours.
- The Lambda execution role is restricted to:
  - `s3:PutObject` on the created S3 bucket
  - `dynamodb:PutItem` on the created DynamoDB table
- The Lambda function writes deterministic S3 object keys and uses conditional writes for DynamoDB to reduce duplicates on retries.
