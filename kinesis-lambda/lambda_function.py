import base64
import json
import os
import time
import uuid
import boto3

s3 = boto3.client(
    "s3",
    region_name="us-east-1",
    endpoint_url=os.environ.get("FLOCI_ENDPOINT", "http://host.docker.internal:4566"),
    aws_access_key_id="test",
    aws_secret_access_key="test",
)

BUCKET = os.environ.get("RAW_BUCKET", "data-platform-raw")
PREFIX = "streaming/pr-events"


def lambda_handler(event, context):
    decoded_rows = []

    for record in event.get("Records", []):
        raw_bytes = base64.b64decode(record["kinesis"]["data"])
        payload = json.loads(raw_bytes.decode("utf-8"))

        # Basic normalization matching your PR schema
        payload["repo"] = str(payload.get("repo", "")).strip().lower()
        payload["author_type"] = str(payload.get("author_type", "")).strip().lower()
        payload["kinesis_seq"] = record["kinesis"]["sequenceNumber"]
        payload["kinesis_shard"] = record["eventID"].split(":")[0]
        decoded_rows.append(payload)

    if not decoded_rows:
        return {"status": "empty"}

    body = "\n".join(json.dumps(r) for r in decoded_rows) + "\n"
    key = f"{PREFIX}/batch_{int(time.time())}_{uuid.uuid4().hex[:8]}.jsonl"

    s3.put_object(Bucket=BUCKET, Key=key, Body=body.encode("utf-8"))
    print(f"Wrote {len(decoded_rows)} records to s3://{BUCKET}/{key}")

    return {"written": len(decoded_rows), "key": key}