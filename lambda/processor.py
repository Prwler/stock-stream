import json
import base64
import boto3
import os
from datetime import datetime, timezone

# These environment variables are set in the Lambda console
S3_BUCKET = os.environ.get('S3_RAW_BUCKET')
DYNAMO_TABLE = os.environ.get('DYNAMO_TABLE_NAME')

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(DYNAMO_TABLE)


def handler(event, context):
    """
    Triggered by Kinesis. Each event contains a batch of records.
    For each record we:
      1. Decode and parse the JSON payload
      2. Write the raw event to S3 partitioned by date/hour
      3. Update DynamoDB with the latest price for that ticker
    """
    records_processed = 0

    for record in event['Records']:
        # Kinesis data is base64 encoded
        payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
        data = json.loads(payload)

        ticker = data['ticker']
        price = data['price']
        volume = data['volume']
        timestamp = data['timestamp']

        # Parse timestamp for S3 partitioning
        dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))

        # ── Write raw event to S3 ────────────────────────────
        s3_key = (
            f'events/'
            f'year={dt.year}/'
            f'month={str(dt.month).zfill(2)}/'
            f'day={str(dt.day).zfill(2)}/'
            f'hour={str(dt.hour).zfill(2)}/'
            f'{ticker}_{dt.strftime("%Y%m%d%H%M%S%f")}.json'
        )

        s3.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=json.dumps(data),
            ContentType='application/json',
        )

        # ── Update latest price in DynamoDB ──────────────────
        table.put_item(
            Item={
                'ticker': ticker,
                'price': str(price),      # DynamoDB requires Decimal or string for floats
                'volume': volume,
                'timestamp': timestamp,
                'updated_at': datetime.now(timezone.utc).isoformat(),
            }
        )

        records_processed += 1

    print(f'[processor] Processed {records_processed} records.')
    return {'statusCode': 200, 'recordsProcessed': records_processed}