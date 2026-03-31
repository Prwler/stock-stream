import boto3
import json
import os
from dotenv import load_dotenv

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')
S3_BUCKET = os.getenv('S3_RAW_BUCKET')
KINESIS_STREAM = os.getenv('KINESIS_STREAM_NAME')
DYNAMO_TABLE = os.getenv('DYNAMO_TABLE_NAME')


def create_kinesis_stream(client):
    print(f'[setup] Creating Kinesis stream: {KINESIS_STREAM}...')
    try:
        client.create_stream(
            StreamName=KINESIS_STREAM,
            ShardCount=1,
        )
        # Wait until stream is active
        waiter = client.get_waiter('stream_exists')
        waiter.wait(StreamName=KINESIS_STREAM)
        print(f'[setup] Kinesis stream {KINESIS_STREAM} is active.')
    except client.exceptions.ResourceInUseException:
        print(f'[setup] Kinesis stream {KINESIS_STREAM} already exists, skipping.')


def create_dynamodb_table(client):
    print(f'[setup] Creating DynamoDB table: {DYNAMO_TABLE}...')
    try:
        client.create_table(
            TableName=DYNAMO_TABLE,
            KeySchema=[
                {'AttributeName': 'ticker', 'KeyType': 'HASH'},
            ],
            AttributeDefinitions=[
                {'AttributeName': 'ticker', 'AttributeType': 'S'},
            ],
            BillingMode='PAY_PER_REQUEST',
        )
        waiter = client.get_waiter('table_exists')
        waiter.wait(TableName=DYNAMO_TABLE)
        print(f'[setup] DynamoDB table {DYNAMO_TABLE} is active.')
    except client.exceptions.ResourceInUseException:
        print(f'[setup] DynamoDB table {DYNAMO_TABLE} already exists, skipping.')


def create_s3_bucket(client):
    print(f'[setup] Creating S3 bucket: {S3_BUCKET}...')
    try:
        if AWS_REGION == 'us-east-1':
            client.create_bucket(Bucket=S3_BUCKET)
        else:
            client.create_bucket(
                Bucket=S3_BUCKET,
                CreateBucketConfiguration={'LocationConstraint': AWS_REGION},
            )
        print(f'[setup] S3 bucket {S3_BUCKET} created.')
    except client.exceptions.BucketAlreadyOwnedByYou:
        print(f'[setup] S3 bucket {S3_BUCKET} already exists, skipping.')


def main():
    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )

    create_kinesis_stream(session.client('kinesis'))
    create_dynamodb_table(session.client('dynamodb'))
    create_s3_bucket(session.client('s3'))

    print('\n[setup] All resources created successfully.')
    print(f'  Kinesis Stream : {KINESIS_STREAM}')
    print(f'  DynamoDB Table : {DYNAMO_TABLE}')
    print(f'  S3 Bucket      : {S3_BUCKET}')


if __name__ == '__main__':
    main()