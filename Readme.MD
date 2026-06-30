# Stock Market Streaming Pipeline

A real-time data pipeline that simulates a stock market feed and processes events through **AWS Kinesis**, **Lambda**, **DynamoDB**, and **S3**, with historical querying via **AWS Athena**. Demonstrates event-driven streaming architecture using fully managed AWS services.

---

## Tech Stack

| Component | Technology |
|---|---|
| Event streaming | AWS Kinesis Data Streams |
| Stream processing | AWS Lambda (Python 3.12) |
| Real-time storage | AWS DynamoDB |
| Historical storage | AWS S3 (partitioned JSON) |
| Historical queries | AWS Athena |
| Producer | Python (local) |

---

## Architecture Overview

```
Stock Price Simulator (Python)
            │
            │ PutRecord — 1 event/sec/ticker
            ▼
┌─────────────────────────────┐
│    Kinesis Data Stream      │  1 shard
│    stock-price-stream       │
└─────────────┬───────────────┘
              │ triggers (batch size: 10)
              ▼
┌─────────────────────────────┐
│        AWS Lambda           │  stock-processor
│        processor.py         │
└──────┬──────────────────────┘
       │                      │
       ▼                      ▼
┌──────────────┐   ┌──────────────────────────────┐
│   DynamoDB   │   │            S3                │
│ Latest price │   │  Partitioned raw JSON        │
│ per ticker   │   │  events/year/month/day/hour/ │
└──────────────┘   └──────────────────────────────┘
                              │
                              ▼
                   ┌──────────────────┐
                   │   AWS Athena     │
                   │ Historical SQL   │
                   └──────────────────┘
```

---

## Getting Started

### Prerequisites
- Python 3.x
- AWS account (free tier is okay)

### Installation

```bash
git clone https://github.com/your-username/stock-stream
cd stock-stream
py -m pip install boto3 python-dotenv
```

### Environment Variables

Copy `.env.example` to `.env` and fill in your values:

```env
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=us-east-1
S3_RAW_BUCKET=stocks-raw-yourname
KINESIS_STREAM_NAME=stock-price-stream
DYNAMO_TABLE_NAME=stock-latest-prices
```

---

## Running the Pipeline

### Step 1 — Create AWS resources

```bash
py infrastructure/setup.py
```

Creates the Kinesis stream, DynamoDB table, and S3 bucket by running the above file.

### Step 2 — Deploy the Lambda function

1. Go to **AWS Lambda → Create function → Author from scratch**
2. Name: `stock-processor`, Runtime: Python 3.12
3. Upload `lambda/processor.py` as a zip file
4. Set handler to `processor.handler`
5. Add environment variables: `S3_RAW_BUCKET` and `DYNAMO_TABLE_NAME`
6. Attach policies: `AmazonS3FullAccess`, `AmazonDynamoDBFullAccess`, `AmazonKinesisReadOnlyAccess`
7. Add Kinesis trigger: select `stock-price-stream`, batch size 10, starting position Latest

### Step 3 — Run the simulator

```bash
py producer/simulator.py
```

Runs for 5 minutes by default(can be adjusted), sending one price event per ticker per second (1,500 total events across 5 tickers).

### Step 4 — Set up Athena

1. Set query results location to `s3://stocks-raw-yourname/athena-results/`
2. Run the setup queries in `athena/queries.sql` to create the database and table
3. Run `MSCK REPAIR TABLE stock_stream.events` to load partitions
4. Run the analytical queries

---

## Stocks Simulated

| Ticker | Base Price | Volatility |
|---|---|---|
| AAPL | $182.00 | ±0.5% per tick |
| TSLA | $245.00 | ±0.5% per tick |
| GOOGL | $140.00 | ±0.5% per tick |
| AMZN | $178.00 | ±0.5% per tick |
| MSFT | $415.00 | ±0.5% per tick |

Prices follow a random walk bounded within ±20% of the base price.

---

## Key Design Decisions

### 1. Kinesis for Real-time Ingestion

Kinesis Data Streams provides ordered, durable ingestion of events at scale. Using `ticker` as the partition key ensures all events for the same stock land on the same shard in order — important for maintaining price sequence integrity.

### 2. Dual Write Pattern (DynamoDB + S3)

Lambda writes each event to two destinations with different purposes:

- **DynamoDB** — optimised for real-time lookups. One row per ticker, always reflecting the latest price with millisecond read latency. This is what a trading dashboard would query.
- **S3** — optimised for historical analysis. Every raw event is archived, partitioned by `year/month/day/hour` so Athena can efficiently scan only the relevant time window without reading the entire dataset.

This separation of real-time and historical storage is a core streaming architecture pattern.

### 3. S3 Partitioning Strategy

Events are stored at:
```
events/year=2026/month=03/day=30/hour=09/AAPL_20260330094256.json
```

Partitioning by time allows Athena to use **partition pruning**  when you filter `WHERE year='2026' AND month='03'`, Athena only scans that partition rather than the entire dataset. This dramatically reduces query cost and latency at scale.

### 4. Event-driven Architecture

The Lambda function is triggered automatically by Kinesis: there is no polling, no scheduler, and no manual intervention. Events flow through the pipeline within seconds of being produced. This is the defining characteristic of event-driven architecture and contrasts directly with the batch processing approach used in the IMDb lakehouse project.

### 5. Infrastructure as Code

All AWS resources (Kinesis stream, DynamoDB table, S3 bucket) are created programmatically via `infrastructure/setup.py` using boto3. This means the entire infrastructure can be torn down and rebuilt in under a minute

---

## Analytical Insights

From a 5 minute simulation run:

- **765 total events**  153 per ticker
- **MSFT** showed the highest price volatility ($36.39 range) due to its higher base price
- **GOOGL** was the most stable ($6.41 range)
- Volume was evenly distributed across tickers (~720K-825K total per ticker)

---

## Project Structure

```
stock-stream/
├── producer/
│   └── simulator.py         # generates and sends events to Kinesis
├── lambda/
│   └── processor.py         # Lambda function deployed to AWS
├── infrastructure/
│   └── setup.py             # creates all AWS resources via boto3
├── athena/
│   └── queries.sql          # table setup + analytical queries
├── .env.example
├── .gitignore
└── README.md
```