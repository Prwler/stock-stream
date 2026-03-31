import boto3
import json
import os
import random
import time
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')
KINESIS_STREAM = os.getenv('KINESIS_STREAM_NAME')

# Base prices for each ticker
STOCKS = {
    'AAPL':  182.00,
    'TSLA':  245.00,
    'GOOGL': 140.00,
    'AMZN':  178.00,
    'MSFT':  415.00,
}

# Track current prices so they drift realistically
current_prices = dict(STOCKS)


def next_price(ticker):
    """
    Simulate realistic price movement using random walk.
    Each tick moves the price up or down by up to 0.5%.
    """
    current = current_prices[ticker]
    change_pct = random.uniform(-0.005, 0.005)
    new_price = round(current * (1 + change_pct), 2)

    # Keep price within 20% of base price
    base = STOCKS[ticker]
    new_price = max(base * 0.8, min(base * 1.2, new_price))

    current_prices[ticker] = new_price
    return new_price


def generate_event(ticker):
    return {
        'ticker': ticker,
        'price': next_price(ticker),
        'volume': random.randint(100, 10000),
        'timestamp': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
    }


def run(duration_seconds=300, interval_seconds=1):
    """
    Sends one price event per ticker every interval_seconds.
    Runs for duration_seconds total (default 5 minutes).
    """
    kinesis = boto3.client(
        'kinesis',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )

    print(f'[simulator] Starting simulation for {duration_seconds}s...')
    print(f'[simulator] Sending events to stream: {KINESIS_STREAM}')
    print(f'[simulator] Tickers: {", ".join(STOCKS.keys())}')
    print('-' * 50)

    start = time.time()
    total_sent = 0

    while time.time() - start < duration_seconds:
        for ticker in STOCKS:
            event = generate_event(ticker)

            kinesis.put_record(
                StreamName=KINESIS_STREAM,
                Data=json.dumps(event),
                PartitionKey=ticker,
            )

            print(
                f'[{event["timestamp"]}] {ticker:5s} '
                f'${event["price"]:>8.2f}  '
                f'vol: {event["volume"]:>6,}'
            )
            total_sent += 1

        time.sleep(interval_seconds)

    print('-' * 50)
    print(f'[simulator] Done. Total events sent: {total_sent}')


if __name__ == '__main__':
    run(duration_seconds=300, interval_seconds=1)