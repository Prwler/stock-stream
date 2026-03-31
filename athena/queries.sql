-- ============================================================
-- Stock Stream — Analytical Queries
-- Run these in AWS Athena against the stock_stream database
-- Replace year/month/day values as needed
-- ============================================================

-- Setup: Create database
CREATE DATABASE IF NOT EXISTS stock_stream;

-- Setup: Create external table
CREATE EXTERNAL TABLE IF NOT EXISTS stock_stream.events (
  ticker STRING,
  price DOUBLE,
  volume INT,
  timestamp STRING
)
PARTITIONED BY (year STRING, month STRING, day STRING, hour STRING)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://stocks-raw-ayoola/events/'
TBLPROPERTIES ('ignore.malformed.json' = 'true');

-- Setup: Load partitions
MSCK REPAIR TABLE stock_stream.events;

-- ============================================================
-- Analytical Queries
-- ============================================================

-- Query 1: All events for a given day ordered by ticker and time
SELECT ticker, price, volume, timestamp
FROM stock_stream.events
WHERE year='2026' AND month='03' AND day='30'
ORDER BY ticker, timestamp DESC;

-- Query 2: Average, min and max price per ticker for a given day
SELECT ticker,
  ROUND(AVG(price), 2) AS avg_price,
  ROUND(MIN(price), 2) AS min_price,
  ROUND(MAX(price), 2) AS max_price,
  COUNT(*) AS total_events
FROM stock_stream.events
WHERE year='2026' AND month='03' AND day='30'
GROUP BY ticker
ORDER BY ticker;

-- Query 3: Price volatility (range) per ticker per hour
SELECT ticker, hour,
  ROUND(MAX(price) - MIN(price), 2) AS price_range,
  COUNT(*) AS events
FROM stock_stream.events
WHERE year='2026' AND month='03' AND day='30'
GROUP BY ticker, hour
ORDER BY ticker, hour;

-- Query 4: Highest volume hour per ticker
SELECT ticker, hour, SUM(volume) AS total_volume
FROM stock_stream.events
WHERE year='2026' AND month='03' AND day='30'
GROUP BY ticker, hour
ORDER BY total_volume DESC;

-- Query 5: Price trend over time for a single ticker
SELECT timestamp, price, volume
FROM stock_stream.events
WHERE year='2026' AND month='03' AND day='30'
AND ticker='AAPL'
ORDER BY timestamp;

-- Query 6: Most volatile ticker of the day
SELECT ticker,
  ROUND(MAX(price) - MIN(price), 2) AS price_range,
  ROUND(AVG(price), 2) AS avg_price,
  ROUND((MAX(price) - MIN(price)) / AVG(price) * 100, 2) AS volatility_pct
FROM stock_stream.events
WHERE year='2026' AND month='03' AND day='30'
GROUP BY ticker
ORDER BY volatility_pct DESC;