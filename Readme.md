This project was made to introduce myself to tools like Kafka, Airflow, and Iceberg. As such, this serves as a demo for a more scalable solution.

# Step 1
Run **ingest_trades.py** to get data. currently set to the btc trades in usd.

# Step 2
With sufficient data, run **build_ohlcv.py** to aggregate ticks into candlelight bars