import asyncio
import json
import polars as pl
from time import time
from datetime import datetime, timezone
import parquet
import websockets as websocket
from websockets.asyncio.client import connect


def flush(rows: dict) -> None:
    # Convert to polars dataframe and use its built-in parquet writer
    df = pl.DataFrame(rows)
    # Write the rows to a Parquet file
    df.write_parquet(f"bronze/trades_{int(time())}.parquet")


async def main():
    # Connect to the binance websocket stream for BTC/USDT
    url = "wss://stream.binance.com:9443/ws/btcusdt@trade"
    #Buffer to hold the incoming ticks so we dont generate too many parquet files
    buffer = []
    FLUSH_SIZE = 100 #Flush the buffer after 100 ticks (1 file per 100 ticks)
    n = 0
    #conenct to the websocket stream
    async with connect(url) as websocket:
        while True:
            message = await websocket.recv()
            raw = json.loads(message)
            #Make the tick naming fields more readable
            tick = {
                "event": raw["e"],
                "event_time": raw["E"],
                "symbol": raw["s"],
                "trade_id": raw["t"],
                "price": raw["p"],
                "quantity": raw["q"],
                "trade_time": raw["T"],
                "is_buyer_maker": raw["m"],
                "ingest_time": datetime.now(timezone.utc).isoformat()  # Add ingest time to determine tiebreakers in deduplication
            }
            buffer.append(tick)
            #Increment the counter and print every 10 ticks received
            n += 1
            if n % 10 == 0:
                print(f"Received {n} ticks")

            #After every 100 ticks, flush the buffer to a parquet file and clear the buffer
            if len(buffer) >= FLUSH_SIZE:
                print(f"Flushing buffer with {len(buffer)} items")
                flush(buffer)
                buffer.clear()
                n = 0

if __name__ == "__main__":
    asyncio.run(main())