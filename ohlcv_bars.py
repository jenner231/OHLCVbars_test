import polars as pl
from datetime import datetime
from pathlib import Path

#Import local functions to clean the data and go from bronze to silver
from bronze_silver_transforms import type_cast, deduplicate, validate

def bronze_to_silver(bronze_df: pl.DataFrame):
    # Type cast the columns to the appropriate types
    type_df = type_cast(bronze_df)

    #Deduplicate the dataframe based on the trade_id column
    dedup_df = deduplicate(type_df)

    #Validate the deduped dataframe
    null_count, duplicate_count = validate(dedup_df)

    if null_count == 0 and duplicate_count == 0:
        #Make a folder for the silver directory if it doesn't exist
        Path("silver").mkdir(exist_ok=True)
        # Successfully cleaned the dataframe, write to silver directory
        dedup_df.write_parquet(f"silver/trades_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.parquet")



def build_ohlcv(silver_df: pl.DataFrame, interval: str = "1m") -> pl.DataFrame:
    bars = (
        silver_df
        # Sort the dataframe by trade_time and trade_id to ensure the correct order of trades for each bar
        .sort("trade_time", "trade_id")
        # Assign ticks to their interval by truncating trade_time to nearest interval.
        .with_columns(
            pl.col("trade_time").dt.truncate(interval).alias("bar_start")
        )
        .group_by("symbol", "bar_start")
        .agg(
            pl.col("price").first().alias("open"),
            pl.col("price").max().alias("high"),
            pl.col("price").min().alias("low"),
            pl.col("price").last().alias("close"),
            pl.col("quantity").sum().alias("volume"),
            pl.len().alias("trade_count"),
            )
        .sort("symbol", "bar_start")
    )

    # Validate the OHLC invariant: high >= max(open, close) and low <= min(open, close)
    assert bars.filter(
        (pl.col("high") < pl.col("open")) | (pl.col("high") < pl.col("close")) |
        (pl.col("low")  > pl.col("open")) | (pl.col("low")  > pl.col("close"))
    ).height == 0, "OHLC invariant violated"


    return bars

def write_gold(bars: pl.DataFrame) -> None:
    #Make a folder for the gold directory if it doesn't exist
    Path("gold").mkdir(exist_ok=True)
    bars = bars.with_columns(pl.col("bar_start").dt.date().alias("bar_date"))
    bars.write_parquet(f"gold/ohlcv_bars_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.parquet")


def main():
    # Read parquet files from bronze directory into df
    df = pl.read_parquet("bronze/*.parquet")

    #Process the data to achieve silver level quality and write to silver directory
    bronze_to_silver(df)

    # Read the cleaned silver data and build OHLCV bars
    silver_df = pl.read_parquet("silver/*.parquet")
    ohlcv_bars = build_ohlcv(silver_df)

    # Write the OHLCV bars to the gold directory
    write_gold(ohlcv_bars)


if __name__ == "__main__":
    main()



