import polars as pl
from datetime import datetime
from pathlib import Path



def validate(df):
    # Validate the dataframe by checking for null values and duplicates
    # We "sum_horizontal" to get the total number of nulls and duplicates across all columns. With normal sum, we would get a df with a sum of nulls in each column
    # Item extracts the value from the series
    null_count = df.null_count().sum_horizontal().item()
    print(f"Null count: {null_count}")

    # As is_duplicated returns a boolean series, we dont need to sum horizontally or itemize
    duplicate_count = df.is_duplicated().sum()
    print(f"Duplicate count: {duplicate_count}")

    if null_count > 0:
        raise ValueError("Dataframe contains null values")
    if duplicate_count > 0:
        raise ValueError("Dataframe contains duplicate rows")
    print("Dataframe validation passed")
    return null_count, duplicate_count

def deduplicate(df):
    # Deduplicate the dataframe based on the trade_id column
    #Sort by ingest time in descending order to keep the most recent record. Sort uniqueness by both symbol and trade id as binance
    #can have idendtical trade ids for different symbols
    deduped_df = df.sort("ingest_time", descending=True).unique(subset=["symbol","trade_id"])
    return deduped_df

def type_cast(df):
    # Cast the columns to the appropriate types
    df = df.with_columns([
        pl.col("event").cast(pl.Utf8),
        pl.col("event_time").cast(pl.Int64),
        pl.col("symbol").cast(pl.Utf8),
        pl.col("trade_id").cast(pl.Int64),
        pl.col("price").cast(pl.Float64),
        pl.col("quantity").cast(pl.Float64),
        pl.col("trade_time").cast(pl.Int64),
        pl.col("is_buyer_maker").cast(pl.Boolean)
    ])
    #Cast the event_time and trade_time columns to datetime format
    df = df.with_columns([
        pl.col("event_time").cast(pl.Datetime("ms")).alias("event_time"),
        pl.col("trade_time").cast(pl.Datetime("ms")).alias("trade_time")
    ])
    return df

def main():
    # Read parquet files from bronze directory into df
    df = pl.read_parquet("bronze/*.parquet")
    # Type cast the columns to the appropriate types
    type_df = type_cast(df)

    #Deduplicate the dataframe based on the trade_id column
    dedup_df = deduplicate(type_df)

    #Validate the deduped dataframe
    null_count, duplicate_count = validate(dedup_df)

    if null_count == 0 and duplicate_count == 0:
        #Make a folder for the silver directory if it doesn't exist
        Path("silver").mkdir(exist_ok=True)
        # Successfully cleaned the dataframe, write to silver directory
        dedup_df.write_parquet(f"silver/trades_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.parquet")



if __name__ == "__main__":
    main()



