import polars as pl
from decimal import Decimal

# Used to validate the transformation from bronze to silver quality
# Can add more functionality here in the future
def validate(df: pl.DataFrame) -> tuple[int | float | Decimal, int | float | Decimal]:
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

# Deduplicate the dataframe based on the trade_id and symbol columns.
def deduplicate(df: pl.DataFrame) -> pl.DataFrame:
    #Sort by ingest time in descending order to keep the most recent record. Sort uniqueness by both symbol and trade id as binance
    #can have idendtical trade ids for different symbols
    deduped_df = df.sort("ingest_time", descending=True).unique(subset=["symbol","trade_id"])
    return deduped_df

# Cast the columns to the appropriate types
def type_cast(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns([
        pl.col("event").cast(pl.Utf8),
        pl.col("event_time").cast(pl.Int64),    #Intermediate step to cast to int64 before converting to datetime
        pl.col("symbol").cast(pl.Utf8),
        pl.col("trade_id").cast(pl.Int64),
        pl.col("price").cast(pl.Float64),
        pl.col("quantity").cast(pl.Float64),
        pl.col("trade_time").cast(pl.Int64),    #Intermediate step to cast to int64 before converting to datetime
        pl.col("is_buyer_maker").cast(pl.Boolean)
    ])
    #Cast the event_time and trade_time columns to datetime format
    df = df.with_columns([
        pl.col("event_time").cast(pl.Datetime("ms")).alias("event_time"),
        pl.col("trade_time").cast(pl.Datetime("ms")).alias("trade_time")
    ])
    return df