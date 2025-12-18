from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, date_format, year, month, dayofmonth, hour, minute
import os
from dotenv import load_dotenv

load_dotenv()
spark = SparkSession.builder.appName("CryptoETL").config("spark.jars.packages", "org.postgresql:postgresql:42.6.0").getOrCreate()
spark.sparkContext.setLogLevel("ERROR") # to suppress long warning

jdbc_url = "jdbc:postgresql://localhost:5432/crypto_analytics"
jdbc_props = {
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "driver": "org.postgresql.Driver"
}

df = spark.read.csv('./data/*.csv', header=True, inferSchema=True) # read all the CSVs

# coins table 
existing_coins = spark.read.jdbc(url=jdbc_url, table="coins", properties=jdbc_props)

coins_df = (
    df.select(
        col("id").alias("coin_id"),
        "symbol",
        "name"
    )
    .dropDuplicates(["coin_id"])
)

new_coins = coins_df.join(existing_coins, on="coin_id", how="left_anti") # filter out coins that already exist using anti-join

new_coins_count = new_coins.count()
if new_coins_count > 0:
    new_coins.write.jdbc(
        url=jdbc_url,
        table="coins",
        mode="append",
        properties=jdbc_props
    )
    print(f"Inserted {new_coins_count} new coins")
else:
    print("No new coins to insert")

# time_intervals table 
existing_time_intervals = spark.read.jdbc(url=jdbc_url, table="time_intervals", properties=jdbc_props)

time_df = (
    df.select(
        to_timestamp("collected_at").alias("timestamp")
    )
    .dropDuplicates()
)

time_intervals_df = (
    time_df
    .withColumn("time_key", date_format("timestamp", "yyyyMMddHHmm").cast("long"))
    .withColumn("date", col("timestamp").cast("date"))
    .withColumn("year", year("timestamp"))
    .withColumn("month", month("timestamp"))
    .withColumn("day", dayofmonth("timestamp"))
    .withColumn("hour", hour("timestamp"))
    .withColumn("minute", minute("timestamp"))
    .select(
        "time_key",
        "timestamp",
        "date",
        "hour",
        "minute",
        "day",
        "month",
        "year"
    )
)

new_time_intervals = time_intervals_df.join( # filter out time intervals that already exist using anti-join
    existing_time_intervals.select("time_key"), 
    on="time_key", 
    how="left_anti"
)

new_time_intervals_count = new_time_intervals.count()
if new_time_intervals_count > 0:
    new_time_intervals.write.jdbc(
        url=jdbc_url,
        table="time_intervals",
        mode="append",
        properties=jdbc_props
    )
    print(f"Inserted {new_time_intervals_count} new time intervals")
else:
    print("No new time intervals to insert")

# crypto_prices table
coins_dim = spark.read.jdbc(
    url=jdbc_url,
    table="coins",
    properties=jdbc_props
)

time_intervals_dim = spark.read.jdbc(
    url=jdbc_url,
    table="time_intervals",
    properties=jdbc_props
)

fact_df = (
    df
    .withColumn(
        "timestamp",
        to_timestamp("collected_at")
    )
    .withColumn(
        "time_key",
        date_format(col("timestamp"), "yyyyMMddHHmm").cast("long")
    )
)

fact_with_coins = (
    fact_df
    .join(
        coins_dim,
        fact_df.id == coins_dim.coin_id,
        "inner"
    )
)

fact_with_coins_and_time = (
    fact_with_coins
    .join(
        time_intervals_dim.select("time_key"),
        on="time_key",
        how="inner"
    )
)

crypto_prices_df = fact_with_coins_and_time.select(
    col("coin_key"),
    col("time_key"),
    col("current_price").alias("price_usd"),
    col("market_cap"),
    col("total_volume").alias("volume_24h"),
    col("price_change_percentage_24h").alias("price_change_pct_24h"),
    col("timestamp").alias("collected_at")
)

existing_prices = spark.read.jdbc(url=jdbc_url, table="crypto_prices", properties=jdbc_props) # read existing prices to check how many new records to add
new_prices = crypto_prices_df.join(
    existing_prices.select("coin_key", "time_key"),
    on=["coin_key", "time_key"],
    how="left_anti"
)

new_prices_count = new_prices.count()
if new_prices_count > 0:
    new_prices.write.jdbc(
        url=jdbc_url,
        table="crypto_prices",
        mode="append",
        properties=jdbc_props
    )
    print(f"Inserted {new_prices_count} new price records")
else:
    print("No new price records to insert")

print("ETL pipeline completed successfully!")