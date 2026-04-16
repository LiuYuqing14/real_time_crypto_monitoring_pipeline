"""Spark Structured Streaming consumer for real-time crypto market monitoring."""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    abs as spark_abs,
    avg,
    col,
    count as spark_count,
    current_timestamp,
    from_json,
    lit,
    max as spark_max,
    max_by,
    min as spark_min,
    min_by,
    round as spark_round,
    stddev_pop,
    sum as spark_sum,
    to_timestamp,
    when,
    window,
)
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "crypto_ticks_raw")

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = os.environ.get("CLICKHOUSE_PORT", "8123")
CLICKHOUSE_DATABASE = os.environ.get("CLICKHOUSE_DATABASE", "stocks")
CLICKHOUSE_RAW_TABLE = os.environ.get("CLICKHOUSE_RAW_TABLE", "crypto_ticks_raw")
CLICKHOUSE_AGG_TABLE = os.environ.get("CLICKHOUSE_AGG_TABLE", "crypto_metrics_1m")

RAW_BATCH_INTERVAL = os.environ.get("RAW_BATCH_INTERVAL", "3 seconds")
AGG_BATCH_INTERVAL = os.environ.get("AGG_BATCH_INTERVAL", "10 seconds")
WATERMARK_DELAY = os.environ.get("WATERMARK_DELAY", "30 seconds")
WINDOW_DURATION = os.environ.get("WINDOW_DURATION", "1 minute")
RETURN_ALERT_THRESHOLD = float(os.environ.get("RETURN_ALERT_THRESHOLD", "2.0"))
VOLUME_ALERT_THRESHOLD = float(os.environ.get("VOLUME_ALERT_THRESHOLD", "5.0"))

CHECKPOINT_DIR_RAW = os.environ.get("CHECKPOINT_DIR_RAW", "checkpoints/crypto_raw_ticks")
CHECKPOINT_DIR_AGG = os.environ.get("CHECKPOINT_DIR_AGG", "checkpoints/crypto_metrics_1m")

spark = (
    SparkSession.builder.appName("CryptoTickConsumer-Streaming")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "com.clickhouse:clickhouse-jdbc:0.6.0,"
        "org.apache.httpcomponents.client5:httpclient5:5.3.1",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("Spark Structured Streaming Consumer (Crypto Monitoring)")
print(f"Reading from Kafka: {KAFKA_BOOTSTRAP_SERVERS}/{KAFKA_TOPIC}")
print(f"Raw sink        : {CLICKHOUSE_DATABASE}.{CLICKHOUSE_RAW_TABLE}")
print(f"Metrics sink    : {CLICKHOUSE_DATABASE}.{CLICKHOUSE_AGG_TABLE}")
print(f"Watermark delay : {WATERMARK_DELAY}")
print(f"Window duration : {WINDOW_DURATION}")
print("=" * 60)

crypto_schema = StructType([
    StructField("event_time", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("source", StringType(), True),
    StructField("event_type", StringType(), True),
])

df_raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

df_parsed = (
    df_raw.selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), crypto_schema).alias("data"))
    .select(
        to_timestamp(col("data.event_time")).alias("event_time"),
        col("data.symbol").alias("symbol"),
        col("data.product_id").alias("product_id"),
        col("data.price").alias("price"),
        col("data.volume").alias("volume"),
        col("data.source").alias("source"),
        col("data.event_type").alias("event_type"),
    )
    .filter(
        col("event_time").isNotNull()
        & col("symbol").isNotNull()
        & col("price").isNotNull()
        & col("volume").isNotNull()
    )
    .withColumn("ingest_time", current_timestamp())
)

df_with_watermark = df_parsed.withWatermark("event_time", WATERMARK_DELAY)

CLICKHOUSE_JDBC_URL = f"jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DATABASE}"
CLICKHOUSE_PROPERTIES = {
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    "user": os.environ.get("CLICKHOUSE_USER", "default"),
    "password": os.environ.get("CLICKHOUSE_PASSWORD", ""),
    "isolationLevel": "NONE",
}


def write_raw_to_clickhouse(batch_df, batch_id):
    row_count = batch_df.count()
    if row_count == 0:
        print(f"[Raw Batch {batch_id}] No new rows")
        return

    print(f"[Raw Batch {batch_id}] Writing {row_count} rows -> {CLICKHOUSE_RAW_TABLE}")
    (
        batch_df.select(
            "event_time",
            "symbol",
            "product_id",
            "price",
            "volume",
            "source",
            "event_type",
            "ingest_time",
        )
        .write.jdbc(
            url=CLICKHOUSE_JDBC_URL,
            table=CLICKHOUSE_RAW_TABLE,
            mode="append",
            properties=CLICKHOUSE_PROPERTIES,
        )
    )


raw_query = (
    df_with_watermark.writeStream.foreachBatch(write_raw_to_clickhouse)
    .outputMode("append")
    .trigger(processingTime=RAW_BATCH_INTERVAL)
    .option("checkpointLocation", CHECKPOINT_DIR_RAW)
    .queryName("crypto_raw_ticks_to_clickhouse")
    .start()
)

df_windowed = (
    df_with_watermark.groupBy(col("symbol"), col("product_id"), window(col("event_time"), WINDOW_DURATION))
    .agg(
        min_by(col("price"), col("event_time")).alias("open_price"),
        max_by(col("price"), col("event_time")).alias("close_price"),
        avg(col("price")).alias("avg_price"),
        spark_min(col("price")).alias("min_price"),
        spark_max(col("price")).alias("max_price"),
        spark_sum(col("volume")).alias("total_volume"),
        avg(col("volume")).alias("avg_trade_size"),
        spark_count("*").alias("trade_count"),
        stddev_pop(col("price")).alias("price_stddev"),
    )
    .withColumn(
        "return_pct_1m",
        when(col("open_price") > 0, ((col("close_price") - col("open_price")) / col("open_price")) * 100.0).otherwise(lit(0.0)),
    )
    .withColumn(
        "price_range_pct",
        when(col("min_price") > 0, ((col("max_price") - col("min_price")) / col("min_price")) * 100.0).otherwise(lit(0.0)),
    )
    .withColumn(
        "volatility_pct",
        when(col("avg_price") > 0, (col("price_stddev") / col("avg_price")) * 100.0).otherwise(lit(0.0)),
    )
    .withColumn("is_price_spike", spark_abs(col("return_pct_1m")) >= lit(RETURN_ALERT_THRESHOLD))
    .withColumn("is_volume_anomaly", col("total_volume") >= lit(VOLUME_ALERT_THRESHOLD))
    .select(
        col("symbol"),
        col("product_id"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        spark_round(col("open_price"), 8).alias("open_price"),
        spark_round(col("close_price"), 8).alias("close_price"),
        spark_round(col("avg_price"), 8).alias("avg_price"),
        spark_round(col("min_price"), 8).alias("min_price"),
        spark_round(col("max_price"), 8).alias("max_price"),
        spark_round(col("total_volume"), 8).alias("total_volume"),
        spark_round(col("avg_trade_size"), 8).alias("avg_trade_size"),
        col("trade_count"),
        spark_round(col("price_stddev"), 8).alias("price_stddev"),
        spark_round(col("return_pct_1m"), 4).alias("return_pct_1m"),
        spark_round(col("price_range_pct"), 4).alias("price_range_pct"),
        spark_round(col("volatility_pct"), 4).alias("volatility_pct"),
        col("is_price_spike"),
        col("is_volume_anomaly"),
        current_timestamp().alias("updated_at"),
    )
)


def write_agg_to_clickhouse(batch_df, batch_id):
    row_count = batch_df.count()
    if row_count == 0:
        print(f"[Agg Batch {batch_id}] No updated windows")
        return

    print(f"[Agg Batch {batch_id}] Writing {row_count} rows -> {CLICKHOUSE_AGG_TABLE}")
    (
        batch_df.write.jdbc(
            url=CLICKHOUSE_JDBC_URL,
            table=CLICKHOUSE_AGG_TABLE,
            mode="append",
            properties=CLICKHOUSE_PROPERTIES,
        )
    )


agg_query = (
    df_windowed.writeStream.foreachBatch(write_agg_to_clickhouse)
    .outputMode("update")
    .trigger(processingTime=AGG_BATCH_INTERVAL)
    .option("checkpointLocation", CHECKPOINT_DIR_AGG)
    .queryName("crypto_metrics_1m_to_clickhouse")
    .start()
)

print("\nStreaming consumer is running with 2 streams:")
print(f"  1. Raw ticks  -> {CLICKHOUSE_DATABASE}.{CLICKHOUSE_RAW_TABLE} ({RAW_BATCH_INTERVAL})")
print(f"  2. Metrics    -> {CLICKHOUSE_DATABASE}.{CLICKHOUSE_AGG_TABLE} ({AGG_BATCH_INTERVAL})")
print("\nPress Ctrl+C to stop.\n")

spark.streams.awaitAnyTermination()
