from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    count,
    window,
    when,
    max,
    greatest,
    lit
)
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql.streaming import StreamingQueryListener
import datetime


# --------------------------------------
# Kafka Configuration
# --------------------------------------
SOURCE_TOPIC = "ad-events-2"
RESULT_TOPIC = "results"
BOOTSTRAP_SERVERS = "localhost:9092"

spark = (
    SparkSession.builder
    .appName("Assn-4")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------
# Schema for Payload JSON
# --------------------------------------
event_schema = StructType([
    StructField("user_id", StringType()),
    StructField("page_id", StringType()),
    StructField("ad_id", StringType()),
    StructField("ad_type", StringType()),
    StructField("event_type", StringType()),
    StructField("event_time", LongType()),  # ms timestamp from producer
    StructField("ip_address", StringType())
])


# --------------------------------------
# Watermark Listener (for printing)
# --------------------------------------
class WatermarkKafkaListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        pass

    def onQueryTerminated(self, event):
        pass

    def onQueryProgress(self, event):
        wm = event.progress.eventTime.get("watermark")

        if wm:
            print(
                f"[WM] batch={event.progress.batchId} wm={wm} "
                f"processing_time={datetime.datetime.now().isoformat()} "
                f"rows={event.progress.numInputRows}"
            )
            print(f"[SINK] {event.progress.sink}")


spark.streams.addListener(WatermarkKafkaListener())


# --------------------------------------
# Read Stream from Kafka
# --------------------------------------
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("subscribe", SOURCE_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

# kafka timestamp → event-time for latency
df = df.withColumn("kafka_ts", col("timestamp").cast("timestamp"))


# --------------------------------------
# Parse JSON payload
# --------------------------------------
parsed_df = (
    df.selectExpr(
            "CAST(key AS STRING) as campaign_id",
            "CAST(value AS STRING) as json_str",
            "kafka_ts"
        )
      .select(
            from_json(col("json_str"), event_schema).alias("data"),
            "campaign_id",
            "kafka_ts"
      )
      .select(
            "campaign_id",
            "kafka_ts",
            "data.*"
      )
      # Convert event_time (ms) -> proper event timestamp
      .withColumn("event_ts", (col("event_time") / 1000).cast("timestamp"))
)


# --------------------------------------
# Apply Watermark
# --------------------------------------
events_with_watermark = parsed_df.withWatermark("event_ts", "1 second")


# (Optional) Debug stream of watermark
watermark_df = (
    events_with_watermark
    .select(
        "campaign_id",
        "event_ts",
    )
)


# --------------------------------------
# Compute Views
# --------------------------------------
view_counts = (
    events_with_watermark
    .filter(col("event_type") == "view")
    .groupBy(
        col("campaign_id"),
        window(col("event_ts"), "10 seconds")
    )
    .agg(
        count("*").alias("views"),
        max("kafka_ts").alias("max_kafka_ts_views")
    )
)


# --------------------------------------
# Compute Clicks
# --------------------------------------
click_counts = (
    events_with_watermark
    .filter(col("event_type") == "click")
    .groupBy(
        col("campaign_id"),
        window(col("event_ts"), "10 seconds")
    )
    .agg(
        count("*").alias("clicks"),
        max("kafka_ts").alias("max_kafka_ts_clicks")
    )
)


# --------------------------------------
# Final Combined CTR Window
# --------------------------------------
ctr_df = (
    view_counts.join(
        click_counts,
        on=["campaign_id", "window"],
        how="left"
    )
    .na.fill({"clicks": 0})   # for windows with no clicks
    .withColumn(
        "ctr",
        when(col("views") > 0, col("clicks").cast("float") / col("views")).otherwise(lit(0.0))
    )
    .withColumn(
        "max_kafka_ts",
        greatest(
            col("max_kafka_ts_views"),
            col("max_kafka_ts_clicks")
        )
    )
    .select(
        col("campaign_id"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "views",
        "clicks",
        "ctr",
        "max_kafka_ts"
    )
)


# --------------------------------------
# Write Results to Kafka
# --------------------------------------
result_df = (
    ctr_df.selectExpr(
        "CAST(campaign_id AS STRING) AS key",
        """to_json(named_struct(
            'campaign_id', campaign_id,
            'window_start', window_start,
            'window_end', window_end,
            'views', views,
            'clicks', clicks,
            'ctr', ctr,
            'max_kafka_ts', max_kafka_ts
        )) AS value"""
    )
)


query = (
    result_df.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("topic", RESULT_TOPIC)
    .outputMode("append")
    .option("checkpointLocation", "checkpoints/ctr_windows")
    .start()
)

query.awaitTermination()
