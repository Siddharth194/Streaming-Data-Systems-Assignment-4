from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    count,
    window,
    when,
    max,
    greatest
)
from pyspark.sql.types import StructType, StructField, StringType, LongType

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

event_schema = StructType([
    StructField("user_id", StringType()),
    StructField("page_id", StringType()),
    StructField("ad_id", StringType()),
    StructField("ad_type", StringType()),
    StructField("event_type", StringType()),
    StructField("event_time", LongType()),
    StructField("ip_address", StringType()),
    StructField("timestamp", LongType())
])

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("subscribe", SOURCE_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

df = df.withColumn("kafka_ts", col("timestamp").cast("timestamp"))

parsed_df = (
    df.selectExpr(
            "CAST(key AS STRING) as campaign_id",
            "CAST(value AS STRING) as json_str",
            "kafka_ts"
        )
      .select(from_json(col("json_str"), event_schema).alias("data"),
              col("campaign_id"),
              col("kafka_ts"))
      .select("campaign_id", "kafka_ts", "data.*")
      # convert payload event_time (ms) to a proper timestamp column for event-time processing
      .withColumn("event_ts", (col("event_time") / 1000).cast("timestamp"))
)

events_with_watermark = (
    parsed_df
    # use event_ts (from payload) as the event-time and watermark column
    .withWatermark("event_ts", "1 second")
)

view_counts = (
    events_with_watermark
    .filter(col("event_type") == "view")
    .groupBy(
        col("campaign_id"),
        window(col("event_ts"), "10 seconds")
    )
    .agg(count("*").alias("views"),
        max("kafka_ts").alias("max_kafka_ts_views"))
)

click_counts = (
    events_with_watermark
    .filter(col("event_type") == "click")
    .groupBy(
        col("campaign_id"),
        window(col("event_ts"), "10 seconds")
    )
    .agg(count("*").alias("clicks"),
        max("kafka_ts").alias("max_kafka_ts_clicks"))
)

ctr_df = (
    view_counts.join(
        click_counts,
        on=["campaign_id", "window"],
        how="left"
    )
    .withColumn(
        "ctr",
        col("clicks").cast("float") / when(col("views") != 0, col("views"))
    )
    .withColumn(
        "max_kafka_ts",
        greatest(col("max_kafka_ts_views"), col("max_kafka_ts_clicks"))
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
