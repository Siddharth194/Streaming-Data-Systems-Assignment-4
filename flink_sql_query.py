import os
import pickle
from pyflink.table import TableEnvironment, EnvironmentSettings
from pyflink.table.types import DataTypes

# --- Flink Environment Setup ---
print("Initializing Flink Table Environment...")
env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)
t_env.get_config().set("table.exec.source.idle-timeout", "40 s")

# --- Dependency Configuration ---
KAFKA_CONNECTOR_JAR = "/home/siddharth/StreamingDataSystems/assn_4/Streaming-Data-Systems-Assignment-4/flink-sql-connector-kafka-4.0.1-2.0.jar"
t_env.get_config().set("pipeline.jars", f"file://{KAFKA_CONNECTOR_JAR}")

# --- Constants ---
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC = "ad-events-2"
OUTPUT_TOPIC = "results"

# --- 1. Define Source Table (Kafka) ---
print(f"Configuring Kafka Source: {INPUT_TOPIC}")

t_env.execute_sql(f"""
CREATE TABLE ad_events (
    user_id STRING,
    page_id STRING,
    ad_id STRING,
    ad_type STRING,
    event_type STRING,
    event_time BIGINT,     -- ms from Spark
    ip_address STRING,
    -- insert_time BIGINT,      -- ms from Spark
    campaign_id INT,
    production_timestamp BIGINT,
    event_ts AS TO_TIMESTAMP_LTZ(event_time, 3),

    WATERMARK FOR event_ts AS event_ts - INTERVAL '1' SECOND

) WITH (
  'connector' = 'kafka',
  'topic' = 'ad-events-2',
  'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
  'properties.group.id' = 'flink_consumer_group',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

""")


# t_env.execute_sql("SELECT * FROM ad_events;").print()    

# --- 3. Define Sink Table (Not Used) ---
# The sink is not defined as we are printing the results.

# --- 4. Flink SQL Transformation (Debug: Print Join Result) ---
print("Executing Flink SQL Query ")
t_env.execute_sql(f"""
    CREATE TABLE results (
        window_start STRING,
        campaign_id INT,
        ctr DOUBLE,
        max_produce_time BIGINT
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{OUTPUT_TOPIC}',
        'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
        'format' = 'json'
    )
""")

join_debug_query = f"""
INSERT INTO results
SELECT
        CAST(TUMBLE_START(event_ts, INTERVAL '10' SECOND) AS STRING) AS window_start,
        campaign_id,
        CAST(SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) AS DOUBLE) AS clicks,
        (SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) +1) AS views,
        CAST(SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) AS DOUBLE) /
        (SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) + 1) AS ctr,
        MAX(production_timestamp) AS max_produce_time           -- renamed from produce_time
    FROM
        ad_events
    GROUP BY
        TUMBLE(event_ts, INTERVAL '10' SECOND),      -- event_ts = corrected Spark timestamp
        campaign_id
"""


t_env.execute_sql(join_debug_query).wait()