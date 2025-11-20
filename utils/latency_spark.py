from confluent_kafka import Consumer, TopicPartition
from datetime import datetime, timezone
import json
import uuid


BOOTSTRAP_SERVERS = "localhost:9092"
RESULT_TOPIC = "results"


# ------------------------------------------------------------
# Helper: parse "2025-11-20T01:57:28.492+05:30"
# ------------------------------------------------------------
def parse_ts(ts_str: str) -> int:
    """
    Converts ISO timestamp with timezone into epoch milliseconds.
    """
    dt = datetime.fromisoformat(ts_str)
    return int(dt.timestamp() * 1000)


# ------------------------------------------------------------
# Kafka Consumer Setup
# ------------------------------------------------------------
consumer_conf = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": f"latency-reader-{uuid.uuid4()}",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
}

consumer = Consumer(consumer_conf)
consumer.subscribe([RESULT_TOPIC])

print("Reading all records from Kafka...\n")

records_by_window = {}


# ------------------------------------------------------------
# Read all messages from topic
# ------------------------------------------------------------
while True:
    msg = consumer.poll(0.1)
    if msg is None:
        continue
    if msg.error():
        print("Error:", msg.error())
        continue

    # kafka append timestamp
    _, kafka_append_ts = msg.timestamp()   # epoch millis

    value = json.loads(msg.value().decode("utf-8"))

    window_start = value["window_start"]
    max_kafka_ts_str = value["max_kafka_ts"]

    if window_start not in records_by_window:
        records_by_window[window_start] = []

    records_by_window[window_start].append({
        "max_kafka_ts_parsed": parse_ts(max_kafka_ts_str),
        "kafka_append_ts": kafka_append_ts,
        "raw": value
    })

    # detect end of partition
    tp = TopicPartition(msg.topic(), msg.partition())
    pos = consumer.position([tp])[0].offset
    end = consumer.get_watermark_offsets(tp)[1]
    if pos >= end:
        break

consumer.close()


# ------------------------------------------------------------
# LATENCY CALCULATION
# ------------------------------------------------------------
print("\n--- LATENCY REPORT ---")

for window_start, recs in records_by_window.items():

    # --------------------------------------------------------
    # Find the absolute latest max_kafka_ts among ALL campaigns
    # --------------------------------------------------------
    latest_max_kafka_ts = max(r["max_kafka_ts_parsed"] for r in recs)

    print(f"\nWindow: {window_start}")
    print(f"  Latest max_kafka_ts: {latest_max_kafka_ts} ms")

    # --------------------------------------------------------
    # Per-record latency
    # --------------------------------------------------------
    with open('average_latency_spark','a') as f:
        for r in recs:
            append_ts = r["kafka_append_ts"]
            latency = append_ts - latest_max_kafka_ts

            print(f"    Record for campaign {r['raw']['campaign_id']}:")
            print(f"        kafka_append_ts = {append_ts}")
            print(f"        latency = {latency} ms")
            f.write(f"{latency}\n")
