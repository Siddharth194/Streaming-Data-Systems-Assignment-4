from confluent_kafka import Consumer, TopicPartition
from datetime import datetime, timezone
import json
import uuid

BOOTSTRAP_SERVERS = "localhost:9092"
RESULT_TOPIC = "results"

# ------------------------------------------------------------
# Setup the Consumer
# ------------------------------------------------------------
consumer_conf = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": f"latency-debugger-{uuid.uuid4()}",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
}

consumer = Consumer(consumer_conf)
consumer.subscribe([RESULT_TOPIC])

print("Reading messages...")

records_by_window = {}

# ------------------------------------------------------------
# Read until end of topic
# ------------------------------------------------------------
while True:
    msg = consumer.poll(timeout=0.1)
    if msg is None:
        continue
    if msg.error():
        print("Error:", msg.error())
        continue

    # Kafka log append timestamp (tuple: (type, ts_millis))
    _, kafka_ts = msg.timestamp()

    value = json.loads(msg.value().decode("utf-8"))
    window_start = value["window_start"]
    max_produce_time = value["max_produce_time"]

    if window_start not in records_by_window:
        records_by_window[window_start] = []

    records_by_window[window_start].append({
        "max_produce_time": max_produce_time,
        "kafka_timestamp": kafka_ts
    })

    # Check if reached end of partition
    pos = consumer.position([TopicPartition(msg.topic(), msg.partition())])[0].offset
    end = consumer.get_watermark_offsets(TopicPartition(msg.topic(), msg.partition()))[1]

    if pos >= end:
        break

consumer.close()


print("\n--- LATENCY REPORT ---")

for window_start, recs in records_by_window.items():

    # Latest max_produce_time among all records in this window
    max_produce_ts = max((r["max_produce_time"]) for r in recs)

    print(f"\nWindow: {window_start}")
    print(f"  Max produce time (ms): {max_produce_ts}")

    with open('average_latency_flink','a') as f:
        for r in recs:
            kafka_ts = r["kafka_timestamp"]
            latency_ms = kafka_ts - max_produce_ts

            print(f"    Kafka Append: {kafka_ts}")
            print(f"    Latency: {latency_ms} ms")

            f.write(f"{latency_ms}\n")
