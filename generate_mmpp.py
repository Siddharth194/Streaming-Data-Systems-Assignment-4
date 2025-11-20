import time
import random
import uuid
import math
import subprocess

import multiprocessing as mp

from confluent_kafka import Producer
import psycopg2


KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092',
    'queue.buffering.max.messages': 500000,
    'queue.buffering.max.kbytes': 2097151,
    'linger.ms': 5,
    'batch.num.messages': 10000,
}
DB_CONFIG = dict(
    dbname="advertisementdata",
    user="postgres",
    password="pwd",
    host="localhost",
    port=5432
)

TOPIC = "ad-events-2"

def generate_bursty_events(T=1000, iterations=2, d_H=2, d_L=8, factor=4.5):
    """
    Deterministic ON/OFF bursty generator with average throughput T.

    Args:
        T: target average throughput (events/sec)
        iterations: number of ON+OFF cycles
        d_H: duration of HIGH state (seconds)
        d_L: duration of LOW state (seconds)
        factor: how much faster bursts are than average (λ_H = factor * T)
    """

    ad_ids = [str(uuid.uuid4()) for _ in range(1000)]
    user_ids = [str(uuid.uuid4()) for _ in range(1000)]
    page_ids = [str(uuid.uuid4()) for _ in range(1000)]
    ip_pool = [f"192.168.0.{i}" for i in range(1, 256)]

    ad_types = ["banner", "modal", "sponsored-search", "mail", "mobile"]
    event_types = ["view", "click", "purchase"]

    all_events = []
    max_event_time = 0

    ad_to_campaign = {ad_id: i // 100 for i, ad_id in enumerate(ad_ids)}

    p = Producer(KAFKA_CONFIG)

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("DELETE FROM mappings;")
    for ad_id, campaign_id in ad_to_campaign.items():
        cur.execute(
            "INSERT INTO mappings (ad_id, campaign_id) VALUES (%s, %s);",
            (ad_id, campaign_id)
        )
    conn.commit()
    cur.close()
    conn.close()

    λ_H = factor * T
    λ_L = (T * (d_H + d_L) - λ_H * d_H) / d_L

    if λ_L < 0:
        raise ValueError(
            f"Invalid parameters: λ_L={λ_L:.2f}. "
            f"Reduce factor ({factor}) or increase d_L."
        )

    print(f"Using λ_H={λ_H:.2f}, λ_L={λ_L:.2f} to maintain average {T} ev/s")

    start_time = time.time()

    for it in range(iterations):
        p.flush(5)
        # --- HIGH state ---

        end_high = start_time + d_H

        count = 0

        while start_time < end_high:
            dt = -math.log(random.random()) / λ_H
            start_time += dt

            chosen_ad = random.choice(ad_ids)
            cid = ad_to_campaign[chosen_ad]

            event_time = int(start_time * 1000)
            event = {
                "user_id": random.choice(user_ids),
                "page_id": random.choice(page_ids),
                "ad_id": chosen_ad,
                "ad_type": random.choice(ad_types),
                "event_type": random.choice(event_types),
                "event_time": event_time,
                "ip_address": random.choice(ip_pool),
            }
            all_events.append(event)
            if event_time > max_event_time:
                max_event_time = event_time

            count += 1

            print(
                f"{event['event_time']}\t"
                f"{event['user_id']}\t{event['page_id']}\t{event['ad_id']}\t"
                f"{event['ad_type']}\t{event['event_type']}\t{event['ip_address']}"
            )

            p.produce(
            TOPIC,
            key=str(cid).encode(),
            value=str(event).encode(),
            )

        temp_count = count
        print(f"High throughput events produced = {count}")

        # --- LOW state ---
        end_low = start_time + d_L
        if λ_L > 0:
            while start_time < end_low:
                dt = -math.log(random.random()) / λ_L
                start_time += dt

                chosen_ad = random.choice(ad_ids)
                cid = ad_to_campaign[chosen_ad]

                event_time = int(start_time * 1000)
                event = {
                    "user_id": random.choice(user_ids),
                    "page_id": random.choice(page_ids),
                    "ad_id": chosen_ad,
                    "ad_type": random.choice(ad_types),
                    "event_type": random.choice(event_types),
                    "event_time": event_time,
                    "ip_address": random.choice(ip_pool),
                }
                all_events.append(event)
                if event_time > max_event_time:
                    max_event_time = event_time
                
                count += 1

                print(
                    f"{event['event_time']}\t"
                    f"{event['user_id']}\t{event['page_id']}\t{event['ad_id']}\t"
                    f"{event['ad_type']}\t{event['event_type']}\t{event['ip_address']}"
                )

                p.produce(
                TOPIC,
                key=str(cid).encode(),
                value=str(event).encode(),
                )
            print(f"High throughput events produced = {count-temp_count}")

        else:
            # No events during OFF period
            time.sleep(d_L)

        print(f"Produced {count} events for iteration i = {it}")

    print("Max event_time:", max_event_time)
    return all_events, max_event_time


if __name__ == "__main__":
    throughput = int(input("Enter throughput: "))
    iterations = int(input("Enter number of iterations: "))
    # d_H = int(input("Enter time to run the system for (high): "))
    # d_L = int(input("Enter time to run the system for (low): "))

    p1 = mp.Process(target=generate_bursty_events, args=(throughput, iterations ))
    p2 = mp.Process(target=generate_bursty_events, args=(throughput, iterations ))
    p3 = mp.Process(target=generate_bursty_events, args=(throughput, iterations ))
    p4 = mp.Process(target=generate_bursty_events, args=(throughput, iterations ))


    time.sleep(2)
    p1.start()
    p2.start()
    p3.start()
    p4.start()

    p1.join()
    p2.join()
    p3.join()
    p4.join()
    # p5.join()

    print("All workers finished")

    time.sleep(5)
    
    subprocess.run(["python", "average_latency.py"])
