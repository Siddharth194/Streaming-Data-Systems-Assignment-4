import time
import random
import uuid
import math
from confluent_kafka import Producer
import psycopg2
import multiprocessing as mp
import subprocess

KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092',
    'queue.buffering.max.messages': 500000,
}

DB_CONFIG = dict(
    dbname="advertisementdata",
    user="postgres",
    password="pwd",
    host="localhost",
    port=5432
)

TOPIC = "ad-events-2"

def generate_poisson_events(throughput=10, duration=5):
    """
    Generate YSB-style events with Poisson arrivals (simulated time).
    """

    p = Producer(KAFKA_CONFIG)

    ad_ids = [str(uuid.uuid4()) for _ in range(1000)]
    user_ids = [str(uuid.uuid4()) for _ in range(1000)]
    page_ids = [str(uuid.uuid4()) for _ in range(1000)]
    ip_pool = [f"192.168.0.{i}" for i in range(1, 256)]

    ad_types = ["banner", "modal", "sponsored-search", "mail", "mobile"]
    event_types = ["view", "click", "purchase"]

    all_events = []
    total_events = 0
    max_event_time = 0

    ad_to_campaign = {ad_id: i // 100 for i, ad_id in enumerate(ad_ids)}

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

    sim_time = 0
    start_wall = time.time()

    i = 0
    batch_start = start_wall

    while sim_time < duration:
        U = random.random()
        delta_t = -math.log(U) / throughput
        sim_time += delta_t

        event_time = int((start_wall + sim_time) * 1000)

        chosen_ad = random.choice(ad_ids)
        cid = ad_to_campaign[chosen_ad]

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

        total_events += 1
        i += 1

        if i == throughput:
            batch_end = time.time()
            elapsed = batch_end - batch_start
            print(f"Produced {throughput} events in simulated 1 second "
                  f"(real elapsed = {elapsed:.3f} sec, "
                  f"effective rate ≈ {throughput/elapsed:.2f} events/sec). "
                  f"Total events = {total_events}")
            
            batch_start = batch_end
            i = 0
            p.flush(5)

    print(f"Total events by producer = {total_events}")
    print("Max event_time:", max_event_time)
    return all_events, max_event_time



def main():
    throughput = int(input("Enter throughput: "))
    duration = int(input("Enter time to run the system for: "))

    p1 = mp.Process(target=generate_poisson_events, args=(throughput, duration))
    p2 = mp.Process(target=generate_poisson_events, args=(throughput, duration))
    p3 = mp.Process(target=generate_poisson_events, args=(throughput, duration))
    p4 = mp.Process(target=generate_poisson_events, args=(throughput, duration))
    # p5 = mp.Process(target=generate_poisson_events, args=(throughput, iterations, 4))


    time.sleep(2)
    p1.start()
    p2.start()
    p3.start()
    p4.start()
    # p5.start()

    p1.join()
    p2.join()
    p3.join()
    p4.join()
    # p5.join()

    print("All workers finished")

    time.sleep(5)
    
    subprocess.run(["python", "average_latency.py"])


if __name__ == "__main__":
    main()
