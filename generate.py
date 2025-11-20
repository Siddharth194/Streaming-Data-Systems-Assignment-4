import random
import multiprocessing as mp
import time
from confluent_kafka import Producer
import uuid
import psycopg2
import subprocess
import json

KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092'
}

TOPIC = "ad-events-2"

def generate_ysb_events(throughput=10, iterations=5, worker_number=0):
    p = Producer(KAFKA_CONFIG)

    user_ids = [str(uuid.uuid4()) for _ in range(1000)]
    page_ids = [str(uuid.uuid4()) for _ in range(1000)]
    ad_ids = [str(uuid.uuid4()) for _ in range(1000)]
    ip_pool = [f"192.168.0.{i}" for i in range(1, 256)]

    ad_to_campaign = {ad_id: i // 100 for i, ad_id in enumerate(ad_ids)}

    ad_types = ["banner", "modal", "sponsored-search", "mail", "mobile"]
    event_types = ["view", "click", "purchase"]

    max_time = 0

    time_now = int(time.time() * 1000)

    for _ in range(iterations):
        # time_now = int(time.time() * 1000)
        time_now += 1000
        for i in range(throughput):
            event_time = time_now + (i % 1000)

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
                "campaign_id": cid,
                "production_timestamp": time.time()*1000
            }

            print(
                f"{event['event_time']}\t{event['user_id']}\t{event['page_id']}\t"
                f"{event['ad_id']}\t{event['ad_type']}\t{event['event_type']}\t{event['ip_address']}"
            )

            p.produce(
                TOPIC,
                key=str(cid).encode(),
                value=json.dumps(event).encode(),      # <- was str(event)
            )

            if max_time < event_time:
                max_time = event_time
            
            if i % 9999 == 0:
                p.flush(5) 

        time.sleep(1)


    p.flush()
    print(f"[Worker {worker_number}] Finished. Max event_time: {max_time}")


def main():
    throughput = int(input("Enter throughput: "))
    iterations = int(input("Enter time to run the system for: "))

    p1 = mp.Process(target=generate_ysb_events, args=(throughput, iterations, 0))
    p2 = mp.Process(target=generate_ysb_events, args=(throughput, iterations, 1))
    p3 = mp.Process(target=generate_ysb_events, args=(throughput, iterations, 2))
    p4 = mp.Process(target=generate_ysb_events, args=(throughput, iterations, 3))
    p5 = mp.Process(target=generate_ysb_events, args=(throughput, iterations, 4))


    time.sleep(2)
    p1.start()
    p2.start()
    p3.start()
    p4.start()

    p1.join()
    p2.join()
    p3.join()
    p4.join()

    print("All workers finished")

    time.sleep(5)

if __name__ == "__main__":
    main()
