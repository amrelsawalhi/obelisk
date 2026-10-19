"""
Seed realistic clickstream & user telemetry events into MongoDB port 27018 (DB: sourcedb, coll: clickstream_events)
"""
import random
import uuid
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient
from app.core.host_resolver import resolve_connection_host

def seed_mongo():
    host = resolve_connection_host("127.0.0.1", 27018)
    client = MongoClient(f"mongodb://mongoadmin:mongopass@{host}:27018/?authSource=admin", serverSelectionTimeoutMS=5000)
    db = client["sourcedb"]
    coll = db["clickstream_events"]
    coll.drop()

    events = []
    base_time = datetime(2026, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
    event_types = ["page_view", "product_clicked", "add_to_cart", "remove_from_cart", "checkout_step", "promo_applied"]
    devices = ["mobile_ios", "mobile_android", "desktop_chrome", "desktop_safari", "desktop_edge"]

    for i in range(250):
        evt_type = random.choice(event_types)
        dt = base_time + timedelta(days=random.randint(0, 90), hours=random.randint(0, 23), minutes=random.randint(0, 59))
        doc = {
            "session_id": str(uuid.uuid4()),
            "user_id": random.randint(100, 150),
            "event_type": evt_type,
            "device": random.choice(devices),
            "page_path": f"/products/{random.choice(['electronics', 'apparel', 'home', 'fitness'])}/{random.randint(1, 50)}",
            "referrer": random.choice(["google", "direct", "twitter", "facebook", "newsletter", None]),
            "timestamp": dt,
            "latency_ms": random.randint(45, 650),
            "cart_size": random.randint(0, 5) if evt_type in ("add_to_cart", "checkout_step") else 0,
            "client_ip": f"192.168.{random.randint(1, 254)}.{random.randint(1, 254)}"
        }
        events.append(doc)

    coll.insert_many(events)
    print(f"Seeded {coll.count_documents({})} clickstream events into MongoDB.")
    client.close()

if __name__ == "__main__":
    seed_mongo()
