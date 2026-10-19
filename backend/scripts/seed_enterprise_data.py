"""
Realistic Enterprise Dataset Generator & Seeder for Obelisk
Generates multi-entity e-commerce relational dataset:
- customers
- orders
- order_items
- payment_transactions
Includes diverse data types: timestamps with timezones, UUIDs, decimals, categorical statuses, JSON fields, and nullable fields.
"""
import uuid
import json
import random
import psycopg2
from psycopg2.extras import register_uuid
register_uuid()
from datetime import datetime, timedelta, timezone

def generate_and_seed():
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5434,
        user="testuser",
        password="testpass",
        dbname="sourcedb",
        connect_timeout=5,
    )
    conn.autocommit = True
    cur = conn.cursor()

    print("Connected to PostgreSQL on port 5434.")

    # 1. Drop existing e-commerce tables if existing
    cur.execute("""
        DROP TABLE IF EXISTS ecommerce_payment_transactions CASCADE;
        DROP TABLE IF EXISTS ecommerce_order_items CASCADE;
        DROP TABLE IF EXISTS ecommerce_orders CASCADE;
        DROP TABLE IF EXISTS ecommerce_customers CASCADE;
    """)

    # 2. Create Schema
    cur.execute("""
        CREATE TABLE ecommerce_customers (
            customer_id UUID PRIMARY KEY,
            first_name VARCHAR(100) NOT NULL,
            last_name VARCHAR(100) NOT NULL,
            email VARCHAR(255) UNIQUE NOT NULL,
            tier VARCHAR(50) NOT NULL,
            country_code VARCHAR(3) NOT NULL,
            signup_timestamp TIMESTAMPTZ NOT NULL,
            metadata_json JSONB
        );

        CREATE TABLE ecommerce_orders (
            order_id SERIAL PRIMARY KEY,
            order_uuid UUID NOT NULL UNIQUE,
            customer_id UUID NOT NULL REFERENCES ecommerce_customers(customer_id),
            order_status VARCHAR(50) NOT NULL,
            currency VARCHAR(3) NOT NULL,
            subtotal_amount NUMERIC(12, 2) NOT NULL,
            tax_amount NUMERIC(12, 2) NOT NULL,
            discount_amount NUMERIC(12, 2),
            total_amount NUMERIC(12, 2) NOT NULL,
            order_placed_at TIMESTAMPTZ NOT NULL,
            shipping_address JSONB NOT NULL
        );

        CREATE TABLE ecommerce_order_items (
            item_id SERIAL PRIMARY KEY,
            order_id INT NOT NULL REFERENCES ecommerce_orders(order_id),
            product_sku VARCHAR(64) NOT NULL,
            product_category VARCHAR(100) NOT NULL,
            unit_price NUMERIC(10, 2) NOT NULL,
            quantity INT NOT NULL,
            line_total NUMERIC(12, 2) NOT NULL
        );

        CREATE TABLE ecommerce_payment_transactions (
            transaction_id UUID PRIMARY KEY,
            order_id INT NOT NULL REFERENCES ecommerce_orders(order_id),
            payment_method VARCHAR(50) NOT NULL,
            transaction_status VARCHAR(50) NOT NULL,
            amount NUMERIC(12, 2) NOT NULL,
            processor_response_code VARCHAR(32) NOT NULL,
            authorized_at TIMESTAMPTZ NOT NULL
        );
    """)
    print("Tables created successfully.")

    # 3. Seed Customers
    first_names = ["Emma", "Liam", "Olivia", "Noah", "Sophia", "Jackson", "Ava", "Aiden", "Isabella", "Lucas", "Mia", "Ethan", "Harper", "Mason", "Evelyn"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez"]
    tiers = ["Standard", "Silver", "Gold", "Platinum"]
    countries = ["USA", "GBR", "DEU", "FRA", "CAN", "JPN", "AUS"]

    base_time = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    customers = []
    customer_ids = []

    for i in range(50):
        c_id = uuid.uuid4()
        customer_ids.append(c_id)
        fn = random.choice(first_names)
        ln = random.choice(last_names)
        email = f"{fn.lower()}.{ln.lower()}.{i+100}@enterprise-demo.com"
        tier = random.choice(tiers)
        country = random.choice(countries)
        signup_dt = base_time + timedelta(days=random.randint(0, 180), hours=random.randint(0, 23))
        meta = json.dumps({
            "referral_source": random.choice(["organic_search", "google_ads", "linkedin", "partner", "direct"]),
            "preferred_channel": random.choice(["email", "sms", "push_notification"]),
            "vip_flag": tier in ("Gold", "Platinum")
        })
        customers.append((str(c_id), fn, ln, email, tier, country, signup_dt, meta))

    cur.executemany("""
        INSERT INTO ecommerce_customers (customer_id, first_name, last_name, email, tier, country_code, signup_timestamp, metadata_json)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, customers)
    print(f"Seeded {len(customers)} customers.")

    # 4. Seed Orders & Items & Transactions
    statuses = ["completed", "completed", "completed", "shipped", "processing", "pending", "cancelled", "refunded"]
    categories = ["Electronics", "Apparel", "Home & Kitchen", "Books", "Fitness", "Office"]
    methods = ["credit_card", "stripe", "paypal", "apple_pay", "crypto"]

    order_rows = []
    item_rows = []
    tx_rows = []

    order_seq = 1
    for i in range(120):
        c_id = random.choice(customer_ids)
        order_uuid = str(uuid.uuid4())
        status = random.choice(statuses)
        currency = random.choice(["USD", "USD", "EUR", "GBP"])
        order_dt = base_time + timedelta(days=random.randint(60, 280), hours=random.randint(0, 23), minutes=random.randint(0, 59))
        
        # Shipping address JSON
        ship_addr = json.dumps({
            "street": f"{random.randint(100, 9999)} Market Street",
            "city": random.choice(["San Francisco", "Austin", "Berlin", "London", "Toronto", "Tokyo"]),
            "postal_code": f"{random.randint(10000, 99999)}",
            "is_commercial": random.choice([True, False])
        })

        # Calculate items
        num_items = random.randint(1, 4)
        subtotal = 0.0
        order_items_temp = []
        for j in range(num_items):
            cat = random.choice(categories)
            sku = f"SKU-{cat[:3].upper()}-{random.randint(100, 999)}"
            price = round(random.uniform(15.00, 450.00), 2)
            qty = random.randint(1, 3)
            ltotal = round(price * qty, 2)
            subtotal += ltotal
            order_items_temp.append((order_seq, sku, cat, price, qty, ltotal))

        subtotal = round(subtotal, 2)
        tax = round(subtotal * 0.0825, 2)
        discount = round(subtotal * 0.10, 2) if random.random() < 0.4 else None
        total = round(subtotal + tax - (discount or 0.0), 2)

        cur.execute("""
            INSERT INTO ecommerce_orders (
                order_uuid, customer_id, order_status, currency, subtotal_amount,
                tax_amount, discount_amount, total_amount, order_placed_at, shipping_address
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING order_id;
        """, (order_uuid, c_id, status, currency, subtotal, tax, discount, total, order_dt, ship_addr))
        real_order_id = cur.fetchone()[0]

        # Insert items with real_order_id
        for itm in order_items_temp:
            item_rows.append((real_order_id, itm[1], itm[2], itm[3], itm[4], itm[5]))

        # Payment Transaction
        tx_id = str(uuid.uuid4())
        pm = random.choice(methods)
        tx_status = "captured" if status in ("completed", "shipped", "processing") else ("refunded" if status == "refunded" else "failed")
        resp_code = "AUTH_200_SUCCESS" if tx_status in ("captured", "settled") else "AUTH_402_DECLINED"
        tx_rows.append((tx_id, real_order_id, pm, tx_status, total, resp_code, order_dt + timedelta(seconds=random.randint(3, 45))))

        order_seq += 1

    cur.executemany("""
        INSERT INTO ecommerce_order_items (order_id, product_sku, product_category, unit_price, quantity, line_total)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, item_rows)
    print(f"Seeded {len(item_rows)} order items.")

    cur.executemany("""
        INSERT INTO ecommerce_payment_transactions (transaction_id, order_id, payment_method, transaction_status, amount, processor_response_code, authorized_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, tx_rows)
    print(f"Seeded {len(tx_rows)} payment transactions.")

    # Check row counts
    for tbl in ["ecommerce_customers", "ecommerce_orders", "ecommerce_order_items", "ecommerce_payment_transactions"]:
        cur.execute(f"SELECT COUNT(*) FROM {tbl};")
        cnt = cur.fetchone()[0]
        print(f"Table '{tbl}': {cnt} rows")

    cur.close()
    conn.close()
    print("Database seeding completed successfully.")

if __name__ == "__main__":
    generate_and_seed()
