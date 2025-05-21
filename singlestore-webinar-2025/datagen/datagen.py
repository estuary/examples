import os
import time
import random
import psycopg2
import sqlalchemy
from faker import Faker
from dotenv import load_dotenv
from google.cloud.sql.connector import Connector
from datetime import datetime

# Load environment variables
load_dotenv()

# Local Postgres settings
DB_NAME = os.getenv("POSTGRES_DB", "postgres")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")

# Cloud SQL (optional)
USE_CLOUD_SQL = os.getenv("USE_CLOUD_SQL", "false").lower() == "true"
CLOUD_SQL_CONNECTION_NAME = os.getenv("CLOUD_SQL_CONNECTION_NAME")
CLOUD_SQL_USER = os.getenv("CLOUD_SQL_USER")
CLOUD_SQL_PASSWORD = os.getenv("CLOUD_SQL_PASSWORD")
CLOUD_SQL_DB = os.getenv("CLOUD_SQL_DB")

fake = Faker()
connector = Connector()

ORDER_STATUSES = ["placed", "packed", "shipped", "delivered", "cancelled"]
STATUS_FLOW = {
    "placed": ["packed", "cancelled"],
    "packed": ["shipped", "cancelled"],
    "shipped": ["delivered", "cancelled"],
    "delivered": [],
    "cancelled": []
}

PRODUCT_CATALOG = [
    "Dog Chew Toy",
    "Cat Scratching Post",
    "Aquarium Filter",
    "Bird Feeder",
    "Hamster Wheel",
    "Flea Shampoo",
    "Pet Carrier",
    "Puppy Training Pads",
    "Lizard Heat Lamp",
    "Fish Food Pellets"
]

def getconn():
    return connector.connect(
        CLOUD_SQL_CONNECTION_NAME,
        "pg8000",
        user=CLOUD_SQL_USER,
        password=CLOUD_SQL_PASSWORD,
        db=CLOUD_SQL_DB,
    )

def get_db_connection():
    if USE_CLOUD_SQL:
        pool = sqlalchemy.create_engine("postgresql+pg8000://", creator=getconn)
        return pool.raw_connection()
    else:
        conn_str = f"dbname='{DB_NAME}' user='{DB_USER}' password='{DB_PASSWORD}' host='{DB_HOST}' port='{DB_PORT}'"
        return psycopg2.connect(conn_str)

def create_orders_table_if_not_exists(conn):
    cursor = conn.cursor()
    cursor.execute("""
        CREATE EXTENSION IF NOT EXISTS "pgcrypto";

        CREATE TABLE IF NOT EXISTS orders (
            order_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            customer_name TEXT NOT NULL,
            product_name TEXT NOT NULL,
            status TEXT NOT NULL CHECK (
                status IN ('placed', 'packed', 'shipped', 'delivered', 'cancelled')
            ),
            created_at TIMESTAMPTZ DEFAULT now()
        );
    """)
    conn.commit()
    cursor.close()

def generate_order_record():
    customer_name = fake.name()
    product_name = random.choice(PRODUCT_CATALOG)
    status = "placed"
    created_at = fake.date_time_this_year()
    return customer_name, product_name, status, created_at

def insert_order(conn):
    cursor = conn.cursor()
    order = generate_order_record()
    cursor.execute("""
        INSERT INTO orders (customer_name, product_name, status, created_at)
        VALUES (%s, %s, %s, %s)
        RETURNING order_id
    """, order)
    conn.commit()
    cursor.close()

def update_random_order(conn):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT order_id, status
        FROM orders
        WHERE status != 'delivered' AND status != 'cancelled'
        ORDER BY RANDOM()
        LIMIT 1
    """)
    row = cursor.fetchone()

    if not row:
        cursor.close()
        return

    order_id, current_status = row
    next_status_options = STATUS_FLOW.get(current_status, [])
    if not next_status_options:
        cursor.close()
        return

    next_status = random.choice(next_status_options)

    cursor.execute("""
        UPDATE orders
        SET status = %s
        WHERE order_id = %s
    """, (next_status, order_id))

    conn.commit()
    cursor.close()
    print(f"Updated order {order_id} from {current_status} → {next_status}")

def main():
    conn = get_db_connection()
    print("Connected to the database.")

    try:
        create_orders_table_if_not_exists(conn)
        print("Orders table is ready.")

        while True:
            action = random.choices(["insert", "update"], weights=[0.7, 0.3], k=1)[0]
            if action == "insert":
                insert_order(conn)
                print("Inserted new order.")
            else:
                update_random_order(conn)

            time.sleep(0.1)

    except KeyboardInterrupt:
        print("Stopped by user.")
    finally:
        conn.close()
        print("Database connection closed.")

if __name__ == "__main__":
    main()
