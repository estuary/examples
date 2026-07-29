"""Continuously write realistic orders into Postgres.

The generator simulates a live operational table so the CDC pipeline always has
something to stream. Every tick it:

  * inserts brand new orders,
  * advances the status of recent orders (pending -> shipped -> delivered),
  * occasionally deletes an order,

so the capture sees inserts, updates, and deletes -- all three operations that
real change data capture has to handle.

Configuration comes from two environment variables:

  DATABASE_URL      postgresql://user:pass@host:port/db
  RATE_PER_SECOND   how many operations to perform per second (default: 2)
"""

import os
import random
import time

import psycopg2
from faker import Faker

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/postgres"
)
RATE_PER_SECOND = float(os.getenv("RATE_PER_SECOND", "2"))

# The lifecycle a real order moves through. We nudge recent orders one step
# forward at a time so updates look believable.
NEXT_STATUS = {
    "pending": "shipped",
    "shipped": "delivered",
}

fake = Faker()


def connect():
    """Connect to Postgres, retrying while the container finishes starting."""
    while True:
        try:
            conn = psycopg2.connect(DATABASE_URL)
            conn.autocommit = True
            print("Connected to Postgres.", flush=True)
            return conn
        except psycopg2.OperationalError as exc:
            print(f"Postgres not ready yet ({exc}); retrying in 2s...", flush=True)
            time.sleep(2)


def insert_order(cur):
    cur.execute(
        """
        insert into public.orders (customer_name, amount, status)
        values (%s, %s, 'pending')
        returning order_id
        """,
        (fake.name(), round(random.uniform(5.0, 500.0), 2)),
    )
    order_id = cur.fetchone()[0]
    print(f"insert  {order_id}", flush=True)


def advance_order(cur):
    """Move a recent, non-terminal order to its next status."""
    cur.execute(
        """
        select order_id, status
        from public.orders
        where status in ('pending', 'shipped')
        order by updated_at asc
        limit 1
        """
    )
    row = cur.fetchone()
    if row is None:
        return
    order_id, status = row
    cur.execute(
        """
        update public.orders
        set status = %s, event_ts = now(), updated_at = now()
        where order_id = %s
        """,
        (NEXT_STATUS[status], order_id),
    )
    print(f"update  {order_id} -> {NEXT_STATUS[status]}", flush=True)


def delete_order(cur):
    cur.execute("select order_id from public.orders order by random() limit 1")
    row = cur.fetchone()
    if row is None:
        return
    (order_id,) = row
    cur.execute("delete from public.orders where order_id = %s", (order_id,))
    print(f"delete  {order_id}", flush=True)


def main():
    conn = connect()
    cur = conn.cursor()

    interval = 1.0 / RATE_PER_SECOND if RATE_PER_SECOND > 0 else 0.5
    print(f"Generating orders at ~{RATE_PER_SECOND}/s. Ctrl-C to stop.", flush=True)

    try:
        while True:
            action = random.choices(
                ["insert", "update", "delete"],
                weights=[0.6, 0.3, 0.1],
                k=1,
            )[0]
            try:
                if action == "insert":
                    insert_order(cur)
                elif action == "update":
                    advance_order(cur)
                else:
                    delete_order(cur)
            except psycopg2.Error as exc:
                print(f"Query failed ({exc}); reconnecting...", flush=True)
                conn = connect()
                cur = conn.cursor()

            time.sleep(interval)
    except KeyboardInterrupt:
        print("Stopping generator.", flush=True)
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
