import os
import psycopg2
import time
from faker import Faker
import random

# Database connection parameters
DB_NAME = os.getenv("POSTGRES_DB", "postgres")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")

fake = Faker()


# Function to generate a new sample transaction with anomalies
def generate_transaction():
    user_id = random.randint(1, 20)
    transaction_date = fake.date_time_this_year()

    # Introduce randomness for transaction amount
    anomaly_chance = random.random()
    if anomaly_chance < 0.05:  # 5% chance for anomaly
        amount = round(
            fake.random.uniform(1000.0, 10000.0), 2
        )  # Generate unusually high amount
    elif anomaly_chance < 0.1:  # 5% chance for anomaly
        amount = round(
            fake.random.uniform(0.01, 1.0), 2
        )  # Generate unusually low amount
    else:
        amount = round(
            fake.random.uniform(10.0, 1000.0), 2
        )  # Normal transaction amount

    return user_id, transaction_date, amount


# Function to insert a new transaction into the database
def insert_transaction(conn, transaction):
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO transactions (user_id, transaction_date, amount) VALUES (%s, %s, %s)",
        transaction,
    )
    conn.commit()
    cursor.close()


# Function to update a transaction in the database
def update_transaction(conn, new_transaction):
    cursor = conn.cursor()
    cursor.execute("SELECT transaction_id FROM transactions ORDER BY RANDOM() LIMIT 1")
    row = cursor.fetchone()
    if row:
        transaction_id = row[0]
        cursor.execute(
            "UPDATE transactions SET user_id=%s, transaction_date=%s, amount=%s WHERE transaction_id=%s",
            (*new_transaction, transaction_id),
        )
        conn.commit()
        print(
            "Updated transaction ID", transaction_id, "with new data:", new_transaction
        )
    else:
        print("No transactions found in the database, skipping update.")
    cursor.close()


# Function to delete a transaction from the database
def delete_transaction(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT transaction_id FROM transactions ORDER BY RANDOM() LIMIT 1")
    row = cursor.fetchone()
    if row:
        transaction_id = row[0]
        cursor.execute(
            "DELETE FROM transactions WHERE transaction_id=%s", (transaction_id,)
        )
        conn.commit()
        print("Deleted transaction ID", transaction_id)
    else:
        print("No transactions found in the database, skipping delete.")
    cursor.close()


def main():
    conn_str = f"dbname='{DB_NAME}' user='{DB_USER}' password='{DB_PASSWORD}' host='{DB_HOST}' port='{DB_PORT}'"
    conn = psycopg2.connect(conn_str)
    print("Connected to the database!")

    # Main loop to continuously insert, update, or delete transactions
    try:
        while True:
            action = random.choices(
                ["insert", "delete", "update"], weights=[0.7, 0.2, 0.1], k=1
            )[0]

            if action == "insert":
                new_transaction = generate_transaction()
                insert_transaction(conn, new_transaction)
                print("Inserted new transaction:", new_transaction)
            elif action == "update":
                new_transaction = generate_transaction()
                update_transaction(conn, new_transaction)
            elif action == "delete":
                delete_transaction(conn)

            time.sleep(1)  # Wait for 1 second before the next operation
    except KeyboardInterrupt:
        print("Process interrupted by user.")
    finally:
        conn.close()
        print("Database connection closed.")


if __name__ == "__main__":
    main()
