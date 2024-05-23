import os
import pyodbc
import time
from faker import Faker
import random

# Database connection parameters
DB_NAME = os.getenv("DB_NAME", "SampleDB")
DB_USER = os.getenv("DB_USER", "sa")
DB_PASSWORD = os.getenv("DB_PASSWORD", "SuperSecurePassword1")
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = os.getenv("DB_PORT", "1433")

fake = Faker()


# Function to generate a new sample sale
def generate_sale():
    product_id = random.randint(1, 100)
    customer_id = random.randint(1, 1000)
    sale_date = fake.date_time_this_year()
    quantity = fake.random_int(min=1, max=10)
    unit_price = round(random.uniform(10.0, 100.0), 2)
    total_price = round(quantity * unit_price, 2)
    return product_id, customer_id, sale_date, quantity, unit_price, total_price


# Function to insert a new sale into the database
def insert_sale(conn, sale):
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO sales (product_id, customer_id, sale_date, quantity, unit_price, total_price) VALUES (?, ?, ?, ?, ?, ?)",
        sale,
    )
    conn.commit()
    cursor.close()


# Function to update a sale in the database
def update_sale(conn, new_sale):
    cursor = conn.cursor()
    cursor.execute("SELECT TOP 1 sale_id FROM sales ORDER BY NEWID()")
    row = cursor.fetchone()
    if row:
        sale_id = row[0]
        cursor.execute(
            "UPDATE sales SET product_id=?, customer_id=?, sale_date=?, quantity=?, unit_price=?, total_price=? WHERE sale_id=?",
            (*new_sale, sale_id),
        )
        conn.commit()
        print("Updated sale ID", sale_id, "with new data:", new_sale)
    else:
        print("No sales found in the database, skipping update.")
    cursor.close()


# Function to delete a sale from the database
def delete_sale(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT TOP 1 sale_id FROM sales ORDER BY NEWID()")
    row = cursor.fetchone()
    if row:
        sale_id = row[0]
        cursor.execute("DELETE FROM sales WHERE sale_id=?", (sale_id,))
        conn.commit()
        print("Deleted sale ID", sale_id)
    else:
        print("No sales found in the database, skipping delete.")
    cursor.close()


def connect_to_database(conn_str, retries=3, delay=5):
    attempt = 0
    while attempt < retries:
        try:
            conn = pyodbc.connect(conn_str)
            print("Connected to the database!")
            return conn
        except pyodbc.Error as e:
            print(f"Connection failed: {e}")
            attempt += 1
            if attempt < retries:
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print("All retries failed. Unable to connect to the database.")
    return None


def main():
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={DB_HOST},{DB_PORT};"
        f"DATABASE={DB_NAME};"
        f"UID={DB_USER};"
        f"PWD={DB_PASSWORD};"
        f"TrustServerCertificate=yes"
    )
    conn = connect_to_database(conn_str)

    if not conn:
        print("Unable to connect to the database.")
        exit(1)

    print("Connected to the database!")

    # Main loop to continuously insert, update, or delete sales
    try:
        while True:
            action = random.choices(
                ["insert", "delete", "update"], weights=[0.7, 0.2, 0.1], k=1
            )[0]

            if action == "insert":
                new_sale = generate_sale()
                insert_sale(conn, new_sale)
                print("Inserted new sale:", new_sale)
            elif action == "update":
                new_sale = generate_sale()
                update_sale(conn, new_sale)
            elif action == "delete":
                delete_sale(conn)

            time.sleep(1)  # Wait for 1 second before the next operation
    except KeyboardInterrupt:
        print("Process interrupted by user.")
    finally:
        conn.close()
        print("Database connection closed.")


if __name__ == "__main__":
    main()
