import os
import time
from faker import Faker
import random
from google.cloud.sql.connector import Connector
import sqlalchemy

from dotenv import load_dotenv

load_dotenv()

# Database connection parameters
DB_NAME = os.getenv('DB_NAME', "postgres")
DB_USER = os.getenv('DB_USER', "postgres")
DB_PASSWORD = os.getenv('DB_PASSWORD', "postgres")

INSTANCE_CONNECTION_NAME = f"{os.getenv('GCP_PROJECT_ID')}:{os.getenv('GCP_REGION')}:{os.getenv('GCP_CLOUDSQL_INSTANCE_NAME')}"
print(f"Your instance connection name is: {INSTANCE_CONNECTION_NAME}")

# initialize Connector object
connector = Connector()

# function to return the database connection object
def getconn():
    conn = connector.connect(
        INSTANCE_CONNECTION_NAME,
        "pg8000",
        user=DB_USER,
        password=DB_PASSWORD,
        db=DB_NAME
    )
    return conn

# create connection pool with 'creator' argument to our connection object function
pool = sqlalchemy.create_engine(
    "postgresql+pg8000://",
    creator=getconn,
)

fake = Faker()

def create_sales_table(db_conn):
    create_table_stmt = sqlalchemy.text("""
    CREATE TABLE IF NOT EXISTS sales (
        sale_id SERIAL PRIMARY KEY,
        product_id INTEGER NOT NULL,
        customer_id INTEGER NOT NULL,
        sale_date TIMESTAMP NOT NULL,
        quantity INTEGER NOT NULL,
        unit_price DECIMAL(10, 2) NOT NULL,
        total_price DECIMAL(10, 2) NOT NULL
    )
    """)
    db_conn.execute(create_table_stmt)
    db_conn.commit()
    print("Sales table created or already exists.")


# Function to generate a new sample sale
def generate_sale():
    product_id = random.randint(1, 100)
    customer_id = random.randint(1, 1000)
    sale_date = fake.date_time_this_year()
    quantity = fake.random_int(min=1, max=10)
    unit_price = round(fake.random.uniform(10.0, 100.0), 2)
    total_price = round(quantity * unit_price, 2)
    return product_id, customer_id, sale_date, quantity, unit_price, total_price

# Function to insert a new sale into the database
def insert_sale(db_conn, sale):
    insert_stmt = sqlalchemy.text(
        "INSERT INTO sales (product_id, customer_id, sale_date, quantity, unit_price, total_price) "
        "VALUES (:product_id, :customer_id, :sale_date, :quantity, :unit_price, :total_price)"
    )
    db_conn.execute(insert_stmt, parameters={
        "product_id": sale[0],
        "customer_id": sale[1],
        "sale_date": sale[2],
        "quantity": sale[3],
        "unit_price": sale[4],
        "total_price": sale[5]
    })
    db_conn.commit()

# Function to update a sale in the database
def update_sale(db_conn, new_sale):
    select_stmt = sqlalchemy.text("SELECT sale_id FROM sales ORDER BY RANDOM() LIMIT 1")
    result = db_conn.execute(select_stmt).fetchone()
    if result:
        sale_id = result[0]
        update_stmt = sqlalchemy.text(
            "UPDATE sales SET product_id=:product_id, customer_id=:customer_id, sale_date=:sale_date, "
            "quantity=:quantity, unit_price=:unit_price, total_price=:total_price WHERE sale_id=:sale_id"
        )
        db_conn.execute(update_stmt, parameters={
            "product_id": new_sale[0],
            "customer_id": new_sale[1],
            "sale_date": new_sale[2],
            "quantity": new_sale[3],
            "unit_price": new_sale[4],
            "total_price": new_sale[5],
            "sale_id": sale_id
        })
        db_conn.commit()
        print("Updated sale ID", sale_id, "with new data:", new_sale)
    else:
        print("No sales found in the database, skipping update.")

# Function to delete a sale from the database
def delete_sale(db_conn):
    select_stmt = sqlalchemy.text("SELECT sale_id FROM sales ORDER BY RANDOM() LIMIT 1")
    result = db_conn.execute(select_stmt).fetchone()
    if result:
        sale_id = result[0]
        delete_stmt = sqlalchemy.text("DELETE FROM sales WHERE sale_id=:sale_id")
        db_conn.execute(delete_stmt, parameters={"sale_id": sale_id})
        db_conn.commit()
        print("Deleted sale ID", sale_id)
    else:
        print("No sales found in the database, skipping delete.")

def main():
    # Create the sales table if it doesn't exist
    with pool.connect() as db_conn:
        create_sales_table(db_conn)

    # Main loop to continuously insert, update, or delete sales
    try:
        while True:
            with pool.connect() as db_conn:
                action = random.choices(['insert', 'delete', 'update'], weights=[0.7, 0.2, 0.1], k=1)[0]

                if action == 'insert':
                    new_sale = generate_sale()
                    insert_sale(db_conn, new_sale)
                    print("Inserted new sale:", new_sale)
                elif action == 'update':
                    new_sale = generate_sale()
                    update_sale(db_conn, new_sale)
                elif action == 'delete':
                    delete_sale(db_conn)

            time.sleep(1)  # Wait for 1 second before the next operation
    except KeyboardInterrupt:
        print("Process interrupted by user.")
    finally:
        connector.close()
        print("Database connection closed.")

if __name__ == "__main__":
    main()
