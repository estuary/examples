import os
import psycopg2
import time
import random
import openai
from faker import Faker
from google.cloud.sql.connector import Connector
import sqlalchemy
import pathlib

from dotenv import load_dotenv

load_dotenv()

# Database connection parameters
DB_NAME = os.getenv("POSTGRES_DB", "postgres")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")

# GCP Cloud SQL parameters
USE_CLOUD_SQL = os.getenv("USE_CLOUD_SQL", "false").lower() == "true"
CLOUD_SQL_CONNECTION_NAME = os.getenv(
    "CLOUD_SQL_CONNECTION_NAME"
)
CLOUD_SQL_USER = os.getenv("CLOUD_SQL_USER")
CLOUD_SQL_PASSWORD = os.getenv("CLOUD_SQL_PASSWORD")
CLOUD_SQL_DB = os.getenv("CLOUD_SQL_DB")

# OpenAI API key (set your key as an environment variable)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
openai.api_key = OPENAI_API_KEY

fake = Faker()

# Initialize the Cloud SQL Python Connector object
connector = Connector()

# Function to get Cloud SQL connection
def getconn():
    conn = connector.connect(
        CLOUD_SQL_CONNECTION_NAME,
        "pg8000",
        user=CLOUD_SQL_USER,
        password=CLOUD_SQL_PASSWORD,
        db=CLOUD_SQL_DB,
    )
    return conn

# OpenAI function to generate realistic reviews
def generate_review_text(product_name):
    try:

        sentiment = random.choices(["positive", "negative"], weights=[0.8, 0.2], k=1)[0]

        if sentiment == "positive":
            prompt = f"""
            Write a short, detailed and realistic customer review for a product called '{product_name}' in a pet store. Positive sentiment.
            """
        else:    
            prompt = f"""
            Write a short, detailed and realistic customer review for a product called '{product_name}' in a pet store. Negative sentiment.
            """

        client = openai.OpenAI()
        completion = client.chat.completions.create(
            model="gpt-3.5-turbo-0125",
            messages=[
                {
                    "role": "system",
                    "content": "You are a bot that generates realistic customer reviews for pet store products.",
                },
                {"role": "user", "content": prompt},
            ],
            max_tokens=100,
        )

        review = completion.choices[0].message.content
        return review.strip()
    except Exception as e:
        print("Error generating review:", e)
        return "Great product! My pet loves it."


# Establish database connection
def get_db_connection():
    if USE_CLOUD_SQL:
        # Create connection pool with 'creator' argument to our connection object function
        pool = sqlalchemy.create_engine(
            "postgresql+pg8000://",
            creator=getconn,
        )
        conn = pool.raw_connection()
        return conn
    else:
        conn_str = f"dbname='{DB_NAME}' user='{DB_USER}' password='{DB_PASSWORD}' host='{DB_HOST}' port='{DB_PORT}'"
        return psycopg2.connect(conn_str)


# Function to get existing product and user IDs
def get_existing_ids(conn):
    cursor = conn.cursor()

    cursor.execute("SELECT product_id, name FROM products")
    products = cursor.fetchall()
    product_ids = [p[0] for p in products]
    product_names = {p[0]: p[1] for p in products}  # Map product_id to product_name

    cursor.close()
    return product_ids, product_names


# Function to generate a transaction
def generate_transaction(product_ids):
    product_id = random.choice(product_ids)
    transaction_time = fake.date_time_this_year()

    payment_methods = ["credit_card", "debit_card", "paypal", "crypto", "bank_transfer"]
    payment_method = random.choice(payment_methods)

    # Introduce anomalies in transaction amounts, small for normal payments high chance for crypto
    anomaly_chance = random.random()

    if anomaly_chance < 0.05:  # 5% chance for a very high amount
        transaction_amount = round(random.uniform(500.0, 1000.0), 2)
    elif anomaly_chance < 0.1:  # 5% chance for a very low amount
        transaction_amount = round(random.uniform(0.01, 5.0), 2)
    else:
        transaction_amount = round(random.uniform(5.0, 150.0), 2)



    return product_id, transaction_amount, transaction_time, payment_method


# Function to insert a transaction
def insert_transaction(conn, product_ids):
    cursor = conn.cursor()
    transaction = generate_transaction(product_ids)
    cursor.execute(
        "INSERT INTO transactions (product_id, amount, transaction_date, payment_method) VALUES (%s, %s, %s, %s) RETURNING transaction_id",
        transaction,
    )
    conn.commit()
    cursor.close()


# Function to generate a review
def generate_review(product_ids, product_names):
    product_id = random.choice(product_ids)
    product_name = product_names[product_id]
    rating = random.randint(1, 5)

    # Generate review text using OpenAI API
    review_text = generate_review_text(product_name)

    return product_id, rating, review_text, fake.date_time_this_year()


# Function to insert a review
def insert_review(conn, product_ids, product_names):
    cursor = conn.cursor()
    review = generate_review(product_ids, product_names)
    cursor.execute(
        "INSERT INTO reviews (product_id, rating, review_text, review_time) VALUES (%s, %s, %s, %s) RETURNING review_id",
        review,
    )
    conn.commit()
    cursor.close()


# Function to initialize database schema
def initialize_database(conn):
    try:
        cursor = conn.cursor()
        
        # Read and execute the init.sql file
        init_sql_path = pathlib.Path(__file__).parent.parent / 'postgres' / 'init.sql'
        with open(init_sql_path, 'r') as sql_file:
            sql_content = sql_file.read()
            
        # Split the SQL content into individual statements
        # This handles both semicolon-terminated statements and dollar-quoted blocks
        statements = []
        current_statement = []
        in_dollar_quote = False
        dollar_quote_tag = ''
        
        for line in sql_content.split('\n'):
            line = line.strip()
            if not line or line.startswith('--'):  # Skip empty lines and comments
                continue
                
            if not in_dollar_quote:
                # Check for dollar quote start
                if line.startswith('$'):
                    quote_end = line.find('$', 1)
                    if quote_end != -1:
                        in_dollar_quote = True
                        dollar_quote_tag = line[:quote_end+1]
                        current_statement.append(line)
                        continue
                
                # Regular statement handling
                current_statement.append(line)
                if line.endswith(';'):
                    statements.append('\n'.join(current_statement))
                    current_statement = []
            else:
                # In dollar quote, look for matching end tag
                current_statement.append(line)
                if dollar_quote_tag in line:
                    in_dollar_quote = False
                    if line.endswith(';'):
                        statements.append('\n'.join(current_statement))
                        current_statement = []
        
        # Execute each statement
        for statement in statements:
            if statement.strip():
                try:
                    cursor.execute(statement)
                    conn.commit()
                except Exception as e:
                    print(f"Warning: Error executing statement (this may be normal if objects already exist): {str(e)}")
                    conn.rollback()
        
        print("Database initialization completed successfully!")
        
    except Exception as e:
        print(f"Error initializing database: {str(e)}")
        raise
    finally:
        cursor.close()


# Main function
def main():
    conn = get_db_connection()
    print("Connected to the database!")

    try:
        # Initialize database schema
        # initialize_database(conn)
        
        while True:
            product_ids, product_names = get_existing_ids(conn)

            action = random.choices(["insert_transaction", "insert_review"], weights=[0.6, 0.4], k=1)[0]

            if action == "insert_transaction":
                insert_transaction(conn, product_ids)
                print("Inserted new transaction.")

            elif action == "insert_review":
                insert_review(conn, product_ids, product_names)
                print("Inserted new review.")

            time.sleep(1)  # Wait for 1 second before the next operation

    except KeyboardInterrupt:
        print("Process interrupted by user.")
    finally:
        conn.close()
        print("Database connection closed.")


if __name__ == "__main__":
    main()
