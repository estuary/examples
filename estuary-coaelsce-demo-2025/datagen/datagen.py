import os
import psycopg2
import time
import random
import uuid
import openai
from faker import Faker

# Database connection parameters
DB_NAME = os.getenv("POSTGRES_DB", "postgres")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")

# OpenAI API key (set your key as an environment variable)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
openai.api_key = OPENAI_API_KEY

fake = Faker()


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


# Main function
def main():
    conn = get_db_connection()
    print("Connected to the database!")

    try:
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
