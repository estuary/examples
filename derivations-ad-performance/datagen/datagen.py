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


# Function to generate a new ad impression
def generate_impression():
    ad_id = random.randint(1, 100)
    campaign_id = random.randint(1, 10)
    platform = random.choice(["Google Ads", "Facebook Ads", "Twitter Ads"])
    impression_timestamp = fake.date_time_this_year()
    user_id = fake.uuid4()
    country = fake.country_code()
    device_type = random.choice(["mobile", "desktop", "tablet"])
    return (
        ad_id,
        campaign_id,
        platform,
        impression_timestamp,
        user_id,
        country,
        device_type,
    )


# Function to generate a new ad click
def generate_click(impression_id):
    ad_id = random.randint(1, 100)
    campaign_id = random.randint(1, 10)
    platform = random.choice(["Google Ads", "Facebook Ads", "Twitter Ads"])
    click_timestamp = fake.date_time_this_year()
    user_id = fake.uuid4()
    landing_page_url = fake.url()
    conversion_flag = random.choice([True, False])
    return (
        impression_id,
        ad_id,
        campaign_id,
        platform,
        click_timestamp,
        user_id,
        landing_page_url,
        conversion_flag,
    )


# Function to insert a new impression into the database
def insert_impression(conn, impression):
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO ad_impressions (ad_id, campaign_id, platform, impression_timestamp, user_id, country, device_type) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING impression_id",
        impression,
    )
    impression_id = cursor.fetchone()[0]
    conn.commit()
    cursor.close()
    return impression_id


# Function to insert a new click into the database
def insert_click(conn, click):
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO ad_clicks (impression_id, ad_id, campaign_id, platform, click_timestamp, user_id, landing_page_url, conversion_flag) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        click,
    )
    conn.commit()
    cursor.close()

def main():
    conn_str = f"dbname='{DB_NAME}' user='{DB_USER}' password='{DB_PASSWORD}' host='{DB_HOST}' port='{DB_PORT}'"
    conn = psycopg2.connect(conn_str)
    print("Connected to the database!")

    # Main loop to continuously insert, update, or delete ad metrics data
    try:
        while True:
            new_impression = generate_impression()
            impression_id = insert_impression(conn, new_impression)
            print("Inserted new impression:", new_impression)

            # Simulate a click for about 10% of impressions
            if random.random() < 0.1:
                new_click = generate_click(impression_id)
                insert_click(conn, new_click)
                print("Inserted new click:", new_click)

            time.sleep(1)  # Wait for 0.1 second before the next operation
    except KeyboardInterrupt:
        print("Process interrupted by user.")
    finally:
        conn.close()
        print("Database connection closed.")


if __name__ == "__main__":
    main()
