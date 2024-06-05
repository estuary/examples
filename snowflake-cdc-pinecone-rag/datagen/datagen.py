import os
import time
import random
import snowflake.connector
from faker import Faker
from openai import OpenAI
import dotenv

dotenv.load_dotenv()

client = OpenAI()

# Snowflake configuration
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_TABLE = os.getenv("SNOWFLAKE_TABLE", "SUPPORT_REQUESTS")

fake = Faker()

# Connect to Snowflake
conn = snowflake.connector.connect(
    account=SNOWFLAKE_ACCOUNT,
    user=SNOWFLAKE_USER,
    role=SNOWFLAKE_ROLE,
    password=SNOWFLAKE_PASSWORD,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA,
)

# Create a cursor object
cursor = conn.cursor()

# Create the table if it doesn't exist
create_table_query = f"""
CREATE TABLE IF NOT EXISTS {SNOWFLAKE_TABLE} (
    REQUEST_ID INT,
    CUSTOMER_ID INT,
    REQUEST_DATE STRING,
    REQUEST_TYPE STRING,
    STATUS STRING,
    DESCRIPTION STRING
);
"""
cursor.execute(create_table_query)


# Keywords and phrases for tech support text blurbs
keywords = [
    "integration issue",
    "API error",
    "authentication failure",
    "response time",
    "latency",
    "timeout",
    "data sync problem",
    "server error",
    "connection lost",
    "user interface",
    "dashboard",
    "analytics",
    "bot training",
    "model update",
    "subscription",
    "billing",
    "account access",
    "feature request",
    "bug report",
    "documentation",
    "support ticket",
    "customer feedback",
    "performance",
    "scalability",
    "security",
    "compliance",
    "user experience",
    "version upgrade",
    "deployment",
    "maintenance",
]


# Function to generate a realistic support request description
def generate_support_description(
    customer_id, product_id, order_id, request_type, status
):
    prompt = f"""
    Generate a detailed customer support request description for the following scenario:

    - Issue type: {random.choice(keywords)}
    - Product id: {product_id}
    - Customer id: {customer_id}
    - Order id: {order_id}
    - Request type: {request_type}
    - Status: {status}
    """

    completion = client.chat.completions.create(
        model="gpt-3.5-turbo-0125",
        messages=[
            {
                "role": "system",
                "content": "You are a bot that generates random customer support requests for a finctional ecommerce store",
            },
            {"role": "user", "content": prompt},
        ],
        max_tokens=100,
    )

    description = completion.choices[0].message.content
    return description


# Function to generate a new sample support request
def generate_support_request():
    request_id = random.randint(1, 5000)
    customer_id = random.randint(1, 50)
    request_date = fake.date_time_this_year().strftime("%Y-%m-%d %H:%M:%S")
    request_type = random.choice(
        ["Technical Issue", "Billing Issue", "General Inquiry"]
    )
    status = random.choice(["Open", "In Progress", "Closed"])
    description = generate_support_description(
        request_id, customer_id, request_type, request_type, status
    )
    return [request_id, customer_id, request_date, request_type, status, description]


# Function to insert a new support request into Snowflake
def insert_support_request(new_request):
    select_query = f"SELECT REQUEST_ID FROM {SNOWFLAKE_TABLE} WHERE REQUEST_ID = {new_request[0]}"
    cursor.execute(select_query)
    request_ids = [row[0] for row in cursor.fetchall()]
    if request_ids:
        update_support_request(new_request)
        print("Support request ID already existing, updated instead")
    else:
        insert_query = f"""
        INSERT INTO {SNOWFLAKE_TABLE} (REQUEST_ID, CUSTOMER_ID, REQUEST_DATE, REQUEST_TYPE, STATUS, DESCRIPTION)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, new_request)
        conn.commit()
        print("Inserted new support request:", new_request)


# Function to update a support request in Snowflake
def update_support_request(new_request):
    update_query = f"""
    UPDATE {SNOWFLAKE_TABLE}
    SET CUSTOMER_ID = %s, REQUEST_DATE = %s, REQUEST_TYPE = %s, STATUS = %s, DESCRIPTION = %s
    WHERE REQUEST_ID = %s
    """
    cursor.execute(update_query, new_request[1:] + [new_request[0]])
    conn.commit()
    print("Updated support request ID", new_request[0], "with new data:", new_request)


# Function to delete a support request from Snowflake
def delete_support_request():
    select_query = f"SELECT REQUEST_ID FROM {SNOWFLAKE_TABLE}"
    cursor.execute(select_query)
    request_ids = [row[0] for row in cursor.fetchall()]
    if request_ids:
        request_id = random.choice(request_ids)
        delete_query = f"DELETE FROM {SNOWFLAKE_TABLE} WHERE REQUEST_ID = %s"
        cursor.execute(delete_query, (request_id,))
        conn.commit()
        print("Deleted support request ID", request_id)
    else:
        print("No support requests found in the table, skipping delete.")


def main():
    print("Connected to Snowflake!")

    # Main loop to continuously insert, update, or delete support requests
    try:
        while True:
            action = random.choices(
                ["insert", "delete", "update"], weights=[0.7, 0.2, 0.1], k=1
            )[0]

            if action == "insert":
                new_request = generate_support_request()
                insert_support_request(new_request)
            elif action == "update":
                new_request = generate_support_request()
                update_support_request(new_request)
            elif action == "delete":
                delete_support_request()

            time.sleep(2)  # Wait for 2 second before the next operation
    except KeyboardInterrupt:
        print("Process interrupted by user.")
    finally:
        cursor.close()
        conn.close()
        print("Script ended.")


if __name__ == "__main__":
    main()
