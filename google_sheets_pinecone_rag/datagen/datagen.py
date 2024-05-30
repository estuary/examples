import os
import time
import pygsheets
from faker import Faker
import random

import dotenv

from openai import OpenAI

dotenv.load_dotenv()

client = OpenAI()


# Google Sheets configuration
SHEET_NAME = os.getenv("SHEET_NAME", "Fake Customer Support")
WORKSHEET_NAME = os.getenv("WORKSHEET_NAME", "Support Requests")
SA_PATH = os.getenv("PATH_TO_SA_JSON", "/credentials.json")

gc = pygsheets.authorize(service_account_file=SA_PATH)

# Open the Google Sheet
sheet = gc.open(SHEET_NAME)
worksheet = sheet.worksheet_by_title(WORKSHEET_NAME)

fake = Faker()


# Function to generate a new sample support request
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


# Function to insert a new support request into the Google Sheet
def insert_support_request(new_request):
    # Find the next empty row
    next_row = len(worksheet.get_all_values(include_tailing_empty_rows=False)) + 1
    # Update the row with new request data
    worksheet.update_row(next_row, new_request)
    print("Inserted new support request:", new_request)


# Function to update a support request in the Google Sheet
def update_support_request(new_request):
    requests = worksheet.get_all_records()
    if requests:
        request_id = random.choice(range(len(requests)))
        cell_range = f"A{request_id+2}:F{request_id+2}"  # Adding 2 because row numbers start from 1 and we have headers
        worksheet.update_values(cell_range, [new_request])
        print("Updated support request ID", request_id, "with new data:", new_request)
    else:
        print("No support requests found in the sheet, skipping update.")


# Function to delete a support request from the Google Sheet
def delete_support_request():
    requests = worksheet.get_all_records()
    if requests:
        request_id = random.choice(range(len(requests)))
        worksheet.delete_rows(
            request_id + 2
        )  # Adding 2 because row numbers start from 1 and we have headers
        print("Deleted support request ID", request_id)
    else:
        print("No support requests found in the sheet, skipping delete.")


def main():
    print("Connected to the Google Sheet!")

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
        print("Script ended.")


if __name__ == "__main__":
    main()
