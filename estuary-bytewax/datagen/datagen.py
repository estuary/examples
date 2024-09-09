import random
import time
from datetime import datetime, timedelta
from pymongo import MongoClient

# MongoDB connection details (replace with your actual details)
client = MongoClient(f'mongodb://root:example@mongodb:27017')
db = client["space_tourism"]
collection = db["bookings"]

destinations = ["Moon", "Mars", "Venus", "Jupiter's Europa", "Saturn's Titan"]


def generate_booking():
    booking_id = random.randint(1, 10000)
    customer_id = random.randint(1, 1000)
    destination = random.choice(destinations)
    booking_date = datetime.now() + timedelta(days=random.randint(30, 365))
    passengers = random.randint(1, 5)
    total_price = passengers * random.uniform(100000, 1000000)

    return {
        "booking_id": booking_id,
        "customer_id": customer_id,
        "destination": destination,
        "booking_date": booking_date,
        "passengers": passengers,
        "total_price": total_price,
    }


while True:
    operation = random.choice(["INSERT", "UPDATE", "DELETE"])

    if operation == "INSERT":
        booking = generate_booking()
        result = collection.insert_one(booking)
        print(f"Inserted new booking: {booking}")

    elif operation == "UPDATE":
        booking_id = random.randint(1, 10000)
        new_passengers = random.randint(1, 5)
        new_total_price = new_passengers * random.uniform(100000, 1000000)
        result = collection.update_one(
            {"booking_id": booking_id},
            {"$set": {"passengers": new_passengers, "total_price": new_total_price}},
        )
        print(
            f"Updated booking ID {booking_id} with new data: {new_passengers} passengers, ${new_total_price:.2f}"
        )

    else:  # DELETE
        booking_id = random.randint(1, 10000)
        result = collection.delete_one({"booking_id": booking_id})
        print(f"Deleted booking ID {booking_id}")

    time.sleep(1)  # Generate an event every second

client.close()
