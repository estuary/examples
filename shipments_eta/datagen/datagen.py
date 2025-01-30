import random
import time
import os
import datetime

from pymongo import UpdateOne
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

from faker import Faker

# Initialize Faker for realistic data
fake = Faker()

# MongoDB connection
DB_USER = os.getenv('MONGODB_USER', "dani2")
DB_PASSWORD = os.getenv('MONGODB_PASSWORD', "dani2")
DB_HOST = os.getenv('MONGODB_HOST', "cluster0.x4ulygj.mongodb.net")

mongo_client = MongoClient(f"mongodb+srv://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/?retryWrites=true&w=majority&appName=Cluster0", server_api=ServerApi('1'))
db = mongo_client.shipping

print(f"Connected to MongoDB: {mongo_client}")

# Collections
shipments_col = db.shipments
checkpoints_col = db.checkpoints
traffic_weather_col = db.traffic_weather

# Predefined routes for realism
ROUTES = [
    {"origin": "Port of Santos, Brazil", "destination": "Los Angeles, USA", "route_id": "1"},
    {"origin": "Shanghai, China", "destination": "Long Beach, USA", "route_id": "2"},
    {"origin": "Rotterdam, Netherlands", "destination": "New York, USA", "route_id": "3"},
    {"origin": "Mumbai, India", "destination": "London, UK", "route_id": "4"},
    {"origin": "Hamburg, Germany", "destination": "Singapore", "route_id": "5"},
]

# Predefined statuses for shipments
STATUSES = ["Pending", "In Transit", "At Checkpoint", "Delayed", "Delivered", "Canceled"]

def generate_mock_shipments(n=10):
    """Generate new shipments and insert them into MongoDB."""
    new_shipments = []
    for _ in range(n):
        route = random.choice(ROUTES)
        shipment = {
            "shipment_id": fake.uuid4(),
            "customer_id": fake.uuid4(),
            "origin": route["origin"],
            "destination": route["destination"],
            "status": "Pending",
            "current_location": None,  # Updates later as it moves
            "expected_delivery_date": (datetime.datetime.now() + datetime.timedelta(days=random.randint(5, 15))).isoformat(),
            "events": [],
            "delays": [],
            "route_id": route["route_id"],
        }
        new_shipments.append(shipment)

    if new_shipments:
        shipments_col.insert_many(new_shipments)
    print(f"Inserted {len(new_shipments)} new shipments.")

def update_shipments():
    """Update shipments' status, locations, and delay reasons."""
    shipments = list(shipments_col.find({"status": {"$ne": "Delivered"}}))
    updates = []

    for shipment in shipments:
        new_status = random.choice(STATUSES)

        # Update location if in transit
        if new_status == "In Transit":
            shipment["current_location"] = {
                "latitude": round(random.uniform(-90, 90), 5),
                "longitude": round(random.uniform(-180, 180), 5),
            }
            checkpoints = list(checkpoints_col.find({}))
            shipment["events"].append({
                "checkpoint": random.choice(checkpoints)["name"],
                "timestamp": datetime.datetime.now().isoformat()
            })

        # Add random delays
        if new_status == "Delayed":
            shipment["delays"].append({
                "reason": fake.sentence(),
                "duration_minutes": random.randint(30, 300),
                "timestamp": datetime.datetime.now().isoformat()
            })

        shipment["status"] = new_status

        # Update the record
        updates.append(UpdateOne({"shipment_id": shipment["shipment_id"]}, {"$set": shipment}))

    if updates:
        shipments_col.bulk_write(updates)
    print(f"Updated {len(updates)} shipments.")

def delete_old_shipments():
    """Randomly delete a few shipments to simulate completed deliveries."""
    deletions = shipments_col.delete_many({"status": "Delivered"})
    print(f"Deleted {deletions.deleted_count} delivered shipments.")

def generate_checkpoints():
    """Create predefined checkpoint locations."""
    print("Generating checkpoints...")
    if checkpoints_col.count_documents({}) == 0:
        checkpoints = [
            {"checkpoint_id": fake.uuid4(), "name": fake.city(), "status": "Operational"}
            for _ in range(10)
        ]
        checkpoints_col.insert_many(checkpoints)
        print("Inserted checkpoint data.")

def generate_traffic_weather():
    """Generate traffic and weather data impacting shipments."""

    traffic_data = [
        {
            "route_id": random.choice(ROUTES)["route_id"],
            "traffic_condition": random.choice(["Clear", "Moderate", "Heavy"]),
            "weather_condition": random.choice(["Sunny", "Rain", "Storm"]),
            "impact_on_ETA_minutes": random.randint(0, 120),
            "timestamp": datetime.datetime.now().isoformat(),
        }
        for _ in range(5)
    ]
    traffic_weather_col.insert_many(traffic_data)
    print("Updated traffic and weather data.")

def simulate_cdc_workload():
    """Continuously insert, update, and delete data to simulate real-time CDC events."""
    generate_checkpoints()
    
    while True:
        generate_mock_shipments(n=random.randint(2, 5))  # Insert new shipments
        update_shipments()  # Update shipments (locations, delays, statuses)
        # delete_old_shipments()  # Remove delivered shipments

        if random.random() < 0.5:
            generate_traffic_weather()  # Update traffic and weather conditions sometimes
        
        sleep_for = random.randint(5, 15)
        print(f"Sleeping for {sleep_for} seconds...")
        time.sleep(sleep_for)

if __name__ == "__main__":
    # Run simulation
    print("Starting simulation...")
    simulate_cdc_workload()
