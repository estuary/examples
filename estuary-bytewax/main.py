import json
import os
from datetime import datetime, timedelta, timezone

import bytewax.operators as op
import bytewax.operators.windowing as w
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource
from bytewax.operators.windowing import TumblingWindower, EventClock
from bytewax.connectors.kafka import KafkaSource

# Kafka configuration (using Estuary Flow's Dekaf)
KAFKA_BOOTSTRAP_SERVERS = ["dekaf.estuary.dev:9092"]
KAFKA_TOPIC = "/full/nameof/collection/mongodb.space_tourism.bookings"

messages = [
    # Insert events
    json.dumps(
        {
            "operationType": "insert",
            "fullDocument": {
                "booking_id": "B001",
                "customer_id": "C123",
                "destination": "Paris",
                "booking_date": "2024-09-09T10:30:00Z",
                "passengers": 2,
                "total_price": 1500.00,
            },
        }
    ),
    json.dumps(
        {
            "operationType": "insert",
            "fullDocument": {
                "booking_id": "B002",
                "customer_id": "C456",
                "destination": "New York",
                "booking_date": "2024-09-09T10:30:10Z",
                "passengers": 1,
                "total_price": 800.00,
            },
        }
    ),
    json.dumps(
        {
            "operationType": "insert",
            "fullDocument": {
                "booking_id": "B003",
                "customer_id": "C789",
                "destination": "London",
                "booking_date": "2024-09-09T10:30:30Z",
                "passengers": 4,
                "total_price": 2200.00,
            },
        }
    ),
    json.dumps(
        {
            "operationType": "insert",
            "fullDocument": {
                "booking_id": "B004",
                "customer_id": "C101",
                "destination": "Tokyo",
                "booking_date": "2024-09-09T10:31:00Z",
                "passengers": 3,
                "total_price": 3000.00,
            },
        }
    ),
    json.dumps(
        {
            "operationType": "insert",
            "fullDocument": {
                "booking_id": "B005",
                "customer_id": "C202",
                "destination": "Berlin",
                "booking_date": "2024-09-09T10:32:00Z",
                "passengers": 2,
                "total_price": 1800.00,
            },
        }
    ),
    # Update events (modifying previous bookings)
    json.dumps(
        {
            "operationType": "update",
            "fullDocument": {
                "booking_id": "B001",
                "customer_id": "C123",
                "destination": "Paris",
                "booking_date": "2024-09-09T10:32:30Z",
                "passengers": 3,  # Changed number of passengers
                "total_price": 1600.00,  # Updated price
            },
        }
    ),
    json.dumps(
        {
            "operationType": "update",
            "fullDocument": {
                "booking_id": "B002",
                "customer_id": "C456",
                "destination": "New York",
                "booking_date": "2024-09-09T10:32:40Z",
                "passengers": 1,
                "total_price": 850.00,  # Updated price
            },
        }
    ),
    json.dumps(
        {
            "operationType": "update",
            "fullDocument": {
                "booking_id": "B003",
                "customer_id": "C789",
                "destination": "London",
                "booking_date": "2024-09-09T10:33:00Z",
                "passengers": 4,
                "total_price": 2100.00,  # Discount applied
            },
        }
    ),
    json.dumps(
        {
            "operationType": "update",
            "fullDocument": {
                "booking_id": "B004",
                "customer_id": "C101",
                "destination": "Tokyo",
                "booking_date": "2024-09-09T10:33:00Z",
                "passengers": 4,  # Added a passenger
                "total_price": 3200.00,  # Updated price
            },
        }
    ),
    json.dumps(
        {
            "operationType": "update",
            "fullDocument": {
                "booking_id": "B005",
                "customer_id": "C202",
                "destination": "Berlin",
                "booking_date": "2024-09-09T10:34:00Z",
                "passengers": 1,  # Reduced number of passengers
                "total_price": 1700.00,  # Updated price
            },
        }
    ),
    # Additional insert events
    json.dumps(
        {
            "operationType": "insert",
            "fullDocument": {
                "booking_id": "B006",
                "customer_id": "C303",
                "destination": "Sydney",
                "booking_date": "2024-09-09T10:35:00Z",
                "passengers": 2,
                "total_price": 2500.00,
            },
        }
    ),
    json.dumps(
        {
            "operationType": "insert",
            "fullDocument": {
                "booking_id": "B007",
                "customer_id": "C404",
                "destination": "Dubai",
                "booking_date": "2024-09-09T10:36:00Z",
                "passengers": 5,
                "total_price": 4000.00,
            },
        }
    ),
    json.dumps(
        {
            "operationType": "insert",
            "fullDocument": {
                "booking_id": "B008",
                "customer_id": "C505",
                "destination": "San Francisco",
                "booking_date": "2024-09-09T10:36:00Z",
                "passengers": 3,
                "total_price": 3200.00,
            },
        }
    ),
    # Update and delete events
    json.dumps(
        {
            "operationType": "update",
            "fullDocument": {
                "booking_id": "B006",
                "customer_id": "C303",
                "destination": "Sydney",
                "booking_date": "2024-09-09T10:36:00Z",
                "passengers": 3,  # Updated passengers
                "total_price": 2700.00,  # Updated price
            },
        }
    ),
    json.dumps(
        {
            "operationType": "delete",
            "documentKey": {
                "booking_id": "B006",
                "customer_id": "C303",
                "destination": "Sydney",
                "booking_date": "2024-09-09T10:40:00Z",
                "passengers": 3,  # Updated passengers
                "total_price": 2700.00,  # Updated price
            },
        }
    ),
    json.dumps(
        {
            "operationType": "delete",
            "documentKey": {
                "booking_id": "B001",
                "customer_id": "C123",
                "destination": "Paris",
                "booking_date": "2024-09-09T10:40:00Z",
            },
        }
    ),
    # Insert more events for new customers
    json.dumps(
        {
            "operationType": "insert",
            "fullDocument": {
                "booking_id": "B009",
                "customer_id": "C606",
                "destination": "Rome",
                "booking_date": "2024-09-09T10:41:00Z",
                "passengers": 1,
                "total_price": 1200.00,
            },
        }
    ),
    json.dumps(
        {
            "operationType": "insert",
            "fullDocument": {
                "booking_id": "B010",
                "customer_id": "C707",
                "destination": "Moscow",
                "booking_date": "2024-09-09T10:42:00Z",
                "passengers": 2,
                "total_price": 1800.00,
            },
        }
    ),
]


# Parse the incoming JSON message
def parse_message(msg):
    data = json.loads(msg)
    # MongoDB CDC events have a different structure, so we need to extract the relevant data
    if data["operationType"] in ["insert", "update"]:
        booking = data["fullDocument"]
    elif data["operationType"] == "delete":
        booking = data["documentKey"]
    else:
        return None  # Ignore other operation types

    return (
        booking["booking_id"],
        {
            "operation": data["operationType"],
            "customer_id": booking.get("customer_id"),
            "destination": booking.get("destination"),
            "booking_date": datetime.fromisoformat(
                booking.get("booking_date", "").replace("Z", "+00:00")
            ),
            "passengers": booking.get("passengers"),
            "total_price": booking.get("total_price"),
        },
    )


# Calculate metrics for the current window
def calculate_metrics(key__win_id__values):
    key, (window_id, values) = key__win_id__values
    total_bookings = sum(1 for v in values if v["operation"] in ["insert", "update"])
    total_cancellations = sum(1 for v in values if v["operation"] == "delete")
    total_passengers = sum(
        v["passengers"] for v in values if v["operation"] in ["insert", "update"]
    )
    total_revenue = sum(
        v["total_price"] for v in values if v["operation"] in ["insert", "update"]
    )
    popular_destinations = {}

    for v in values:
        if v["operation"] in ["insert", "update"]:
            dest = v["destination"]
            popular_destinations[dest] = popular_destinations.get(dest, 0) + 1

    most_popular = (
        max(popular_destinations, key=popular_destinations.get)
        if popular_destinations
        else "N/A"
    )

    return {
        "window_end": key,
        "total_bookings": total_bookings,
        "total_cancellations": total_cancellations,
        "total_passengers": total_passengers,
        "total_revenue": total_revenue,
        "most_popular_destination": most_popular,
    }


# Create the dataflow
# src = KafkaSource(
#     brokers=KAFKA_BOOTSTRAP_SERVERS,
#     topics=[KAFKA_TOPIC],
#     add_config={
#         "security.protocol": "SASL_SSL",
#         "sasl.mechanism": "PLAIN",
#         "sasl.username": "{}",
#         "sasl.password": os.getenv("DEKAF_TOKEN"),
#     },
# )

flow = Dataflow("trip-metrics")

# inp = op.input("stream", flow, src) # Uncomment for Kafka

inp = op.input("input", flow, TestingSource(messages))  # Comment out for Kafka
msgs = op.filter_map("parse-msgs", inp, parse_message)

op.inspect("msgs", msgs)

# Configure the `collect_window` operator to use the event time.
cc = EventClock(
    lambda x: x["booking_date"], wait_for_system_duration=timedelta(seconds=10)
)
align_to = datetime(2024, 9, 1, tzinfo=timezone.utc)
wc = TumblingWindower(align_to=align_to, length=timedelta(minutes=5))

windowed_msgs = w.collect_window("windowed-msgs", msgs, cc, wc)

op.inspect("windowed", windowed_msgs.down)

computed = op.map("compute", windowed_msgs.down, calculate_metrics)
op.output("output", computed, StdOutSink())
