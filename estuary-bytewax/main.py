import json
import os
from datetime import datetime, timedelta

from bytewax.connectors.kafka import KafkaSource
from bytewax.dataflow import Dataflow
from bytewax.inputs import KafkaInputConfig
from bytewax.outputs import StdOutputConfig
from bytewax.window import TumblingWindowConfig, SystemClockConfig

# Kafka configuration (using Estuary Flow's Dekaf)
KAFKA_BOOTSTRAP_SERVERS = "dekaf.estuary.dev:9092"
KAFKA_TOPIC = "/full/nameof/collection/mongodb.space_tourism.bookings"


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
def calculate_metrics(key, values):
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
src = KafkaSource(
    brokers=KAFKA_BOOTSTRAP_SERVERS,
    topics=[KAFKA_TOPIC],
    add_config={
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": "{}",
        "sasl.password": os.getenv("DEKAF_TOKEN"),
    },
)

flow = Dataflow()
flow.input("input", src)
flow.map(parse_message)
flow.filter(lambda x: x is not None)
flow.window(
    TumblingWindowConfig(SystemClockConfig(), timedelta(minutes=5)),
    calculate_metrics,
)
flow.output("output", StdOutputConfig())
