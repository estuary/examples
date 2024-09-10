import json
import os
from datetime import datetime, timedelta, timezone

import bytewax.operators as op
import bytewax.operators.windowing as w
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.operators.windowing import TumblingWindower, EventClock
from bytewax.connectors.kafka import KafkaSource

# Kafka configuration (using Estuary Flow's Dekaf)
KAFKA_BOOTSTRAP_SERVERS = ["dekaf.estuary.dev:9092"]

# TODO Change this to the name of your Estuary Flow collection
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

flow = Dataflow("trip-metrics")

inp = op.input("input", flow, src)
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
