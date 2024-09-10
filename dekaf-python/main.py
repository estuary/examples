import os
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from dotenv import load_dotenv

load_dotenv()

# Configuration
topic = "dani-demo/postgresdemo/sales"
username = "{}"
password = os.getenv("DEKAF_ACCESS_TOKEN")


def get_avro_schema(schema_registry_client, subject):
    # Get the latest version of the schema for the subject
    latest_schema = schema_registry_client.get_latest_version(subject)
    return latest_schema.schema.schema_str


def process_record(record: bytes, deserializer: AvroDeserializer):
    # Deserialize the record using the Avro deserializer
    return deserializer(record, None)


# Create SchemaRegistryClient instance
schema_registry_client = SchemaRegistryClient(
    {
        "url": "https://dekaf.estuary.dev",
        "basic.auth.user.info": f"{username}:{password}",
    }
)

# Define the subject name based on your topic and schema naming convention & get Avro schema
schema_str = get_avro_schema(schema_registry_client, f"{topic}-value")

# Create Avro Deserializer instance
deserializer = AvroDeserializer(
    schema_str=schema_str, schema_registry_client=schema_registry_client
)

# Create Consumer instance
consumer = Consumer(
    {
        "bootstrap.servers": "dekaf.estuary.dev:9092",
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": username,
        "sasl.password": password,
        "auto.offset.reset": "earliest",
        "group.id": "demo123",
    }
)

# Subscribe to the topic
consumer.subscribe([topic])

# Poll for messages
try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        # Process the record
        key = msg.key()
        value = msg.value()

        if value:
            # Process the value using the deserializer
            processed_value = process_record(value, deserializer)
            print(f"Received value: {processed_value}")

except KeyboardInterrupt:
    print("Interrupted by user")
finally:
    consumer.close()
