import os
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

from dotenv import load_dotenv

load_dotenv()

# Configuration
dekaf_url = "dekaf.estuary-data.com"
schema_registry_url = "https://dekaf.estuary-data.com"
topic = "recentchange-sampled" # collection name

username = os.getenv("DEKAF_TASK_NAME")
password = os.getenv("DEKAF_ACCESS_TOKEN")

def kafka_config():
    return {
        "bootstrap.servers": dekaf_url,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": username,
        "sasl.password": password,
        "group.id": "my-group",
        "auto.offset.reset": "latest",
    }

schema_registry_conf = {
    "url": schema_registry_url,
    "basic.auth.user.info": f"{username}:{password}"
}

schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Define a function to deserialize Avro-encoded messages
def avro_deserializer(schema_registry_client, topic):
    latest_schema = schema_registry_client.get_latest_version(f"{topic}-value").schema
    return AvroDeserializer(schema_registry_client, latest_schema)

def consume():
    consumer = Consumer(kafka_config())
    consumer.subscribe([topic])

    deserializer = avro_deserializer(schema_registry_client, topic)

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            data = deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

            if data:
                print(f"Consumed record: {data}")

    except KeyboardInterrupt:
        print("Consumption interrupted.")
    finally:
        consumer.close()


if __name__ == "__main__":
    consume()