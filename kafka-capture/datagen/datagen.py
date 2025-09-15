import json, random, time, argparse, os, socket
from datetime import datetime, timezone, timedelta
from faker import Faker
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import KafkaError
from kafka.sasl.oauth import AbstractTokenProvider
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
import boto3


import dotenv

dotenv.load_dotenv()

# Parse command line arguments
parser = argparse.ArgumentParser(description='IoT Data Generator for Kafka')
parser.add_argument('--bootstrap', '-b',
                   default=None,
                   help='Kafka bootstrap servers (comma-separated)')
args = parser.parse_args()


# MSK Configuration
BOOTSTRAP = "borker1,broker2,broker3"  # Replace with your brokers or set MSK_BROKERS env var


TOPIC_READINGS = "iot.readings"
TOPIC_DEVICES  = "iot.devices"

# MSK IAM Authentication Configuration
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')

print(f"Connecting to Kafka at: {BOOTSTRAP}")
print(f"Using MSK IAM Authentication with region: {AWS_REGION}")

class MSKTokenProvider(AbstractTokenProvider):
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(AWS_REGION)
        return token
    

sts = boto3.client("sts", region_name=AWS_REGION)
print("Caller identity:", sts.get_caller_identity())

tp = MSKTokenProvider()

# Create topics if they don't exist
admin_config = {
    'bootstrap_servers': BOOTSTRAP,
    'security_protocol': 'SASL_SSL',
    'sasl_mechanism': 'OAUTHBEARER',
    'sasl_oauth_token_provider': tp,
    'client_id': socket.gethostname(),
    'api_version': (2, 8, 1),  # Specify API version for MSK compatibility
    'request_timeout_ms': 30000,
    'connections_max_idle_ms': 540000,
}

try:
    admin_client = KafkaAdminClient(**admin_config)
    topics = [
        NewTopic(name=TOPIC_READINGS, num_partitions=3, replication_factor=2),
        NewTopic(name=TOPIC_DEVICES, num_partitions=1, replication_factor=2)
    ]
    
# Create topics if they don't exist (kafka-python Admin API returns a response object, not futures)
    try:
        resp = admin_client.create_topics(new_topics=topics, validate_only=False)
        # resp is a CreateTopicsResponse_v3 with .topic_errors: list of tuples
        # Each item: (topic, error_code, error_message)
        created_any = False
        for topic_name, error_code, error_message in getattr(resp, "topic_errors", []):
            if error_code == 0:
                print(f"Topic {topic_name} created successfully")
                created_any = True
            elif error_code in (36,):  # 36 = TopicAlreadyExists
                print(f"Topic {topic_name} already exists")
            elif error_code == 29:     # 29 = AuthorizationFailed
                print(f"Authorization failed creating topic {topic_name}: {error_message}")
                print("Check cluster/identity policy for kafka-cluster:CreateTopic on this cluster/topic ARN.")
            else:
                print(f"Failed to create topic {topic_name}: [{error_code}] {error_message}")
        if not getattr(resp, "topic_errors", None):
            print("CreateTopics returned no per-topic errors; verify with a describe call below.")
    except Exception as e:
        print(f"Error during topic creation: {e}")
        print("Continuing without topic creation - topics may already exist or need admin creation")
    finally:
        admin_client.close()

except Exception as e:
    print(f"Error creating topics: {e}")

print("Creating Kafka producer...")
try:
    p = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        security_protocol='SASL_SSL',
        sasl_mechanism='OAUTHBEARER',
        sasl_oauth_token_provider=tp,
        client_id=socket.gethostname(),
        value_serializer=lambda x: x,  # We'll handle encoding manually
        key_serializer=lambda x: x,    # We'll handle encoding manually
        acks='all',
        retries=5,
        request_timeout_ms=30000,
        api_version=(2, 8, 1),  # Specify API version for MSK compatibility
        metadata_max_age_ms=300000,  # 5 minutes
        retry_backoff_ms=100,
        max_block_ms=60000,  # Maximum time to block during send()
        connections_max_idle_ms=540000,  # 9 minutes
    )
    print("Successfully created Kafka producer")
    
    # Test the connection by getting cluster metadata
    print("Testing Kafka connection...")
    metadata = p._metadata
    metadata.request_update()
    
    # Wait for metadata to be available
    import time
    timeout = 30
    start_time = time.time()
    
    while not metadata.brokers and (time.time() - start_time) < timeout:
        time.sleep(0.1)
    
    if metadata.brokers:
        print(f"Successfully connected to Kafka cluster with {metadata.brokers} brokers")
    else:
        raise Exception("Failed to connect to Kafka cluster - no brokers available")
        
except Exception as e:
    print(f"Failed to create or test Kafka producer: {e}")
    print("This might be due to:")
    print("1. Network connectivity issues")
    print("2. Authentication/authorization problems")
    print("3. Incorrect broker addresses")
    print("4. Firewall or security group restrictions")
    print("5. MSK cluster configuration issues")
    raise
fake = Faker()
random.seed(42)

DEVICE_IDS = [f"thermo-{i:05d}" for i in range(1, 21)]
MODELS = ["T900", "T1000", "T2000", "T3000", "T4000", "T5000", "T6000", "T7000", "T8000", "T9000"]
SITES = ["nyc_manhattan_hq", "sp_sao_paulo_lab", "ldn_office"]
ROOMS = ["conf_a", "conf_b", "open_floor", "server_room"]
FW = ["1.3.0", "1.3.1", "1.3.2", "1.4.0"]

# Start with one metadata row per device
device_state = {}
now = datetime.now(timezone.utc)
for d in DEVICE_IDS:
    device_state[d] = {
        "device_id": d,
        "effective_from": now.isoformat().replace("+00:00", "Z"),
        "model": random.choice(MODELS),
        "firmware_version": random.choice(FW[:3]),
        "site": random.choice(SITES),
        "room": random.choice(ROOMS),
        "lat": float(fake.latitude()),
        "lon": float(fake.longitude()),
    }
    try:
        future = p.send(
            TOPIC_DEVICES,
            key=d.encode(),
            value=json.dumps(device_state[d]).encode()
        )
        # Wait for confirmation with better error handling
        try:
            future.get(timeout=10)
            print(f"Sent device metadata for {d}")
        except Exception as send_e:
            if "TopicAuthorizationFailedError" in str(send_e):
                print(f"Authorization failed for topic {TOPIC_DEVICES}: {send_e}")
                print("Skipping device metadata - check topic permissions")
                break  # Skip remaining device metadata
            else:
                raise send_e
    except Exception as e:
        print(f"Failed to send device metadata for {d}: {e}")
        if "TopicAuthorizationFailedError" in str(e):
            print("Authorization failed - check topic permissions and existence")

try:
    p.flush(timeout=30)
    print("Initial device metadata flush completed")
except Exception as e:
    print(f"Error flushing initial metadata: {e}")

def maybe_emit_metadata_change():
    """Occasionally change firmware or room (SCD-2 new row)."""
    if random.random() < 0.10:  # 10% chance per tick
        d = random.choice(DEVICE_IDS)
        old = device_state[d].copy()
        t = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        # Randomly change either firmware or room
        if random.random() < 0.5:
            new_fw = random.choice(FW)
            device_state[d]["firmware_version"] = new_fw
        else:
            device_state[d]["room"] = random.choice(ROOMS)
        device_state[d]["effective_from"] = t
        try:
            future = p.send(
                TOPIC_DEVICES,
                key=d.encode(),
                value=json.dumps(device_state[d]).encode()
            )
            print(f"Emitted metadata change for {d}: {old} -> {device_state[d]}")
            future.get(timeout=10)
        except Exception as e:
            if "TopicAuthorizationFailedError" in str(e):
                print(f"Authorization failed for topic {TOPIC_DEVICES}: {e}")
                # Don't continue trying metadata changes if authorization fails
                return
            else:
                print(f"Failed to send device metadata change for {d}: {e}")

def emit_reading():
    d = random.choice(DEVICE_IDS)
    ts = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    base = 22.0 + (hash(d) % 5)  # per-device baseline
    temp = round(random.normalvariate(base, 0.6), 2)
    hum  = round(random.normalvariate(45, 3.0), 2)
    bat  = max(0.0, min(100.0, round(90 - (time.time() % 3600)/3600*20 + random.uniform(-1.0,1.0), 2)))
    status = "ok"
    if temp > 26.5 or temp < 18.0 or hum > 60:
        status = "warn"
    if temp > 30 or hum > 75:
        status = "error"

    evt = {
        "device_id": d,
        "ts": ts,
        "temperature_c": temp,
        "humidity_pct": hum,
        "battery_pct": bat,
        "status": status
    }
    try:
        future = p.send(TOPIC_READINGS, key=d.encode(), value=json.dumps(evt).encode())
        # For high-throughput, we don't wait for each message, but we could add this for debugging:
        # future.get(timeout=1)
        print(f"Emitted reading: {evt}")
    except Exception as e:
        if "TopicAuthorizationFailedError" in str(e):
            print(f"Authorization failed for topic {TOPIC_READINGS}: {e}")
            print("Check topic permissions and existence - readings may not be delivered")
        else:
            print(f"Failed to produce reading for {d}: {e}")

print("Streaming... Ctrl+C to stop.")
try:
    tick = 0
    while True:
        emit_reading()
        if tick % 20 == 0:
            maybe_emit_metadata_change()
        # No need for poll() with kafka-python
        tick += 1
        time.sleep(0.01)  # ~10 events/sec overall
except KeyboardInterrupt:
    pass
finally:
    p.flush()