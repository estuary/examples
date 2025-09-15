import json, os, socket, argparse
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from kafka.sasl.oauth import AbstractTokenProvider
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

# Parse command line arguments
parser = argparse.ArgumentParser(description='Kafka Consumer for IoT Data')
parser.add_argument('--bootstrap', '-b',
                   default=None,
                   help='Kafka bootstrap servers (comma-separated)')
parser.add_argument('--topic', '-t',
                   default='iot.readings',
                   help='Topic to consume from (default: iot.readings)')
parser.add_argument('--group', '-g',
                   default='iot-consumer-group',
                   help='Consumer group ID (default: iot-consumer-group)')
parser.add_argument('--from-beginning', action='store_true',
                   help='Consume from beginning of topic')
args = parser.parse_args()

# MSK Configuration
MSK_BROKERS_ENV = os.getenv('MSK_BROKERS')
if args.bootstrap:
    BOOTSTRAP = args.bootstrap
elif MSK_BROKERS_ENV:
    BOOTSTRAP = MSK_BROKERS_ENV
else:
    # Default MSK brokers
    BOOTSTRAP = "borker1,broker2,broker3"  # Replace with your brokers or set MSK_BROKERS env var
    
# MSK IAM Authentication Configuration
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')

print(f"Connecting to Kafka at: {BOOTSTRAP}")
print(f"Using MSK IAM Authentication with region: {AWS_REGION}")
print(f"Consuming from topic: {args.topic}")
print(f"Consumer group: {args.group}")

class MSKTokenProvider(AbstractTokenProvider):
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(AWS_REGION)
        return token

tp = MSKTokenProvider()

try:
    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=BOOTSTRAP,
        security_protocol='SASL_SSL',
        sasl_mechanism='OAUTHBEARER',
        sasl_oauth_token_provider=tp,
        client_id=socket.gethostname(),
        group_id=args.group,
        auto_offset_reset='earliest' if args.from_beginning else 'latest',
        enable_auto_commit=True,
        value_deserializer=lambda m: m.decode('utf-8'),
        key_deserializer=lambda m: m.decode('utf-8') if m else None,
        consumer_timeout_ms=10000,  # Exit after 10 seconds of no messages
    )
    print("Successfully created Kafka consumer")
    
    print("Consuming messages... (Press Ctrl+C to stop)")
    message_count = 0
    
    try:
        for message in consumer:
            message_count += 1
            
            # Parse JSON if possible
            try:
                data = json.loads(message.value)
                print(f"\n--- Message {message_count} ---")
                print(f"Topic: {message.topic}")
                print(f"Partition: {message.partition}")
                print(f"Offset: {message.offset}")
                print(f"Key: {message.key}")
                print(f"Timestamp: {message.timestamp}")
                print(f"Data: {json.dumps(data, indent=2)}")
            except json.JSONDecodeError:
                print(f"\n--- Message {message_count} ---")
                print(f"Topic: {message.topic}")
                print(f"Partition: {message.partition}")
                print(f"Offset: {message.offset}")
                print(f"Key: {message.key}")
                print(f"Timestamp: {message.timestamp}")
                print(f"Raw Value: {message.value}")
            
            # Print summary every 10 messages
            if message_count % 10 == 0:
                print(f"\n>>> Consumed {message_count} messages so far...")
                
    except KeyboardInterrupt:
        print(f"\n\nStopping consumer... Consumed {message_count} messages total.")
    except Exception as e:
        if "timeout" in str(e).lower():
            print(f"\n\nNo more messages after timeout. Consumed {message_count} messages total.")
        else:
            print(f"\n\nError consuming messages: {e}")
            print(f"Consumed {message_count} messages before error.")
    
    finally:
        consumer.close()
        print("Consumer closed")

except Exception as e:
    print(f"Failed to create Kafka consumer: {e}")
    raise