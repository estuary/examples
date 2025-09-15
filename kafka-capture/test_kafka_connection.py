#!/usr/bin/env python3
"""
Simple Kafka connectivity test script for MSK with OAUTHBEARER authentication
"""
import os
import socket
from kafka import KafkaProducer, KafkaAdminClient
from kafka.sasl.oauth import AbstractTokenProvider
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
import dotenv

dotenv.load_dotenv()

# MSK Configuration
BOOTSTRAP = "borker1,broker2,broker3"  # Replace with your brokers or set MSK_BROKERS env var
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')

print(f"Testing connection to: {BOOTSTRAP}")
print(f"Using region: {AWS_REGION}")

class MSKTokenProvider(AbstractTokenProvider):
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(AWS_REGION)
        return token

tp = MSKTokenProvider()

def test_admin_client():
    print("\n1. Testing Admin Client...")
    try:
        admin_config = {
            'bootstrap_servers': BOOTSTRAP,
            'security_protocol': 'SASL_SSL',
            'sasl_mechanism': 'OAUTHBEARER',
            'sasl_oauth_token_provider': tp,
            'client_id': socket.gethostname(),
            'api_version': (2, 8, 1),
            'request_timeout_ms': 30000,
            'connections_max_idle_ms': 540000,
        }
        
        admin_client = KafkaAdminClient(**admin_config)
        
        # Try to get cluster metadata
        cluster_metadata = admin_client.describe_cluster()
        print(f"✓ Admin client connected to cluster with {len(cluster_metadata.brokers)} brokers")
        
        # List topics
        topics_metadata = admin_client.list_topics(timeout_ms=30000)
        print(f"✓ Found {len(topics_metadata.topics)} topics: {list(topics_metadata.topics.keys())}")
        
        admin_client.close()
        return True
        
    except Exception as e:
        print(f"✗ Admin client failed: {e}")
        return False

def test_producer():
    print("\n2. Testing Producer...")
    try:
        producer_config = {
            'bootstrap_servers': BOOTSTRAP,
            'security_protocol': 'SASL_SSL',
            'sasl_mechanism': 'OAUTHBEARER',
            'sasl_oauth_token_provider': tp,
            'client_id': socket.gethostname(),
            'api_version': (2, 8, 1),
            'request_timeout_ms': 30000,
            'metadata_max_age_ms': 300000,
            'retry_backoff_ms': 100,
            'max_block_ms': 60000,
            'connections_max_idle_ms': 540000,
        }
        
        producer = KafkaProducer(**producer_config)
        
        # Test metadata retrieval
        print("Getting cluster metadata...")
        metadata = producer._metadata
        metadata.request_update()
        
        import time
        timeout = 30
        start_time = time.time()
        
        while not metadata.brokers and (time.time() - start_time) < timeout:
            time.sleep(0.1)
        
        if metadata.brokers:
            print(f"✓ Producer connected - found {len(metadata.brokers)} brokers")
            
            # List available topics
            topics = metadata.topics()
            print(f"✓ Available topics: {list(topics)}")
            
        else:
            print("✗ Producer failed - no brokers found")
            return False
            
        producer.close()
        return True
        
    except Exception as e:
        print(f"✗ Producer failed: {e}")
        return False

def main():
    print("=== Kafka Connection Test ===")
    
    admin_success = test_admin_client()
    producer_success = test_producer()
    
    if admin_success and producer_success:
        print("\n✓ All tests passed! Kafka connection is working properly.")
        return True
    else:
        print("\n✗ Some tests failed. Check your configuration and network connectivity.")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)