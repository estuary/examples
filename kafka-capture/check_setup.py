#!/usr/bin/env python3
"""
Script to verify AWS credentials and MSK connectivity setup
"""

import os
import sys
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

def check_aws_credentials():
    """Check if AWS credentials are configured"""
    print("🔍 Checking AWS credentials...")
    
    try:
        # Try to get caller identity
        sts = boto3.client('sts')
        response = sts.get_caller_identity()
        
        print("✅ AWS credentials found!")
        print(f"   Account: {response['Account']}")
        print(f"   User ARN: {response['Arn']}")
        print(f"   User ID: {response['UserId']}")
        return True
        
    except NoCredentialsError:
        print("❌ No AWS credentials found!")
        print("   Please configure credentials using one of these methods:")
        print("   1. Environment variables: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY")
        print("   2. AWS credentials file: ~/.aws/credentials")
        print("   3. IAM role (if running on EC2/ECS/Lambda)")
        return False
        
    except ClientError as e:
        print(f"❌ AWS credentials error: {e}")
        return False

def check_region():
    """Check if AWS region is configured"""
    print("\n🌍 Checking AWS region...")
    
    region = os.getenv('AWS_REGION')
    if not region:
        # Try to get from boto3 session
        session = boto3.Session()
        region = session.region_name
    
    if region:
        print(f"✅ AWS region configured: {region}")
        return region
    else:
        print("❌ AWS region not found!")
        print("   Please set AWS_REGION environment variable")
        return None

def check_msk_brokers():
    """Check MSK brokers configuration"""
    print("\n🔗 Checking MSK brokers...")
    
    brokers = os.getenv('MSK_BROKERS')
    if brokers:
        print(f"✅ MSK brokers configured: {brokers}")
        return brokers
    else:
        print("⚠️  MSK_BROKERS not set in environment")
        print("   You can set it with: export MSK_BROKERS='your-msk-bootstrap-servers'")
        print("   Or pass it as argument: --bootstrap 'your-servers'")
        return None

def test_token_generation(region):
    """Test MSK token generation"""
    print(f"\n🎫 Testing MSK token generation...")
    
    try:
        # Generate token with debug info
        token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(region, aws_debug_creds=True)
        
        if token:
            print("✅ MSK auth token generated successfully!")
            print(f"   Token length: {len(token)} characters")
            print(f"   Expires in: {(expiry_ms - 1000 * __import__('time').time()) / 1000:.0f} seconds")
            return True
        else:
            print("❌ Failed to generate MSK auth token")
            return False
            
    except Exception as e:
        print(f"❌ Error generating MSK token: {e}")
        return False

def check_msk_permissions(region):
    """Check basic MSK permissions"""
    print(f"\n🔐 Checking MSK permissions...")
    
    try:
        kafka_client = boto3.client('kafka', region_name=region)
        
        # Try to list clusters
        response = kafka_client.list_clusters(MaxResults=10)
        clusters = response.get('ClusterInfoList', [])
        
        if clusters:
            print(f"✅ MSK permissions OK - Found {len(clusters)} cluster(s):")
            for cluster in clusters[:3]:  # Show first 3
                print(f"   - {cluster['ClusterName']} ({cluster['State']})")
        else:
            print("⚠️  No MSK clusters found (might be permissions or region issue)")
            
        return True
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'AccessDeniedException':
            print("❌ Access denied to MSK service")
            print("   Your AWS credentials need MSK permissions")
        else:
            print(f"❌ MSK permission check failed: {error_code}")
        return False
        
    except Exception as e:
        print(f"❌ Error checking MSK permissions: {e}")
        return False

def main():
    """Main setup checker"""
    print("🚀 MSK IAM Setup Checker")
    print("=" * 40)
    
    success_count = 0
    total_checks = 5
    
    # Check AWS credentials
    if check_aws_credentials():
        success_count += 1
    
    # Check region
    region = check_region()
    if region:
        success_count += 1
    
    # Check MSK brokers
    if check_msk_brokers():
        success_count += 1
    
    # Test token generation
    if region and test_token_generation(region):
        success_count += 1
    
    # Check MSK permissions
    if region and check_msk_permissions(region):
        success_count += 1
    
    # Summary
    print(f"\n📊 Setup Status: {success_count}/{total_checks} checks passed")
    
    if success_count == total_checks:
        print("🎉 Your setup is ready for MSK IAM authentication!")
    elif success_count >= 3:
        print("⚠️  Setup mostly ready, but some issues need attention")
    else:
        print("❌ Setup has significant issues that need to be resolved")
        
    print("\n📖 For detailed setup instructions, see README.md")

if __name__ == "__main__":
    main()