"""
Simple S3 connection test
"""
import boto3
import os
from pathlib import Path

# Try to load from .env file
try:
    from dotenv import load_dotenv
    # Look for .env in parent directory (anime-analytics/)
    env_path = Path(__file__).parent.parent / '.env'
    print(f"Looking for .env at: {env_path}")
    
    if env_path.exists():
        load_dotenv(env_path)
        print(f"✓ Found .env file")
    else:
        print(f"✗ .env file not found at {env_path}")
        print("Creating one now...")
        print("Please edit the .env file with your credentials")
except ImportError:
    print("python-dotenv not installed. Run: pip install python-dotenv")

# Get credentials from environment
access_key = os.getenv('AWS_ACCESS_KEY_ID')
secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
region = os.getenv('AWS_DEFAULT_REGION', 'us-east-2')
bucket = os.getenv('AWS_S3_BUCKET', 'anime-analytics-bucket')

print(f"\n=== AWS Configuration ===")
print(f"Access Key: {access_key[:10]}... (hidden for security)")
print(f"Secret Key: {'*' * 20} (hidden)")
print(f"Region: {region}")
print(f"Bucket: {bucket}")

# Check if credentials are loaded
if not access_key or not secret_key:
    print("\n✗ ERROR: Credentials not found!")
    print("Make sure .env file has:")
    print("  AWS_ACCESS_KEY_ID=your_key")
    print("  AWS_SECRET_ACCESS_KEY=your_secret")
    exit(1)

print("\n=== Testing S3 Connection ===")

try:
    # Create S3 client
    s3_client = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region
    )
    
    # List buckets
    response = s3_client.list_buckets()
    
    print("✓ Successfully connected to AWS S3!")
    print("\nYour buckets:")
    for bucket_info in response['Buckets']:
        print(f"  - {bucket_info['Name']}")
    
    # List objects in your bucket
    print(f"\n✓ Accessing bucket: {bucket}")
    objects = s3_client.list_objects_v2(Bucket=bucket, MaxKeys=10)
    
    if 'Contents' in objects:
        print(f"Objects in bucket ({len(objects['Contents'])} shown):")
        for obj in objects['Contents']:
            print(f"  - {obj['Key']}")
    else:
        print("(Bucket is empty or only contains folders)")
    
    print("\n✓ All tests passed!")
    
except Exception as e:
    print(f"\n✗ Error: {e}")
    print("\nTroubleshooting:")
    print("1. Verify .env file exists: cat .env")
    print("2. Check credentials are correct (from IAM user)")
    print("3. Verify bucket name matches your S3 bucket")
    print("4. Make sure IAM user has S3 permissions")