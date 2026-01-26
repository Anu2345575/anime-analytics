"""
Upload sample anime data to S3
"""
import json
import os
from pathlib import Path
from dotenv import load_dotenv
import boto3
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load credentials
load_dotenv(Path(__file__).parent.parent / '.env')

def get_s3_client():
    """Create S3 client"""
    return boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
    )

def upload_to_s3(local_file, s3_key, bucket_name):
    """
    Upload file to S3
    
    Args:
        local_file: Path to local file
        s3_key: S3 object key (path in bucket)
        bucket_name: S3 bucket name
    
    Returns:
        True if successful, False otherwise
    """
    try:
        s3 = get_s3_client()
        
        logger.info(f"Uploading {local_file} to s3://{bucket_name}/{s3_key}")
        
        s3.upload_file(
            local_file,
            bucket_name,
            s3_key,
            ExtraArgs={'Metadata': {
                'source': 'local_test',
                'original_filename': Path(local_file).name
            }}
        )
        
        logger.info(f"✓ Successfully uploaded {Path(local_file).name}")
        return True
        
    except Exception as e:
        logger.error(f"✗ Error uploading {local_file}: {e}")
        return False

def main():
    """Main upload function"""
    
    bucket_name = os.getenv('AWS_S3_BUCKET', 'anime-analytics-bucket')
    s3_prefix = 'anime-impact/raw/'
    
    print("=" * 60)
    print("UPLOADING SAMPLE DATA TO S3")
    print("=" * 60)
    print(f"Bucket: {bucket_name}")
    print(f"Prefix: {s3_prefix}\n")
    
    # Find all JSON files in data directory
    data_dir = Path(__file__).parent.parent / 'data'
    json_files = list(data_dir.glob('anime_*.json'))
    
    print(f"Found {len(json_files)} JSON files to upload\n")
    
    if not json_files:
        logger.warning("No JSON files found in data directory")
        return
    
    successful = 0
    failed = 0
    
    for json_file in sorted(json_files):
        # Extract anime_id from filename
        anime_id = json_file.stem.replace('anime_', '')
        
        # Upload to S3 path like: anime-impact/raw/1/full.json
        s3_key = f"{s3_prefix}{anime_id}/full.json"
        
        if upload_to_s3(str(json_file), s3_key, bucket_name):
            successful += 1
        else:
            failed += 1
    
    print(f"\n" + "=" * 60)
    print(f"RESULTS: {successful} successful, {failed} failed")
    print("=" * 60)
    
    # List what we uploaded
    print(f"\nVerifying upload in S3...")
    try:
        s3 = get_s3_client()
        response = s3.list_objects_v2(
            Bucket=bucket_name,
            Prefix=s3_prefix,
            MaxKeys=50
        )
        
        if 'Contents' in response:
            print(f"\nObjects in s3://{bucket_name}/{s3_prefix}:")
            for obj in response['Contents']:
                # Convert bytes to MB for display
                size_kb = obj['Size'] / 1024
                print(f"  - {obj['Key']} ({size_kb:.1f} KB)")
        else:
            print("No objects found (might take a moment to appear)")
    except Exception as e:
        logger.error(f"Error listing S3 objects: {e}")

if __name__ == "__main__":
    main()